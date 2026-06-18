# coding:utf8
"""workers.lib.mc_extractor -- 通用 MaxCompute SQL 执行

封装对 MaxComputerClient 的调用，按 SQLQueryConfig 列表有序执行 SQL 查询。
支持通过 depends_on 声明 SQL 之间的依赖关系，框架通过拓扑排序确定执行顺序。

调用链路（默认模式）：
  拓扑排序 SQLQueryConfig 列表
    -> 读取 SQL 文件内容
    -> MaxComputerClient.execute_sql(sql)
    -> instance.wait_for_success()
    -> instance.open_reader(tunnel=True).to_pandas()  # Instance Tunnel
    -> 返回 {query.name -> DataFrame}

调用链路（临时表模式，use_temp_table=True）：
    -> 读取 SQL 文件内容
    -> 包装为 CREATE TABLE temp AS <sql>
    -> MaxComputerClient.execute_sql(ctas_sql)
    -> instance.wait_for_success()
    -> odps.read_table(temp_table).to_pandas()  # Table Tunnel
    -> DROP TABLE temp
    -> 返回 {query.name -> DataFrame}

临时表模式适用场景：
  RAM 用户缺少源表的 odps:Download 权限，无法通过 Instance Tunnel 下载查询结果，
  但拥有 CREATE TABLE 权限，可通过创建临时表后用 Table Tunnel 下载。
"""

import logging
import re
import time
from collections import deque
from pathlib import Path

import pandas as pd

from .models import SQLQueryConfig

logger = logging.getLogger("workers.lib.mc_extractor")


def execute_all_queries(
    client,
    queries: list[SQLQueryConfig],
    sql_base_dir: Path,
    hints: dict = None,
    params: dict[str, str] = None,
) -> dict[str, pd.DataFrame]:
    """按依赖顺序执行多个 SQL 查询

    Args:
        client:       MaxComputerClient 客户端实例
        queries:      SQLQueryConfig 配置列表
        sql_base_dir: SQL 文件所在目录的绝对路径
        hints:        MaxCompute SQL hints（可选），如 odps.sql.allow.fullscan 等
        params:       SQL 模板参数（可选），替换 SQL 文件中的 ${key} 占位符
                      典型用法: DateRangeParams.sql_params() 返回的日期参数

    Returns:
        dict[str, pd.DataFrame]：{query.name -> DataFrame}

    Raises:
        ValueError: 当依赖关系存在环时
        FileNotFoundError: 当 SQL 文件不存在时
        Exception:  SQL 执行失败时
    """
    if not queries:
        logger.info("No SQL queries to execute")
        return {}

    if hints:
        logger.info(f"Using SQL hints: {list(hints.keys())}")

    # 1. 拓扑排序
    sorted_queries = _topological_sort(queries)
    logger.info(f"SQL execution order: {[q.name for q in sorted_queries]}")

    # 2. 按序执行
    results: dict[str, pd.DataFrame] = {}
    for query in sorted_queries:
        logger.info(f"Executing SQL query: {query.name} (file: {query.sql_file})")
        try:
            df = _execute_single_query(client, query, sql_base_dir, hints=hints, params=params)
            results[query.name] = df
            logger.info(f"Query '{query.name}' completed: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            logger.error(f"Failed to execute SQL query '{query.name}': {e}")
            raise

    return results


def _execute_single_query(
    client,
    query: SQLQueryConfig,
    sql_base_dir: Path,
    hints: dict = None,
    params: dict[str, str] = None,
) -> pd.DataFrame:
    """执行单个 SQL 查询并返回 DataFrame

    Args:
        client:       MaxComputerClient 客户端
        query:        SQLQueryConfig 配置
        sql_base_dir: SQL 文件所在目录
        hints:        MaxCompute SQL hints（可选）
        params:       SQL 模板参数（可选），替换 SQL 文件中的 ${key} 占位符

    Returns:
        pd.DataFrame：查询结果

    Raises:
        FileNotFoundError: SQL 文件不存在
        RuntimeError:      SQL 执行失败
    """
    sql_path = sql_base_dir / query.sql_file
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_content = sql_path.read_text(encoding="utf-8").strip()
    if not sql_content:
        raise ValueError(f"SQL file is empty: {sql_path}")

    # SQL 模板参数渲染（替换 ${key} 占位符）
    if params:
        for key, value in params.items():
            placeholder = "${" + key + "}"
            sql_content = sql_content.replace(placeholder, value)
        logger.debug(f"SQL params rendered for '{query.name}': {list(params.keys())}")
    logger.debug(f"SQL content for '{query.name}':\n{sql_content[:200]}...")

    if query.use_temp_table:
        # 临时表模式：CTAS → Table Tunnel 下载 → 清理
        df = _execute_via_temp_table(client, query, sql_content, hints=hints)
    else:
        # 默认模式：直接通过 Instance Tunnel 下载
        instance = client.execute_sql(sql_content, hints=hints)
        instance.wait_for_success()

        with instance.open_reader(tunnel=True) as reader:
            df = reader.to_pandas()

    row_count = len(df)
    if row_count >= 10000:
        logger.warning(
            f"Query '{query.name}' returned {row_count} rows (>= 10000). "
            f"Verify Instance Tunnel is enabled to avoid truncation."
        )
    return df


# --------------------------------------------------------------------------
# 临时表模式
# --------------------------------------------------------------------------

def _execute_via_temp_table(
    client,
    query: SQLQueryConfig,
    sql_content: str,
    hints: dict = None,
) -> pd.DataFrame:
    """通过临时表执行 SQL 并下载结果

    流程：
      1. 生成唯一临时表名
      2. 包装 SQL 为 CREATE TABLE temp AS <sql>
      3. 执行 CTAS 语句
      4. 通过 ODPS Table Tunnel 从临时表下载数据
      5. 清理临时表（无论成功失败均执行 DROP）

    为什么需要临时表：
      Instance Tunnel 需要对源表有 odps:Download 权限，
      而 Table Tunnel 下载自己创建的表不需要源表权限。

    Args:
        client:      MaxComputerClient 客户端实例
        query:       SQLQueryConfig 配置
        sql_content: SQL 文件内容
        hints:       MaxCompute SQL hints

    Returns:
        pd.DataFrame
    """
    temp_table_name = _generate_temp_table_name(query.name)
    project = query.temp_table_project
    schema = query.temp_table_schema

    # 构建临时表全名（用于日志和 DROP）
    full_name_parts = []
    if project:
        full_name_parts.append(project)
    if schema:
        full_name_parts.append(schema)
    full_name_parts.append(temp_table_name)
    full_table_name = ".".join(full_name_parts)

    # 去除原始 SQL 末尾分号（CTAS 语法要求）
    clean_sql = re.sub(r';\s*$', '', sql_content)

    # 构建 CTAS 语句（LIFECYCLE 1 作为安全网：即使 DROP 失败，1 天后自动清理）
    ctas_sql = f"CREATE TABLE {full_table_name} LIFECYCLE 1 AS\n{clean_sql};"
    logger.info(
        f"Query '{query.name}': using temp table mode -> {full_table_name}"
    )
    logger.debug(f"CTAS SQL:\n{ctas_sql[:500]}...")

    # 1. 执行 CTAS
    instance = client.execute_sql(ctas_sql, hints=hints)
    instance.wait_for_success()
    logger.info(f"Query '{query.name}': CTAS completed, reading from temp table...")

    # 2. 通过 ODPS REST API 读取临时表数据（try/finally 确保清理）
    #    使用 odps.read_table() 遍历记录并转为 DataFrame，
    #    避免 Table Tunnel 端点配置问题。
    try:
        odps_client = client._client
        records = list(odps_client.read_table(
            temp_table_name,
            project=project,
            schema=schema,
        ))
        if records:
            columns = [col.name for col in records[0]._columns]
            data = [[record[col] for col in columns] for record in records]
            df = pd.DataFrame(data, columns=columns)
        else:
            df = pd.DataFrame()
        logger.info(
            f"Query '{query.name}': read {len(df)} rows from temp table "
            f"via ODPS REST API"
        )
        return df
    finally:
        # 3. 清理临时表
        _drop_temp_table(client, full_table_name)


def _generate_temp_table_name(query_name: str) -> str:
    """生成唯一临时表名: _tmp_{query_name}_{timestamp}"""
    timestamp = int(time.time())
    # MaxCompute 表名只允许字母、数字、下划线
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', query_name)
    return f"_tmp_{safe_name}_{timestamp}"


def _drop_temp_table(client, full_table_name: str) -> None:
    """清理临时表，失败仅记录警告不抛出异常"""
    try:
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name};"
        instance = client.execute_sql(drop_sql)
        instance.wait_for_success()
        logger.info(f"Temp table '{full_table_name}' dropped successfully")
    except Exception as e:
        logger.warning(
            f"Failed to drop temp table '{full_table_name}': {e}. "
            f"Please clean up manually."
        )


def _topological_sort(queries: list[SQLQueryConfig]) -> list[SQLQueryConfig]:
    """对 SQLQueryConfig 列表进行拓扑排序（Kahn 算法）

    无依赖的查询排在前面，有依赖的按依赖顺序排列。
    若存在循环依赖则抛出 ValueError。

    Args:
        queries: SQLQueryConfig 配置列表

    Returns:
        list[SQLQueryConfig]：按执行顺序排列的查询列表

    Raises:
        ValueError: 存在循环依赖或引用了不存在的查询名
    """
    # 构建邻接表和入度表
    name_to_query = {q.name: q for q in queries}
    in_degree = {q.name: 0 for q in queries}
    adjacency = {q.name: [] for q in queries}  # name -> list of names that depend on it

    for q in queries:
        for dep in q.depends_on:
            if dep not in name_to_query:
                raise ValueError(
                    f"Query '{q.name}' depends on '{dep}', but '{dep}' is not defined. "
                    f"Available queries: {list(name_to_query.keys())}"
                )
            adjacency[dep].append(q.name)
            in_degree[q.name] += 1

    # BFS：从入度为 0 的节点开始
    queue = deque([name for name, deg in in_degree.items() if deg == 0])
    sorted_names = []

    while queue:
        name = queue.popleft()
        sorted_names.append(name)
        for dependent in adjacency[name]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_names) != len(queries):
        # 存在环
        remaining = [name for name, deg in in_degree.items() if deg > 0]
        raise ValueError(
            f"Circular dependency detected among SQL queries: {remaining}"
        )

    return [name_to_query[name] for name in sorted_names]
