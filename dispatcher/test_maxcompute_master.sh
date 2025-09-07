#!/bin/bash
# 测试MaxCompute连接 - 整合版
# 此脚本提供多种方式测试MaxCompute连接

# 定义颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 使用方法函数
usage() {
  echo -e "${GREEN}MaxCompute连接测试工具 - 整合版${NC}"
  echo
  echo -e "用法: ${YELLOW}$0 [选项]${NC}"
  echo "选项:"
  echo "  -c, --connection   使用Airflow连接进行测试 (推荐)"
  echo "  -e, --env          使用环境变量进行测试"
  echo "  -h, --help         显示此帮助信息"
  echo
  echo "示例:"
  echo "  $0 -c              使用Airflow连接进行测试"
  echo "  $0 -e              使用环境变量进行测试"
  echo
}

# 检查容器状态函数
check_container() {
  echo -e "${YELLOW}检查容器状态...${NC}"
  if ! docker ps | grep -q dispatcher-airflow-1; then
    echo -e "${RED}错误: 容器 dispatcher-airflow-1 未运行，请先启动容器${NC}"
    exit 1
  fi
  echo -e "${GREEN}容器运行中${NC}"
}

# 测试使用Airflow连接
test_with_connection() {
  echo -e "${GREEN}===== 开始MaxCompute连接测试 (使用Airflow Connections) =====${NC}"
  
  check_container
  
  # 复制Python测试脚本到容器内
  echo -e "\n${YELLOW}复制测试脚本到容器...${NC}"
  docker cp $(dirname "$0")/test_maxcompute_conn.py dispatcher-airflow-1:/tmp/
  
  # 执行Python测试脚本
  echo -e "\n${YELLOW}执行测试脚本...${NC}"
  docker exec -it dispatcher-airflow-1 su airflow -c "PYTHONPATH=/opt/airflow python3 /tmp/test_maxcompute_conn.py"
  
  # 检查执行结果
  if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}✅ MaxCompute连接测试成功${NC}"
  else
    echo -e "\n${RED}❌ MaxCompute连接测试失败${NC}"
  fi
}

# 测试使用环境变量
test_with_env() {
  echo -e "${GREEN}===== 开始MaxCompute连接测试 (使用环境变量) =====${NC}"
  
  check_container
  
  # 检查是否设置了必要的环境变量
  echo -e "\n${YELLOW}检查环境变量...${NC}"
  local missing=false
  local env_vars=("MAXCOMPUTE_ACCESS_ID" "MAXCOMPUTE_ACCESS_KEY" "MAXCOMPUTE_PROJECT" "MAXCOMPUTE_ENDPOINT")
  
  for var in "${env_vars[@]}"; do
    if [ -z "${!var}" ]; then
      echo -e "${RED}❌ 未设置环境变量: ${var}${NC}"
      missing=true
    else
      echo -e "${GREEN}✅ 已设置环境变量: ${var}${NC}"
    fi
  done
  
  if [ "$missing" = true ]; then
    echo -e "${RED}请设置所有必要的环境变量${NC}"
    return 1
  fi
  
  # 准备环境变量命令
  local env_cmd=""
  for var in "${env_vars[@]}"; do
    env_cmd="${env_cmd} ${var}='${!var}'"
  done
  
  # 将环境变量传递给容器
  echo -e "\n${YELLOW}在容器中设置环境变量并执行测试...${NC}"
  docker exec -it dispatcher-airflow-1 bash -c "export ${env_cmd} && su airflow -c 'python3 -c \"
import os
import sys
from datetime import datetime

print(f\\\"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\\")
print(f\\\"Python版本: {sys.version}\\\")

try:
    # 尝试导入ODPS模块
    try:
        from odps import ODPS
        print('✅ ODPS模块导入成功')
    except ImportError:
        print('❌ 无法导入ODPS模块，请确保已安装PyODPS')
        sys.exit(1)
    
    # 从环境变量获取连接信息
    access_id = os.environ.get('MAXCOMPUTE_ACCESS_ID')
    access_key = os.environ.get('MAXCOMPUTE_ACCESS_KEY')
    project = os.environ.get('MAXCOMPUTE_PROJECT')
    endpoint = os.environ.get('MAXCOMPUTE_ENDPOINT')
    
    # 显示连接信息（隐藏敏感信息）
    print('从环境变量获取的参数:')
    print(f'  project: {project}')
    print(f'  endpoint: {endpoint}')
    if access_id:
        masked_id = access_id[:4] + '*' * (len(access_id) - 8) + access_id[-4:] if len(access_id) > 8 else '****'
        print(f'  access_id: {masked_id}')
    if access_key:
        masked_key = access_key[:4] + '*' * (len(access_key) - 8) + access_key[-4:] if len(access_key) > 8 else '****'
        print(f'  secret_access_key: {masked_key}')
    
    # 创建ODPS客户端
    print('\\n尝试创建ODPS客户端...')
    odps = ODPS(
        access_id=access_id,
        secret_access_key=access_key,
        project=project,
        endpoint=endpoint
    )
    print('✅ ODPS客户端创建成功')
    
    # 执行简单查询
    print('\\n尝试执行SQL: SELECT 1 as test')
    instance = odps.execute_sql('SELECT 1 as test')
    print('SQL执行成功，结果:')
    
    with instance.open_reader() as reader:
        for record in reader:
            print(f'  {record}')
    print('✅ SQL查询成功')
    
    print('\\n✅ MaxCompute连接测试成功')
    
except Exception as e:
    print(f'❌ 出现错误: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)

print(f\\\"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\\")
\"'"
  
  # 检查执行结果
  if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}✅ MaxCompute连接测试成功${NC}"
  else
    echo -e "\n${RED}❌ MaxCompute连接测试失败${NC}"
  fi
}

# 主函数
main() {
  # 如果没有参数，显示帮助
  if [ $# -eq 0 ]; then
    usage
    exit 1
  fi
  
  # 解析参数
  while [ $# -gt 0 ]; do
    case "$1" in
      -c|--connection)
        test_with_connection
        ;;
      -e|--env)
        test_with_env
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo -e "${RED}错误: 未知选项 $1${NC}"
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# 执行主函数
main "$@"
