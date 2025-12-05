#coding:utf-8
"""
Maxcompute连接配置
用于配置Airflow中的Maxcompute连接
"""

# Maxcompute连接配置示例
MAXCOMPUTE_CONNECTION_CONFIG = {
    "conn_id": "maxcompute_dev",
    "conn_type": "maxcompute",
    "host": "http://service.cn.maxcompute.aliyun.com",  # Maxcompute服务端点
    "login": "your_access_key_id",  # AccessKey ID
    "password": "your_access_key_secret",  # AccessKey Secret
    "extra": {
        "project": "your_project_name",  # 项目名称
        "endpoint": "http://service.cn.maxcompute.aliyun.com",  # 服务端点
        "tunnel_endpoint": "http://dt.cn.maxcompute.aliyun.com",  # 数据通道端点
        "region": "cn-hangzhou"  # 区域
    }
}

# 不同环境的配置
MAXCOMPUTE_ENVIRONMENTS = {
    "dev": {
        "project": "your_dev_project",
        "endpoint": "http://service.cn.maxcompute.aliyun.com",
        "region": "cn-hangzhou"
    },
    "test": {
        "project": "your_test_project", 
        "endpoint": "http://service.cn.maxcompute.aliyun.com",
        "region": "cn-hangzhou"
    },
    "prod": {
        "project": "your_prod_project",
        "endpoint": "http://service.cn.maxcompute.aliyun.com", 
        "region": "cn-hangzhou"
    }
}

# 连接测试SQL
TEST_SQL = "SELECT 1 as test_column"

# 连接验证函数
def validate_maxcompute_connection(conn_config):
    """
    验证Maxcompute连接配置
    
    Args:
        conn_config (dict): 连接配置字典
        
    Returns:
        bool: 连接是否有效
    """
    try:
        import pyodps
        
        # 创建连接
        odps = pyodps.Odps(
            access_id=conn_config["login"],
            secret_access_key=conn_config["password"],
            project=conn_config["extra"]["project"],
            endpoint=conn_config["extra"]["endpoint"]
        )
        
        # 测试连接
        result = odps.execute_sql(TEST_SQL)
        print(f"连接测试成功: {result}")
        return True
        
    except Exception as e:
        print(f"连接测试失败: {e}")
        return False

if __name__ == "__main__":
    # 测试连接配置
    print("Maxcompute连接配置:")
    print(f"连接ID: {MAXCOMPUTE_CONNECTION_CONFIG['conn_id']}")
    print(f"连接类型: {MAXCOMPUTE_CONNECTION_CONFIG['conn_type']}")
    print(f"服务端点: {MAXCOMPUTE_CONNECTION_CONFIG['host']}")
    print(f"项目名称: {MAXCOMPUTE_CONNECTION_CONFIG['extra']['project']}")
    print(f"区域: {MAXCOMPUTE_CONNECTION_CONFIG['extra']['region']}")