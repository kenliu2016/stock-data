# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'mydb',
    'user': 'myuser',
    'password': 'mypassword',
    'port': '5432'
}

# 文件路径配置
FILE_CONFIG = {
    # 股票代码CSV文件路径
    'stock_codes_dir': '/Users/aaronkliu/Documents/project/stock/data',
}

# 定时任务配置
SCHEDULE_CONFIG = {
    'enable_schedule': True,         # 是否启用定时任务
    'codes_update_interval': 24,     # 股票代码更新间隔（小时）
    'price_update_interval': 1       # 价格数据更新间隔（分钟）
}

# API配置
API_CONFIG = {
    'max_retries': 3,                # API请求最大重试次数
    'retry_interval': 2              # 重试间隔（秒）
}