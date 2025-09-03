# 股票代码爬虫项目

# 股票数据获取与分析系统

本项目使用akshare库和yfinance库定期获取美国股市、中国股市和香港股市的所有股票代码和实时价格数据，并将数据保存到PostgreSQL数据库中。

## 功能特点
- 自动获取美国、中国和香港股市的股票代码及基本信息
- 获取分钟级别的实时股票价格数据
- 数据存储到PostgreSQL数据库，支持快速查询
- 模块化设计，便于扩展和维护
- 提供统一的命令行接口管理不同市场的数据
- 支持定时自动更新数据

## 环境要求
- Python 3.7+  
- PostgreSQL 9.5+

## 项目结构

```
stock/
├── main.py                # 主入口文件
├── config.py              # 配置文件
├── requirements.txt       # 项目依赖
├── setup_postgres.py      # PostgreSQL数据库安装脚本
├── create_realtime_tables.py # 实时数据表创建脚本
├── create_realtime_tables.sql # 实时数据表创建SQL
├── init.sql               # 数据库初始化SQL
├── stock_cn_code.py       # A股代码获取脚本
├── stock_cn_price.py      # A股价格获取脚本
├── stock_hk_code.py       # 港股代码获取脚本
├── stock_hk_price.py      # 港股价格获取脚本
├── stock_us_code.py       # 美股代码获取脚本
├── stock_us_price.py      # 美股价格获取脚本
└── data/                  # 数据备份目录
    ├── A_shares_stock_codes.csv
    ├── HK_shares_stock_codes.csv
    └── US_shares_stock_codes.csv
```

## 安装步骤

1. 克隆或下载本项目代码

2. 安装依赖库
```bash
pip install -r requirements.txt
```

3. 初始化PostgreSQL数据库

   运行以下命令初始化数据库（支持macOS和Ubuntu）：
   ```bash
   python setup_postgres.py
   ```
   该脚本会：
   - 检查并安装PostgreSQL（如果尚未安装）
   - 创建用户：`myuser`（密码：`mypassword`）
   - 创建数据库：`mydb`
   - 创建必要的表结构

4. 创建实时数据表

```bash
python create_realtime_tables.py
```

5. （可选）修改配置文件

   如果需要自定义数据库连接参数或定时任务配置，可以打开`config.py`文件进行修改。

## 使用方法

### 基本使用

使用统一的主入口获取所有市场的股票代码和价格数据：

```bash
python main.py
```

### 命令行参数

```
-h, --help            显示帮助信息
--market {cn,hk,us,all}  指定要获取数据的市场 (默认: all)
--action {codes,prices,both}  指定要执行的操作 (默认: both)
--schedule            是否启动定时任务
```

### 示例

只获取A股的股票代码：

```bash
python main.py --market cn --action codes
```

更新美股的价格数据：

```bash
python main.py --market us --action prices
```

启动定时任务持续更新所有市场数据：

```bash
python main.py --schedule
```

### 后台运行（推荐）

为了让定时任务持续运行，建议使用nohup在后台运行：
```bash
nohup python main.py --schedule > stock_log.txt 2>&1 &
```

## 数据结构

### 数据库表

1. **股票基本信息表**：
   - `us_stocks`：美股股票列表
   - `cn_stocks`：A股股票列表
   - `hk_stocks`：港股股票列表

2. **实时数据表**：
   - `cn_data_realtime`：A股实时分钟数据
   - `hk_data_realtime`：港股实时分钟数据
   - `us_data_realtime`：美股实时分钟数据

### 实时数据表字段

- `code`：股票代码
- `datetime`：时间戳
- `open`：开盘价
- `high`：最高价
- `low`：最低价
- `close`：收盘价
- `volume`：成交量
- `update_time`：更新时间

## 配置说明

在`config.py`文件中可以配置以下参数：

- 数据库连接参数
- 定时任务配置
- API重试次数和间隔

## 注意事项

1. 使用前请确保网络连接正常，本系统需要连接互联网获取股票数据
2. 频繁请求API可能会受到限制，请合理设置请求间隔
3. 默认配置下，定时任务会每24小时更新一次股票代码，每分钟更新一次价格数据
4. 数据获取失败时会尝试读取本地备份文件
- `update_time`: 更新时间

### 股票价格数据表

- `stock_minute_data`: 股票1分钟级别历史价格数据

股票价格数据表字段说明：
- `id`: 自增主键
- `symbol`: 股票代码（带市场标识）
- `datetime`: 时间点
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `update_time`: 更新时间

## 测试数据库连接

可以运行以下脚本来测试数据库连接是否正常：
```bash
python test_db_connection.py
```

如果连接成功，会显示类似以下信息：
```
数据库连接成功!
PostgreSQL 版本: 15.14
```

## 注意事项
1. 确保PostgreSQL服务正常运行
2. 首次运行时可能需要较长时间获取数据
3. 定时任务需要保持程序一直运行，推荐使用nohup在后台运行
4. 如果遇到API调用限制或akshare库函数参数变化，可以调整代码中的API调用部分
5. macOS系统上，可能需要手动启动PostgreSQL服务：`brew services start postgresql`

## 常见问题

### 数据库连接失败
- 检查PostgreSQL服务是否启动
- 确认用户名(myuser)、密码(mypassword)和数据库名称(mydb)是否正确
- 检查防火墙设置是否允许连接
- 如果遇到"role \"postgres\" does not exist"错误，说明系统默认用户不是postgres，请使用`setup_postgres.py`脚本初始化数据库

### 无法获取数据
- 检查网络连接是否正常
- 确认akshare库版本是否最新，可使用`pip install akshare --upgrade`更新
- 查看程序输出的错误信息，可能需要调整API调用参数
- 如果akshare库的API函数参数发生变化，请参考akshare官方文档更新代码中的函数调用

## 许可证
本项目采用MIT许可证。