import akshare as ak
import pandas as pd
import pytz
from sqlalchemy import create_engine, text
from config import DB_CONFIG
import time
from datetime import datetime, timedelta

# 创建数据库连接引擎
db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)

def get_cn_daily_data(stock_code, max_retries=3, retry_interval=2):
    """通过akshare的stock_zh_a_daily方法获取A股日K线数据，增加重试机制"""
    retry_count = 0
    while retry_count <= max_retries:
        try:
            # 直接使用传入的代码，不再添加市场前缀
            if retry_count == 0:
                print(f"正在获取 {stock_code} 的日K线数据...")
            elif retry_count > 0:
                print(f"重试获取 {stock_code} 数据 ({retry_count}/{max_retries})")
            
            # 计算过去1年的日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 调用akshare获取日K线数据
            data = ak.stock_zh_a_daily(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")

            if data.empty:
                print(f"数据为空，请检查代码是否正确或市场是否交易")
                return None

            return data
        except Exception as e:
            retry_count += 1
            if retry_count <= max_retries:
                print(f"❌ 获取 {stock_code} 日K线数据失败: {str(e)[:200]}，将在 {retry_interval} 秒后重试 ({retry_count}/{max_retries})")
                time.sleep(retry_interval)
            else:
                print(f"❌ 获取 {stock_code} 日K线数据失败: {str(e)[:200]}，已达到最大重试次数")
                import traceback
                traceback.print_exc()
                return None

def save_to_db(df: pd.DataFrame, code: str):
    """保存A股日K线数据到PostgreSQL，使用code+datetime作为主键"""
    if df.empty:
        print("数据为空，跳过保存")
        return False

    # 确保时间索引正确处理
    if isinstance(df.index, pd.DatetimeIndex):
        # 先创建一个datetime列
        df = df.reset_index()
        # 重命名index列为datetime
        if 'index' in df.columns:
            df = df.rename(columns={'index': 'datetime'})

    # 统一字段名
    rename_dict = {}
    
    # 检查是否有day列并优先使用
    if 'day' in df.columns:
        rename_dict['day'] = 'datetime'
    elif 'date' in df.columns:
        rename_dict['date'] = 'datetime'
    else:
        # 查找其他可能的时间列
        time_columns = ['datetime', 'time', 'Datetime', '时间', '日期时间']
        for col in time_columns:
            if col in df.columns:
                rename_dict[col] = 'datetime'
                break

    # 如果还是没有时间列，添加一个
    if not rename_dict:
        df['datetime'] = pd.Timestamp.now()
    else:
        # 应用重命名
        df = df.rename(columns=rename_dict)

    # 映射价格相关列名
    chinese_to_english = {
        '开盘': 'open',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '最新价': 'close',
        '成交量': 'volume',
        '成交额': 'amount'
    }
    for chinese_col, english_col in chinese_to_english.items():
        if chinese_col in df.columns:
            rename_dict[chinese_col] = english_col

    # 应用重命名
    df = df.rename(columns=rename_dict)
    
    # 去掉股票代码前缀
    clean_code = code.lstrip('sh').lstrip('sz').lstrip('bj')
    df["code"] = clean_code

    # 检查关键字段
    required_columns = ["code", "datetime", "open", "high", "low", "close", "volume"]
    if "datetime" not in df.columns or "close" not in df.columns:
        return False

    # 确保datetime字段类型正确且只保留日期部分
    if 'datetime' in df.columns:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                # 尝试多种格式解析时间
                formats = ['%Y/%m/%d', '%Y-%m-%d', '%Y%m%d']
                parsed = False
                for fmt in formats:
                    try:
                        df['datetime'] = pd.to_datetime(df['datetime'], format=fmt)
                        parsed = True
                        break
                    except ValueError:
                        continue
                
                if not parsed:
                    # 最后尝试自动解析
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                    
            # 只保留日期部分，去掉时间部分
            df['datetime'] = df['datetime'].dt.date
                
        except Exception:
            return False

    # 确保数值字段类型正确
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            try:
                if col == 'volume':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except Exception:
                # 发生转换错误时，设置默认值
                if col == 'volume':
                    df[col] = 0
                else:
                    df[col] = 0.0

    # 数据清洗：删除无效的时间记录
    if 'datetime' in df.columns:
        df = df.dropna(subset=['datetime'])

    # 选择需要保存的字段
    save_columns = [col for col in required_columns if col in df.columns]
    df_save = df[save_columns].copy()

    # 写入数据库
    try:
        with engine.connect() as conn:
            # 构建插入SQL
            columns_str = ', '.join(df_save.columns)
            placeholders = ', '.join([f':{col}' for col in df_save.columns])
            insert_sql = text(
                f"""INSERT INTO cn_data_day ({columns_str}, update_time)
                   VALUES ({placeholders}, NOW())
                   ON CONFLICT (code, datetime) DO UPDATE
                   SET open = EXCLUDED.open,
                       high = EXCLUDED.high,
                       low = EXCLUDED.low,
                       close = EXCLUDED.close,
                       volume = EXCLUDED.volume,
                       update_time = NOW()
                """)
            
            data_to_insert = df_save.to_dict(orient='records')
            batch_size = 1000  # 批量插入大小
            total_rows = len(data_to_insert)
            
            # 分批插入数据
            for i in range(0, total_rows, batch_size):
                batch = data_to_insert[i:i+batch_size]
                for row in batch:
                    # 处理可能的空值
                    row_with_defaults = {}
                    for key, value in row.items():
                        if value is None or (isinstance(value, float) and pd.isna(value)):
                            if key == 'volume':
                                row_with_defaults[key] = 0
                            elif key in ['open', 'high', 'low', 'close']:
                                row_with_defaults[key] = 0.0
                            else:
                                row_with_defaults[key] = ''
                        else:
                            row_with_defaults[key] = value
                    conn.execute(insert_sql, row_with_defaults)
            
            conn.commit()
            print(f"{clean_code} 日K线数据已写入数据库, 共 {len(df)} 行")
            return True
    except Exception:
        return False

def main():
    """主函数：获取A股日K线数据并保存"""
    
    # 测试示例：贵州茅台，直接使用带市场前缀的代码
    stock_code = "sh600519"
    
    # 获取日K线数据
    daily_data = get_cn_daily_data(stock_code)
    
    if daily_data is not None and not daily_data.empty:
        # 保存到数据库
        save_result = save_to_db(daily_data, stock_code)
        if not save_result:
            print("数据保存失败")
    else:
        print("未能获取到有效日K线数据")

if __name__ == "__main__":
    main()