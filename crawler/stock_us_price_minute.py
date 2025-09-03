import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from config import DB_CONFIG

# 创建数据库连接引擎
db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)

def save_to_db(df: pd.DataFrame, code: str):
    """保存美股数据到PostgreSQL，使用code+datetime作为主键"""
    if df.empty:
        print(f"⚠️ {code} 数据为空，跳过")
        return

    # 处理多层列索引
    if isinstance(df.columns, pd.MultiIndex):
        unique_tickers = {col[1] for col in df.columns if len(col) > 1}
        if len(unique_tickers) == 1:
            df = df.droplevel(1, axis=1)
        else:
            df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]

    # 统一字段名
    rename_dict = {}
    yfinance_columns = {
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    }
    
    for yf_col, target_col in yfinance_columns.items():
        if yf_col in df.columns:
            rename_dict[yf_col] = target_col
    
    df = df.rename(columns=rename_dict)
    
    # 处理时间索引
    if isinstance(df.index, pd.DatetimeIndex):
        df['datetime'] = df.index
    else:
        time_columns = ['datetime', 'date', 'time', 'timestamp', 'Datetime']
        found_time_col = False
        for col in time_columns:
            if col in df.columns:
                df['datetime'] = df[col]
                found_time_col = True
                break
        
        if not found_time_col:
            print(f"⚠️ {code} 找不到时间列，无法保存")
            return
    
    # 添加股票代码
    df["code"] = code

    # 检查关键字段
    required_columns = ["code", "datetime", "open", "high", "low", "close", "volume"]
    if "datetime" not in df.columns or "close" not in df.columns:
        print(f"⚠️ {code} 缺少关键字段，无法保存")
        return
    
    # 确保datetime字段是datetime类型
    if 'datetime' in df.columns:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                if df['datetime'].isna().any():
                    df = df.dropna(subset=['datetime'])
        except Exception as e:
            print(f"⚠️ {code} 解析datetime字段失败: {e}")
            return
    
    # 确保数值字段类型正确
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            try:
                if col == 'volume':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                print(f"⚠️ {code} 转换{col}字段类型失败: {e}")
    
    # 写入数据库
    try:
        df_to_insert = df[[col for col in required_columns if col in df.columns]].copy()
        data_dict = df_to_insert.to_dict('records')
        
        batch_size = 100
        total_batches = (len(data_dict) + batch_size - 1) // batch_size
        
        with engine.connect() as conn:
            for i in range(total_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(data_dict))
                batch_data = data_dict[start_idx:end_idx]
                
                insert_sql = text("""
                    INSERT INTO us_data_realtime (code, datetime, open, high, low, close, volume, update_time)
                    VALUES (:code, :datetime, :open, :high, :low, :close, :volume, CURRENT_TIMESTAMP)
                    ON CONFLICT (code, datetime) DO NOTHING
                """)
                
                for row in batch_data:
                    row_with_defaults = {
                        'code': row.get('code', code),
                        'datetime': row.get('datetime'),
                        'open': row.get('open'),
                        'high': row.get('high'),
                        'low': row.get('low'),
                        'close': row.get('close'),
                        'volume': row.get('volume')
                    }
                    conn.execute(insert_sql, row_with_defaults)
                
            conn.commit()
            print(f"✅ 美股{code} 数据已写入数据库, 共 {len(df)} 行")
    except Exception as e:
        print(f"❌ 保存美股{code} 数据失败: {e}")


if __name__ == "__main__":
    print("开始获取美股数据...")
    try:
        import datetime
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=1)
        
        us_data = yf.download(tickers="AAPL", start=start_date, end=end_date, interval="1m")
        save_to_db(us_data, "AAPL")
    except Exception as e:
        print(f"⚠️ 获取美股数据时发生错误: {e}")
    
    print("美股数据获取完成！")