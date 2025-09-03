import pandas as pd
import yfinance as yf
import time
import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG

# 创建数据库连接引擎
db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)

import numpy as np
def save_to_db(df: pd.DataFrame, code: str):
    """保存港股数据到PostgreSQL，使用code+datetime作为主键"""
    if df.empty:
        print(f"⚠️ {code} 数据为空，跳过")
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
    time_columns = ['datetime', 'date', 'time', 'Datetime', 'day', '时间', '日期时间']
    for col in time_columns:
        if col in df.columns:
            rename_dict[col] = 'datetime'
            break
    
    # 如果还是没有datetime列，添加一个
    if 'datetime' not in df.columns:
        print(f"⚠️ {code} 没有找到时间列，使用当前时间")
        df['datetime'] = pd.Timestamp.now()
    
    # 映射价格相关列名
    if '收盘' in df.columns:
        rename_dict['收盘'] = 'close'
    elif '最新价' in df.columns:
        rename_dict['最新价'] = 'close'
    elif '最新' in df.columns:
        rename_dict['最新'] = 'close'
    elif 'Close' in df.columns:
        rename_dict['Close'] = 'close'
    
    # yfinance标准字段映射
    yfinance_columns = {
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Volume': 'volume'
    }
    for yf_col, standard_col in yfinance_columns.items():
        if yf_col in df.columns:
            rename_dict[yf_col] = standard_col
    
    # 中文字段映射
    chinese_to_english = {
        '开盘': 'open',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount'
    }
    for chinese_col, english_col in chinese_to_english.items():
        if chinese_col in df.columns:
            rename_dict[chinese_col] = english_col
    
    # 应用重命名
    df = df.rename(columns=rename_dict)
    df["code"] = code

    # 检查关键字段
    required_columns = ["code", "datetime", "open", "high", "low", "close", "volume"]
    if "datetime" not in df.columns or "close" not in df.columns:
        print(f"⚠️ {code} 缺少关键字段，无法保存")
        return False
    
    # 确保datetime字段类型正确
    if 'datetime' in df.columns:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                if df['datetime'].isna().any():
                    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
        except Exception as e:
            print(f"⚠️ {code} 解析datetime字段失败: {e}")
            return False
    
    # 确保数值字段类型正确 - 增强版，处理二维数据
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            try:
                # 检查数据类型，如果不是Series，尝试转换
                if not isinstance(df[col], pd.Series):
                    # 处理可能的元组或其他非标准类型
                    df[col] = pd.Series(df[col])
                
                # 检查是否为二维数据
                if hasattr(df[col].values, 'shape') and len(df[col].values.shape) > 1:
                    # 如果是二维数据，尝试转换为一维
                    if df[col].values.shape[1] == 1:
                        df[col] = pd.Series(df[col].values.ravel())
                    else:
                        # 取第一列
                        df[col] = pd.Series([x[0] if isinstance(x, (list, tuple, np.ndarray)) else x for x in df[col].values])
                
                if col == 'volume':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except Exception as e:
                print(f"⚠️ {code} 转换字段 {col} 失败: {e}")
                # 发生转换错误时，设置默认值
                if col == 'volume':
                    df[col] = 0
                else:
                    df[col] = 0.0
    
    # 选择需要保存的字段
    save_columns = [col for col in required_columns if col in df.columns]
    df_save = df[save_columns].copy()
    
    # 写入数据库
    try:
        with engine.connect() as conn:
            columns_str = ', '.join(df_save.columns)
            placeholders = ', '.join([f':{col}' for col in df_save.columns])
            insert_sql = text(
                f"""INSERT INTO hk_data_realtime ({columns_str}, update_time)
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
            batch_size = 1000
            total_rows = len(data_to_insert)
            
            for i in range(0, total_rows, batch_size):
                batch = data_to_insert[i:i+batch_size]
                for row in batch:
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
            print(f"✅ 港股{code} 数据已写入数据库, 共 {len(df)} 行")
            return True
    except Exception as e:
        print(f"❌ 保存港股{code} 数据失败: {e}")
        return False

def get_hk_minute_data(stock_code):
    """获取港股分钟级数据"""
    try:
        full_code = f"{stock_code[1:]}.HK"  # 去掉前导0，格式为0700.HK
        # 显式指定auto_adjust=True以避免FutureWarning
        data = yf.download(full_code, period='1d', interval='1m', auto_adjust=True)
        
        if data.empty:
            print(f"⚠️ 未能获取到 {stock_code} 的数据")
            return None
        
        # 转换时区为北京时间
        data.index = data.index.tz_convert('Asia/Shanghai')
        
        # 添加symbol字段
        data['symbol'] = stock_code
        
        # 检查并处理可能的二维数据
        # 创建一个新的DataFrame来存储处理后的数据
        cleaned_data = pd.DataFrame()
        cleaned_data['datetime'] = data.index
        cleaned_data['symbol'] = stock_code
        
        # 处理常见的价格和成交量字段
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in data.columns:
                # 检查列是否为二维数据
                if hasattr(data[col], 'shape') and len(data[col].shape) > 1:
                    # 如果是二维数据，尝试转换为一维
                    if data[col].shape[1] == 1:
                        # 对于单列二维数据，使用ravel转换为一维
                        cleaned_data[col.lower()] = data[col].values.ravel()
                    else:
                        # 对于多列二维数据，取第一列
                        cleaned_data[col.lower()] = data[col].iloc[:, 0].values
                else:
                    # 一维数据直接使用
                    cleaned_data[col.lower()] = data[col]
        
        # 如果没有提取到任何数据，返回原始数据
        if len(cleaned_data) == 0:
            return data
        
        return cleaned_data
    except Exception as e:
        print(f"⚠️ 获取{stock_code}数据时出错: {e}")
        return None


if __name__ == "__main__":
    print("开始获取港股数据...")
    hk_stocks = ["00700"]  # 示例：腾讯控股
    all_success = True
    
    for stock_code in hk_stocks:
        data = get_hk_minute_data(stock_code)
        if data is not None:
            success = save_to_db(data, stock_code)
            if not success:
                all_success = False
        else:
            all_success = False
        
        # 避免请求过于频繁
        time.sleep(1)
    
    if all_success:
        print("✅ 成功获取港股数据！")
    else:
        print("⚠️ 部分股票数据获取失败，请检查日志")