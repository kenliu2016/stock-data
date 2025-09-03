from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawler.config import DB_CONFIG

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)  # 启用CORS以允许前端访问

# 创建数据库连接引擎
def create_db_engine():
    try:
        db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)
        print(f"成功连接到数据库: {DB_CONFIG['database']}")
        return engine
    except Exception as e:
        print(f"创建数据库连接失败: {e}")
        return None

engine = create_db_engine()

# 获取股票分钟级数据的通用函数
def get_stock_minute_data(market_type, stock_code, date='', limit=200):
    if engine is None:
        print("数据库连接失败")
        return None, "数据库连接失败"
    
    # 根据市场类型选择对应的表 - 使用实际存在的表名
    table_map = {
        'cn': 'cn_data_realtime',
        'hk': 'hk_data_realtime',
        'us': 'us_data_realtime'
    }
    
    if market_type not in table_map:
        print(f"不支持的市场类型: {market_type}")
        return None, "不支持的市场类型"
    
    table_name = table_map[market_type]
    
    try:
        with engine.connect() as conn:
            print(f"执行查询: market={market_type}, code={stock_code}, date={date}, table={table_name}")
            
            # 构建SQL查询，根据是否提供日期参数决定是否添加日期过滤条件
            if date:
                # 首先尝试数据库层面的日期过滤
                sql_executed = False
                try:
                    # 方式1：使用DATE函数
                    sql = text(f"""
                        SELECT datetime, open, high, low, close, volume 
                        FROM {table_name} 
                        WHERE code = :code 
                        AND DATE(datetime) = :date
                        ORDER BY datetime ASC
                    """)
                    print(f"执行SQL方式1: {sql}")
                    result = conn.execute(sql, {'code': stock_code, 'date': date})
                    rows = result.fetchall()
                    print(f"查询结果行数: {len(rows)}")
                    sql_executed = True
                except Exception as sql_error:
                    print(f"日期查询方式1出错: {str(sql_error)}")
                    # 在出错时回滚事务
                    conn.rollback()
                    
                    try:
                        # 方式2：使用TO_CHAR函数
                        sql = text(f"""
                            SELECT datetime, open, high, low, close, volume 
                            FROM {table_name} 
                            WHERE code = :code 
                            AND TO_CHAR(datetime, 'YYYY-MM-DD') = :date
                            ORDER BY datetime ASC
                        """)
                        print(f"执行SQL方式2: {sql}")
                        result = conn.execute(sql, {'code': stock_code, 'date': date})
                        rows = result.fetchall()
                        print(f"查询结果行数: {len(rows)}")
                        sql_executed = True
                    except Exception as sql_error2:
                        print(f"日期查询方式2也出错: {str(sql_error2)}")
                        # 再次回滚事务
                        conn.rollback()
                        
                        # 如果两种SQL方式都失败，直接获取该股票的所有数据，然后在内存中过滤
                        sql = text(f"""
                            SELECT datetime, open, high, low, close, volume 
                            FROM {table_name} 
                            WHERE code = :code 
                            ORDER BY datetime ASC
                        """)
                        print(f"尝试无日期过滤的SQL: {sql}")
                        result = conn.execute(sql, {'code': stock_code})
                        rows = result.fetchall()
                        print(f"无日期过滤查询结果行数: {len(rows)}")
                        sql_executed = True
            else:
                # 如果没有提供日期参数，获取最新的N条数据
                sql = text(f"""
                    SELECT datetime, open, high, low, close, volume 
                    FROM {table_name} 
                    WHERE code = :code 
                    ORDER BY datetime DESC 
                    LIMIT :limit
                """)
                print(f"执行SQL(无日期): {sql}")
                result = conn.execute(sql, {'code': stock_code, 'limit': limit})
                rows = result.fetchall()
                print(f"无日期查询结果行数: {len(rows)}")
            
            # 只有当执行了SQL查询并且确实没有数据时才返回错误
            # 注意：当使用日期过滤时，如果数据库中没有该日期的数据，但有其他日期的数据，不应该返回错误
            if date:
                # 对于带日期的查询，如果没有找到数据，不返回错误，而是让后续的内存过滤处理
                if not rows:
                    print(f"数据库中没有找到{stock_code}在{date}的分钟级数据，将尝试获取所有数据后在内存中过滤")
                    # 尝试获取该股票的所有数据
                    sql = text(f"""
                        SELECT datetime, open, high, low, close, volume 
                        FROM {table_name} 
                        WHERE code = :code 
                        ORDER BY datetime ASC
                    """)
                    result = conn.execute(sql, {'code': stock_code})
                    rows = result.fetchall()
                    print(f"获取到{stock_code}的所有数据，共{len(rows)}条")
            else:
                # 对于不带日期的查询，如果没有找到数据，返回错误
                if not rows:
                    print(f"未找到{stock_code}的分钟级数据")
                    return None, f"未找到{stock_code}的分钟级数据"
            
            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
            # 按时间升序排序
            df = df.sort_values('datetime')
            
            # 无论是否通过SQL进行了日期过滤，都在内存中再次过滤，确保只返回一个交易日的数据
            if date:
                # 确保datetime列是datetime类型
                if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                    # 删除转换失败的行
                    df = df.dropna(subset=['datetime'])
                
                # 将日期字符串转换为日期对象
                try:
                    target_date = pd.to_datetime(date).date()
                    # 执行日期过滤
                    df_filtered = df[df['datetime'].dt.date == target_date]
                    print(f"内存中日期过滤后的数据行数: {len(df_filtered)}")
                    
                    # 如果过滤后有数据，使用过滤后的数据
                    if len(df_filtered) > 0:
                        df = df_filtered
                    else:
                        # 如果过滤后没有数据，尝试获取最近日期的数据作为备选
                        print(f"{stock_code}在{date}无数据，尝试获取最近日期的数据")
                        # 重新获取所有数据用于查找最近日期
                        if engine is not None:
                            with engine.connect() as conn:
                                sql_all = text(f"""
                                    SELECT datetime, open, high, low, close, volume 
                                    FROM {table_name} 
                                    WHERE code = :code 
                                    ORDER BY datetime DESC
                                    LIMIT 1000
                                """)
                                result_all = conn.execute(sql_all, {'code': stock_code})
                                all_rows = result_all.fetchall()
                                if all_rows:
                                    # 创建包含所有数据的DataFrame
                                    all_df = pd.DataFrame(all_rows, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
                                    all_df['datetime'] = pd.to_datetime(all_df['datetime'], errors='coerce')
                                    all_df = all_df.dropna(subset=['datetime'])
                                    
                                    # 获取最近的一个交易日
                                    if not all_df.empty:
                                        latest_date = all_df['datetime'].dt.date.max()
                                        print(f"找到最近交易日: {latest_date}")
                                        # 过滤最近交易日的数据
                                        df = all_df[all_df['datetime'].dt.date == latest_date]
                                        print(f"最近交易日{latest_date}的数据行数: {len(df)}")
                except Exception as date_error:
                    print(f"日期转换或过滤出错: {str(date_error)}")
            
            # 确保价格和成交量是数字类型
            df['open'] = pd.to_numeric(df['open'], errors='coerce').fillna(0)
            df['high'] = pd.to_numeric(df['high'], errors='coerce').fillna(0)
            df['low'] = pd.to_numeric(df['low'], errors='coerce').fillna(0)
            df['close'] = pd.to_numeric(df['close'], errors='coerce').fillna(0)
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
            
            # 转换为字典格式
            data = {
                'datetime': df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                'open': df['open'].tolist(),
                'high': df['high'].tolist(),
                'low': df['low'].tolist(),
                'close': df['close'].tolist(),
                'volume': df['volume'].tolist()
            }
            
            print(f"成功返回数据，共{len(data['datetime'])}条记录")
            return data, None
            
    except Exception as e:
        print(f"查询数据失败: {str(e)}")
        # 提供更具体的错误信息
        if 'datetime' in str(e).lower():
            return None, f"日期格式错误: {str(e)}"
        return None, f"查询数据失败: {str(e)}"    

# 获取股票列表的函数
def get_stock_list(market_type):
    if engine is None:
        return None, "数据库连接失败"
    
    # 根据市场类型选择对应的表
    table_map = {
        'cn': 'cn_stocks',
        'hk': 'hk_stocks',
        'us': 'us_stocks'
    }
    
    if market_type not in table_map:
        return None, "不支持的市场类型"
    
    table_name = table_map[market_type]
    
    try:
        with engine.connect() as conn:
            # 查询SQL，获取股票代码和名称
            sql = text(f"""
                SELECT DISTINCT code, name 
                FROM {table_name} 
                LIMIT 100
            """)
            
            result = conn.execute(sql)
            rows = result.fetchall()
            
            if not rows:
                return None, f"未找到{market_type}市场的股票列表"
            
            # 转换为列表格式
            stock_list = [{'code': row[0], 'name': row[1]} for row in rows]
            
            return stock_list, None
            
    except Exception as e:
        return None, f"查询股票列表失败: {str(e)}"

# API路由：获取股票分钟级数据
@app.route('/api/stock/minute', methods=['GET'])
def api_get_stock_minute_data():
    market_type = request.args.get('market', 'cn')
    stock_code = request.args.get('code', '')
    date = request.args.get('date', '')
    limit = request.args.get('limit', 200, type=int)
    
    if not stock_code:
        return jsonify({'error': '股票代码不能为空'}), 400
    
    data, error = get_stock_minute_data(market_type, stock_code, date, limit)
    
    if error:
        return jsonify({'error': error}), 500
    
    return jsonify(data)

# API路由：获取股票列表
@app.route('/api/stock/list', methods=['GET'])
def api_get_stock_list():
    market_type = request.args.get('market', 'cn')
    
    stock_list, error = get_stock_list(market_type)
    
    if error:
        return jsonify({'error': error}), 500
    
    return jsonify(stock_list)

# 健康检查路由
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

# 根路径路由，提供index.html文件
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

if __name__ == '__main__':
    print("股票数据API服务启动...")
    # 以调试模式运行在localhost:5001，因为5000端口被占用
    app.run(host='0.0.0.0', port=5001, debug=True)