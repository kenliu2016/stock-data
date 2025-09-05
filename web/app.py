from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
import traceback

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawler.config import DB_CONFIG

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# 创建数据库连接引擎
def create_db_engine():
    try:
        db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)
        # 测试连接
        with engine.connect():
            pass
        return engine
    except Exception as e:
        print(f"数据库连接错误: {str(e)}")
        return None

engine = create_db_engine()

# 获取股票数据的通用函数
def get_stock_data(market_type, stock_code, data_type, is_realtime, start_date='', end_date='', limit=200):
    if engine is None:
        print("错误: 数据库连接未初始化")
        return None, "数据库连接失败"
    
    table_name = f"{market_type}_{data_type}_{'realtime' if is_realtime else 'prediction'}"
    
    try:
        with engine.connect() as conn:
            # 构建SQL查询
            if data_type == 'minute':
                if start_date and end_date:
                    sql = text("""
                        SELECT datetime, open, high, low, close, volume 
                        FROM """ + table_name + """
                        WHERE code = :code 
                        AND datetime BETWEEN :start_date AND :end_date
                        ORDER BY datetime ASC
                    """)
                    result = conn.execute(sql, {'code': stock_code, 'start_date': start_date, 'end_date': end_date})
                else:
                    sql = text("""
                        SELECT datetime, open, high, low, close, volume 
                        FROM """ + table_name + """
                        WHERE code = :code 
                        ORDER BY datetime DESC 
                        LIMIT :limit
                    """)
                    result = conn.execute(sql, {'code': stock_code, 'limit': limit})
            else:
                if start_date and end_date:
                    sql = text("""
                        SELECT datetime, open, high, low, close, volume 
                        FROM """ + table_name + """
                        WHERE code = :code 
                        AND datetime BETWEEN :start_date AND :end_date
                        ORDER BY datetime ASC
                    """)
                    result = conn.execute(sql, {'code': stock_code, 'start_date': start_date, 'end_date': end_date})
                else:
                    sql = text("""
                        SELECT datetime, open, high, low, close, volume 
                        FROM """ + table_name + """
                        WHERE code = :code 
                        ORDER BY datetime DESC 
                        LIMIT :limit
                    """)
                    result = conn.execute(sql, {'code': stock_code, 'limit': limit})
            
            rows = result.fetchall()
            
            if not rows:
                return None, f"未找到{stock_code}的{data_type}数据"
            
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            
            # 数据处理
            if data_type == 'minute':
                if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                    df = df.dropna(subset=['datetime'])
                df = df.sort_values('datetime')
            else:
                date_col = 'datetime' if 'datetime' in df.columns else 'date'
                if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    df = df.dropna(subset=[date_col])
                df = df.sort_values(date_col)
            
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'volume' in df.columns:
                df['volume'] = df['volume'].astype(int)
            
            # 转换为返回格式
            data = {
                'type': data_type,
                'is_realtime': is_realtime,
                'color': '#1890ff' if is_realtime else '#f5222d'
            }
            
            if data_type == 'minute':
                data['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
            else:
                date_col = 'datetime' if 'datetime' in df.columns else 'date'
                data['date'] = df[date_col].dt.strftime('%Y-%m-%d').tolist()
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    data[col] = df[col].tolist()
            
            return data, None
            
    except Exception as e:
        print(f"查询数据错误: {str(e)}")
        print(traceback.format_exc())
        return None, f"查询数据失败: {str(e)}"

# 获取股票列表的函数
def get_stock_list(market_type):
    if engine is None:
        print("错误: 数据库连接未初始化")
        return None, "数据库连接失败"
    
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
            sql = text("""
                SELECT DISTINCT code, name 
                FROM """ + table_name + """
                LIMIT 100
            """)
            
            result = conn.execute(sql)
            rows = result.fetchall()
            
            if not rows:
                return None, f"未找到{market_type}市场的股票列表"
            
            stock_list = [{'code': row[0], 'name': row[1]} for row in rows]
            
            return stock_list, None
            
    except Exception as e:
        print(f"查询股票列表错误: {str(e)}")
        return None, f"查询股票列表失败: {str(e)}"

# API路由：获取股票数据
@app.route('/api/stock/data', methods=['GET'])
def api_get_stock_data():
    try:
        market_type = request.args.get('market', 'cn')
        stock_code = request.args.get('code', '')
        data_type = request.args.get('dataType', 'minute')
        is_realtime = request.args.get('isRealtime', 'true').lower() == 'true'
        start_date = request.args.get('startTime', '')
        end_date = request.args.get('endTime', '')
        limit = request.args.get('limit', 200, type=int)
        
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        
        if market_type == 'cn' and stock_code.lower().startswith(('sh', 'sz')):
            stock_code = stock_code[2:]
        
        if data_type not in ['minute', 'day']:
            return jsonify({'error': '数据类型必须是 minute 或 day'}), 400
        
        data, error = get_stock_data(market_type, stock_code, data_type, is_realtime, start_date, end_date, limit)
        
        if error:
            if '未找到' in error:
                return jsonify({'error': error}), 404
            else:
                return jsonify({'error': error}), 500
        
        return jsonify(data)
    except Exception as e:
        print(f"API错误 (/api/stock/data): {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': '服务器内部错误'}), 500

# API路由：获取预测数据
@app.route('/api/stock/prediction', methods=['GET'])
def api_get_prediction_data():
    try:
        market_type = request.args.get('market', 'cn')
        stock_code = request.args.get('code', '')
        data_type = request.args.get('dataType', 'minute')
        is_realtime = False
        start_date = request.args.get('startTime', '')
        end_date = request.args.get('endTime', '')
        limit = request.args.get('limit', 200, type=int)
        
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        
        if market_type == 'cn' and stock_code.lower().startswith(('sh', 'sz')):
            stock_code = stock_code[2:]
        
        if data_type not in ['minute', 'day']:
            return jsonify({'error': '数据类型必须是 minute 或 day'}), 400
        
        data, error = get_stock_data(market_type, stock_code, data_type, is_realtime, start_date, end_date, limit)
        
        if error:
            if '未找到' in error:
                return jsonify({'error': error}), 404
            else:
                return jsonify({'error': error}), 500
        
        return jsonify(data)
    except Exception as e:
        print(f"API错误 (/api/stock/prediction): {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': '服务器内部错误'}), 500

# API路由：获取多条股票数据
@app.route('/api/stock/multi_data', methods=['GET'])
def api_get_multi_stock_data():
    try:
        market_type = request.args.get('market', 'cn')
        stock_code = request.args.get('code', '')
        data_type = request.args.get('dataType', 'minute')
        include_realtime = request.args.get('include_realtime', 'true').lower() == 'true'
        include_prediction = request.args.get('include_prediction', 'false').lower() == 'true'
        start_date = request.args.get('startTime', '')
        end_date = request.args.get('endTime', '')
        limit = request.args.get('limit', 200, type=int)
        
        if not stock_code:
            return jsonify({'error': '股票代码不能为空'}), 400
        
        if data_type not in ['minute', 'day']:
            return jsonify({'error': '数据类型必须是 minute 或 day'}), 400
        
        if not include_realtime and not include_prediction:
            return jsonify({'error': '至少需要包含实时或预测数据'}), 400
        
        results = []
        
        if include_realtime:
            realtime_data, realtime_error = get_stock_data(market_type, stock_code, data_type, True, start_date, end_date, limit)
            if realtime_error:
                print(f"获取实时数据失败: {realtime_error}")
            else:
                results.append(realtime_data)
        
        if include_prediction:
            prediction_data, prediction_error = get_stock_data(market_type, stock_code, data_type, False, start_date, end_date, limit)
            if prediction_error:
                print(f"获取预测数据失败: {prediction_error}")
            else:
                results.append(prediction_data)
        
        if not results:
            return jsonify({'error': '未能获取任何数据'}), 500
        
        return jsonify(results)
    except Exception as e:
        print(f"API错误 (/api/stock/multi_data): {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': '服务器内部错误'}), 500

# API路由：获取股票列表
@app.route('/api/stock/list', methods=['GET'])
def api_get_stock_list():
    try:
        market_type = request.args.get('market', 'cn')
        
        stock_list, error = get_stock_list(market_type)
        
        if error:
            return jsonify({'error': error}), 500
        
        return jsonify(stock_list)
    except Exception as e:
        print(f"API错误 (/api/stock/list): {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': '服务器内部错误'}), 500

# 健康检查路由
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

# 根路径路由，提供index.html文件
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)