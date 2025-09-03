import akshare as ak
from sqlalchemy import create_engine
from config import DB_CONFIG, FILE_CONFIG
import pandas as pd
from datetime import datetime
import time
import os
import akshare as ak


def create_db_engine():
    """
    创建数据库连接引擎
    从config.py导入数据库配置参数
    """
    try:
        # 从config.py获取数据库配置
        db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)
        print(f"成功连接到数据库: {DB_CONFIG['database']}")
        return engine
    except Exception as e:
        print(f"创建数据库连接失败: {e}")
        return None


def get_us_stocks(max_retries=3, retry_interval=2):
    """
    获取美股所有股票的基本信息，包含重试机制和上市公司过滤
    参数：
        max_retries: 最大重试次数
        retry_interval: 重试间隔（秒）
    返回：股票信息DataFrame或None
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"正在使用stock_us_spot获取美股股票数据... (尝试 {retry_count+1}/{max_retries})")
            stock_list = ak.stock_us_spot()
            
            if stock_list is not None and not stock_list.empty:
                print(f"成功获取美股股票数据，共{len(stock_list)}条记录")
                
                # 添加更新时间
                stock_list['update_time'] = datetime.now()
                
                # 过滤只保留上市公司数据（排除退市和即将上市的股票）
                # 根据美股代码规则过滤：
                # 美股主要交易所代码通常为1-5个字母
                # 排除特殊代码和退市股票
                listed_stocks = stock_list
                
                # 根据美股代码规则进行过滤
                # 假设代码列名为'代码'或'symbol'
                if '代码' in stock_list.columns:
                    listed_stocks = stock_list[stock_list['代码'].str.match(r'^[A-Za-z]{1,5}$')]
                elif 'symbol' in stock_list.columns:
                    listed_stocks = stock_list[stock_list['symbol'].str.match(r'^[A-Za-z]{1,5}$')]
                
                # 检查过滤后的结果
                if not listed_stocks.empty:
                    print(f"成功过滤出上市公司数据，共{len(listed_stocks)}条记录")
                    return listed_stocks
                else:
                    print("过滤后没有符合条件的上市公司数据")
                    # 如果过滤后没有数据，返回原始数据
                    return stock_list
            else:
                print("stock_us_spot返回空数据")
        except Exception as e:
            error_msg = str(e)
            print(f"stock_us_spot调用失败: {error_msg[:100]}...")
            # 检查是否返回HTML内容而非JSON数据
            if '<' in error_msg:
                print("警告: API返回了HTML内容而非JSON数据，可能是网站结构变化或访问限制导致")
        
        retry_count += 1
        if retry_count < max_retries:
            print(f"所有API调用失败，{retry_interval}秒后重试...")
            time.sleep(retry_interval)
        
    # 所有重试都失败，尝试读取本地CSV文件作为备份
    print("所有API调用失败，尝试读取本地备份CSV文件...")
    try:
        csv_file = os.path.join(FILE_CONFIG['stock_codes_dir'], 'US_shares_stock_codes.csv')
        if os.path.exists(csv_file):
            stock_list = pd.read_csv(csv_file)
            if not stock_list.empty:
                print(f"成功读取本地备份数据，共{len(stock_list)}条记录")
                # 更新时间戳
                stock_list['update_time'] = datetime.now()
                return stock_list
            else:
                print("本地备份文件为空")
        else:
            print("本地备份文件不存在")
    except Exception as e:
        print(f"读取本地备份文件失败: {e}")
        
    print("无法获取美股股票数据")
    return None


def save_to_csv(stock_list, filename='US_shares_stock_codes.csv'):
    """
    将股票数据保存为CSV文件到配置的目录
    """
    try:
        # 确保股票代码CSV文件夹存在
        data_dir = FILE_CONFIG['stock_codes_dir']
        os.makedirs(data_dir, exist_ok=True)
        # 如果文件名不包含路径，则保存到配置的目录下
        if not os.path.dirname(filename):
            filename = os.path.join(data_dir, filename)
        stock_list.to_csv(filename, index=False)
        print(f"数据已保存为: {filename}")
    except Exception as e:
        print(f"保存CSV文件失败: {e}")


def save_to_database(stock_list, engine):
    """
    将股票数据保存到PostgreSQL数据库
    使用us_stocks表名，与项目结构保持一致
    """
    if engine is None:
        print("数据库引擎未初始化，无法保存数据")
        return
    
    try:
        # 重命名列名以匹配数据库表结构
        # 美股数据的列名可能与A股不同，根据实际API返回调整
        stock_data = stock_list.copy()
        
        # 确保必要的列存在
        if '代码' in stock_data.columns:
            stock_data = stock_data.rename(columns={'代码': 'code'})
        elif 'symbol' in stock_data.columns:
            stock_data = stock_data.rename(columns={'symbol': 'code'})
        
        if '英文名称' in stock_data.columns:
            stock_data = stock_data.rename(columns={'英文名称': 'name'})
        elif 'name' in stock_data.columns:
            stock_data = stock_data.rename(columns={'name': 'name'})

        if '中文名称' in stock_data.columns:
            stock_data = stock_data.rename(columns={'中文名称': 'cname'})
        elif 'cname' in stock_data.columns:
            stock_data = stock_data.rename(columns={'cname': 'cname'})
        
        # 选择需要保存的列
        columns_to_save = ['code', 'name', 'cname','update_time']
        
        # 只保留存在的列
        existing_columns = [col for col in columns_to_save if col in stock_data.columns]
        
        # 如果缺少必要的列，尝试补充
        if 'code' not in stock_data.columns:
            print("警告: 缺少code列，无法保存数据")
            return
        
        if 'name' not in stock_data.columns:
            stock_data['name'] = ''
            existing_columns.append('name')
            print("警告: 缺少name列，使用空字符串填充")
        
        # 保存到数据库，使用replace模式覆盖数据，确保根据code的唯一性更新
        stock_data[existing_columns].to_sql('us_stocks', engine, if_exists='replace', index=False)
        print(f"成功保存{len(stock_data)}条股票数据到数据库表: us_stocks")
    except Exception as e:
        print(f"保存数据到数据库失败: {e}")


def main():
    """
    主函数：获取美股股票数据并保存
    """
    print(f"开始执行美股股票数据获取任务: {datetime.now()}")
    
    # 创建数据库连接
    engine = create_db_engine()
    
    # 获取美股股票数据
    stock_list = get_us_stocks()
    
    if stock_list is not None:
        # 保存到CSV文件
        save_to_csv(stock_list)
        
        # 保存到数据库
        save_to_database(stock_list, engine)
    
    print(f"美股股票数据获取任务执行完成: {datetime.now()}")


if __name__ == "__main__":
    main()