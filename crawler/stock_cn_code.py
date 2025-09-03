import akshare as ak
from sqlalchemy import create_engine
from config import DB_CONFIG, FILE_CONFIG
import pandas as pd
from datetime import datetime
import time
import os
import sys

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


def get_cn_stocks(max_retries=3, retry_interval=2):
    """
    获取A股所有股票的基本信息，包含重试机制和上市公司过滤
    参数：
        max_retries: 最大重试次数
        retry_interval: 重试间隔（秒）
    返回：股票信息DataFrame或None
    """
    retry_count = 0
    
    # 确保股票代码CSV文件夹存在
    data_dir = FILE_CONFIG['stock_codes_dir']
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    csv_file = os.path.join(data_dir, 'A_shares_stock_codes.csv')
    
    while retry_count < max_retries:
        try:
            print(f"正在使用stock_zh_a_spot获取A股股票数据... (尝试 {retry_count+1}/{max_retries})")
            stock_list = ak.stock_zh_a_spot()
            
            if stock_list is not None and not stock_list.empty:
                print(f"成功获取A股股票数据，共{len(stock_list)}条记录")
                
                # 添加市场标识，取code的前两位
                stock_list['market'] = stock_list['代码'].str[:2]
                
                # 添加更新时间
                stock_list['update_time'] = datetime.now()
                
                # 过滤只保留上市公司数据（排除退市和即将上市的股票）
                # 支持多种代码格式：
                # 1. API返回的原始格式：纯数字代码（如600000, 000001）
                # 2. 带市场前缀的格式：sh（上海）、sz（深圳）、bj（北京）开头+数字
                # 使用更全面的正则表达式匹配
                listed_stocks = stock_list[stock_list['代码'].str.match(r'^(sh|sz|bj)?[0-9]{6}$')]
                
                # 检查过滤后的结果
                if not listed_stocks.empty:
                    print(f"成功过滤出上市公司数据，共{len(listed_stocks)}条记录")
                    return listed_stocks
                else:
                    print("过滤后没有符合条件的上市公司数据")
            else:
                print("stock_zh_a_spot返回空数据")
        except Exception as e:
            error_msg = str(e)[:100]
            if '<' in error_msg:  # 检测HTML标签特征
                print(f"stock_zh_a_spot调用失败: 返回了HTML内容而非JSON数据，可能是网络问题或API限流...")
            else:
                print(f"stock_zh_a_spot调用失败: {error_msg}...")
        
        # 调用失败，等待一段时间后重试
        retry_count += 1
        if retry_count < max_retries:
            print(f"API调用失败，{retry_interval}秒后重试...")
            time.sleep(retry_interval)
        
    # 所有重试都失败，尝试读取本地CSV文件作为备份
    print("API调用失败，尝试读取本地备份CSV文件...")
    try:
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
        
    print("无法获取A股股票数据")
    return None


def save_to_csv(stock_list, filename=None):
    """
    将股票数据保存为CSV文件到配置的目录
    """
    # 确保股票代码CSV文件夹存在
    data_dir = FILE_CONFIG['stock_codes_dir']
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # 如果没有指定文件名，使用默认文件名
    if filename is None:
        filename = os.path.join(data_dir, 'A_shares_stock_codes.csv')
    
    try:
        stock_list.to_csv(filename, index=False)
        print(f"数据已保存为: {filename}")
    except Exception as e:
        print(f"保存CSV文件失败: {e}")


def save_to_database(stock_list, engine):
    """
    将股票数据保存到PostgreSQL数据库
    使用cn_stocks表名，与项目结构保持一致
    """
    if engine is None:
        print("数据库引擎未初始化，无法保存数据")
        return
    
    try:
        # 重命名列名以匹配数据库表结构
        stock_data = stock_list.rename(columns={
            '代码': 'code',
            '名称': 'name',
            '最新价': 'price',
            '涨跌幅': 'change_percent',
            '涨跌额': 'change_amount',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_ttm',
            '市净率': 'pb'
        })
        
        # 选择需要保存的列
        columns_to_save = ['code', 'name', 'market', 'update_time']
        # 只保留存在的列
        existing_columns = [col for col in columns_to_save if col in stock_data.columns]
        
        # 保存到数据库，使用replace模式覆盖数据，确保根据code的唯一性更新
        stock_data[existing_columns].to_sql('cn_stocks', engine, if_exists='replace', index=False)
        print(f"成功保存{len(stock_data)}条股票数据到数据库表: cn_stocks")
    except Exception as e:
        print(f"保存数据到数据库失败: {e}")


def main():
    """
    主函数：获取A股股票数据并保存
    """
    print(f"开始执行A股股票数据获取任务: {datetime.now()}")
    
    # 创建数据库连接
    engine = create_db_engine()
    
    # 获取A股股票数据
    stock_list = get_cn_stocks()
    
    if stock_list is not None:
        # 保存到CSV文件
        save_to_csv(stock_list)
        
        # 保存到数据库
        save_to_database(stock_list, engine)
    
    print(f"A股股票数据获取任务执行完成: {datetime.now()}")


if __name__ == "__main__":
    main()