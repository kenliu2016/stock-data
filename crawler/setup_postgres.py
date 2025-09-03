import platform
import subprocess
import psycopg2
import os

# 从config.py导入数据库配置
from config import DB_CONFIG

def run_command(cmd):
    """运行系统命令"""
    print(f"运行命令: {cmd}")
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print("错误信息:", result.stderr)
    else:
        print(result.stdout)
    return result.returncode == 0

def install_postgres():
    """安装 PostgreSQL（支持 macOS 和 Ubuntu）"""
    system = platform.system().lower()
    
    if "darwin" in system:  # macOS
        print("检测到 macOS, 使用 Homebrew 安装 PostgreSQL...")
        run_command("brew update")
        run_command("brew install postgresql")
        run_command("brew services start postgresql")
    
    elif "linux" in system:  # Ubuntu
        print("检测到 Linux (Ubuntu), 使用 apt 安装 PostgreSQL...")
        run_command("sudo apt update")
        run_command("sudo apt install -y postgresql postgresql-contrib")
        run_command("sudo systemctl enable postgresql")
        run_command("sudo systemctl start postgresql")
    else:
        raise OSError("不支持的系统，仅支持 macOS 和 Ubuntu")

def init_postgres():
    """初始化 PostgreSQL：从config.py获取配置，创建用户、数据库、表"""
    try:
        # 从config.py获取数据库配置
        username = DB_CONFIG['user']
        password = DB_CONFIG['password']
        dbname = DB_CONFIG['database']
        host = DB_CONFIG['host']
        port = DB_CONFIG['port']
        
        # 获取当前系统用户名
        system_user = os.environ.get('USER') or os.environ.get('LOGNAME') or "aaronkliu"
        print(f"当前系统用户: {system_user}")
        
        # 尝试使用psql命令行工具来创建用户和数据库
        print("尝试使用psql命令创建用户和数据库...")
        
        # 创建用户命令（避免使用$$，直接尝试创建用户）
        print(f"尝试创建用户 {username}...")
        create_user_cmd = f"psql -U {system_user} -c 'CREATE USER {username} WITH PASSWORD ''{password}'';'".replace("''", "\\'")
        user_created = run_command(create_user_cmd)
        
        # 如果创建失败，可能是用户已存在，继续尝试创建数据库
        if not user_created:
            print(f"用户 {username} 可能已存在，或创建失败，继续尝试创建数据库")
        
        # 创建数据库命令
        print(f"尝试创建数据库 {dbname}...")
        create_db_cmd = f"psql -U {system_user} -c 'CREATE DATABASE {dbname} OWNER {username};'"
        db_created = run_command(create_db_cmd)
        
        # 如果创建失败，可能是数据库已存在，继续尝试连接
        if not db_created:
            print(f"数据库 {dbname} 可能已存在，或创建失败，继续尝试连接")
        
        # 连接到新创建的数据库
        try:
            conn = psycopg2.connect(dbname=dbname, user=username, password=password, host=host, port=port)
            conn.autocommit = True
            cur = conn.cursor()
        except Exception as e:
            print(f"连接到数据库失败: {e}")
            print("请确保用户和数据库已正确创建，并且您有访问权限")
            return
        
        # 创建用户
        cur.execute(f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{username}') THEN CREATE USER {username} WITH PASSWORD '{password}'; END IF; END $$;")
        print(f"用户 {username} 已创建或已存在")
        
        # 创建数据库
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {dbname} OWNER {username};")
            print(f"数据库 {dbname} 已创建")
        else:
            print(f"数据库 {dbname} 已存在")

        cur.close()
        conn.close()

        # 连接到新数据库，创建表
        conn = psycopg2.connect(dbname=dbname, user=username, password=password, host=host)
        conn.autocommit = True
        cur = conn.cursor()
        
        # 从init.sql文件读取并执行SQL语句
        try:
            with open('init.sql', 'r', encoding='utf-8') as f:
                sql_commands = f.read()
                
            # 按分号分割SQL命令（简单处理，实际应用中可能需要更复杂的解析）
            commands = sql_commands.split(';')
            
            for command in commands:
                command = command.strip()
                if command:  # 跳过空命令
                    cur.execute(command)
                    
            print("成功从init.sql文件执行所有表结构创建命令")
        except Exception as e:
            print(f"读取或执行init.sql文件失败: {e}")

        cur.close()
        conn.close()
        
    except Exception as e:
        print("数据库初始化失败:", e)

if __name__ == "__main__":
    install_postgres()
    init_postgres()