from dbclass.sqlite import SQLiteDB
from dbclass.pgsql import PostgresDB
from dbclass.duck import DuckDB
import os
class DatabaseManager:
    def is_db_available(self):
        """
        检查数据库实例是否可用
        """
        if not hasattr(self, 'db') or not self.db:
            print("数据库实例不可用，请检查数据库连接配置.")
            return False
        return True
    def __init__(self, db_type='sqlite', **kwargs):
        if db_type == 'sqlite':
            db_file = kwargs.get('db_file', 'sqlite.db')
            if not os.path.exists(db_file):
                print(f"配置文件 '{db_file}' 不存在，请检查配置路径.")
                raise FileNotFoundError(f"配置文件 '{db_file}' 不存在，请检查配置路径.")
            self.db = SQLiteDB(db_file)
        elif db_type == 'postgres':
            config_file = kwargs.get('config_file', 'config.toml')
            if not os.path.exists(config_file):
                print(f"配置文件 '{config_file}' 不存在，请检查配置路径.")
                raise FileNotFoundError(f"配置文件 '{config_file}' 不存在，请检查配置路径.")
            self.db = PostgresDB(config_file=config_file)
        elif db_type == 'duckdb':
            db_file = kwargs.get('db_file', 'duckdb.db')
            self.db = DuckDB(db_file=db_file)
        else:
            raise ValueError("不支持的数据库类型. 必须是 'sqlite', 'postgres' 或 'duckdb'.")
    def upsert(self, table_name, data):
        if self.is_db_available():
            self.db.upsert(table_name, data)

    def query(self, query, return_type='pandas'):
        if self.is_db_available():
            return self.db.query(query, return_type)

    def execute(self, command):
        if  self.is_db_available():
            return self.db.execute(command)

    def clear_cache(self):
        self.db.clear_cache()

    def close(self):
        self.db.close()

if __name__ == "__main__":
    # 连接到 SQLite 数据库
    import pandas as pd
    db = DatabaseManager(db_type='duckdb',db_file="../duckdb.db")
    db.clear_cache()
    # 批量插入或更新数据（主键冲突时更新）
    data = {
        "id": [5, 6, 7],
        "column1": [6, '2', '333'],
        "column2": [8, '4444', '6666']
    }
    df = pd.DataFrame(data)
    df.set_index("id", inplace=True)
    db.upsert("test666", df)


    # 查询数据并返回 pandas DataFrame
    df = db.query("SELECT * FROM test666", return_type="pandas")
    print(df)

    # 关闭连接
    db.close()
