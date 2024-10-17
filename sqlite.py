import sqlite3
import pandas as pd
import os
import pickle

import sqlite3
import pandas as pd
import os
import pickle

class SQLiteDB:
    def __init__(self, db_file='sqlite.db'):
        """
        初始化 SQLite 数据库连接
        """
        # 如果数据库文件不存在，则创建数据库文件
        if not os.path.exists(db_file):
            open(db_file, 'w').close()

        try:
            self.cache_file = 'db_cache.pkl'
            self.cache = self._load_cache()
            self.connection = sqlite3.connect(db_file)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f"SQLite 数据库连接失败: {e}")
            return

        # 加载缓存


    def _load_cache(self):
        """
        从本地加载缓存
        """
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'rb') as f:
                return pickle.load(f)
        return {}

    def _save_cache(self):
        """
        保存缓存到本地
        """
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.cache, f)

    def clear_cache(self):
        """
        清空缓存
        """
        self.cache = {}
        self._save_cache()
        print("缓存已清空")

    def _get_primary_key(self, table_name):
        """
        获取指定表的主键列名，使用缓存
        :param table_name: 表名
        :return: 主键列名
        """
        if table_name in self.cache and 'primary_key' in self.cache[table_name]:
            return self.cache[table_name]['primary_key']

        conflict_column_query = f"PRAGMA table_info({table_name});"

        try:
            self.cursor.execute(conflict_column_query)
            rows = self.cursor.fetchall()
            conflict_columns = [row[1] for row in rows if row[5] == 1]  # 第 5 列表示是否为主键
            # 缓存主键信息
            if table_name not in self.cache:
                self.cache[table_name] = {}
            self.cache[table_name]['primary_key'] = conflict_columns
            self._save_cache()
            return conflict_columns
        except Exception as e:
            print(f"无法确定主键列: {e}")
            return None

    def _create_table_from_dataframe(self, table_name, df):
        """
        根据 DataFrame 创建新表
        :param table_name: 表名
        :param df: pandas DataFrame，用于定义表的结构
        """
        columns_with_types = []
        for column in df.columns:
            dtype = df[column].dtype
            if dtype == 'int64':
                columns_with_types.append(f"{column} INTEGER")
            elif dtype == 'float64':
                columns_with_types.append(f"{column} REAL")
            else:
                columns_with_types.append(f"{column} TEXT")

        # 使用 DataFrame 的索引作为主键，可能是多字段主键
        primary_keys = [pk for pk in df.index.names if pk is not None]
        if not primary_keys:
            print("无索引，创建表失败.")
            return

        primary_keys_str = ', '.join(primary_keys)
        for pk in primary_keys:
            if pk not in df.columns:
                columns_with_types.append(f"{pk} TEXT")

        create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns_with_types)}, PRIMARY KEY ({primary_keys_str}));"
        try:
            self.cursor.execute(create_table_query)
            print(f"表 '{table_name}' 创建成功.")
        except Exception as e:
            print(f"创建表 '{table_name}' 失败: {e}")

    def upsert_from_dataframe(self, table_name, df):
        """
        批量插入数据到指定的表中（从 pandas DataFrame），如果主键冲突则更新
        :param table_name: 表名
        :param df: pandas DataFrame，包含要插入的数据
        """
        if df.empty:
            print("未提供任何要插入的数据.")
            return

        # 检查表是否存在，不存在则创建
        if not self._get_primary_key(table_name):
            self._create_table_from_dataframe(table_name, df)

        columns = df.reset_index().columns.tolist()
        values_list = df.reset_index().values.tolist()

        # 构建 SQL 批量插入语句，遇到冲突时替换
        placeholders = ', '.join('?' * len(columns))
        insert_statement = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            self.cursor.executemany(insert_statement, values_list)
            self.connection.commit()
            print(f"批量数据已插入到表 {table_name}")
        except Exception as e:
            print(f"批量插入数据到表 {table_name} 失败: {e}")

    def upsert_from_dict(self, table_name, data):
        """
        插入数据到指定的表中（从字典），如果主键冲突则更新
        :param table_name: 表名
        :param data: 字典，包含要插入的数据 {'column1': value1, 'column2': value2, ...}
        """
        if not data:
            print("未提供任何要插入的数据.")
            return

        columns = list(data.keys())
        values = [data[column] for column in columns]

        # 检查表是否存在，不存在则创建
        df = pd.DataFrame([data])
        if not self._get_primary_key(table_name):
            self._create_table_from_dataframe(table_name, df)

        # 构建 SQL 插入语句，遇到冲突时替换
        placeholders = ', '.join('?' * len(columns))
        insert_statement = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            self.cursor.execute(insert_statement, values)
            self.connection.commit()
            print(f"数据已插入到表 {table_name}")
        except Exception as e:
            print(f"插入数据到表 {table_name} 失败: {e}")

    def upsert(self, table_name, data):
        """
        通用的 upsert 函数，根据 data 类型选择插入方式
        :param table_name: 表名
        :param data: 要插入的数据，可以是字典或 pandas DataFrame
        """
        if isinstance(data, pd.DataFrame):
            self.upsert_from_dataframe(table_name, data)
        elif isinstance(data, dict):
            self.upsert_from_dict(table_name, data)
        else:
            print("不支持的 upsert 数据类型. 必须是字典或 pandas DataFrame.")

    def query(self, query, return_type='pandas'):
        """
        查询数据并返回 pandas DataFrame 或字典格式
        :param query: SQL 查询语句
        :param return_type: 返回类型，'pandas' 返回 DataFrame，'dict' 返回字典
        :return: pandas DataFrame 或 字典
        """
        try:
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()

            if return_type == 'pandas':
                return pd.DataFrame(rows, columns=columns)
            elif return_type == 'dict':
                return [dict(zip(columns, row)) for row in rows]
            else:
                raise ValueError("无效的返回类型. 使用 'pandas' 或 'dict'.")
        except Exception as e:
            print(f"查询失败: {e}")
            return None
    def is_db_available(self):
        """
        检查数据库实例是否可用
        """
        if not hasattr(self, 'connection') or not self.connection:
            print("数据库实例不可用，请检查数据库连接配置.")
            return False
        return True
    def execute(self, command):
        """
        执行 SQLite 自有的命令
        :param command: SQLite 命令字符串
        """
        try:
            self.cursor.execute(command)
            self.connection.commit()
            print(f"命令 '{command}' 执行成功.")
        except Exception as e:
            print(f"命令 '{command}' 执行失败: {e}")
    def close(self):
        """
        关闭数据库连接
        """
        self.cursor.close()
        self.connection.close()
        print("数据库连接已关闭")
# 示例使用
if __name__ == "__main__":
    # 连接到 SQLite 数据库
    db = SQLiteDB(db_file='../sqlite.db')
    db.clear_cache()
    # 批量插入或更新数据（主键冲突时更新）
    data = {"id": 5, "u1": "value5", "u2": 6}
    df = pd.DataFrame([data])
    df.set_index("id", inplace=True)
    db.upsert("test666", df)


    # 查询数据并返回 pandas DataFrame
    df = db.query("SELECT * FROM test666", return_type="pandas")
    print(df)

    # 关闭连接
    db.close()
