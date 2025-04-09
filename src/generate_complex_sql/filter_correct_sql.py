import pandas as pd
import psycopg2
import time

DB_CONFIG = {
    "dbname": "tpch10g",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# CSV 文件路径
input_csv_path = "./filtered_where_parallel_condition_sql.csv"
output_csv_path = "./correct_where_parallel_condition_queries.csv"

# 读取 CSV 文件
df = pd.read_csv(input_csv_path)

# 确保 'original_sql' 列存在
if 'original_sql' not in df.columns:
    raise ValueError("CSV 文件中找不到 'original_sql' 列")

def execute_sql(sql_query):
    """
    在 PostgreSQL 中执行 SQL 语句：
    - 没有在短时间内报语法错误 认为是正确的查询
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # 让查询直接执行，不受事务影响
        cursor = conn.cursor()

        cursor.execute("SET statement_timeout = 5000")  # 设置 SQL 执行超时

        start_time = time.time()
        cursor.execute(sql_query)  # 执行查询
        
        # 获取查询结果
        result = cursor.fetchone()
        elapsed_time = time.time() - start_time

        cursor.close()
        conn.close()

        # 如果查询在 5 秒内完成，且没有报错，并且没有返回结果，则视为有效查询
        if elapsed_time < 5 and result is None:
            return True
        return False

    except psycopg2.errors.QueryCanceled as e:
        # 如果出现超时错误，将其视为正确的查询
        if "canceling statement due to statement timeout" in str(e):
            return True
        print(f"SQL 执行超时: {e}")
        return False

    except Exception as e:
        print(f"SQL 执行错误: {e}")
        return False

# 读取 SQL 并验证
valid_queries = []

for sql in df['original_sql']:
    if execute_sql(sql):
        valid_queries.append([sql])  # 存储执行正常的且没有输出的 SQL

# 保存正确的 SQL 语句
valid_df = pd.DataFrame(valid_queries, columns=['original_sql'])
valid_df.to_csv(output_csv_path, index=False)

print(f"筛选完成，已保存 {len(valid_queries)} 条正确的 SQL 语句到: {output_csv_path}")
