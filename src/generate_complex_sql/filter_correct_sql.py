import time
import pandas as pd
import psycopg2
import argparse

DB_CONFIG = {
    "dbname": "tpch",
    "user": "leshanchen",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

def execute_sql(sql_query):
    """
    在 PostgreSQL 中执行 SQL 语句：
    - 没有在短时间内报语法错误 认为是正确的查询
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("connection successful!")
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
            print("query finished in 5 seconds")
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

def main(input_file, output_file):
    # 读取 CSV 文件
    df = pd.read_csv(input_file)

    # 确保 'updated_sql' 列存在
    if 'updated_sql' not in df.columns:
        raise ValueError(f"CSV 文件中找不到 'updated_sql' 列: {input_file}")

    # 读取 SQL 并验证
    valid_queries = []

    for sql in df['updated_sql']:
        if execute_sql(sql):
            valid_queries.append([sql])  # 存储执行正常的且没有输出的 SQL

    # 保存正确的 SQL 语句
    valid_df = pd.DataFrame(valid_queries, columns=['updated_sql'])
    valid_df.to_csv(output_file, index=False)

    print(f"筛选完成，已保存 {len(valid_queries)} 条正确的 SQL 语句到: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Filter SQL queries by executing them against PostgreSQL')
    parser.add_argument('-i', '--input', required=True, help='Input CSV file containing SQL queries to test')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file for valid SQL queries')
    
    args = parser.parse_args()
    
    main(args.input, args.output)

