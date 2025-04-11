import pandas as pd
import sqlglot
from sqlglot import expressions as exp


def find_deepest_subquery(node):
    """ 迭代查找最内层子查询，避免递归深度溢出 """
    subqueries = list(node.find_all(exp.Subquery))  # 获取所有子查询
    if not subqueries:
        return None

    # 计算每个子查询的嵌套深度
    def count_subquery_depth(subq):
        return len(list(subq.find_all(exp.Subquery)))

    # 找到嵌套深度最小的
    deepest_subquery = min(subqueries, key=count_subquery_depth)
    return deepest_subquery


def extract_deepest_subquery(original_sql: str) -> str:
    """ 提取最内层子查询的 SQL 内容 """
    ast = sqlglot.parse_one(original_sql.strip() + ";")  # 解析 SQL
    inner_subquery = find_deepest_subquery(ast)  # 查找最内层子查询
    return inner_subquery.sql(pretty=True, dialect="postgres").replace("\n", " ").replace("\r", "") if inner_subquery else "No subquery found"


# 读取CSV文件
df = pd.read_csv("../../data/data_llmr2/queries/queries_tpch_test.csv")

# 处理每一行数据，提取最内层子查询
def process_row(row):
    try:
        original_sql = row["original_sql"]
        deepest_sql = extract_deepest_subquery(original_sql)
        return pd.Series([original_sql, deepest_sql])
    except Exception as e:
        return pd.Series([row["original_sql"], f"Error: {e}"])


# 应用处理函数，提取每个 SQL 的最内层子查询
df[["original_sql", "deepest_subquery"]] = df.apply(process_row, axis=1)

# 保存到新CSV文件，确保 SQL 在一行内且没有换行
df.to_csv("./deepest_subqueries.csv", index=False, header=True)

print("Processing complete. Results saved to 'deepest_subqueries.csv'.")

