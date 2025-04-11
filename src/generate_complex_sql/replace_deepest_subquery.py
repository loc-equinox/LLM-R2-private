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

def replace_deepest_subquery_in_sql(original_sql: str, deepest_subquery: str) -> str:
    """ 替换原始 SQL 中的最内层子查询部分 """
    try:
        # 查找原始 SQL 中的最深子查询
        ast = sqlglot.parse_one(original_sql.strip() + ";")
        deepest_subquery_ast = sqlglot.parse_one(deepest_subquery.strip() + ";")
        inner_subquery = find_deepest_subquery(ast)
        
        if inner_subquery:
            # 替换最深子查询部分
            ast.set("subquery", deepest_subquery_ast)  # 替换成最深的子查询
            return ast.sql(pretty=True, dialect="postgres")
        else:
            return original_sql
    except Exception as e:
        return f"Error: {e}"

# 读取包含 original_sql 和 deepest_subquery 的 CSV 文件
df = pd.read_csv("./complex_deepest_subqueries.csv")

# 处理每一行数据，替换最深子查询
def process_row(row):
    try:
        original_sql = row["original_sql"]
        deepest_subquery = row["complex_deepest_subquery"]
        updated_sql = replace_deepest_subquery_in_sql(original_sql, deepest_subquery)
        return pd.Series([updated_sql, original_sql, deepest_subquery])
    except Exception as e:
        return pd.Series([f"Error: {e}", row["original_sql"], row["deepest_subquery"]])

# 应用处理函数，更新 SQL
df[["updated_sql", "original_sql", "complex_deepest_subquery"]] = df.apply(process_row, axis=1)

# 保存到新的 CSV 文件
df.to_csv("./complex_deep_queries.csv", index=False, header=True)

print("Processing complete. Results saved to 'complex_deep_queries.csv'.")
