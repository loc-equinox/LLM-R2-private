import pandas as pd
import sqlglot
from sqlglot import expressions as exp

def replace_where_in_ast(original_sql: str, new_where: str) -> str:
    """ 替换SQL语句的WHERE条件 """
    # 解析原始SQL为AST
    ast = sqlglot.parse_one(original_sql.strip() + ";")  # 自动补充分号
    
    # 解析新WHERE条件
    temp_ast = sqlglot.parse_one(f"SELECT * FROM t {new_where.strip()};")
    new_where_node = temp_ast.find(exp.Where)
    
    # 替换原始AST中的WHERE条件
    if select_node := ast.find(exp.Select):
        select_node.set("where", new_where_node.copy())
    
    return ast.sql(pretty=True, dialect="postgres")

# 读取CSV文件
df = pd.read_csv("./where_parallel_condition.csv")

# 处理每一行数据
def process_row(row):
    try:
        complex_sql = replace_where_in_ast(row["original_sql"], row["complex_where_condition"])
        return complex_sql
    except Exception as e:
        return f"Error: {e}"

df["complex_sql"] = df.apply(process_row, axis=1)

# 保存到新CSV文件
df.to_csv("./complex_sql_where_parallel_condition.csv", index=False)
