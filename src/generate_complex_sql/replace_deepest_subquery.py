import argparse
import pandas as pd
import sqlglot
from sqlglot import expressions as exp


def find_deepest_subquery(node):
    """ 迭代查找最内层子查询，避免递归深度溢出 """
    subqueries = list(node.find_all(exp.Subquery))  # 获取所有子查询
    if not subqueries:
        print("No subqueries!")
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
    return inner_subquery.sql(pretty=True, dialect="postgres").\
        replace("\n", " ").replace("\r", "")\
        if inner_subquery else "No subquery found"


def replace_deepest_subquery_in_sql(original_sql: str, deepest_subquery: str) -> str:
    """Replace the deepest subquery in original_sql with deepest_subquery."""
    '''
    testing the replace_deepest_subquery_in_sql
from replace_deepest_subquery import *
original = "SELECT * FROM (SELECT * FROM table) AS subq"
replacement = "SELECT 1"
rewritten = replace_deepest_subquery_in_sql(original, replacement)
print(rewritten)  # Output: "SELECT * FROM (SELECT 1) AS subq

    '''
    try:
        # Parse SQL and ensure semicolon termination
        original_ast = sqlglot.parse_one(original_sql.strip() + ";")
        replacement_ast = sqlglot.parse_one(deepest_subquery.strip() + ";")
        print("Parsing completed")

        # Find deepest subquery node
        deepest_node = find_deepest_subquery(original_ast)
        if not deepest_node:
            return deepest_subquery
        else:
            print("deepest_node found")
            deepest_node.set("this", replacement_ast)
        # print(original_ast)
        return original_ast.sql(pretty=True, dialect="postgres")
    except Exception:
        print("exception occurred!")
        return original_sql


def main(input_file, output_file):
    # 读取包含 original_sql 和 deepest_subquery 的 CSV 文件
    df = pd.read_csv(input_file)

    # 处理每一行数据，替换最深子查询
    def process_row(row):
        try:
            original_sql = row["original_sql"]
            deepest_subquery = row["complex_deepest_subquery"]
            updated_sql = replace_deepest_subquery_in_sql(original_sql,
                                                          deepest_subquery)
            print(updated_sql)
            print("")
            return pd.Series([updated_sql, original_sql, deepest_subquery])
        except Exception as e:
            return pd.Series([f"Error: {e}", row["original_sql"],
                             row["complex_deepest_subquery"]])

    # 应用处理函数，更新 SQL
    df[["updated_sql", "original_sql", "complex_deepest_subquery"]] =\
        df.apply(process_row, axis=1)

    # 保存到新的 CSV 文件
    df.to_csv(output_file, index=False, header=True)

    print(f"Processing complete. Results saved to '{output_file}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Replace deepest subqueries\
        in SQL with complex versions')
    parser.add_argument('-i', '--input', required=True, help='Input CSV file\
        containing original SQL and complex subqueries')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file\
        for modified SQL queries')

    args = parser.parse_args()

    main(args.input, args.output)
