import pandas as pd
import sqlglot
from sqlglot import expressions as exp
import argparse

def replace_where_in_ast(original_sql: str, new_where: str) -> str:
    """Replace WHERE clause in SQL statement"""
    try:
        # Parse original SQL to AST
        ast = sqlglot.parse_one(original_sql.strip() + ";")

        # Parse new WHERE condition
        temp_ast = sqlglot.parse_one(f"SELECT * FROM t {new_where.strip()};")
        new_where_node = temp_ast.find(exp.Where)

        # Replace WHERE in original AST
        if select_node := ast.find(exp.Select):
            select_node.set("where", new_where_node.copy())

        return ast.sql(pretty=True, dialect="postgres")
    except Exception as e:
        return f"Error: {e}"

def process_file(input_file: str, output_file: str):
    """Process input file and save results to output file"""
    df = pd.read_csv(input_file)
    
    # Process each row
    df["updated_sql"] = df.apply(
        lambda row: replace_where_in_ast(row["original_sql"], row["complex_where_condition"]),
        axis=1
    )
    
    # Save results
    df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Replace WHERE conditions in SQL queries')
    parser.add_argument('-i', '--input', required=True,
                        help='Input CSV file with original SQL and WHERE conditions')
    parser.add_argument('-o', '--output', required=True,
                        help='Output CSV file for modified SQL queries')

    args = parser.parse_args()
    process_file(args.input, args.output)
