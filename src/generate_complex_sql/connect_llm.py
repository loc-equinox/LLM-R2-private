from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
import pandas as pd
import csv
from typing import Dict, List, Tuple
import argparse
from generator import DB_CONFIG
import psycopg2
from psycopg2 import OperationalError

# 设置 OpenAI API 连接
client = OpenAI(
    api_key=os.environ.get("ARK_API_KEY"),
    base_url="https://ark.cn-beijing.volces.com/api/v3",
)


def get_user_tables(db_config: Dict) -> Dict[str, List[Tuple[str, str]]]:
    """
    Retrieve user-created tables (public schema) with columns and data types

    Args:
        db_config: Database connection parameters

    Returns:
        Dictionary of {table_name: [(column_name, data_type)]}
    """
    result = {}
    conn = None

    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            # Query for user-created tables in public schema
            cursor.execute("""
                SELECT
                    table_name,
                    column_name,
                    data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """)

            # Build the result structure
            for table, column, data_type in cursor.fetchall():
                if table not in result:
                    result[table] = []
                result[table].append((column, data_type))

    except OperationalError as e:
        print(f"Connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return result

os.environ["TOKENIZERS_PARALLELISM"] = "false"
pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

def query_turbo_model(prompt):
    chat_completion = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="ep-20250208072708-5r255",
        temperature=0,
    )
    return chat_completion.choices[0].message.content


def get_initial_prompt(method, target_snippet, original_sql, schema_info):
    """
    Returns the prompt that is used when connecting to
    the LLM to process a given snippet for the first time.
    """
    prompt = ""
    if method in ["deepest_query_once", "deepest_query_multiple"]:
        prompt = f"""
You are a SQL expert. Please rewrite the provided subquery to make it significantly more complex.

Requirements:
- Only rewrite the subquery part; do not modify the rest of the original SQL query.
- The new subquery should contain multiple levels of nested subqueries to increase complexity.
- Avoid reusing the same structure or logic from the original subquery.
- Ensure the rewritten subquery is syntactically correct and can be seamlessly integrated back into the original SQL.
- All attributes, tables, and logic used must be valid and coherent.

Original SQL Query: {original_sql}

Original Subquery to be rewritten: {target_snippet}

Schemas in the database, for reference: {schema_info}

Output Instructions:
- Return only the rewritten complex subquery **on a single line**.
- Do **not** include line breaks or any additional explanation.
- Do **not** return the full original query—just the rewritten subquery.

Rewritten Complex Subquery:
"""
    return prompt


def get_recovery_prompt(method, target_snippet, original_sql,
                        error_snippet, error_msg, schema_info):
    """
    Returns the prompt that is used when connecting to
    the LLM to re-process a given snippet, since previous
    attempts had led to incorrect SQL.
    """
    prompt = ""
    if method in ["deepest_query_once", "deepest_query_multiple"]:
        prompt = f"""
You are a SQL expert. Please rewrite the provided subquery to make it significantly more complex.

Requirements:
- Only rewrite the subquery part; do not modify the rest of the original SQL query.
- The new subquery should contain multiple levels of nested subqueries to increase complexity.
- Avoid reusing the same structure or logic from the original subquery.
- Ensure the rewritten subquery is syntactically correct and can be seamlessly integrated back into the original SQL.
- All attributes, tables, and logic used must be valid and coherent.

Original SQL Query: {original_sql}

Original Subquery to be rewritten: {target_snippet}

Schemas in the database, for reference: {schema_info}

Another SQL expert has tried to rewrite the subquery but
failed. His attempt: {error_snippet}

The error message generated: {error_msg}

You should build upon his work and make the subquery work, so please
at least check that your response is DIFFERENT from his, since his
attempt is obviously not correct.

Also, you should make sure to take account of the error message
generated for the previous faulty subquery, and make sure you
do not make the same mistakes.

Output Instructions:
- Return only the rewritten complex subquery **on a single line**.
- Do **not** include line breaks or any additional explanation.
- Do **not** return the full original query—just the rewritten subquery.

Rewritten Complex Subquery:
"""

    return prompt


def validate_snippet(method, snippet, db_config):
    """
    Validates the given snippet.
    @Returns: (is_valid: bool, error_msg: str)

    Different methods may return different types of
    snippets, for example, one method may return just
    a complex FROM clause, while another may return
    a subquery. So we use different ways to turn them
    into (theoretically) valid SQL and run them.
    """
    if method in ["deepest_query_once", "deepest_query_multiple"]:
        # Already a subquery, only need to add semicolon if neccessary.
        if not snippet.endswith(';'):
            snippet = snippet + ';'
    try:
        # Connect to database (using context manager for auto-close)
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = True

        with conn.cursor() as cursor:
            try:
                # Use EXPLAIN to validate without executing
                cursor.execute(f"EXPLAIN {snippet}")
                return (True, "")
            except psycopg2.Error as e:
                # Return the first line of error message
                print(str(e))
                return (False, str(e))

    except OperationalError as e:
        return (False, f"Connection error: {str(e)}")

    return (True, "")


def generate_complex_sql(method, target_snippet,
                         original_sql, schema_info):
    initial_prompt = get_initial_prompt(method, target_snippet,
                                        original_sql, schema_info)
    result = query_turbo_model(initial_prompt)
    tries = 0
    validation = validate_snippet(method, result, DB_CONFIG)
    while validation[0] is False:
        tries += 1
        if tries > 5:
            print("LLM rewrite still unsuccessful, giving up...")
            return original_sql
        print("LLM rewrite unsuccessful, retrying...")
        print(f"Attempt {tries}")
        print(f"Failed snippet: {result}")
        recovery_prompt = get_recovery_prompt(method, target_snippet,
                                              original_sql, result,
                                              validation[1], schema_info)
        result = query_turbo_model(recovery_prompt)
        validation = validate_snippet(method, result, DB_CONFIG)
    print("LLM rewrite successful!")
    return result


def main(input_file, output_file, method="deepest_query_once"):

    # The names of the columns that corresponds to
    # the target_snippet and the result_snippet
    snippet_column_name = {
        "deepest_query_once": ("deepest_subquery",
                               "complex_deepest_subquery"),
        "deepest_query_multiple": ("deepest_subquery",
                                   "complex_deepest_subquery"),
        }
    target_snippet, result_snippet = snippet_column_name[method]
    df = pd.read_csv(input_file)

    # 创建 CSV 文件并写入表头
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", result_snippet])

    schema_info = get_user_tables(DB_CONFIG)
    # 逐行处理并写入 CSV
    # i = 0
    for index, row in df.iterrows():
        # i += 1
        # if i > 5:
        #    break
        snippet = row[target_snippet]
        original_sql = row["original_sql"]
        complex_snippet = generate_complex_sql(method, snippet, original_sql, schema_info)

        # 打印输出
        print("original snippet:", snippet)
        print("complex snippet:", complex_snippet)
        print("-" * 80)

        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, original_sql, complex_snippet])

    print(f"The result has been saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate complex versions\
        of SQL snippets using GPT')
    parser.add_argument('-i', '--input',
                        required=True,
                        help='Path to input CSV file containing\
                        target snippets')
    parser.add_argument('-o', '--output',
                        required=True,
                        help='Path to output CSV file for complex snippets')
    parser.add_argument('-m', '--method',
                        default='deepest_query_once',
                        help='The method used to make the\
                        snippet more complex')

    args = parser.parse_args()

    main(args.input, args.output, args.method)
