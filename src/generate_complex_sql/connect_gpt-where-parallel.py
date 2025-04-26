from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
import pandas as pd
import csv
import argparse


# Initialize OpenAI client
client = OpenAI(
    api_key=os.environ.get("ARK_API_KEY"),
    base_url="https://ark.cn-beijing.volces.com/api/v3",
)

os.environ["TOKENIZERS_PARALLELISM"] = "false"
pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

def query_turbo_model(prompt):
    chat_completion = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="ep-20250208072708-5r255",
        temperature=0,
    )
    return chat_completion.choices[0].message.content

def generate_complex_parallel_where_condition(simple_sql):
    prompt = f"""
You are a SQL expert. Based on the original SQL query below, generate a significantly more complex WHERE clause.

The new WHERE clause should:
- Only replace the WHERE clause from the original query (do NOT include SELECT or other clauses).
- Contain a tree-like logical structure with multiple parallel branches using AND/OR combinations.
- Include deeply nested subqueries (at least three levels).
- Introduce multiple conditions involving multiple tables and attributes.
- Avoid duplicating any part of the original WHERE clause.
- Be syntactically correct and realistic in terms of table and column usage.
- Each part of the WHERE clause should be longer and logically intricate, with nested subqueries and combined filters.

Your output should:
- Be a **single line** SQL `WHERE` clause starting with `WHERE ...`.
- Not contain line breaks or any additional commentary—only the modified WHERE clause.

Original SQL: {simple_sql}
Return only the rewritten complex WHERE clause below:
"""
    return query_turbo_model(prompt)

def process_file(input_file, output_file):
    df = pd.read_csv(input_file)

    df = df.head(5)

    # Create CSV file with headers
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", "complex_where_condition"])

    # Process each row
    for index, row in df.iterrows():
        simple_sql = row["original_sql"]
        complex_where = generate_complex_parallel_where_condition(simple_sql)

        print("original SQL:", simple_sql)
        print("complex where condition:", complex_where)
        print("-" * 80)

        # Append to CSV
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, simple_sql, complex_where])

    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate complex parallel WHERE conditions for SQL queries')
    parser.add_argument('-i', '--input', required=True, help='Input CSV file containing SQL queries')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file for complex WHERE conditions')

    args = parser.parse_args()

    process_file(args.input, args.output)
