from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
import pandas as pd
import csv
import argparse

# 设置 OpenAI API 连接
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

def generate_complex_sql(subquery, original_sql):
    prompt = f"""
You are a SQL expert. Please rewrite the provided subquery to make it significantly more complex.

Requirements:
- Only rewrite the subquery part; do not modify the rest of the original SQL query.
- The new subquery should contain multiple levels of nested subqueries to increase complexity.
- Avoid reusing the same structure or logic from the original subquery.
- Ensure the rewritten subquery is syntactically correct and can be seamlessly integrated back into the original SQL.
- All attributes, tables, and logic used must be valid and coherent.

Original SQL Query: {original_sql}

Original Subquery to be rewritten: {subquery}

Output Instructions:
- Return only the rewritten complex subquery on a single line.
- Do **not** include line breaks or any additional explanation.
- Do **not** return the full original query—just the rewritten subquery.

Rewritten Complex Subquery:
"""
    return query_turbo_model(prompt)

def main(input_file, output_file):
    df = pd.read_csv(input_file)

    # 创建 CSV 文件并写入表头
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", "complex_deepest_subquery"])

    # 逐行处理并写入 CSV
    # i = 0
    for index, row in df.iterrows():
        # i += 1
        # if i > 5:
        #    break
        subquery = row["deepest_subquery"]
        original_sql = row["original_sql"]
        complex_where = generate_complex_sql(subquery, original_sql)

        # 打印输出
        print("original subquery:", subquery)
        print("complex subquery:", complex_where)
        print("-" * 80)

        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, original_sql, complex_where])

    print(f"The result has been saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate complex versions of SQL subqueries using GPT')
    parser.add_argument('-i', '--input',
                        required=True,
                        help='Path to input CSV file containing subqueries')
    parser.add_argument('-o', '--output',
                        required=True,
                        help='Path to output CSV file for complex subqueries')
    
    args = parser.parse_args()
    
    main(args.input, args.output)
