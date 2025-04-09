from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
import pandas as pd
import csv

os.environ['HTTP_PROXY'] = "http://127.0.0.1:7890"
os.environ['HTTPS_PROXY'] = "http://127.0.0.1:7890"


# 设置 OpenAI API 连接
client = OpenAI(
    api_key=""
)

os.environ["TOKENIZERS_PARALLELISM"] = "false"
pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

def query_turbo_model(prompt):
    chat_completion = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="gpt-3.5-turbo",
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


if __name__ == "__main__":
    file_path = "/home/wangyiyan/LLM-R2/src/generate_complex_sql/deepest_subqueries.csv"   #处理后的原始查询+最深子查询
    output_path = "./complex_deepest_subqueries.csv"
    
    df = pd.read_csv(file_path)

    # 创建 CSV 文件并写入表头
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", "complex_deepest_subquery"])

    # 逐行处理并写入 CSV
    for index, row in df.iterrows():
        subquery = row["deepest_subquery"]
        original_sql = row["original_sql"]
        complex_where = generate_complex_sql(subquery,original_sql)

        # 打印输出
        print("original subquery:", subquery)
        print("complex subquery:", complex_where)
        print("-" * 80)

        with open(output_path, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, original_sql, complex_where])

    print(f"The result has been saved to {output_path}")
