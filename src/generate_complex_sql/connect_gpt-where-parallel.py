from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
import pandas as pd
import csv


os.environ['HTTP_PROXY'] = "http://127.0.0.1:7890"
os.environ['HTTPS_PROXY'] = "http://127.0.0.1:7890"

# 设置 OpenAI API 连接
client = OpenAI(
    api_key="your_api_key_here"  
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




if __name__ == "__main__":
    file_path = "../LLM-R2/data/data_llmr2/queries/queries_tpch_test.csv"  # llmr2中的查询作为模板
    output_path = "./where_parallel_condition.csv"
    
    df = pd.read_csv(file_path)

    # 创建 CSV 文件并写入表头
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", "complex_where_condition"])

    # 逐行处理并写入 CSV
    for index, row in df.iterrows():
        simple_sql = row["original_sql"]
        complex_where = generate_complex_parallel_where_condition(simple_sql)

        # 打印输出
        print("original SQL:", simple_sql)
        print("complex where condition:", complex_where)
        print("-" * 80)

        # 写入 CSV
        with open(output_path, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, simple_sql, complex_where])

    print(f"The result has been saved to {output_path}")
