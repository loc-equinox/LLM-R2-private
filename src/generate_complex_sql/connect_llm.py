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
from abc import ABC, abstractmethod

# Base class for all generation methods
class GenerateMethod(ABC):
    def __init__(self):
        self.client = OpenAI(
            api_key=os.environ.get("ARK_API_KEY"),
            base_url="https://ark.cn-beijing.volces.com/api/v3",
        )
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        self.pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

    @abstractmethod
    def get_initial_prompt(self, target_snippet: str, original_sql: str,
                           schema_info: Dict) -> str:
        pass

    @abstractmethod
    def get_recovery_prompt(self, target_snippet: str, original_sql: str,
                            error_snippet: str, error_msg: str,
                            schema_info: Dict) -> str:
        pass

    @abstractmethod
    def validate_snippet(self, snippet: str, db_config: Dict)\
            -> Tuple[bool, str]:
        pass

    @property
    @abstractmethod
    def snippet_column_names(self) -> Tuple[str, str]:
        pass

    def query_turbo_model(self, prompt: str) -> str:
        chat_completion = self.client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="ep-20250208072708-5r255",
            temperature=0,
        )
        return chat_completion.choices[0].message.content

    def generate_complex_sql(self, target_snippet: str, original_sql: str, schema_info: Dict) -> str:
        initial_prompt = self.get_initial_prompt(target_snippet, original_sql, schema_info)
        result = self.query_turbo_model(initial_prompt)
        tries = 0
        validation = self.validate_snippet(result, DB_CONFIG)
        
        while validation[0] is False:
            tries += 1
            if tries > 5:
                print("LLM rewrite still unsuccessful, giving up...")
                return original_sql
            print("LLM rewrite unsuccessful, retrying...")
            print(f"Attempt {tries}")
            print(f"Failed snippet: {result}")
            recovery_prompt = self.get_recovery_prompt(
                target_snippet, original_sql, result, validation[1], schema_info
            )
            result = self.query_turbo_model(recovery_prompt)
            validation = self.validate_snippet(result, DB_CONFIG)
        
        print("LLM rewrite successful!")
        return result

# Concrete implementation for deep subquery generation
class DeepSubquery(GenerateMethod):
    def get_initial_prompt(self, target_snippet: str, original_sql: str, schema_info: Dict) -> str:
        return f"""
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

    def get_recovery_prompt(self, target_snippet: str, original_sql: str,
                          error_snippet: str, error_msg: str, schema_info: Dict) -> str:
        return f"""
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

    def validate_snippet(self, snippet: str, db_config: Dict) -> Tuple[bool, str]:
        if not snippet.endswith(';'):
            snippet = snippet + ';'
        try:
            with psycopg2.connect(**db_config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(f"EXPLAIN {snippet}")
                    return (True, "")
        except psycopg2.Error as e:
            return (False, str(e))
        except OperationalError as e:
            return (False, f"Connection error: {str(e)}")

    @property
    def snippet_column_names(self) -> Tuple[str, str]:
        return ("deepest_subquery", "complex_deepest_subquery")

# Factory function to create method instances
def get_method_instance(method_name: str) -> GenerateMethod:
    methods = {
        "deepest_query_once": DeepSubquery,
        "deepest_query_multiple": DeepSubquery,
        # Add new methods here as they're implemented
    }

    if method_name not in methods:
        raise ValueError(f"Unknown method: {method_name}")

    return methods[method_name]()

# Database utility functions
def get_user_tables(db_config: Dict) -> Dict[str, List[Tuple[str, str]]]:
    result = {}
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """)
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

def main(input_file: str, output_file: str,
         method_name: str = "deepest_query_once"):
    method = get_method_instance(method_name)
    schema_info = get_user_tables(DB_CONFIG)
    df = pd.read_csv(input_file)
    target_col, result_col = method.snippet_column_names

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id", "original_sql", result_col])

    for index, row in df.iterrows():
        snippet = row[target_col]
        original_sql = row["original_sql"]
        complex_snippet = method.\
            generate_complex_sql(snippet, original_sql, schema_info)

        print("original snippet:", snippet)
        print("complex snippet:", complex_snippet)
        print("-" * 80)

        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([index, original_sql, complex_snippet])

    print(f"The result has been saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate complex versions of SQL snippets using GPT')
    parser.add_argument('-i', '--input', required=True, help='Path to input CSV file')
    parser.add_argument('-o', '--output', required=True, help='Path to output CSV file')
    parser.add_argument('-m', '--method', default='deepest_query_once', help='Generation method to use')

    args = parser.parse_args()

    main(args.input, args.output, args.method)
