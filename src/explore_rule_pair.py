import argparse
from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
from difflib import SequenceMatcher
from all_sequences import test_all_sequences
from typing import Dict, List, Tuple
import psycopg2
from psycopg2 import OperationalError

DB_CONFIG = {
    "dbname": "tpch",
    "user": "leshanchen",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

client = OpenAI(
    api_key=os.environ.get("ARK_API_KEY"),
    base_url="https://ark.cn-beijing.volces.com/api/v3",
)
os.environ["TOKENIZERS_PARALLELISM"] = "false"
pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

def query_LLM(prompt):
    chat_completion = client.chat.completions.create(
        model="ep-20250208072708-5r255",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return chat_completion.choices[0].message.content


def get_initial_prompt(rule1: str, rule2: str, schema_info):
    """Generates the initial prompt"""
    return f"""
You are a SQL expert. You will be given two rules for SQL rewriting.
Please provide an example query using TPCH schema so that, when applying rule1 then rule2 is better than applying rule2 then rule1.

rule1: {rule1}
rule2: {rule2}

Make sure your query follows the tpch schema:
{schema_info}

Output instructions:
- Return only the example query **on a single line**
- Do **not** include line breaks or any additional explanation
- Make sure your query follows the tpch schema, do not make up nonexistent tables.

Example query:
"""


def get_recovery_prompt(rule1: str, rule2: str, failure_info: str, schema_info):
    """Generates the recovery prompt"""
    return f"""
You are a SQL expert. You will be given two rules for SQL rewriting.
Please provide an example query using TPCH schema so that, applying rule1 then
rule2 is better than applying rule2 then rule1.

rule1: {rule1}
rule2: {rule2}

Someone has made an attempt but failed, below is his query, and
the intermediate queries when applying the rules in different
orders. The final rewritten query in both orders are identical, so
this query is not valid.
{failure_info}

Make sure your query follows the tpch schema:
{schema_info}

Output instructions:
- Return only the example query **on a single line**
- Do **not** include line breaks or any additional explanation
- Make sure your query follows the tpch schema, do not make up nonexistent tables

Example query:
"""

def get_user_tables(db_config: Dict) -> Dict[str, List[Tuple[str, str]]]:
    """Get schemas of a database"""
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


def check_valid(rule1: str, rule2: str, query: str):
    """Check whether the query that the LLM provides is valid"""
    rule_list = [get_rule_name(rule1), get_rule_name(rule2)]
    try:
        log = test_all_sequences("tpch", query, rule_list)
        return log
    except Exception as e:
        return ""


def get_rule_name(rule):
    """Discard a rule's explanation and retain only its name"""
    return rule.split(':')[0][1:]


def caps(str):
    """Return all capital letters of a string"""
    return ''.join([char for char in str if char.isupper()])


def main(rule1: str, rule2: str):
    schema_info = get_user_tables(DB_CONFIG)
    print(schema_info)
    attempt = query_LLM(get_initial_prompt(rule1, rule2, schema_info))
    print(attempt)
    count = 0
    log = check_valid(rule1, rule2, attempt)
    failed_queries = []
    while log != "success":
        # Provide LLM with the log info during later attempts
        failed_queries.append(attempt)
        print("Query not valid")
        if "'NA'" in log or "apache" in log:
            # The calcite rewriter errors, indicating
            # syntax errors in LLM's response.
            print("error detected!")
            log = ""
        count += 1
        if count > 20:
            break
        print(f"Retrying...(attempt {count})")
        # print(failed_queries)
        # print(attempt)
        # diff = sum([SequenceMatcher(None, caps(q), caps(attempt)).ratio() for q in failed_queries]) / len(failed_queries)
        # print(diff)
        attempt = query_LLM(get_recovery_prompt(rule1, rule2, log, schema_info))
        print(attempt)
        log = check_valid(rule1, rule2, attempt)

    if count > 20:
        print("Exploration failed")
    else:
        print("Exploration succeeded, valid query:")
        print(attempt)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate rewrite\
        -rder-sensitive queries given two rewrite rules.')
    parser.add_argument('-r1', '--rule1', required=True, help='The\
        first rewrite rule')
    parser.add_argument('-r2', '--rule2', required=True, help='The\
        second rewrite rule')
    args = parser.parse_args()

    main(args.rule1, args.rule2)
