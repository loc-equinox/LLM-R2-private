import argparse
from openai import OpenAI
import os
from sentence_transformers import SentenceTransformer
from difflib import SequenceMatcher
from typing import Dict, List, Tuple
import psycopg2
from psycopg2 import OperationalError
import sys
sys.path.append("generate_complex_sql")
from connect_llm import get_user_tables
from rewriter import call_rewriter
from compare_plans import plan_similarity

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
# pre_lang_model = SentenceTransformer('all-MiniLM-L6-v2')

def query_LLM(prompt):
    chat_completion = client.chat.completions.create(
        model="ep-20250208072708-5r255",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return chat_completion.choices[0].message.content


def get_initial_prompt(rules, schema_info):
    """Generates the initial prompt"""
    return f"""
You are an SQL expert. You will be given a list of SQL
rewrite rules. Please provide an example query using
TPCH schema so that, when applying the rules in the list
one by one, the query's plan will change for every rule
applied.

rule_list: {rules}

Make sure your query follows the tpch schema:
{schema_info}

Output instructions:
- Return only the example query **on a single line**
- Do **not** include line breaks or any additional explanation
- Make sure your query follows the tpch schema, do not make up
  nonexistent tables.

Example query:
"""


def get_recovery_prompt(rules, failure_info: str, schema_info):
    """Generates the recovery prompt"""
    return f"""
You are an SQL expert. You will be given a list of SQL
rewrite rules. Please provide an example query using
TPCH schema so that, when applying the rules in the list
one by one, the query's plan will change for every rule
applied.

rule_list: {rules}

Make sure your query follows the tpch schema:
{schema_info}

Someone has attempted to provide an example but failed.
It could be either that:
1. The query has syntax errors.
2. The exists some rule that did not cause a change
   in the query plan when applied.
In case 1, the error message from the database is provided.
In case 2, all the intermediate rewrite rules are given.

**You should make sure that your response is very different from his.**

Failure_info:
{failure_info}

Output instructions:
- Return only the example query **on a single line**
- Do **not** include line breaks or any additional explanation
- Make sure your query follows the tpch schema, do not make up
  nonexistent tables.

Example query:
"""


def check_valid(rule_list, query: str):
    """Check whether the query that the LLM provides is valid"""
    log = "Problematic query:\n" + query + "\n"

    syntax_check = check_query_syntax(query, DB_CONFIG)
    if syntax_check[0] is False:
        print("Syntax error")
        print(syntax_check[1])
        log += "Syntax error\n" + syntax_check[1]
        return log

    # First check the semantic diff between the queries
    # rewrite_result is a string of all the intermediate queries
    # concatenated together
    rewrite_result = call_rewriter("tpch", query, rule_list)
    intermediate_results = rewrite_result.split(';')
    similarity = []
    similarity.append(SequenceMatcher(None, query,
                                      intermediate_results[0]).ratio())
    for i in range(len(rule_list) - 1):
        q1 = intermediate_results[i]
        q2 = intermediate_results[i + 1]
        similarity.append(SequenceMatcher(None, q1, q2).ratio())
    print(similarity)
    '''
    # Exit early if the intermediate queries are identical.
    # No need to examine plans
    for s in similarity:
        if s > 0.99:
            log += "Some rewrite rule had no effect\n" \
                   + rewrite_result + "\n"
            return log
    '''

    # Then check the diff in plans between the queries
    intermediate_plans = [syntax_check[1]]
    for r in intermediate_results:
        # The syntax is guaranteed to be correct in this case,
        # and we use check_query_syntax to obtain the plan
        syntax_check = check_query_syntax(r, DB_CONFIG)
        if syntax_check[0] is False:
            print("Syntax error")
            print(syntax_check[1])
            log += "Syntax error\n" + syntax_check[1]
            return log
        intermediate_plans.append(syntax_check[1])
    similarity = []
    for i in range(len(intermediate_plans) - 1):
        p1 = intermediate_plans[i]
        p2 = intermediate_plans[i + 1]
        similarity.append(plan_similarity(p1, p2))
    print(intermediate_plans)
    print(similarity)
    for s in similarity:
        if s > 0.7:
            log += "Some rewrite rule had no effect\n" \
                   + rewrite_result + "\n"
            return log

    return "success"


def check_query_syntax(query: str, db_config: Dict) -> Tuple[bool, str]:
    try:
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("EXPLAIN " + query)
                query_plan = cursor.fetchall()
                plan_str = " ".join(line[0]
                                    .replace("\n", " ")
                                    .strip()
                                    for line in query_plan)
                return (True, plan_str)
    except psycopg2.Error as e:
        return (False, str(e))
    except OperationalError as e:
        return (False, f"Connection error: {str(e)}")


def get_rule_name(rule):
    """Discard a rule's explanation and retain only its name"""
    return rule.split(':')[0][1:]


def main(rules: str):
    schema_info = get_user_tables(DB_CONFIG)
    print(schema_info)

    # This part parses the rules string:
    # Before: "[SORT_UNION_TRANSPOSE: Rule that pushes a Sort past a Union] \
    #          ;[UNION_PULL_UP_CONSTANTS: Rule that pulls up constants through\
    #          a Union operator]"
    # After: ['SORT_UNION_TRANSPOSE', 'UNION_PULL_UP_CONSTANTS']
    rule_list = rules.split(';')
    rule_list[:] = [get_rule_name(rule) for rule in rule_list]
    print(rule_list)

    attempt = query_LLM(get_initial_prompt(rules, schema_info))
    print(attempt)
    count = 0
    failure_log = ""
    log = check_valid(rule_list, attempt)
    while log != "success":
        # The query may be invalid for two reasons:
        # 1. It has syntax errors.
        # 2. Some rewrite rules are ineffective when applied
        #    on the query.
        # In case 1, check_valid will return the error message
        # from the database.
        # In case 2, check_valid will return all the intermediate
        # rewrite rules.
        print("Query not valid")
        failure_log += log
        count += 1
        if count > 20:
            break
        print(f"Retrying...(attempt {count})")
        attempt = query_LLM(get_recovery_prompt(rules, failure_log,
                                                schema_info))
        print(attempt)
        log = check_valid(rule_list, attempt)

    if count > 20:
        print("Exploration failed")
    else:
        print("Exploration succeeded, valid query:")
        print(attempt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Given a rewrite rule \
        sequence, generate a query whose plan will change for every \
        rule applied.')
    parser.add_argument('-r', '--rules', required=True, help='The \
        sequence of rewrite rules')
    args = parser.parse_args()

    main(args.rules)
