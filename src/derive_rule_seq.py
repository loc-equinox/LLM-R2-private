from openai import OpenAI
import psycopg2
from psycopg2 import OperationalError
from difflib import SequenceMatcher
from typing import List, Dict, Tuple
import argparse
from rewriter import call_rewriter
from compare_plans import plan_similarity, plan_is_effective

def parse_args():
    parser = argparse.ArgumentParser(description='Process database queries with DeepSeek API.')
    parser.add_argument('--start', type=int, default=0, help='Starting index for processing')
    parser.add_argument('--end', type=int, default=-1, help='Ending index for processing (-1 for all)')
    parser.add_argument('--key', type=str, required=True, help='DeepSeek API key')
    return parser.parse_args()


args = parse_args()
client = OpenAI(api_key=args.key, base_url="https://api.deepseek.com")


def query_LLM(prompt):
    chat_completion = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5,
    )
    return chat_completion.choices[0].message.content


def execute_sql_commands(dbname: str, sql_commands: List[str]) -> bool:
    """Execute a list of SQL commands against the specified database"""
    if not sql_commands:
        return True

    for sql in sql_commands:
        print(f"\n\033[1mExecuting SQL on {dbname}:\033[0m {sql[:100]}...")  # Print first 100 chars
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                user="leshanchen",
                dbname=dbname
            )
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f"\n\033[91mError executing in {dbname}: {e}\033[0m")
                return False
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"\n\033[91mError connecting to {dbname}: {e}\033[0m")
            return False
    return True

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

def get_prompt(schema, valid_queries, failed_queries):
    return f"""
You are an SQL expert. You will be given a list of valid queries
on a database, and you need to create a different query based on these queries.

Requirements:
- The query must be different from any of the valid queries.
- Change the table or column names in one of the valid queries according to the schema provided.
- Change the predicates as well.

Database schema: {schema}

Valid_queries: {valid_queries}

Someone has attempted to rewrite them but failed, here are their attempts: {failed_queries}


Output instructions:
- Return only the rewritten query **on a single line**.
- Do **not** include line breaks or any additional explanation.

Rewritten query:
"""

def parse_results_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Remove empty lines and strip whitespace
    lines = [line.strip() for line in lines if line.strip()]

    result = []
    i = 0
    while i < len(lines):
        # First line is the list (evaluate it safely)
        try:
            items_list = eval(lines[i])
        except:
            items_list = []

        # Second line is the SQL query
        if i + 1 < len(lines):
            sql_query = lines[i+1]
        else:
            sql_query = ""

        # Add the tuple to our result
        result.append((items_list, sql_query))

        # Move to next group
        i += 2

    return result


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


def check_valid(rule_list, query: str):
    """Check whether the query that the LLM provides is valid"""
    log = "Problematic query:\n" + query + "\n"

    syntax_check = check_query_syntax(query, DB_CONFIG)
    if syntax_check[0] is False:
        print("Syntax error")
        print(syntax_check[1])
        log += "Syntax error\n" + syntax_check[1]
        return False

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
            return False
        intermediate_plans.append(syntax_check[1])

    if not plan_is_effective(intermediate_plans[0], intermediate_plans[-1]):
        plan_similarity(intermediate_plans[0], intermediate_plans[-1])
        print("The overall cost went up.")
        log += "The overall cost went up.\n"
        return False

    similarity = []
    for i in range(len(intermediate_plans) - 1):
        p1 = intermediate_plans[i]
        p2 = intermediate_plans[i + 1]
        similarity.append(plan_similarity(p1, p2))
    # print(intermediate_plans)
    print(similarity)
    for s in similarity:
        if s > 0.99:
            log += "Some rewrite rule had no effect\n" \
                   + rewrite_result + "\n"
            return False

    return True

def filter_derived(derived_query, obj):
    print(f"filtering derived queries(total {len(derived_query)})...")
    filtered = []
    for query in derived_query:
        if check_valid(obj[0], query):
            print(f"Found valid query(cost went down, plan changed): {query}")
            filtered.append(query)
    return filtered


DB_CONFIG = {
    "dbname": "tpch",
    "user": "leshanchen",
    "password": "",
    "host": "localhost",
    "port": "5432"
}
schema = get_user_tables(DB_CONFIG)

def main():
    args = parse_args()
    parsed_data = parse_results_file('results.txt')
     
    if args.end == -1:
        objects = parsed_data[args.start:]
    else:
        objects = parsed_data[args.start:args.end + 1]
    
    for obj in objects:
        derived_queries = [obj[1]]
        failed_queries = []
        fail_streak = 0
        for j in range(20):
            attempt = 0
            success = False
            while attempt < 5 and not success:
                attempt += 1
                prompt = get_prompt(schema, failed_queries, derived_queries)
                response = query_LLM(prompt)
                success = execute_sql_commands("tpch", ["EXPLAIN " + response])
                sim = 0
                for d_query in derived_queries:
                    tsim = SequenceMatcher(None, response.lower(), d_query.lower()).ratio()
                    print(tsim)
                    sim += tsim
                    if tsim == 1:
                        success = False
                        break
                print(f"derived attempt: {response}")
                print(derived_queries)
                sim /= len(derived_queries)
                print(sim)
                print(f"cycle: {j}, attempt: {attempt}, fail streak: {fail_streak}")
                if sim > 0.992:
                    success = False
                if not success:
                    failed_queries.append(response)
                else:
                    print(f"Derived query: {response}")
                    derived_queries.append(response)
            if not success:
                fail_streak += 1
                if fail_streak >= 5:
                    break
            else:
                fail_streak = 0
        derived_queries = filter_derived(derived_queries, obj)
        if len(derived_queries) > 0:
            derived_file = f"derived_{args.start}_{args.end}.txt"
            with open(derived_file, "a") as w:
                for query in derived_queries:
                    w.write(str(obj[0]) + "\n")
                    w.write(str(query) + "\n")


if __name__ == "__main__":
    main()
