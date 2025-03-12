import ast
import psycopg2
from difflib import SequenceMatcher
from rewriter import *

db_params = {
    "database": "tpch",
    "user": "leshanchen",
    "port": 5432,
    "host": "127.0.0.1",
    "password": ""
}

def get_query_plan(query, connection):
    if not query.endswith(';'):
        query = query + ';'
    try:
        with connection.cursor() as cursor:
            cursor.execute("EXPLAIN " + query)
            plan = cursor.fetchall()
            plan_text = "\n".join([" ".join(row) for row in plan])
            return plan_text
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while getting query plan for query: {query}. Error: {error}")
        return None

def get_key_rule(sql_input, rules):
    """
    Returns the key rule among the list of rewrite rules
    that causes the most dramatic change in query plan
    when applied.
    """
    # Obtain intermediate queries
    # For some reasons the rewritten queries contain '$'
    # symbols which causes syntax error when obtaining
    # the query plans, so we remove them.
    rule_list = ast.literal_eval(rules)
    queries_str = call_rewriter("tpch", sql_input, rule_list)
    queries_str = queries_str.replace("$", "")  # '$' removal
    intermediate_queries = queries_str.split(";")
    intermediate_queries[-1] += ";"
    

    # Obtain intermediate plans
    query_plans = []
    try:
        connection = psycopg2.connect(**db_params)
        print("Connection successful!")
        query_plans.append(get_query_plan(sql_input, connection))
        for query in intermediate_queries:
            query_plans.append(get_query_plan(query, connection))
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while connecting to the database or processing queries. Error: {error}")
    finally:
        if connection:
            connection.close()

    # Compute the similarity between 'adjacent' plans
    similarity = []
    for i in range(0, len(query_plans) - 1):
        plan1 = query_plans[i]
        plan2 = query_plans[i + 1]
        similarity.append(SequenceMatcher(None, plan1, plan2).ratio())

    # Find the rule that causes the most dramatic change in query plan
    index_of_key = similarity.index(min(similarity))
    return rule_list[index_of_key]
# sql = "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'gainsboro%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'MOROCCO' order by s_name;"
# rules = "['FILTER_INTO_JOIN', 'FILTER_REDUCE_EXPRESSIONS']"
# print(get_key_rule(sql, rules))
