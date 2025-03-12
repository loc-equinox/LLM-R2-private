import ast
from rewriter import *

import csv

# Open the input CSV file
with open('tpch_rewrites.csv', 'r', encoding='utf-8') as input_file:
    reader = csv.reader(input_file)
    # Skip the header if there is one
    header = next(reader, None)

    # Open the output CSV file
    with open('intermediate.csv', 'w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        # Write the new header
        writer.writerow(['id', 'db_id', 'sql_input', 'rule_input', 'intermediate_queries'])

        # Iterate through each row in the input file
        for row in reader:
            id_value = row[0]
            db_id = row[1]
            sql_input = row[2]
            rule_input = row[4]
            rule_list = ast.literal_eval(rule_input)
            intermediate_queries = call_rewriter(db_id, sql_input, rule_list)

            # Write the selected columns to the output file
            writer.writerow([id_value, db_id, sql_input, rule_input, intermediate_queries])
