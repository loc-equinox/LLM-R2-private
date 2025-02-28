import csv
import time
import psycopg2


def execute_query_and_get_time(query, connection):
    """
    Execute a single SQL query and return the execution time in seconds.
    """
    start_time = time.time()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    end_time = time.time()
    return end_time - start_time


# Connect to the PostgreSQL database
try:
    connection = psycopg2.connect(
        database="job_syn",
        user="leshanchen",
        password="",
        host="127.0.0.1",
        port="5432"
    )
    print("connection successful!")
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
    raise SystemExit(1)

original_queries_times = []
rewritten_queries_times = []

# Read the input CSV file
with open('/Users/leshanchen/James/CS/lab_work/LLM-R2/job_syn_rewrites.csv', 'r', encoding='utf - 8') as input_file:
    reader = csv.reader(input_file)
    index = 1
    for row in reader:
        # Assume the original query is in the third column (index 2) and the rewritten query is in the fourth column (index 3)
        original_query = row[0]
        rewritten_query = row[1]

        print("group: " + str(index))
        index += 1
        if index <= 218:
            continue

        print("original_time: ")
        original_time = execute_query_and_get_time(original_query, connection)
        print(original_time)
        print("rewritten_time: ")
        rewritten_time = execute_query_and_get_time(rewritten_query, connection)
        print(rewritten_time)

        original_queries_times.append(original_time)
        rewritten_queries_times.append(rewritten_time)


        if len(original_queries_times) % 10 == 0:
            # Write the execution times to a new CSV file in append mode
            with open('/Users/leshanchen/James/CS/lab_work/LLM-R2/LLM-R2/job_syn_execution_time.csv', 'a', encoding='utf-8', newline='') as output_file:
                writer = csv.writer(output_file)
                if len(original_queries_times) == 10:
                    # Write the header only on the first write
                    writer.writerow(['Original Query Execution Time (s)', 'Rewritten Query Execution Time (s)'])
                for i in range(len(original_queries_times) - 10, len(original_queries_times)):
                    writer.writerow([original_queries_times[i], rewritten_queries_times[i]])

# If there are remaining groups less than 10, write them to the file
if len(original_queries_times) % 10 != 0:
    with open('/Users/leshanchen/James/CS/lab_work/LLM-R2/LLM-R2/job_syn_execution_time.csv', 'a', encoding='utf-8', newline='') as output_file:
        writer = csv.writer(output_file)
        if len(original_queries_times) < 10:
            # Write the header only if this is the first write
            writer.writerow(['Original Query Execution Time (s)', 'Rewritten Query Execution Time (s)'])
        for i in range(len(original_queries_times) - (len(original_queries_times) % 10), len(original_queries_times)):
            writer.writerow([original_queries_times[i], rewritten_queries_times[i]])

connection.close()
