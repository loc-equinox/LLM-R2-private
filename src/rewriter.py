# @TIME : 24/5/23 4:08 PM
# @AUTHOR : LZDH
import subprocess
import json
import numpy as np


def call_rewriter(db_id, sql_input, rule_input):
    # 存储最终结果的列表
    final_outputs = []

    # 逐步应用规则
    if len(rule_input) > 20:
        return ""
    for i in range(len(rule_input)):
        # 构建当前规则子集
        current_rules = rule_input[:i + 1]

        # Provide a list of strings as input
        input_list = [db_id, sql_input, current_rules]

        # Convert the input list to a JSON string
        input_string = json.dumps(input_list)
        command = 'java -cp rewriter_java.jar src/rule_rewriter.java'

        process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        # process.stdin.write(.encode())
        # Wait for the subprocess to finish and capture the output
        output, error = process.communicate(input=input_string)

        # Print the output and error messages
        # print("Output:\n", output)
        # print("Error:\n", error)
        output = output.replace("\u001B[32m", '').replace("\u001B[0m", '').split('\n')
        ind = 0
        for line in output:
            if not line.startswith('SELECT') and not line.startswith('select') and not line.startswith('with '):
                pass
            else:
                ind = output.index(line)
                break
        queries = output[ind + 1:-3]
        # print(' '.join(queries))
        output = ' '.join(queries).replace('"', '')
        if 'select' in output or 'SELECT' in output or 'Select' in output:
            # change the functions edited to fit calcite back to original ones
            output = output.replace('SUBSTRING', 'SUBSTR')
            output = output.replace('$', '')
            final_outputs.append(output)
        else:
            print(db_id)
            print(sql_input)
            print("Output:\n", output)
            print("Error:\n", error)
            with open('error_logs_gpt_rewrite.txt', 'a+') as f:
                f.write(sql_input)
                f.write('\n')
                f.write(error)
                f.write('\n')
                f.write('\n')
                f.write('\n')
                f.close()
            final_outputs.append('NA')

    # 将所有结果拼接成一个字符串
    final_result = ';'.join(final_outputs)
    return final_result
