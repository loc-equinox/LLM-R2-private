from itertools import permutations
from difflib import SequenceMatcher
from rewriter import call_rewriter

# a = "tpch"
# from all_sequences import *
def test_all_sequences(db_id, query, rule_list):
    log = ""
    print("testing_all_sequences...")
    log += query + "\n"
    # If the final results for both rewrite orders are
    # identical, then the query is not valid
    final_results = []
    for cur_list in permutations(rule_list):
        print(cur_list)
        log += str(cur_list) + "\n"
        rewrite_result = call_rewriter(db_id, query, cur_list)
        intermediate_results = rewrite_result.split(';')
        num = len(rule_list)
        # The similarity index between different versions of
        # the query, ranging from 0 to 1, 1 means identical.
        # This can be used to see whether a rule caused any
        # changes to the query.
        similarity = []
        similarity.append(SequenceMatcher(None, query,
                                          intermediate_results[0]).ratio())
        for i in range(num - 1):
            q1 = intermediate_results[i]
            q2 = intermediate_results[i + 1]
            similarity.append(SequenceMatcher(None, q1, q2).ratio())
        print(similarity)
        log += str(intermediate_results) + "\n\n"
        final_results.append(intermediate_results[-1])

    # print("final results:")
    # print(final_results)
    if "apache" in log:
        return ""
    if final_results[0] == final_results[1]:
        return log
    return "success"

# test = test_all_sequences
