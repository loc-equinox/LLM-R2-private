import time
from itertools import chain
import subprocess
from typing import List
from gen_order_sensitive_queries import aggregate_rewrite_rules, \
    filter_rewrite_rules, join_rewrite_rules, sort_rewrite_rules, \
    union_rewrite_rules, run_command_with_realtime_output

def generate(rule_seq):
    # To construct the rule_sequence string, separate the
    # rewrite rules with semicolons
    cmd = \
        [
            "python3", "-u", "explore_simple_rule_seq.py",
            "-r", rule_seq,
        ]

    if not run_command_with_realtime_output(cmd):
        print("\n\033[91mExploration failed!\033[0m")

def pair_run(r1, r2):
    for u in r1:
        for a in r2:
            rule_seq = u + ";" + a
            generate(rule_seq)
            rule_seq = a + ";" + u
            generate(rule_seq)
            

def main():
    '''
    for u in filter_rewrite_rules:
        for a in aggregate_rewrite_rules:
            rule_seq = u + ";" + a
            generate(rule_seq)
            rule_seq = a + ";" + u
            generate(rule_seq)
    '''
    pair_run(filter_rewrite_rules, aggregate_rewrite_rules)
    pair_run(sort_rewrite_rules, aggregate_rewrite_rules)


if __name__ == "__main__":
    main()
