import time
from itertools import chain
import subprocess
from typing import List
from gen_order_sensitive_queries import aggregate_rewrite_rules, \
    filter_rewrite_rules, join_rewrite_rules, sort_rewrite_rules, \
    union_rewrite_rules, run_command_with_realtime_output

def generate():
    # To construct the rule_sequence string, separate the
    # rewrite rules with semicolons
    rule_seq = filter_rewrite_rules[3] + ";" + sort_rewrite_rules[1]
    cmd = \
        [
            "python3", "-u", "explore_simple_rule_seq.py",
            "-r", rule_seq,
        ]

    if not run_command_with_realtime_output(cmd):
        print("\n\033[91mExploration failed!\033[0m")

def main():
    generate()


if __name__ == "__main__":
    main()
