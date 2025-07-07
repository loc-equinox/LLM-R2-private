import time
from itertools import chain
import subprocess
from typing import List

aggregate_rewrite_rules = ["[AGGREGATE_EXPAND_DISTINCT_AGGREGATES: Rule that expands distinct aggregates (such as COUNT(DISTINCT x)) from a Aggregate]", "[AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN: As AGGREGATE_EXPAND_DISTINCT_AGGREGATES but generates a Join]", "[AGGREGATE_JOIN_TRANSPOSE_EXTENDED: As AGGREGATE_JOIN_TRANSPOSE, but extended to push down aggregate functions]", "[AGGREGATE_PROJECT_MERGE: Rule that recognizes an Aggregate on top of a Project and if possible aggregates through the Project or removes the Project]", "[AGGREGATE_ANY_PULL_UP_CONSTANTS: More general form of AGGREGATE_PROJECT_PULL_UP_CONSTANTS that matches any relational expression]", "[AGGREGATE_UNION_AGGREGATE: Rule that matches an Aggregate whose input is a Union one of whose inputs is an Aggregate]", "[AGGREGATE_UNION_TRANSPOSE: Rule that pushes an Aggregate past a non-distinct Union]", "[AGGREGATE_VALUES: Rule that applies an Aggregate to a Values (currently just an empty Values)]", "[AGGREGATE_REMOVE: Rule that removes an Aggregate if it computes no aggregate functions (that is, it is implementing SELECT DISTINCT), or all the aggregate functions are splittable, and the underlying relational expression is already distinct]"]
filter_rewrite_rules = ["[FILTER_AGGREGATE_TRANSPOSE: Rule that pushes a Filter past an Aggregate]", "[FILTER_CORRELATE: Rule that pushes a Filter above a Correlate into the inputs of the Correlate]", "[FILTER_INTO_JOIN: Rule that tries to push filter expressions into a join condition and into the inputs of the join]", "[JOIN_CONDITION_PUSH: Rule that pushes predicates in a Join into the inputs to the join]", "[FILTER_MERGE: Rule that combines two LogicalFilters]", "[FILTER_MULTI_JOIN_MERGE: Rule that merges a Filter into a MultiJoin, creating a richer MultiJoin]", "[FILTER_PROJECT_TRANSPOSE: The default instance of FilterProjectTransposeRule]", "[FILTER_SET_OP_TRANSPOSE: Rule that pushes a Filter past a SetOp]", "[FILTER_TABLE_FUNCTION_TRANSPOSE: Rule that pushes a LogicalFilter past a LogicalTableFunctionScan]", "[FILTER_SCAN: Rule that matches a Filter on a TableScan]", "[FILTER_REDUCE_EXPRESSIONS: Rule that reduces constants inside a LogicalFilter]", "[PROJECT_REDUCE_EXPRESSIONS: Rule that reduces constants inside a LogicalProject]"]
join_rewrite_rules = ["[JOIN_EXTRACT_FILTER: Rule to convert an inner join to a filter on top of a cartesian inner join: ]", "[JOIN_PROJECT_BOTH_TRANSPOSE: Rule that matches a LogicalJoin whose inputs are LogicalProjects, and pulls the project expressions up]", "[JOIN_PROJECT_LEFT_TRANSPOSE: As JOIN_PROJECT_BOTH_TRANSPOSE but only the left input is a LogicalProject]", "[JOIN_PROJECT_RIGHT_TRANSPOSE: As JOIN_PROJECT_BOTH_TRANSPOSE but only the right input is a LogicalProject]", "[JOIN_LEFT_UNION_TRANSPOSE: Rule that pushes a Join past a non-distinct Union as its left input]", "[JOIN_RIGHT_UNION_TRANSPOSE: Rule that pushes a Join past a non-distinct Union as its right input]", "[SEMI_JOIN_REMOVE: Rule that removes a semi-join from a join tree]", "[JOIN_REDUCE_EXPRESSIONS: Rule that reduces constants inside a Join]"]
sort_rewrite_rules = ["[SORT_JOIN_TRANSPOSE: Rule that pushes a Sort past a Join]", "[SORT_PROJECT_TRANSPOSE: Rule that pushes a Sort past a Project]", "[SORT_UNION_TRANSPOSE: Rule that pushes a Sort past a Union]", "[SORT_REMOVE_CONSTANT_KEYS: Rule that removes keys from a Sort if those keys are known to be constant, or removes the entire Sort if all keys are constant]", "[SORT_REMOVE: Rule that removes a Sort if its input is already sorted]"]
union_rewrite_rules = ["[UNION_MERGE: Rule that flattens a Union on a Union into a single Union]", "[UNION_REMOVE: Rule that removes a Union if it has only one input]", "[UNION_TO_DISTINCT: Rule that translates a distinct Union (all = false) into an Aggregate on top of a non-distinct Union (all = true)]", "[UNION_PULL_UP_CONSTANTS: Rule that pulls up constants through a Union operator]"]


def run_command_with_realtime_output(cmd: List[str]) -> bool:
    """Execute a command with real-time stdout/stderr streaming"""
    print(f"\n\033[1mExecuting: {' '.join(cmd)}\033[0m")  # Bold print for command
    start_time = time.time()

    try:
        # Start the process with pipes for both stdout and stderr
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )

        # Print output in real-time using a generator
        def stream_output(pipe):
            for line in pipe:
                print(line, end='', flush=True)
                yield line

        # Stream both stdout and stderr simultaneously
        for line in chain(stream_output(process.stdout),
                          stream_output(process.stderr)):
            pass

        # Wait for process to complete
        process.wait()

        if process.returncode != 0:
            print(f"\n\033[91mCommand failed with exit code {process.returncode}\033[0m")
            return False

        print(f"\n\033[92mCompleted in {time.time() - start_time:.2f} seconds\033[0m")
        return True

    except Exception as e:
        print(f"\n\033[91mError executing command: {e}\033[0m")
        return False

def generate():
    cmd = \
        [
            "python3", "-u", "explore_rule_pair.py",
            "-r1", "[AGGREGATE_UNION_TRANSPOSE: Rule that pushes an Aggregate past a non-distinct Union]",
            "-r2", "[UNION_TO_DISTINCT: Rule that translates a distinct Union (all = false) into an Aggregate on top of a non-distinct Union]"
        ]

    # To test another rule pair, modify cmd[4] and cmd[6]
    cmd[4] = sort_rewrite_rules[2]
    cmd[6] = union_rewrite_rules[3]

    if not run_command_with_realtime_output(cmd):
        print("\n\033[91mExploration failed!\033[0m")

def main():
    print("agg", len(aggregate_rewrite_rules))
    print("fil", len(filter_rewrite_rules))
    print("so", len(sort_rewrite_rules))
    print("uni", len(union_rewrite_rules))
    print("join", len(join_rewrite_rules))


if __name__ == "__main__":
    main()
