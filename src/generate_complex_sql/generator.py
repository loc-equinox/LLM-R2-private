import subprocess
import argparse
import time
import sys
from typing import List

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
        from itertools import chain
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


def run_pipeline(input_file: str, output_dir: str = ".") -> None:
    """Run the complete SQL processing pipeline with real-time output"""
    commands = [
        # Step 1: Extract deepest subqueries
        [
            "python3", "get_deepest_subquery.py",
            "-i", input_file,
            "-o", f"{output_dir}/deepest_subqueries.csv"
        ],
        # Step 2: Generate complex versions with GPT
        [
            "python3", "-u", "connect_gpt_subquery.py",
            "-i", f"{output_dir}/deepest_subqueries.csv",
            "-o", f"{output_dir}/complex_deepest_subqueries.csv"
        ],
        # Step 3: Replace subqueries in original SQL
        [
            "python3", "replace_deepest_subquery.py",
            "-i", f"{output_dir}/complex_deepest_subqueries.csv",
            "-o", f"{output_dir}/complex_deep_queries.csv"
        ],
        # Step 4: Filter correct SQL queries
        [
            "python3", "filter_correct_sql.py",
            "-i", f"{output_dir}/complex_deep_queries.csv",
            "-o", f"{output_dir}/correct_deep_queries.csv"
        ]
    ]

    for cmd in commands:
        if not run_command_with_realtime_output(cmd):
            print("\n\033[91mPipeline failed! Stopping execution.\033[0m")
            sys.exit(1)

    print("\n\033[1;92mAll processing steps completed successfully!\033[0m")
    print(f"Final output saved to: \033[1m{output_dir}/correct_deep_queries.csv\033[0m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the complete SQL query processing pipeline with real-time output')
    parser.add_argument('-i', '--input', required=True, 
                       help='Input CSV file containing original SQL queries')
    parser.add_argument('-o', '--output-dir', default=".", 
                       help='Output directory for processed files (default: current directory)')

    args = parser.parse_args()

    run_pipeline(args.input, args.output_dir)
