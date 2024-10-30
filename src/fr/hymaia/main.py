import sys
import time
import platform
from importlib import import_module
from src.fr.hymaia.utils.args_helper import parse_args
import psutil
from tabulate import tabulate

# List of jobs to benchmark
jobs = [
    "src.fr.hymaia.exo4.no_udf",
    "src.fr.hymaia.exo4.python_udf",
    "src.fr.hymaia.exo4.python_udf2",
    "src.fr.hymaia.exo4.scala_udf"
]

def print_system_info():
    # Print system information
    print("=== System Information ===")
    print(f"CPU Count (Logical): {psutil.cpu_count(logical=True)}")
    print(f"CPU Count (Physical): {psutil.cpu_count(logical=False)}")
    print(f"CPU Frequency: {psutil.cpu_freq().current} MHz")
    print(f"Total RAM: {psutil.virtual_memory().total / (1024 ** 3):.2f} GB")
    print(f"Available RAM: {psutil.virtual_memory().available / (1024 ** 3):.2f} GB")
    print(f"Used RAM: {psutil.virtual_memory().used / (1024 ** 3):.2f} GB")
    print(f"Memory Usage: {psutil.virtual_memory().percent}%")
    print("==========================")

def get_system_architecture():
    # Get system architecture information
    architecture = platform.architecture()
    machine = platform.machine()
    processor = platform.processor()  # Get processor type
    return f"{architecture[0]}, {machine}, {processor}"

def benchmark_job(job_module_name, job_args, runs):
    # Dynamically import the module once
    job_module = import_module(job_module_name)
    job_main_function = getattr(job_module, "main")

    total_time = 0
    for i in range(runs):
        start_time = time.time()
        job_main_function(**job_args)
        end_time = time.time()
        execution_time = end_time - start_time
        # print(f"Run {i + 1} for {job_module_name} executed in {execution_time:.2f} seconds")
        
        if i > 0:  # Ignore the first run for averaging
            total_time += execution_time

    average_time = total_time / (runs - 1)
    return average_time

def main():
    args_dict = parse_args(sys.argv[1:])
    runs = int(args_dict.pop("runs", 1))
    print_system_info()

    # Collect benchmark results
    results = []
    for job in jobs:
        print(f"\nRunning {job}...")
        average_time = benchmark_job(job, args_dict, runs)
        architecture = get_system_architecture()
        physical_cores = psutil.cpu_count(logical=False)
        total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)  # Convert MB to GB
        results.append([job, average_time, runs, architecture, physical_cores, total_ram_gb])

    # Print results in a tabulated format
    headers = ["Job", "Average Execution Time (s)", "Number of Runs", "Architecture", "Physical Cores", "Total RAM (GB)"]
    print(tabulate(results, headers=headers, tablefmt="pretty"))

if __name__ == '__main__':
    main()
