import os
import json
import time
import threading
from typing import List, Dict, Any
from dbms.postgres import PgDBMS
from optimizer.workload_runner import BenchbaseRunner
import pandas as pd
from pandas.errors import EmptyDataError

benchmark_copy_db = ['tpcc', 'twitter', "sibench", "voter", "tatp", "smallbank", "seats"]   # Some benchmark will insert or delete data, Need to be rewrite each time.
benchmark_latency = ['tpch']

def run_baseline(workload, timeout=100):
    input_file = 'src/baseline/postgres13_143knobs_6250samples_constrained.json'
    output_file = f'src/baseline/postgres13_143knobs_6250samples_constrained_{workload}.json'

    dbms = PgDBMS.from_file('src/dbms/configs/postgres.ini')
    source_db = f"{dbms.db}_template"
    target_db = dbms.db
    if workload in benchmark_copy_db:
        if not dbms.check_template(source_db): # if the template of dbms.db does not exist, create one 
            dbms.create_template(dbms.db)

    results: Dict[Dict[str, Any]] = {}
    if os.path.exists(output_file):
        results = json.load(open(output_file, 'r'))

    configs = json.load(open(input_file, 'r'))

    try:
        for config_id, config in configs.items():
            if config_id in results:
                continue
            print(f"\n>>>>>>>Run {len(results)}/{len(configs)}\n")

            print(f"--- Restore the dbms to default configuration ---")
            dbms.reset_config() 
            dbms.reconfigure()
            # reload the data
            if workload in benchmark_copy_db:
                print("Reloading the data")
                dbms._disconnect()
                dbms._connect(source_db)
                dbms.copy_db(target_db=target_db, source_db=source_db)
                print("Reloading completed")
                time.sleep(12)
                dbms._disconnect()
                time.sleep(4)
                dbms._connect(dbms.db)
                time.sleep(3)

            performance = set_config_and_run(dbms=dbms, config=config, workload=workload, timeout=timeout)
            results[config_id] = {
                'config': config,
                'performance': performance
            }
    finally:
        json.dump(results, open(output_file, 'w'), indent=4)
        print(f"Finish {len(results)}/{len(configs)} storing in `{output_file}`")

def set_config_and_run(dbms:PgDBMS, config:pd.Series, workload: str, timeout: int):

    print(f"--- knob setting procedure ---")
    for knob, value in config.items():
        if knob == 'config_id':
            continue

        if not dbms.set_knob(knob, value):
            print(f"Config {round}: knob {knob} is failed to set to {value}")
        
    dbms.reconfigure()
    if dbms.failed_times == 4:
        return -1
    try:
        print("Begin to run benchbase...")
        runner = BenchbaseRunner(dbms=dbms, test=workload, 
                target_path='./workload_running_results/postgres/temp_results',
                config_path=f'./src/optimizer/configs/postgres/{workload}_config.xml'
                )
        runner.clear_summary_dir()
        t = threading.Thread(target=runner.run_benchmark)
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            print("Benchmark is still running. Terminate it now.")
            runner.process.terminate()
            time.sleep(2)
            raise RuntimeError("Benchmark is still running. Terminate it now.") 
        else:
            print("Benchmark has finished.")
            if runner.check_sequence_in_file():  ### 如果query出错
                raise RuntimeError("ERROR in Query.") 
            throughput, average_latency = runner.get_throughput(), runner.get_latency()

            if workload not in benchmark_latency:
                return throughput
            if workload in benchmark_latency:
                return average_latency

    except Exception as e:
        print(f'Exception for {workload}: {e}')
        # update worst_perf
        return -1
    
if __name__ == '__main__':
    workload = 'tpcc'
    timeout = 100
    run_baseline(workload, timeout)