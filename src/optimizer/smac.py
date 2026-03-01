import os
import sys
import json
import time
import threading
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union
from smac import HyperparameterOptimizationFacade, Scenario, initial_design
from smac.runhistory.dataclasses import TrialValue, TrialInfo
from optimizer.topo_latin_hypercube_design import TopoLatinHypercubeInitialDesign
from optimizer.workload_runner import BenchbaseRunner
from search_space.knowledge_based_space import unify_unit
from rules.check import is_satisfied
from search_space.soft_forbidden import CUSTOM_ENCODERS

class VanillaBO():

    def __init__(self, dbms, workload, search_space, timeout, seed, output_folder, config_path, knob_info):
        self.dbms = dbms
        self.benchmark_copy_db = ['tpcc', 'twitter', "sibench", "voter", "tatp", "smallbank", "seats"]   # Some benchmark will insert or delete data, Need to be rewrite each time.
        self.benchmark_latency = ['tpch']    
        self.round = 0
        self.seed = seed
        self.timeout = timeout
        self.workload = workload
        self.summary_path = f'./workload_running_results/{dbms.name}/temp_results'
        self.config_path = config_path
        self.search_space = search_space
        self.hallucinated_knobs = {} # default empty dict
        self.knob_info = knob_info
        self.output_folder = output_folder

        self.original_config_file = os.path.join(output_folder, f"{self.seed}/original_configs.json")
        self.applied_config_file = os.path.join(output_folder, f"{self.seed}/applied_configs.json")
        self.feasible_config_file = os.path.join(output_folder, f"{self.seed}/feasible_configs.json")
        self.overhead_file = os.path.join(output_folder, f"{self.seed}/overhead.txt")
        if not os.path.exists(os.path.dirname(self.overhead_file)):
            os.makedirs(os.path.dirname(self.overhead_file), exist_ok=True)

        self.init_overhead_file()
        if workload in self.benchmark_copy_db:
            if not dbms.check_template(f"{dbms.db}_template"): # if the template of dbms.db does not exist, create one 
                dbms.create_template(dbms.db)

        self.penalty = self.get_default_result()
        # self.penalty = 1000
        print(f"default penalty: {self.penalty}")
        # exit()

    
    def set_hallucinated_knobs(self, hallucinated_knobs):
        self.hallucinated_knobs = hallucinated_knobs

    def set_constraints(self, constraints):
        self.constraints = constraints

    def optimize(self, name, trials_number, initial_config_number, topo_sampler=None):
        """
        Docstring for optimize
        
        :param self: Description
        :param name: Description
        :param trials_number: Description
        :param initial_config_number: Description
        :param topo_sampling: Description
        :param constraints: Active when topo_sampling is True. List[Tuple(str, str, str)] # (left, op, right)
        :param conditional_activations: Active when topo_sampling is True. 
        """
        scenario = Scenario(
            configspace=self.search_space,
            name=name,
            seed=self.seed,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,
        )
        
        if topo_sampler:
            print("Using TopoLatinHypercubeInitialDesign.")
            init_design = TopoLatinHypercubeInitialDesign(
                scenario=scenario,
                topo_sampler=topo_sampler,
                n_configs=initial_config_number,
                max_ratio=0.8,
                seed=self.seed,
            )
        else:
            print("Using LatinHypercubeInitialDesign.")
            init_design = initial_design.LatinHypercubeInitialDesign(
                scenario,
                n_configs=initial_config_number,
                max_ratio=0.8,  # set this to a value close to 1 to get exact initial_configs as specified
                seed=self.seed
            )

        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=init_design,
            target_function=self.set_and_replay,
            overwrite=False,
        )
        
        smac.optimize()

    def optimize_feeding_initials(self, name, trials_number: int, initial_configurations: List[Dict[str, Any]], costs: List[float]):
        """
        Docstring for optimize
        
        :param self: Description
        :param name: Description
        :param trials_number: Description
        :param initial_configurations: Description
        :param costs: Description
        """
        scenario = Scenario(
            configspace=self.search_space,
            name=name,
            seed=self.seed,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,
        )

        # Modify the initial design to use our custom initial design
        initial_design=HyperparameterOptimizationFacade.get_initial_design(
            scenario, 
            n_configs=0,  # Do not use the default initial design
            additional_configs=initial_configurations  # Use the configurations previously evaluated as initial design
                                            # This only passes the configurations but not the cost!
        )
    
        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=initial_design,
            target_function=self.set_check_and_replay,
            overwrite=False,
        )

        if costs is not None:
            # Convert previously evaluated configurations into TrialInfo and TrialValue instances to pass to SMAC
            trial_infos = [TrialInfo(config=c, seed=self.seed) for c in initial_configurations]
            trial_values = [TrialValue(cost=c) for c in costs]

            # Warmstart SMAC with the trial information and values
            for info, value in zip(trial_infos, trial_values):
                smac.tell(info, value)
        
        smac.optimize()

    def optimize_feeding_initials_with_costs(self, name, trials_number: int, initial_configurations: List[Dict[str, Any]], costs: List[float]):
        """
        Docstring for optimize
        
        :param self: Description
        :param name: Description
        :param trials_number: Description
        :param initial_configurations: Description
        :param costs: Description
        """
        scenario = Scenario(
            configspace=self.search_space,
            name=name,
            seed=self.seed,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,
        )

        # Modify the initial design to use our custom initial design
        initial_design=HyperparameterOptimizationFacade.get_initial_design(
            scenario, 
            n_configs=0,  # Do not use the default initial design
            additional_configs=initial_configurations  # Use the configurations previously evaluated as initial design
                                            # This only passes the configurations but not the cost!
        )
    
        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=initial_design,
            target_function=self.set_and_replay, # tuning process with normal random optimization
            overwrite=False,
        )

        if costs is not None:
            # Convert previously evaluated configurations into TrialInfo and TrialValue instances to pass to SMAC
            trial_infos = [TrialInfo(config=c, seed=self.seed) for c in initial_configurations]
            trial_values = [TrialValue(cost=c) for c in costs]

            # Warmstart SMAC with the trial information and values
            for info, value in zip(trial_infos, trial_values):
                smac.tell(info, value, save=True)
        
        smac.optimize()

    def convert_config(self, config):
        convert_config = {}
        for knob, value in config.items():
            if knob == "max_prepared_transactions_mode":
                continue
            unit = self.knob_info[knob].get('unit', None)

            convert_value = unify_unit(value, 'kB', unit)
            convert_config[knob] = convert_value
        return convert_config

    
    def set_and_replay(self, config, seed=0):
        self._log_original_config(dict(config))
        config = self.convert_config(config=config)
        begin_time = time.time()
        cost = self.set_and_replay_ori(config, seed)
        end_time = time.time()
        self._log_overhead(begin_time, end_time)
        return cost

    def set_check_and_replay(self, config, seed=0):
        self._log_original_config(dict(config))
        begin_time = time.time()
        satisfied, _ = is_satisfied(config, rule_list=self.constraints)  # TODO: add rule_list
        end_time = time.time()
        if not satisfied:
            self.round += 1
            print(f"Tuning round {self.round} ...")
            print("Configuration violates constraints, assign penalty cost.")
            self._log_overhead(begin_time, end_time)
            self._log_feasible_config(False)
            if self.workload not in self.benchmark_latency:
                return -int(self.penalty) / 2
            else:
                return self.penalty * 2
        else:
            config = self.convert_config(config=config)
            begin_time = time.time()
            cost = self.set_and_replay_ori(config, seed)
            end_time = time.time()
            self._log_overhead(begin_time, end_time)
            return cost

    def set_and_replay_ori(self, config, seed=0):
        self.round += 1
        print(f"Tuning round {self.round} ...")
        dbms = self.dbms
        print(f"--- Restore the dbms to default configuration ---")
        dbms.reset_config()
        dbms.reconfigure()
        # reload the data
        if self.workload in self.benchmark_copy_db:
            print("Reloading the data")
            dbms._disconnect()
            dbms._connect(f"{dbms.db}_template")
            dbms.copy_db(target_db=dbms.db, source_db=f"{dbms.db}_template")
            print("Reloading completed")
            time.sleep(12)
            dbms._disconnect()
            time.sleep(4)
            dbms._connect(dbms.db)
            time.sleep(3)

        print(f"--- knob setting procedure ---")
        applied_config = {}
        for knob, value in config.items():
            if knob == 'config_id':
                continue
            
            if knob == 'max_prepared_transactions_mode':
                if value == 'disabled':
                    acutal_value = dbms.set_knob('max_prepared_transactions', 0)
                    applied_config[knob] = acutal_value
                continue

            if knob is self.hallucinated_knobs:
                applied_config[knob] = 'HALLU'
                continue # if knobs are hallucinations, skipped 

            acutal_value = dbms.set_knob(knob, value)
            if acutal_value != value:
                print(f"Config {round}: knob {knob} is failed to set to {value}. It sets to {acutal_value}")
            applied_config[knob] = acutal_value
        
        dbms.reconfigure()
        self._log_applied_config(applied_config)
        if self.workload not in self.benchmark_latency:
            if dbms.failed_times == 4:
                self._log_feasible_config(False)
                return -int(self.penalty) / 2
        else:
            if dbms.failed_times == 4:
                self._log_feasible_config(False)
                return self.penalty * 2
            
        try:
            print("Begin to run benchbase...")
            runner = BenchbaseRunner(dbms=dbms, test=self.workload, 
                                     target_path=self.summary_path,
                                     config_path=self.config_path)
            
            runner.clear_summary_dir()
            t = threading.Thread(target=runner.run_benchmark)
            t.start()
            t.join(timeout=self.timeout)
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

                if self.workload not in self.benchmark_latency and throughput < self.penalty:
                    self.penalty = throughput
                if self.workload in self.benchmark_latency and average_latency > self.penalty:
                    self.penalty = average_latency
            
        except Exception as e:
            print(f'Exception for {self.workload}: {e}')
            self._log_feasible_config(False)
            # update worst_perf
            if self.workload not in self.benchmark_latency:
                return -int(self.penalty) / 2
                ###tpch
            else:
                return self.penalty * 2
    
        self._log_feasible_config(True)
        if self.workload not in self.benchmark_latency:
            return -throughput
        else:
            return average_latency
    

    def get_default_result(self):
        print("Test the result in default conf")
        dbms = self.dbms
        print(f"--- Restore the dbms to default configuration ---")
        dbms.reset_config()
        dbms.reconfigure()

        try:
            if self.workload in self.benchmark_copy_db:
            # reload the data
                print("Reloading the data")
                dbms._disconnect()
                dbms._connect(f"{dbms.db}_template")
                dbms.copy_db(target_db=dbms.db, source_db=f"{dbms.db}_template")
                print("Reloading completed")
                time.sleep(12)
                dbms._disconnect()
                time.sleep(4)
                dbms._connect(dbms.db)
                time.sleep(3)
                
            print("Begin to run benchbase...")
            runner = BenchbaseRunner(dbms=dbms, test=self.workload, target_path=self.summary_path,
                                     config_path=self.config_path)
            runner.clear_summary_dir()
            t = threading.Thread(target=runner.run_benchmark)
            t.start()
            t.join()
            throughput, average_latency = runner.get_throughput(), runner.get_latency()
        except Exception as e:
            print(f'Exception for {self.workload}: {e}')
            exit()

        if self.workload not in self.benchmark_latency:
            return throughput
        else:
            return average_latency

    def init_overhead_file(self):
        with open(self.overhead_file, 'w') as file:
            file.write(f"Round\tStart\tEnd\tBenchmark_Elapsed\tTuning_overhead\n")

    def _log_overhead(self, begin_time, end_time):
        if self.round == 1:
            self.prev_end = begin_time
        with open(self.overhead_file, 'a') as file:
            file.write(f"{self.round}\t{begin_time}\t{end_time}\t{end_time-begin_time}\t{begin_time-self.prev_end}\n")
        self.prev_end = end_time

    def _log_original_config(self, config):
        if os.path.exists(self.original_config_file):
            all_original_config = json.load(open(self.original_config_file, 'r'))
            all_original_config[f"{self.round}"] = config
            json.dump(all_original_config, open(self.original_config_file, 'w'), indent=4)
        else:
            with open(self.original_config_file, 'w') as f:
                json.dump({
                    f"{self.round}": config
                }, f, indent=4)

    def _log_applied_config(self, config):
        if os.path.exists(self.applied_config_file):
            all_applied_config = json.load(open(self.applied_config_file, 'r'))
            all_applied_config[f"{self.round}"] = config
            json.dump(all_applied_config, open(self.applied_config_file, 'w'), indent=4)
        else:
            with open(self.applied_config_file, 'w') as f:
                json.dump({
                    f"{self.round}": config
                }, f, indent=4)
    
    def _log_feasible_config(self, feasible:bool):
        if os.path.exists(self.feasible_config_file):
            all_feasible_config = json.load(open(self.feasible_config_file, 'r'))
            all_feasible_config[f"{self.round}"] = feasible
            json.dump(all_feasible_config, open(self.feasible_config_file, 'w'), indent=4)
        else:
            with open(self.feasible_config_file, 'w') as f:
                json.dump({
                    f"{self.round}": feasible
                }, f, indent=4)