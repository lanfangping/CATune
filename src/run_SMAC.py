
import os
import time
import json
import yaml
import argparse
from search_space.knowledge_based_space import build_search_space, attach_sampler_to_configspace
from optimizer.smac import VanillaBO
from dbms.postgres import PgDBMS
from sampler.topological_sampler import TopoSampler
from rules.rule_lists import rule_v1, rule_v2, rule_v3, rule_v4, rule_v5

import ConfigSpace as CS
import ConfigSpace.hyperparameters as CSH
import ConfigSpace.conditions as CSC
import ConfigSpace.forbidden as CSF

def add_condition_for_max_prepared_transactions_and_max_connections(cs):
        # 1) Max connections as usual
        # hp_max_connections = CSH.UniformIntegerHyperparameter(
        #     "max_connections", lower=10, upper=1000
        # )

        # 2) A mode flag: disabled vs enabled
        hp_mpt_mode = CSH.CategoricalHyperparameter(
            "max_prepared_transactions_mode", choices=["disabled", "enabled"]
        )

        # 3) The actual value, only used when enabled (no 0 here!)
        max_prepared_transactions = cs['max_prepared_transactions']
        max_prepared_transactions.lower = 1
        max_prepared_transactions.default_value = 1
        # cs.remove_hyperparameter('max_prepared_transactions')
        # hp_mpt_value = CSH.UniformIntegerHyperparameter(
        #     "max_prepared_transactions", lower=1, upper=262143
        # )

        cs.add(hp_mpt_mode)

        # 4) Activate mpt_value only if mode == "enabled"
        cs.add(
            CS.EqualsCondition(max_prepared_transactions, hp_mpt_mode, "enabled")
        )

        # 5) When enabled, enforce: max_prepared_transactions_value >= max_connections
        cs.add(
            CSF.ForbiddenLessThanRelation(max_prepared_transactions, cs['max_connections'])
        )
        return cs

def setup_rules(cs:CS, rule_list, all_knob_info, imply_bound=False):
    """
    Docstring for setup_rules
    
    :param search_space: Description
    :param rule_list: Description

    ( "max_wal_senders", "<", "max_connections" ),
    ( "superuser_reserved_connections", "<", "max_connections" ),
    ( "shared_buffers", ">", "max_wal_size" ),
    ( "max_prepared_transactions", ">=", "max_connections" ), # condition  max_prepared_transactions != 0, O disabled prepared transactions

    ( "max_parallel_workers_per_gather", "<=", "max_parallel_workers" ),
    ( "max_parallel_workers_per_gather", "<", "max_worker_processes" ),
    ( "max_parallel_maintenance_workers", "<", "max_parallel_workers" ),
    ( "max_parallel_workers", "<=", "max_worker_processes" ),
    ( "max_logical_replication_workers", "<", "max_worker_processes" ),
    ( "max_sync_workers_per_subscription", "<=", "max_logical_replication_workers" ),

    ( "from_collapse_limit", ">=", "geqo_threshold" ),
    ( "join_collapse_limit", ">=", "geqo_threshold" ),

    ( "vacuum_freeze_table_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_freeze_min_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_multixact_freeze_table_age", "<", "autovacuum_multixact_freeze_max_age" ),
    ( "vacuum_multixact_freeze_min_age", "<", "autovacuum_multixact_freeze_max_age" )
    """
    def get_constant(knob):
        knob_type = all_knob_info[knob]['type']
        if knob_type in ['integer', 'float']:
            return CSH.Constant(knob, eval(all_knob_info[knob]['default']))
        else:
            return CSH.Constant(knob, all_knob_info[knob]['default'])
        
    def add_condition_for_max_prepared_transactions_and_max_connections():
        # 1) Max connections as usual
        # hp_max_connections = CSH.UniformIntegerHyperparameter(
        #     "max_connections", lower=10, upper=1000
        # )

        if 'control_max_prepared_transactions' in cs:
            return

        # 2) A mode flag: disabled vs enabled
        hp_mpt_mode = CSH.CategoricalHyperparameter(
            "max_prepared_transactions_mode", choices=["disabled", "enabled"]
        )

        # 3) The actual value, only used when enabled (no 0 here!)
        max_prepared_transactions = cs['max_prepared_transactions']
        # print(max_prepared_transactions)
        if isinstance(max_prepared_transactions, CSH.Constant):
            return
        max_prepared_transactions.lower = 1
        max_prepared_transactions.default_value = 1
        # cs.remove_hyperparameter('max_prepared_transactions')
        # hp_mpt_value = CSH.UniformIntegerHyperparameter(
        #     "max_prepared_transactions", lower=1, upper=262143
        # )

        cs.add(hp_mpt_mode)

        # 4) Activate mpt_value only if mode == "enabled"
        cs.add(
            CS.EqualsCondition(max_prepared_transactions, hp_mpt_mode, "enabled")
        )

        # 5) When enabled, enforce: max_prepared_transactions_value >= max_connections
        cs.add(
            CSF.ForbiddenLessThanRelation(max_prepared_transactions, cs['max_connections'])
        )

    upper_bounds = {}
    def align_ranges_with_constraints(left_knob, operator, right_knob):
        # print(min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper)), min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper)))
        if operator == '<' or operator == '<=':
            if left_knob.upper > min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper)):
                
                upper_bounds[left_knob.name] = min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper))
                upper_bounds[right_knob.name] = min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper))
            else:
                upper_bounds[left_knob.name] = min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper))
                upper_bounds[right_knob.name] = min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper))
        elif operator == '>' or operator == '>=':
            if right_knob.upper > min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper)):
                upper_bounds[right_knob.name] = min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper))
                upper_bounds[left_knob.name] = min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper))
            else:
                upper_bounds[left_knob.name] = min(left_knob.upper, upper_bounds.get(left_knob.name, left_knob.upper))
                upper_bounds[right_knob.name] = min(right_knob.upper, upper_bounds.get(right_knob.name, right_knob.upper))
        

    for rule in rule_list:
        left_opd, opr, right_opd = rule
        # if left_opd == 'shared_buffers':
        #     print(f"Processing rule: {left_opd} {opr} {right_opd}")
        #     print(cs[left_opd])
        #     print(cs[right_opd])
        #     exit()
        if left_opd in cs:
            print(f"{left_opd} in cs")
            left_knob = cs[left_opd]
        else:
            left_knob = get_constant(left_opd)
            cs.add_hyperparameter(left_knob)
            print(cs)
        
        if right_opd in cs:
            print(f"{right_opd} in cs")
            right_knob = cs[right_opd]
        else:
            right_knob = get_constant(right_opd)
            cs.add_hyperparameter(right_knob)
            print(cs)

        if 'max_prepared_transactions' == left_opd and 'max_connections' == right_opd:
            add_condition_for_max_prepared_transactions_and_max_connections()
            # align_ranges_with_constraints(left_knob, opr, right_knob)
            continue

        # align_ranges_with_constraints(left_knob, opr, right_knob)

        if opr == '<':
            cs.add(CSF.ForbiddenGreaterThanRelation(left_knob, right_knob))
            cs.add(CSF.ForbiddenEqualsRelation(left_knob, right_knob))
        elif opr == '<=':
            cs.add(CSF.ForbiddenGreaterThanRelation(left_knob, right_knob))
        elif opr == '>':
            cs.add(CSF.ForbiddenLessThanRelation(left_knob, right_knob))
            cs.add(CSF.ForbiddenEqualsRelation(left_knob, right_knob))
        elif opr == '>=':
            cs.add(CSF.ForbiddenLessThanRelation(left_knob, right_knob))

    # if imply_bound:
    #     for knob, upper_value in upper_bounds.items():
    #         if upper_value < cs[knob].upper:
    #             cs[knob].upper = upper_value 
    return cs


def run_prelim(args):
    if args.config:
        with open(args.config, "r") as f:
            dic = yaml.safe_load(f)
            for k, v in dic.items():
                setattr(args, k, v)
    
    print("Arguments:", args)

    task = args.task
    tag = args.tag
    workload = args.workload
    workload_config_path = args.workload_config_path
    dbms_name = args.dbms_name
    timeout = args.timeout
    seed = args.seed
    knob_info_file_path = args.knob_info_file_path
    rules = args.rules
    resource_bound = args.resource_bound
    imply_bound = args.imply_bound
    topo_sampling = args.topo_sampling
    initial_topo_only = args.initial_topo_only
    trials = args.trials
    initials = args.initials
    sampling_strategy = args.sampling_strategy
    
    output_folder = f'workload_running_results/{dbms_name}/{task}/{workload}/{tag}'
    smac_output_folder = os.path.join(output_folder, f"{seed}") 
    if not os.path.exists(smac_output_folder):
        os.makedirs(smac_output_folder, exist_ok=True)
    
    if os.path.exists(knob_info_file_path):
        knob_info = json.load(open(knob_info_file_path, 'r'))
    else:
        print("Please specify `knob_info_file_path`")
        exit()

    child_beta_alpha=2
    child_beta_beta=1
    setattr(args, 'smac_output_folder', smac_output_folder)
    setattr(args, 'child_beta_alpha', child_beta_alpha)
    setattr(args, 'child_beta_beta', child_beta_beta)
    print(knob_info)
    
    with open(os.path.join(smac_output_folder, f"config.yaml"), "w") as f:
        yaml.dump(vars(args), f, default_flow_style=False, sort_keys=False)

    with open('src/manual/official_document_pg13_all.json') as f:
        manual_info = json.load(f)
        all_knob_info = {}
        for info in manual_info['params']:
            knob = info['name']
            all_knob_info[knob] = info
    
    search_space = build_search_space(knobs_info=knob_info, seed=seed, resource_bound=resource_bound)
    rule_list = None
    conditional_activations = None
    if rules == 'v1':
        rule_list = rule_v1
    elif rules == 'v2':
        rule_list = rule_v2
    elif rules == 'v3':
        rule_list = rule_v3
        conditional_activations = [
            ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
        ]
    elif rules == 'v4':
        rule_list = rule_v4
        conditional_activations = [
            ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
        ]
    elif rules == 'v5':
        rule_list = rule_v5
        conditional_activations = [
            ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
        ]

    if rule_list:
        search_space = setup_rules(search_space, rule_list, all_knob_info, imply_bound)
    print(search_space)

    topo_sampler = None
    if topo_sampling:
        if conditional_activations is None:
            conditional_activations = [] 
            print("`conditional_activations` is empty.")
        if rule_list is None:
            rule_list = []
            print("`rule_list` is empty.")
        topo_sampler = TopoSampler(
            cs=search_space, 
            constraints=rule_list, 
            conditional_activations=conditional_activations, 
            seed=seed,
            strategy=sampling_strategy,
            child_beta_alpha=child_beta_alpha,
            child_beta_beta=child_beta_beta
        )

    if topo_sampler:
        if not initial_topo_only:
            attach_sampler_to_configspace(cs=search_space, sampler=topo_sampler, method_name='sample_configuration')

    # exit()
    dbms = PgDBMS.from_file('src/dbms/configs/postgres.ini')
    dbms.reset_log_config(log_path=smac_output_folder)
    optimizer_vbo = VanillaBO(
        dbms=dbms,
        workload=workload,
        search_space=search_space,
        timeout=timeout,
        seed=seed,
        output_folder=output_folder,  # smac would autmatically create a folder named as seed under output_folder
        config_path=workload_config_path,
        knob_info = knob_info
    )
    
    optimizer_vbo.optimize(
        name=f'../{output_folder}', # smac would autmatically create a folder named as seed under output_folder
        trials_number=trials, 
        initial_config_number=initials,
        topo_sampler=topo_sampler
    )
    src = os.path.join(output_folder, f"{seed}")
    dst = os.path.join(output_folder, f"seed{seed}_trials{trials}_initials{initials}_rules{rules}_topo{topo_sampling}")
        
    if os.path.exists(dst):
        current_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
        print(f"Warning: {dst} exists, rename it to '{dst}_{current_time}'.")
        os.rename(src, f"{dst}_{current_time}")
        print(f"Rename {src} to {dst}_{current_time} successfully.")
    else:
        os.rename(src, dst)
        print(f"Rename {src} to {dst} successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=None) # config file
    parser.add_argument("--task", type=str, default='prelim') # knob_selection
    parser.add_argument("--tag", type=str, default='clean') # clean, hallu, 
    parser.add_argument("--workload", type=str, default='tpcc')
    parser.add_argument("--workload_config_path", type=str, default='src/optimizer/configs/postgres/tpcc_config.xml')
    parser.add_argument("--dbms_name", type=str, default='postgres')
    parser.add_argument("--timeout", type=int, default=100) # timeout for each configuration
    parser.add_argument("--seed", type=int, default=100)
    parser.add_argument("--knob_info_file_path", type=str, default='src/search_space/knob_info/all_rules_related_knob_info.json')
    parser.add_argument("--rules", type=str, default=None, choices=[None, 'v1', 'v2', 'v3', 'v4', 'v5'])
    parser.add_argument("--resource_bound", action='store_true') # resource limit for each configuration
    parser.add_argument("--imply_bound", action='store_true') # align space with constraints
    parser.add_argument("--topo_sampling", action='store_true') # using topological sampling
    parser.add_argument("--initial_topo_only", action='store_true') # only use topological sampling for initial points
    parser.add_argument("--trials", type=int, default=200) # tuning trials
    parser.add_argument("--initials", type=int, default=10) # initial configurations for BO
    parser.add_argument("--sampling_strategy", type=str, default='adaptive', choices=['adaptive', 'uniform']) # initial configurations for BO
    args = parser.parse_args()
    run_prelim(args)