
import os
import time
import json
import yaml
import argparse
# from collections import defaultdict
from search_space.knowledge_based_space import build_search_space, attach_sampler_to_configspace
# from optimizer.smac import VanillaBO
from optimizer.smac_multipenalty import MultiPenaltyBO
from dbms.postgres import PgDBMS
from sampler.topological_sampler import TopoSampler
from train_ddpg import rule_v1, rule_v2, rule_v3, rule_v4
from run_SMAC import rule_v5, setup_rules

with open('src/manual/official_document_pg13_all.json') as f:
    manual_info = json.load(f)
    all_knob_info = {}
    for knob_info in manual_info['params']:
        knob = knob_info['name']
        all_knob_info[knob] = knob_info

# soft_rule_list1 = [
#     ( "max_wal_senders", "<", "max_connections" ),
#     ( "shared_buffers", "<=", "max_wal_size" ),
#     ( "max_parallel_workers_per_gather", "<", "max_worker_processes" ),
#     ( "max_logical_replication_workers", "<", "max_worker_processes" ),
#     ( "max_sync_workers_per_subscription", "<=", "max_logical_replication_workers" ),
# ]

# strict_rule_list2 = rule_v5  # rule_v4 = rule_v5 + soft_rule_list

# conditional_activations = [
#             ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
#         ]


strict_rule_list2 = [
    ( "superuser_reserved_connections", "<", "max_connections" )
]

soft_rule_list2 = list(set(rule_v4) - set(strict_rule_list2))

conditional_activations = []

def run_prelim(args):

    soft_rule_list = soft_rule_list2
    strict_rule_list = strict_rule_list2
    if args.config:
        with open(args.config, "r") as f:
            dic = yaml.safe_load(f)
            for k, v in dic.items():
                setattr(args, k, v)

    setattr(args, 'soft_rule_list', "soft_rule_list2")
    setattr(args, 'strict_rule_list', "strict_rule_list2")
    
    print("Arguments:", args)

    task = args.task
    tag = args.tag
    workload = args.workload
    workload_config_path = args.workload_config_path
    dbms_name = args.dbms_name
    timeout = args.timeout
    seed = args.seed
    knob_info_file_path = args.knob_info_file_path
    # rules = args.rules
    resource_bound = args.resource_bound
    imply_bound = args.imply_bound
    topo_sampling = args.topo_sampling
    initial_topo_only = args.initial_topo_only
    trials = args.trials
    initials = args.initials
    
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
    setattr(args, 'child_beta_beta', child_beta_beta)
    print(knob_info)
    
    with open(os.path.join(smac_output_folder, f"config.yaml"), "w") as f:
        yaml.dump(vars(args), f, default_flow_style=False, sort_keys=False)
    
    search_space = build_search_space(knobs_info=knob_info, seed=seed, resource_bound=resource_bound)
    # rule_list = None
    # conditional_activations = None
    # if rules == 'v1':
    #     rule_list = rule_v1
    # elif rules == 'v2':
    #     rule_list = rule_v2
    # elif rules == 'v3':
    #     rule_list = rule_v3
        
    # elif rules == 'v4':
    #     rule_list = rule_v4
    #     conditional_activations = [
    #         ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
    #     ]
    # elif rules == 'v5':
    #     rule_list = rule_v5
    #     conditional_activations = [
    #         ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
    #     ]

    search_space = setup_rules(search_space, strict_rule_list, all_knob_info, imply_bound)
    print(search_space)

    topo_sampler = None
    if topo_sampling:
        # if conditional_activations is None:
        #     conditional_activations = [] 
        #     print("`conditional_activations` is empty.")
        # if rule_list is None:
        #     rule_list = []
        #     print("`rule_list` is empty.")
        topo_sampler = TopoSampler(
            cs=search_space, 
            constraints=strict_rule_list, 
            conditional_activations=conditional_activations, 
            seed=seed,
            child_beta_alpha=child_beta_alpha,
            child_beta_beta=child_beta_beta
        )

    if topo_sampler:
        if not initial_topo_only:
            attach_sampler_to_configspace(cs=search_space, sampler=topo_sampler, method_name='sample_configuration')

    # exit()
    dbms = PgDBMS.from_file('src/dbms/configs/postgres.ini')
    dbms.reset_log_config(log_path=smac_output_folder)
    optimizer_vbo = MultiPenaltyBO(
        constraints=soft_rule_list,
        objectives=["performance", "soft_violation"],
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
    dst = os.path.join(output_folder, f"seed{seed}_trials{trials}_initials{initials}_topo{topo_sampling}")
        
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

    parser.add_argument("--resource_bound", action='store_true') # resource limit for each configuration
    parser.add_argument("--imply_bound", action='store_true') # align space with constraints
    parser.add_argument("--topo_sampling", action='store_true') # using topological sampling
    parser.add_argument("--initial_topo_only", action='store_true') # only use topological sampling for initial points
    parser.add_argument("--trials", type=int, default=200) # tuning trials
    parser.add_argument("--initials", type=int, default=10) # initial configurations for BO
    args = parser.parse_args()
    run_prelim(args)