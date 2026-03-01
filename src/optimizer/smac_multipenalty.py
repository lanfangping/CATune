import json
import os
import time
from smac import HyperparameterOptimizationFacade, Scenario, initial_design
from smac.multi_objective.parego import ParEGO
from optimizer.smac import VanillaBO
from optimizer.topo_latin_hypercube_design import TopoLatinHypercubeInitialDesign
from sampler.topological_sampler import Constraint
from typing import Dict, List, Tuple, Set, Optional, Any



class MultiPenaltyBO(VanillaBO):
    """
    Docstring for MultiPenaltyBO

    Do not encode Constraint into ConfigSpace, but use penalty in the objective function.
    """
    def __init__(self, constraints: List[Constraint], objectives: List[str]= ["performance", "soft_violation"], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.constraints = constraints
        self.soft_penalty_file = os.path.join(self.output_folder, f"{self.seed}/soft_penalties.json")
        self.objectives = objectives
    # -----------------------------------------
    # 2) Soft-violation score (ordering prefs)
    # -----------------------------------------
    def relu(self, x: float) -> float:
        return x if x > 0 else 0.0

    def violation_ordered(self, a: float, b: float) -> float:
        """
        Soft violation for preferred constraint a <= b.
        Normalized magnitude so scales are more stable.
        """
        return self.relu(a - b) / (abs(b) + 1.0)

        
    def optimize(self, name, trials_number, initial_config_number, topo_sampler=None):
        
        scenario = Scenario(
            configspace=self.search_space,
            objectives=self.objectives,
            name=name,
            seed=self.seed,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,

        )
        init_design = TopoLatinHypercubeInitialDesign(
            scenario=scenario,
            topo_sampler=topo_sampler,
            n_configs=initial_config_number,
            max_ratio=0.8,
            seed=self.seed,
        )

        multi_objective_algorithm = ParEGO(scenario) # set ParEGO as the multi-objective method

        smac = HyperparameterOptimizationFacade(
            scenario=scenario,
            initial_design=init_design,
            target_function=self.set_and_replay,
            multi_objective_algorithm=multi_objective_algorithm,
            overwrite=False,
        )
        
        smac.optimize()

    def _log_soft_penalty(self, constraint_idx, penalty_soft):
        if os.path.exists(self.soft_penalty_file):
            all_soft_penalties = json.load(open(self.soft_penalty_file, 'r'))
        else:
            all_soft_penalties = {}

        if self.round in all_soft_penalties:
            all_soft_penalties[self.round][f"{constraint_idx}"] = penalty_soft
        else:
            all_soft_penalties[self.round] = {f"{constraint_idx}": penalty_soft}
        json.dump(all_soft_penalties, open(self.soft_penalty_file, 'w'), indent=4)


    def set_and_replay(self, config, seed=0):
        """
        Wrapper around the objective function to include penalties for constraint violations.
        """
        self._log_original_config(dict(config))
        convert_config = self.convert_config(config=config)
        begin_time = time.time()
        penalty_performance = super().set_and_replay_ori(convert_config, seed)
        end_time = time.time()
        self._log_overhead(begin_time, end_time)
        # Example penalty calculations (to be customized based on actual constraints)
        penalty_soft = 0.0
        # Hard constraint example: config['knob1'] + config['knob2'] <= 100
        for constraint_idx,constraint in enumerate(self.constraints):
            left_knob, opr, right_knob = constraint
            p = 0
            if opr == "<=":
                p = self.violation_ordered(config[left_knob], config[right_knob]) # * constraint.weight
                penalty_soft += p
            elif opr == "<":
                p = self.violation_ordered(config[left_knob], config[right_knob] + 1e-5) # * constraint.weight
                penalty_soft += p

            elif opr == ">=":
                p = self.violation_ordered(config[right_knob], config[left_knob]) # * constraint.weight
                penalty_soft += p
            elif opr == ">":
                p = self.violation_ordered(config[right_knob], config[left_knob] + 1e-5) # * constraint.weight
                penalty_soft += p
            elif opr == "==":
                p = abs(config[left_knob] - config[right_knob]) # * constraint.weight
                penalty_soft += p
            else:
                raise ValueError(f"Unsupported operator {opr} in constraint.")
            self._log_soft_penalty(constraint_idx=constraint_idx, penalty_soft=p)

        return {
            "performance": penalty_performance,   # minimize negative throughput or miinimize latency
            "soft_violation": penalty_soft,   # minimize violation
        }
            