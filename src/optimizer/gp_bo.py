
from optimizer.smac import VanillaBO
# from smac.facade.smac_bo_facade import HyperparameterOptimizationFacade
# from smac.optimizer.acquisition import EI
# from smac.optimizer.ei_optimization import LocalAndSortedRandomSearch
# from smac.model.gaussian_process import GaussianProcess 
from optimizer.topo_latin_hypercube_design import TopoLatinHypercubeInitialDesign
from smac import Scenario, initial_design
from smac.facade.blackbox_facade import BlackBoxFacade

class GPBO(VanillaBO):
    def __init__(self, dbms, workload, search_space, timeout, seed, output_folder, config_path, knob_info):
        super().__init__(dbms, workload, search_space, timeout, seed, output_folder, config_path, knob_info)


    def optimize(self, name, trials_number, initial_config_number, topo_sampler=None):

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

        gpbo = BlackBoxFacade(
           scenario=scenario,
           initial_design=init_design,
           target_function=self.set_and_replay,
           overwrite=False
        )
        
        gpbo.optimize()