import ConfigSpace as CS
import ConfigSpace.hyperparameters as CSHP
from ConfigSpace.configuration_space import Configuration, ConfigurationSpace
from search_space.hallucination_generator import valid_knobs_info
from utils.system_resource import get_hardware_info
from sampler.topological_sampler import TopoSampler
from typing import Dict, List, Tuple, Set, Optional, Any


# deprecated_knobs = [
#     "checkpoint_segments"
# ]

resource_dependent_knobs = {
    "ram_bounded_knobs": [
        "shared_buffers",
        "wal_buffers",
        "maintenance_work_mem",
        "work_mem",
        "temp_buffers",
        "effective_cache_size"
    ],
    "disk_bounded_knobs": [
        "max_wal_size",
        "min_wal_size"
    ],
    "cpu_bounded_knobs": [
        # "max_worker_processes",
        # "max_parallel_workers",
        # "max_parallel_workers_per_gather",
        # "max_parallel_maintenance_workers",
        # "max_logical_replication_workers",
        # "max_sync_workers_per_subscription"
    ]
}

unit_mapping = {
    '8kB': 8,
    'kB': 1,
    'MB': 1024,
    'GB': 1024 * 1024,
    's': 1000,
    'ms': 1,

    'KB': 1,
    'TB': 1024 * 1024 * 1024,
    'K': 1,
    'M': 1024,
    'G': 1024 * 1024,
    'B': 1/1024,
    'min': 60000,
    'h': 60 * 60000,
    'hour': 60 * 60000,
    'day': 24 * 60 * 60000,
    'million': 1e6
}

def get_resource_bounded_upper(knob, info, use_default_unit=True, base_unit='kB'):
    cpu, ram, disk = get_hardware_info(unit='GB')
    # print(f"Resource info: cpu={cpu}, ram={ram}, disk={disk}")
    unit = info.get('unit', None)
    
    if knob in resource_dependent_knobs['ram_bounded_knobs']:
        upper_value = ram
    elif knob in resource_dependent_knobs['cpu_bounded_knobs']:
        upper_value = cpu
    elif knob in resource_dependent_knobs['disk_bounded_knobs']:
        upper_value = disk
    else:
        try:
            upper_value = info['range'][1]
        except:
            upper_value = info['max']
    
    if use_default_unit:
        upper_bound = unit_mapping['GB'] * upper_value / unit_mapping[unit] 
    else:
        unit = base_unit
        upper_bound = unit_mapping['GB'] * upper_value / unit_mapping[unit]
    
    return upper_bound


def unify_unit(value, from_unit, to_unit='kB'):
    try:
        if value <= 0:
            return value
    except:
        return value
    
    if from_unit not in unit_mapping or to_unit not in unit_mapping:
        return value  # No conversion possible
    
    # Convert value to base unit (kB for memory, ms for time)
    converted_value = int(value * unit_mapping[from_unit] // unit_mapping[to_unit])
    return converted_value

# ----------------------------
# ConfigSpace sampling wrapper / patch
# ----------------------------
def attach_sampler_to_configspace(
    cs: ConfigurationSpace,
    sampler: TopoSampler,
    *,
    method_name: str = "sample_configuration",
):
    """
    Monkey-patch cs.sample_configuration to use topo_sampler.sample.

    Usage:
        sampler = TopoSampler(cs, constraints, conditional_activations)
        attach_topo_sampler_to_configspace(cs, sampler)
        cfg = cs.sample_configuration()
        cfgs = cs.sample_configuration(10)
    """

    def _patched_sample_configuration(size: Optional[int] = None):
        return sampler.sample(size)

    setattr(cs, method_name, _patched_sample_configuration)

def build_search_space(knobs_info: dict = valid_knobs_info, seed: int = 100, resource_bound=True, topo_sampler=None) -> CS.ConfigurationSpace:
    search_space = CS.ConfigurationSpace(seed=seed)
    for knob, info in knobs_info.items():
        if info['type'] == 'enum':
            try:
                param = CSHP.CategoricalHyperparameter(
                    knob, choices=info['range'], default_value=info['default']
                )
            except:
                param = CSHP.CategoricalHyperparameter(
                    knob, choices=info['enum_values'], default_value=info['default']
                )
        else:
            print(info)
            try:
                lower_bound = unify_unit(info['range'][0], info['unit'])
                upper_bound = unify_unit(info['range'][1], info['unit'])
                default_value = unify_unit(info['default'], info['unit'])
            except:
                lower_bound = unify_unit(info.get('min', None), info.get('unit', None))
                upper_bound = unify_unit(info.get('max', None), info.get('unit', None))
                default_value = unify_unit(info['default'], info.get('unit', None))
            print(f"upper: {upper_bound}")
            resource_bound= get_resource_bounded_upper(knob, info, use_default_unit=False, base_unit='kB')
            if upper_bound > resource_bound:
                upper_bound = resource_bound

            print(f"Knob: {knob}, lower: {lower_bound}, upper: {upper_bound}, resourcebound: {resource_bound}, default: {default_value}\n")

            if info['type'] == 'integer':
                param = CSHP.UniformIntegerHyperparameter(
                            knob, lower=lower_bound, upper=upper_bound, default_value=default_value
                        )
            elif info['type'] == 'float':
                param = CSHP.UniformFloatHyperparameter(
                            knob, lower=lower_bound, upper=upper_bound, default_value=default_value
                        )
            else:
                print(f"Unknown type {info['type']} for knob {knob}.")
                continue
            
        search_space.add_hyperparameter(param)
    print("Unified Search Space with unit kB and ms")

    if topo_sampler:
        attach_sampler_to_configspace(search_space, topo_sampler)
        print("Attached Topological Sampler to ConfigurationSpace.")
    return search_space

if __name__ == "__main__":
    import json
    knob_info_file_path = 'src/search_space/knob_info/all_rules_related_knob_info_default.json'
    knob_info = json.load(open(knob_info_file_path, 'r'))
    search_space = build_search_space(knobs_info=knob_info, seed=100, resource_bound=True)