# CATune: CATune: Structural Constraint-Aware Bayesian Optimization for DBMS Configuration Tuning

## Installation

### Docker Environment for Database
```bash
# start Postgres image
bash docker/start.sh

# stop POstgres image
bash docker/stop.sh
```

### Benchbase
```bash
bash scripts/install_benchbase.sh postgres
```

**Error:**
```
[ERROR] Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.13.0:compile (default-compile) on project benchbase: Fatal error compiling: error: invalid target release: 23 -> [Help 1]
```
*Solve:*
Correct java version to your installed version in `benchbash/pom.xml`
```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <java.version>23</java.version>  <!-- change 23 to 21 -->
    <maven.compiler.source>23</maven.compiler.source> <!-- change 23 to 21 -->
    <maven.compiler.target>23</maven.compiler.target> <!-- change 23 to 21 -->
    <buildDirectory>${project.basedir}/target</buildDirectory>
</properties>
```

**Build benchbase**
```bash
cd benchbase/target/benchbase-postgres

# TPC-C workload
java -jar benchbase.jar -b tpcc -c ../../../src/optimizer/configs/postgres/tpcc_config.xml --create=true --load=true --clear=true --execute=false

# TPC-H workload
java -jar benchbase.jar -b tpcc -c ../../../src/optimizer/configs/postgres/tpch_config.xml --create=true --load=true --clear=true --execute=false
```

## LLM API Key and Base Setup
Create `.env` under root folder:
```bash
OPENAI_API_KEY="your_api_key"
OPENAI_API_BASE="https://api.openai.com/v1/"

DEEPSEEK_API_BASE="https://api.deepseek.com"
DEEPSEEK_API_KEY="your_api_key"

GEMINI_API_BASE="https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_API_KEY="your_api_key"

ANTHROPIC_API_KEY="your_api_key"

SUDO_PASSWORD="your_sudo_password"
```

## Experiments

### Baseline

```bash
# Baseline  - default bound from manual
PYTHONPATH=src python src/run_SMAC.py --task='smac' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=100 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_default.json' --tag='smac_baseline3'

# suggest bound
PYTHONPATH=src python src/run_SMAC.py --task='smac' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=80 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_reasonable_bound.json' --tag='smac_suggestbound3'

```

### Topology-aware Sampling
```bash
# default  + topo sampling
PYTHONPATH=src python src/run_SMAC.py --task='smac' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=100 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_default.json' --tag='smac_rulev5_topo3' --rules='v5' --topo_sampling

# suggest + topo sampling
PYTHONPATH=src python src/run_SMAC.py --task='smac_rule_ablation' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=20 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_reasonable_bound.json' --tag='smac_rulev5_suggestbound' --rules='v5' --topo_sampling
```

### Rejection-based Sampling
```bash
PYTHONPATH=src python src/run_SMAC.py --task='smac' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=60 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_default.json' --tag='smac_rulev4_3' --rules='v4'
```

### Hallucianted Constraint Ablation
```bash
# rule v5 without hallucinated constraints while rule v4 containts two hallucinated constraints
PYTHONPATH=src python src/run_SMAC.py --task='smac_rule_ablation' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=20 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_reasonable_bound.json' --tag='smac_rulev5_suggestbound' --rules='v5' --topo_sampling

PYTHONPATH=src python src/run_SMAC.py --task='smac_rule_ablation' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=20 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_reasonable_bound.json' --tag='smac_rulev4_suggestbound' --rules='v4' --topo_sampling
```

### Multiple Penalties
```bash
# split rules into soft and strict rules
PYTHONPATH=src python src/run_SMAC_multipenalty.py --task='smac_multipenalty' --workload='tpcc' --dbms_name='postgres' --timeout=100 --seed=20 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --knob_info_file_path='src/search_space/knob_info/all_rules_related_knob_info_reasonable_bound.json' --tag='smac_suggestbound_strict2' --topo_sampling
```
### GPTuner

```bash
PYTHONPATH="src:src/GPTuner/src" python src/run_GPTuner.py --task='gptuner' --workload='tpcc' --dbms_name='postgres' --timeout=100 --trials=200 --initials=10 --workload_config_path='src/optimizer/configs/postgres/tpcc_config.xml' --tag='baseline' --model='gpt-5.2' --seed=20 
```
