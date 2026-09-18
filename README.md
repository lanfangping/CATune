<img align='left' src='figs/catune_logo.png' width='180'>

<div align="center">
  <h1>CATune: Structural Constraint-Aware Bayesian Optimization for DBMS Configuration Tuning</h1>
  <a href="https://awesome.re">
    <img src="https://awesome.re/badge.svg" alt="Awesome">
  </a>
  <a href="https://img.shields.io/badge/PRs-Welcome-red">
    <img src="https://img.shields.io/badge/PRs-Welcome-red" alt="PRs Welcome">
  </a>
  <!-- <a href="https://img.shields.io/github/last-commit/withinmiaov/A-Survey-on-Mixture-of-Experts?color=green">
    <img src="https://img.shields.io/github/last-commit/withinmiaov/A-Survey-on-Mixture-of-Experts?color=green" alt="Last Commit">
  </a> -->
</div>

🎯 Modern DBMSs expose hundreds of configuration knobs, resulting in a high-dimensional and heterogeneous search space that makes automated tuning costly. Existing ML-based tuning systems typically treat the configuration domain as box-constrained and rely on workload feedback to implicitly capture inter-knob relation-ships, leading to wasted evaluations of invalid configurations.

💡 DBMS documentation specifies deterministic knob dependency constraints-particularly ordering constraints-that characterize structurally valid regions of the configuration space. We present CATUNE, a constraint-aware Bayesian optimization (BO) framework that models deterministic inter-knob ordering constraints as structural components of the search domain. Instead of learning feasibility boundaries through sampled violations, CATunE performs optimization within a constraint-consistent sub-space.




> [!IMPORTANT]
> **Good news! :tada: Our paper has been successfully accepted by PVLDB Volume 19 (PVLDB'27) . :fire::fire::fire:**
>
> Please find our full version paper: [technical report](https://github.com/lanfangping/CATune/blob/main/docs/Constraint_aware_DB_Tuning__Tech_Report.pdf)
>
> Please let us know if you discover any mistakes or have suggestions by emailing us: fangping.lan@temple.edu | fangpinglan0116@gmail.com

## Installation

### Docker Environment for Database
```bash
# start Postgres image
bash docker/start.sh

# stop Postgres image
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

### Cite
```

```