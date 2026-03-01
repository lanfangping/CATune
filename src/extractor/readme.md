# Constraint Extraction (Paper Workflow)

This file is a reproducible guide for running `src/extractor/extract_constraints.py` inside the **CATune** repository.

The script includes the full 6-step workflow (candidate retrieval -> structured extraction -> self-reflection/judging -> normalization/repair -> rule augmentation -> confidence filtering).

## 0) Working Directory and Execution Mode (Important)

You can use either mode:

- Mode A (recommended): run from repository root and use explicit `src/extractor/...` paths
- Mode B: `cd src/extractor` first and use local relative paths

All examples below use **Mode A** (least likely to hit path issues).

## 1) Environment Setup

```bash
cd /Users/zq/Desktop/CATune
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
```

`extract_constraints.py` uses only Python standard libraries, so no extra `pip install` is required.

## 2) Set API Key (OpenAI-Compatible)

`extract_constraints.py` reads the key from the environment variable specified by `--api-key-env` (default: `DEEPSEEK_API_KEY`).

### DeepSeek

```bash
export DEEPSEEK_API_KEY="your_deepseek_key"
```

### OpenAI

```bash
export OPENAI_API_KEY="your_openai_key"
```

### Other OpenAI-compatible Providers

```bash
export LLM_API_KEY="your_provider_key"
```

## 3) Recommended Run Order

Recommended order:

1. Run a `--dry-run` first to validate paths and inputs  
2. Run full extraction (with API)  
3. Use `--normalize-only` for post-processing reruns when needed  
4. Use `--evaluate` to produce metrics

## 4) Quick Sanity Check (No API Cost)

```bash
python3 src/extractor/extract_constraints.py \
  --dry-run \
  --input src/extractor/pg13_all.txt \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --restrict-primary-to-knob-source \
  --out-raw src/extractor/raw_sanity.json \
  --out-normalized src/extractor/norm_sanity.json
```

## 5) Run the Full Paper Workflow

### 5.1 DeepSeek (default)

```bash
python3 src/extractor/extract_constraints.py \
  --crawl \
  --input src/extractor/pg13_all.txt \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --restrict-primary-to-knob-source \
  --context-window 2 --max-chars 1800 \
  --api-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY \
  --cache src/extractor/cache_paper.json \
  --out-raw src/extractor/raw_paper.json \
  --out-normalized src/extractor/constraints_paper.json \
  --evaluate \
  --eval-gt src/extractor/relation.json \
  --eval-out src/extractor/eval_paper.json \
  --eval-show 20 \
  --progress
```

### 5.2 OpenAI

```bash
python3 src/extractor/extract_constraints.py \
  --crawl \
  --input src/extractor/pg13_all.txt \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --restrict-primary-to-knob-source \
  --context-window 2 --max-chars 1800 \
  --api-url https://api.openai.com/v1 \
  --model gpt-4o-mini \
  --api-key-env OPENAI_API_KEY \
  --cache src/extractor/cache_paper.json \
  --out-raw src/extractor/raw_paper.json \
  --out-normalized src/extractor/constraints_paper.json \
  --evaluate \
  --eval-gt src/extractor/relation.json \
  --eval-out src/extractor/eval_paper.json \
  --eval-show 20 \
  --progress
```

### 5.3 Any OpenAI-Compatible Provider (template)

```bash
python3 src/extractor/extract_constraints.py \
  --crawl \
  --input src/extractor/pg13_all.txt \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --restrict-primary-to-knob-source \
  --context-window 2 --max-chars 1800 \
  --api-url "$LLM_API_URL" \
  --model "$LLM_MODEL" \
  --api-key-env LLM_API_KEY \
  --cache src/extractor/cache_paper.json \
  --out-raw src/extractor/raw_paper.json \
  --out-normalized src/extractor/constraints_paper.json \
  --evaluate \
  --eval-gt src/extractor/relation.json \
  --eval-out src/extractor/eval_paper.json
```

## 6) If You Already Have `pg13_all.txt` (No Crawl)

Just remove `--crawl` from the full command:

```bash
python3 src/extractor/extract_constraints.py \
  --input src/extractor/pg13_all.txt \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --restrict-primary-to-knob-source \
  --api-url https://api.deepseek.com/v1 \
  --model deepseek-chat \
  --api-key-env DEEPSEEK_API_KEY \
  --cache src/extractor/cache_paper.json \
  --out-raw src/extractor/raw_paper.json \
  --out-normalized src/extractor/constraints_paper.json
```

## 7) Normalize-Only Re-Run (No New API Calls)

```bash
python3 src/extractor/extract_constraints.py \
  --normalize-only \
  --normalize-input src/extractor/raw_paper.json \
  --knob-source file --knob-file src/extractor/knobs_46.txt \
  --out-normalized src/extractor/constraints_paper_post.json \
  --evaluate \
  --eval-pred src/extractor/constraints_paper_post.json \
  --eval-gt src/extractor/relation.json \
  --eval-show 20
```

## 8) What This Workflow Covers

1. Candidate retrieval (trigger + knob co-mention + context window)  
2. Schema-constrained extraction (JSON + fixed relation labels)  
3. Reliability guardrail (Self-Reflection + LLM-as-Judge + uncertainty re-check + abstain)  
4. Normalization and repair  
5. Rule-based high-confidence augmentation  
6. Confidence scoring + `Dedupe` + `ResolvePair` + `Filter`

## 9) Output Files

- `raw_paper.json`: initial extracted tuples (raw model output)
- `constraints_paper.json`: final normalized constraints (includes `confidence`)
- `eval_paper.json`: evaluation summary (P/R/F1, TP/FP/FN)
- `cache_paper.json`: LLM cache (reduces repeated API calls)

## 10) Common Ablation Flags (Optional)

```bash
--disable-self-reflection
--disable-llm-judge
--disable-judge-recheck
--disable-confidence-filter
```

## 11) Notes

- `--knob-source relation` may introduce evaluation leakage (it uses knob vocabulary from `relation.json`).
- In this repo, `src/extractor/relation.json` and `src/rules/relation.json` currently contain the same content; choose one consistently for reproducibility.
- If you see `Missing API key in env var ...`, check whether your exported variable name matches `--api-key-env`.
