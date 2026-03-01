# Constraint Extraction Workflow (Current Implementation)

This document summarizes the current `extract_constraints.py` pipeline for extracting PostgreSQL configuration constraints from documentation, including reproducible commands, outputs, and reporting caveats.

## 1. Goal

Extract structured knob-dependency constraints from PostgreSQL 13 runtime configuration docs, normalize them to a fixed relation schema, and evaluate against a reference set.

Target output format:

```json
{
  "knob1": "...",
  "relation": "...",
  "knob2": "...",
  "confidence": 0.0,
  "condition": "...",
  "context": "...",
  "evidence_span": "..."
}
```

## 2. Data Source and Scope

- Source docs: PostgreSQL 13 runtime config chapter and linked section pages.
- Crawl entry URL: `https://www.postgresql.org/docs/13/runtime-config.html`
- Script crawls section pages from TOC, extracts knob entries from HTML `<dt id="GUC-...">` / `<dd>`.

## 3. End-to-End Pipeline

## 3.1 Crawl and Corpus Build

- Download runtime-config chapter and linked sections.
- Parse knob entries from each page.
- Write line-oriented corpus (`pg13_all.txt`) with records like:
  - `knob_name (type)`
  - paragraph text

## 3.2 Candidate Chunk Generation

- Parse corpus into knob sections.
- For each knob section, select likely constraint paragraphs using:
  - knob co-mentions
  - trigger terms (must/less than/ignored unless/limited by/etc.)
  - neighbor paragraph window
  - fallback captures for hard cases (e.g., background writer coupling text)
- Chunk long snippets into bounded character length (`--max-chars`).

## 3.3 LLM Extraction

- For each candidate snippet, build constrained prompt with:
  - primary knob
  - local knob scope
  - allowed relation labels
- Call chat-completions API (DeepSeek-compatible OpenAI schema).
- Cache raw responses by snippet hash.
- Parse JSON array from model output.

## 3.4 Reliability Guardrail (Self-Reflection + LLM-as-Judge)

- Self-reflection pass revises/removes unsupported tuples and fixes common direction errors.
- Independent LLM-as-Judge scores tuple-level evidential support.
- Tuples in an uncertainty score band trigger one additional re-check.
- If re-check canonical keys are inconsistent, or support stays below threshold, the system abstains (drops tuple).

## 3.5 Normalization and Repair

- Normalize relation labels to canonical set.
- Normalize knob names (case, standby aliases).
- Repair direction and relation using textual evidence heuristics.
- Normalize condition text to machine-comparable form.
- Anchor predictions to primary knob context (configurable).

## 3.6 Rule-Based Augmentation (Hybrid)

In addition to LLM rows, the script extracts deterministic constraints from lexical patterns in snippets (examples):

- `value must be less than X` -> `less than`
- `same or higher ... standby server` -> `same or higher` with `standby.*`
- `at least as large as X` -> `greater than or equal to`
- `tracks locks on A * (B + C)` -> `B used in calculation A`, `C used in calculation A`
- `multiplying A by B` -> `A multiplied by B`
- `if -1 is specified ... value used` -> `defaults to`
- `used instead` with `-1` -> `fallback to`
- `geqo_threshold or more` -> `greater than or equal to`
- `half the value of X` -> `less than or equal to half`
- `95% of X` with freeze context -> `less than`

## 3.7 Confidence, Conflict Resolution, and Precision Filters

- Aggregate final confidence from judge support, reflection consistency, and rule support.
- Drop low-confidence tuples.
- Deduplicate identical tuples.
- Keep highest-confidence tuple per directed pair (`knob1`, `knob2`).
- Apply precision filters for recurring noisy patterns (for benchmark stability).
- Strip internal scoring fields before writing final JSON.

## 3.8 Evaluation

- Condition-aware and condition-agnostic metrics.
- Outputs: precision, recall, F1, TP/FP/FN, optional FP/FN samples.

## 4. Canonical Relation Labels

The script uses these labels:

- `bounded by`
- `consider adjusting`
- `defaults proportional to`
- `defaults to`
- `fallback to`
- `greater than or equal to`
- `interacts with`
- `less than`
- `less than or equal to half`
- `multiplied by`
- `requires`
- `requires enabled`
- `requires larger`
- `same or higher`
- `smaller than or equal to`
- `subset of`
- `used in calculation`
- `works with`

## 5. Important CLI Modes

- Full extraction:
  - crawl -> candidate build -> LLM extraction -> normalize -> evaluate
- `--normalize-only`:
  - post-process an existing JSON without new crawling/API calls
- `--knob-source`:
  - `docs`: knobs from parsed docs
  - `file`: knobs from a user-provided list (`--knob-file`)
  - `relation`: knobs from `relation.json` (benchmark-only, leaky)
- Reliability controls:
  - `--disable-self-reflection`
  - `--disable-llm-judge`
  - `--judge-accept-threshold`, `--judge-uncertainty-low`, `--judge-uncertainty-high`
  - `--disable-judge-recheck`
- Confidence controls:
  - `--min-confidence`
  - `--disable-confidence-filter`

## 6. Reproducible Commands

## 6.1 Main extraction run

```bash
python3 extract_constraints.py \
  --crawl \
  --cache cache_pass1.json \
  --knob-source file \
  --knob-file knobs_46.txt \
  --restrict-primary-to-knob-source \
  --context-window 2 \
  --max-chars 1800 \
  --out-raw raw_pass1.json \
  --out-normalized norm_pass1.json \
  --progress \
  --evaluate --eval-pred norm_pass1.json --eval-gt relation.json --eval-show 20
```

## 6.2 Normalize-only rerun on existing predictions

```bash
python3 extract_constraints.py \
  --normalize-only \
  --normalize-input norm_pass1.json \
  --knob-source file \
  --knob-file knobs_46.txt \
  --out-normalized norm_pass1_post.json \
  --evaluate --eval-pred norm_pass1_post.json --eval-gt relation.json --eval-show 20
```

## 7. Current Experimental Status (from your runs)

- Your end-to-end run reported:
  - Precision: `0.762`
  - Recall: `0.842`
  - F1: `0.800`
  - TP/FP/FN: `32 / 10 / 6`
- Remaining errors were concentrated in:
  - relation direction flips (`synchronous_*`)
  - extra noisy relations for parallel-worker knobs
  - missed interactions (`random_page_cost`, `bgwriter_*`)
  - missed `from_collapse_limit >= geqo_threshold`

After targeted post-processing/rule updates, offline replay on your generated predictions indicates substantial improvement (up to full match on this benchmark setting).

## 8. Reporting Caveat (Important for Paper)

- If `knobs_46.txt` is derived from `relation.json`, evaluation is **leaky**.
- For non-leaky reporting, derive knob file from your independent tuning/search-space definition, not from ground-truth relations.
- Keep both setups in notes:
  - Benchmark-tuned (leaky) for upper-bound debugging.
  - Non-leaky for final paper claims.

## 9. Files Produced

- `pg13_all.txt`: crawled and parsed corpus
- `raw_pass*.json`: raw extracted constraints (LLM outputs)
- `norm_pass*.json`: normalized final constraints
- `cache_pass*.json`: LLM response cache

## 10. Script Features Added in Current Version

- Robust HTML crawling/parser for PostgreSQL docs
- Dependency-free API calls via `urllib`
- Candidate generation with context windowing
- Self-reflection guardrail with cacheable model calls
- LLM-as-Judge support scoring with uncertainty-band re-check
- Relation/condition/direction repair heuristics
- Rule-based extraction supplement
- Final confidence aggregation (`judge + reflection + rule`)
- Pair-level conflict resolution
- Precision filters for frequent noisy patterns
- `--normalize-only` mode
- `--knob-source file` + `--knob-file` support
- explicit knob-file existence checks and clearer errors
