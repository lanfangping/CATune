#!/usr/bin/env python3
import argparse
import json
import re
from collections import Counter

from extract_constraints import TRIGGER_TERMS


STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "of",
    "to",
    "in",
    "on",
    "for",
    "with",
    "by",
    "as",
    "is",
    "are",
    "be",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "from",
}


def load_contexts(path):
    """Load context strings from relation.json entries."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    contexts = []
    for item in data:
        context = (item.get("context") or "").strip()
        if context:
            contexts.append(context)
    return contexts


def has_trigger(text, triggers):
    """Check whether any trigger term appears in the text (case-insensitive)."""
    lower = text.lower()
    return any(t in lower for t in triggers)


def tokenize(text):
    """Tokenize text into lowercase alphanumeric tokens."""
    return re.findall(r"[a-z0-9]+", text.lower())


def ngram_counts(texts, n_min, n_max):
    """Count n-grams across texts for a given n-gram range."""
    counts = Counter()
    for text in texts:
        tokens = tokenize(text)
        if not tokens:
            continue
        for n in range(n_min, n_max + 1):
            if len(tokens) < n:
                continue
            for i in range(len(tokens) - n + 1):
                gram = " ".join(tokens[i : i + n])
                counts[gram] += 1
    return counts


def is_candidate(ngram, triggers):
    """Filter n-grams to avoid stopword-only phrases or existing trigger overlap."""
    if ngram in triggers:
        return False
    if any(t in ngram for t in triggers):
        return False
    tokens = ngram.split()
    if all(tok in STOPWORDS for tok in tokens):
        return False
    return True


def main():
    """CLI entry point for trigger coverage analysis and suggestion generation."""
    parser = argparse.ArgumentParser(description="Analyze trigger term coverage in relation.json contexts.")
    parser.add_argument("--relation-json", default="relation.json", help="Ground truth constraints file.")
    parser.add_argument("--ngram-min", type=int, default=2, help="Minimum n-gram size.")
    parser.add_argument("--ngram-max", type=int, default=3, help="Maximum n-gram size.")
    parser.add_argument("--top-k", type=int, default=20, help="Max suggestions to show.")
    parser.add_argument("--min-freq", type=int, default=2, help="Minimum frequency for suggestions.")
    parser.add_argument("--json-out", default="", help="Write summary and suggestions to JSON.")
    args = parser.parse_args()

    contexts = load_contexts(args.relation_json)
    triggers = [t.lower() for t in TRIGGER_TERMS]
    covered = [c for c in contexts if has_trigger(c, triggers)]
    missing = [c for c in contexts if not has_trigger(c, triggers)]

    total = len(contexts)
    coverage = (len(covered) / total * 100.0) if total else 0.0

    print(f"Total contexts: {total}")
    print(f"Covered by triggers: {len(covered)} ({coverage:.1f}%)")
    print(f"Missing trigger coverage: {len(missing)}")

    counts = ngram_counts(missing, args.ngram_min, args.ngram_max)
    suggestions = []
    for gram, count in counts.most_common():
        if count < args.min_freq:
            break
        if is_candidate(gram, triggers):
            suggestions.append({"ngram": gram, "count": count})
        if len(suggestions) >= args.top_k:
            break

    if suggestions:
        print("Suggested triggers (from missing contexts):")
        for item in suggestions:
            print(f"  - {item['ngram']} ({item['count']})")
    else:
        print("Suggested triggers: none (min frequency threshold not met).")

    if args.json_out:
        summary = {
            "total_contexts": total,
            "covered": len(covered),
            "coverage_percent": coverage,
            "missing": len(missing),
            "suggestions": suggestions,
        }
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
