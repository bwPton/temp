import re
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from logging_utils.logger import get_logger
from config.loader import get_config

config = get_config()
log = get_logger("rag-analytics")

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def get_tokens(text):
    return clean_text(text).split()


# === Metric Computation Utilities ===
def compute_precision(expected_tokens, returned_tokens):
    if not returned_tokens:
        return 1.0 if not expected_tokens else 0.0
    matched = sum(1 for token in returned_tokens if token in expected_tokens)
    return matched / len(returned_tokens)


def compute_recall(expected_tokens, returned_tokens):
    if not expected_tokens:
        return 1.0 if not returned_tokens else 0.0
    matched = sum(1 for token in expected_tokens if token in returned_tokens)
    return matched / len(expected_tokens)


def compute_f1(expected_tokens, returned_tokens):
    common = set(expected_tokens) & set(returned_tokens)
    if not expected_tokens or not returned_tokens or not common:
        return 0.0
    precision = len(common) / len(returned_tokens)
    recall = len(common) / len(expected_tokens)
    return 2 * (precision * recall) / (precision + recall)


def compute_similarity(expected_text, returned_text):
    return SequenceMatcher(None, clean_text(expected_text), clean_text(returned_text)).ratio()


def compare_rag_output(expected_text, returned_text, verbose=False):
    expected_text = expected_text.strip()
    returned_text = returned_text.strip()

    expected_tokens = get_tokens(expected_text)
    returned_tokens = get_tokens(returned_text)

    precision = compute_precision(expected_tokens, returned_tokens)
    recall = compute_recall(expected_tokens, returned_tokens)
    f1 = compute_f1(expected_tokens, returned_tokens)
    similarity = compute_similarity(expected_text, returned_text)

    if verbose:
        log.info("=== Comparison Results ===")
        log.info(f"Precision:  {precision:.4f}")
        log.info(f"Recall:     {recall:.4f}")
        log.info(f"F1 Score:   {f1:.4f}")
        log.info(f"Similarity: {similarity:.4f}")

    return {
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "similarity": similarity
    }


# === Evaluation Pipeline Utilities ===
def calculate_metrics(claim_lines, policy, results, n_results):
    grouped_results = results.groupby('claim')['text'].apply(list).to_dict()
    all_results = []

    for i, row in enumerate(claim_lines.itertuples(index=False)):
        claim_id = getattr(row, "claim_id")
        policy_text = policy[i]

        claim_contents = grouped_results.get(claim_id)
        if not claim_contents:
            continue

        for k in range(1, n_results):
            cumulative_text = " ".join(claim_contents[:k])
            rag_metrics = compare_rag_output(policy_text, cumulative_text, verbose=False)
            all_results.append({"claim_id": claim_id, "k": k, **rag_metrics})

    return pd.DataFrame(all_results)


def calculate_metrics_parallel(claim_lines, policy, results, n_results, max_workers=8):
    grouped_results = results.groupby('claim')['text'].apply(list).to_dict()
    tasks = []

    for i, row in enumerate(claim_lines.itertuples(index=False)):
        claim_id = getattr(row, "claim_id")
        policy_text = policy[i]
        claim_contents = grouped_results.get(claim_id)

        if not claim_contents:
            continue

        for k in range(1, n_results):
            cumulative_text = " ".join(claim_contents[:k])
            tasks.append((claim_id, k, policy_text, cumulative_text))

    def process(task):
        claim_id, k, policy_text, cumulative_text = task
        rag_metrics = compare_rag_output(policy_text, cumulative_text, verbose=False)
        return {"claim_id": claim_id, "k": k, **rag_metrics}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process, tasks))

    return pd.DataFrame(results)


# === Plotting Utilities ===
def save_timings_plot(query_times, output_path):
    plt.figure(figsize=(10, 6))
    plt.hist(query_times, bins=30, edgecolor='black')
    plt.title('Histogram of Query Times')
    plt.xlabel('Query Time (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path + "_hist_timings.png")
    plt.close()


def save_metrics_plot(metrics_df, output_path):
    avg_scores = metrics_df.groupby('k').mean()

    plt.figure(figsize=(10, 6))
    for score_type in ['precision', 'recall', 'f1_score']:
        if score_type in avg_scores.columns:
            plt.plot(avg_scores.index, avg_scores[score_type], label=score_type.capitalize())

    plt.xlabel('Number of Retrieved Documents (k)')
    plt.ylabel('Score')
    plt.title('RAG Performance vs Number of Retrieved Documents (k)')
    plt.legend()
    plt.grid(True)
    plt.ylim(0, 1.05)
    plt.tight_layout()
    plt.savefig(output_path + "_ndocs_v_metric.png")
    plt.close()


def save_precision_recall_plot(metrics_df, output_path):
    plt.figure(figsize=(8, 6))
    plt.scatter(metrics_df['precision'], metrics_df['recall'], alpha=0.6)
    plt.xlabel('Precision')
    plt.ylabel('Recall')
    plt.title('Precision vs Recall')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path + "_recall_v_precision.png")
    plt.close()




def save_metric_distributions(metrics_df, output_path):
    for metric in ['precision', 'recall', 'f1_score', 'similarity']:
        plt.figure(figsize=(8, 5))
        plt.hist(metrics_df[metric], bins=30, edgecolor='black')
        plt.title(f'Distribution of {metric.capitalize()}')
        plt.xlabel(metric.capitalize())
        plt.ylabel('Frequency')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_path + "_docs_dist_metrics_diss.png")
        plt.close()
