import os
import pandas as pd
import fitz
from logging_utils.logger import get_logger
def get_policy_paths(document_volume):
    log = get_logger("get-policy-paths")
    log.info("Scanning policy volume for document paths...")
    document_paths = []

    for dataset in os.listdir(document_volume):
        dataset_path = os.path.join(document_volume, dataset)
        for doc_name in os.listdir(dataset_path):
            if doc_name.lower().endswith(".pdf"):
                full_path = os.path.join(dataset_path, doc_name)
                document_paths.append((full_path, dataset))

    log.info(f"Found {len(document_paths)} documents in volume '{document_volume}'.")
    return document_paths

def extract_pdf_metadata(batch_iter):
    
    log = get_logger("pdf-metadata-extraction")
    for batch in batch_iter:
        rows = []
        for path, dataset_name in zip(batch["path"], batch["dataset_name"]):
            try:
                doc = fitz.open(path)
                metadata = doc.metadata or {}
                text = "".join([p.get_text() for p in doc]).strip()
                rows.append({
                    "file_path": path,
                    "dataset_name": dataset_name,
                    "title": metadata.get("title"),
                    "author": metadata.get("author"),
                    "subject": metadata.get("subject"),
                    "keywords": metadata.get("keywords"),
                    "creation_date": metadata.get("creationDate"),
                    "mod_date": metadata.get("modDate"),
                    "text": text
                })
            except Exception as e:
                raise e
        yield pd.DataFrame(rows)
