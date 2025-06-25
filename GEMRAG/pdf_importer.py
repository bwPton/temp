
import os
import logging
import fitz  # PyMuPDF
import pandas as pd
import tiktoken
from pyspark.sql import SparkSession
from logging_utils.logger import get_logger
from config.policy_regex import *
from config.loader import get_config, init_config
import copy
from utils.text_utils import chunk_content_column
from text_processing.clinical_codes import extract_codes
from utils.pdf_utils import get_policy_paths, extract_pdf_metadata
from schema.structs import raw_schema, chunk_schema, heading_struct
init_config('/Workspace/Users/benjamin.wynn@peraton.com/GEMRAG/config/base_config.yaml')
config = get_config()
log = get_logger("pdf-ingestion")

def run_pdf_ingestion_pipeline(document_volume, raw_table, chunked_table, token_limit=512, stride_tokens=50):
    spark = SparkSession.getActiveSession()
    path_dataset_pairs = get_policy_paths(document_volume)
    if not path_dataset_pairs:
        log.warning("No PDF files found.")
        return

    paths_df = spark.createDataFrame(path_dataset_pairs, ["path", "dataset_name"])
    raw_df = paths_df.repartition(64).mapInPandas(extract_pdf_metadata, schema=raw_schema)
    raw_df.write.mode("overwrite").saveAsTable(raw_table)
    log.info(f"Wrote raw extracted PDFs to {raw_table}")

    loaded_df = spark.table(raw_table).repartition(64)
    chunked_df = loaded_df.mapInPandas(
        lambda it: chunk_content_column(it, token_limit=token_limit, stride_tokens=stride_tokens),
        schema=chunk_schema
    )
    chunked_df.write.mode("overwrite").saveAsTable(chunked_table)
    log.info(f"Wrote chunked PDFs to {chunked_table}")

# Invocation
# run_pdf_ingestion_pipeline("/Volumes/sandbox_catalog/default/globaledit-policy-docs", "sandbox_catalog.default.pdf_raw_text", "sandbox_catalog.default.chunked_pdf_text")
