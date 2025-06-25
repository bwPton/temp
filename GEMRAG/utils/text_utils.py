
import tiktoken
import fitz
import pandas as pd
from config.loader import get_config, init_config
from logging_utils.logger import get_logger
from text_processing.clinical_codes import extract_codes
from text_processing.headings_extraction import *
def chunk_content_column(batch_iter, token_limit=512, stride_tokens=50):
    log = get_logger("chunk_content_column")
    tokenizer = tiktoken.get_encoding("cl100k_base")

    for batch in batch_iter:
        rows = []
        for _, row in batch.iterrows():
            content = row.get("text", "")
            file_path = row.get("file_path")
            dataset_name = row.get("dataset_name")
            try:
                doc = fitz.open(file_path)
                page_texts = [p.get_text().strip() for p in doc]
                page_map = []
                offset = 0
                for i, txt in enumerate(page_texts):
                    page_map.extend([(i, j) for j in range(offset, offset + len(txt))])
                    offset += len(txt)

                ctx = {0: [], 1: [], 2: [], 3: [], 4: [], 5: []}

                tokens = tokenizer.encode(content)
                for i in range(0, len(tokens), stride_tokens):
                    token_chunk = tokens[i:i+token_limit]
                    if not token_chunk:
                        continue
                    chunk = tokenizer.decode(token_chunk)
                    chunk_start = content.find(chunk[:10])  # estimate position
                    chunk_end = chunk_start + len(chunk)

                    pages = set(
                        page for (page, pos) in page_map
                        if chunk_start <= pos < chunk_end
                    )
                    deepest = scan(chunk, ctx)
                    heading_info = snapshot(ctx, deepest)
                    code_dict = extract_codes(chunk)
                    rows.append({
                        "file_path": file_path,
                        "dataset_name": dataset_name,
                        "chunk_id": len(rows),
                        "title": row.get("title"),
                        "author": row.get("author"),
                        "subject": row.get("subject"),
                        "keywords": row.get("keywords"),
                        "creation_date": row.get("creation_date"),
                        "mod_date": row.get("mod_date"),
                        "page_numbers": sorted(list(pages)),
                        "text": chunk,
                        "heading_context": heading_info,
                        "hcpcs_codes": code_dict["hcpcs_codes"],
                        "hcpcs_descriptions": code_dict["hcpcs_descriptions"],
                        "cpt_codes": code_dict["cpt_codes"],
                        "cpt_descriptions": code_dict["cpt_descriptions"],
                        "icd10_codes": code_dict["icd10_codes"],
                        "icd10_descriptions": code_dict["icd10_descriptions"],
                        "modifier_codes": code_dict["modifier_codes"],
                        "modifier_descriptions": code_dict["modifier_descriptions"]
                    })
            except Exception as e:
                raise e
        yield pd.DataFrame(rows)
