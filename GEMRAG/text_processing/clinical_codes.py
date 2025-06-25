
import csv, functools, re

from config.policy_regex import (RE_MOD_GROUPS,
    RE_ICD9_RANGE, RE_ICD9_SINGLE,
    RE_ICD10_RANGE, RE_ICD10_SINGLE,
    RE_HCPCS_SINGLE, RE_HCPCS_RANGE,
    RE_CPT_SINGLE, RE_CPT_RANGE, MODIFIERS)

from logging_utils.logger import get_logger
from config.loader import *
init_config('/Workspace/Users/benjamin.wynn@peraton.com/GEMRAG/config/base_config.yaml')
config = get_config()
log = get_logger("code_extraction")

MAPPINGS_FOLDER= config['mappings_folder']

def extract_modifiers(text):
    raw = []
    for g1, g2 in RE_MOD_GROUPS.findall(text.upper()):
        raw += re.split(r'[\s,]+', g1.strip())
        if g2:
            raw.append(g2.strip())
    return [m for m in raw if m in MODIFIERS]

def extract_icd9_codes(text):
    out = set()

    for s, e in RE_ICD9_RANGE.findall(text):
        try:
            bs, be = int(s.split('.')[0]), int(e.split('.')[0])
            if bs == be:
                ds = int(s.split('.')[1]) if '.' in s else 0
                de = int(e.split('.')[1]) if '.' in e else 0
                out.update(f"{bs}.{i}" for i in range(ds, de + 1))
            else:
                out.update(map(str, range(bs, be + 1)))
                
        except ValueError:
            continue
    out.update(RE_ICD9_SINGLE.findall(text))

    return sorted(out)

def extract_icd10_codes(text):
    out = set()
    for a, n1, s1, _, n2, s2 in RE_ICD10_RANGE.findall(text):
        try:
            if n1 == n2 and s1 and s2:
                out.update(f"{a}{n1}{i}" for i in range(int(s1), int(s2) + 1))
            else:
                out.update(f"{a}{i:02d}" for i in range(int(n1), int(n2) + 1))
        except ValueError:
            continue
    
    out.update(c.replace('.', '') for c in RE_ICD10_SINGLE.findall(text))

    return sorted(out)

def extract_hcpcs_codes(text):

    out = set(RE_HCPCS_SINGLE.findall(text))

    for p1, s, p2, e in RE_HCPCS_RANGE.findall(text):
        if p1 == p2:
            out.update(f"{p1}{i:04d}" for i in range(int(s), int(e) + 1))
    return sorted(out)

def extract_cpt_codes(text):
    out = set(RE_CPT_SINGLE.findall(text))

    for s, e in RE_CPT_RANGE.findall(text):
        out.update(str(i) for i in range(int(s), int(e) + 1))

    return sorted(out)


# ── CSV mapping utils (memoised) ─────────────────────────────────────
@functools.lru_cache(maxsize=None)
def load_map(file_name):
    path = MAPPINGS_FOLDER + file_name
    try:
        with open(path, newline='') as fh:
            rdr = csv.reader(fh)
            next(rdr, None)
            return {c.strip(): d.strip() for c, d, *_ in rdr if c and d}
    except FileNotFoundError:
        log.error("Mapping file missing: %s", path)
        return {}

def map_codes(codes, mapping):
    return [c for c in codes if c in mapping], [mapping[c] for c in codes if c in mapping]

def extract_codes(text):
    hcpcs = extract_hcpcs_codes(text)
    cpt   = extract_cpt_codes(text)
    icd10 = extract_icd10_codes(text)
    icd9  = extract_icd9_codes(text)
    mods  = extract_modifiers(text)
    hcpcs, hcpcs_desc = map_codes(hcpcs, load_map("HCPCS.csv"))
    cpt,   cpt_desc   = map_codes(cpt,   load_map("CPT.csv"))
    icd10, icd10_desc = map_codes(icd10, load_map("DGNS.csv"))
    mods,  mods_desc  = map_codes(mods,  load_map("MODIFIERS.csv"))
    return {
        "hcpcs_codes":            hcpcs,
        "hcpcs_descriptions":     hcpcs_desc,
        "cpt_codes":                    cpt,
        "cpt_descriptions":       cpt_desc,
        "icd10_codes":                  icd10,
        "icd10_descriptions":     icd10_desc,
        "modifier_codes":              mods,
        "modifier_descriptions":  mods_desc,
    }
