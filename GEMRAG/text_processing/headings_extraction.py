from __future__ import annotations
import pandas as pd
from logging_utils.logger    import get_logger
from config.policy_regex import * 
from config.loader import *
import copy
init_config('/Workspace/Users/benjamin.wynn@peraton.com/GEMRAG/config/base_config.yaml')
config = get_config()
log = get_logger("heading-extraction")

def _upsert(ctx, lvl, num, title):
    for h in ctx[lvl]:
        if h["num"] == num:
            if title and not h.get("title"): h["title"] = title
            return
    ctx[lvl].append({"num": num, "title": title})

def scan(text, ctx):
    deepest = 0
    for line in (l.strip() for l in text.split("\n") if l.strip()):
        if (m := CHAPTER_RE.match(line)):
            _upsert(ctx, 0, m.group(1), m.group(2)); continue
        if (m := NUMSEC_RE.match(line)):
            nums, title = m.groups(); parts = nums.split('.')
            for i in range(1, len(parts)+1):
                _upsert(ctx, i, ".".join(parts[:i]), title if i == len(parts) else None)
            deepest = max(deepest, len(parts)); continue
        if (m := ALPHASEC_RE.match(line)):
            _upsert(ctx, 1, m.group(1), m.group(2)); deepest = max(deepest, 1)
    return deepest

def snapshot(ctx, max_lvl):
    return {str(lvl): copy.deepcopy(ctx[lvl][-1]) if ctx.get(lvl) else None for lvl in range(1, max_lvl+1)}