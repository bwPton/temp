import re
MODIFIERS = {
    '22', '26', '50', '51', '52', '53', '54', '55', '58', '62', '66', '78', '79',
    '80', '81', '82', 'AA', 'AD', 'AS', 'TC', 'QK', 'QW', 'QY', '59', '76', '77',
    '91', 'XE', 'XP', 'XS', 'XU', 'AB', 'AE', 'AF', 'AG', 'AI', 'AK', 'AM', 'AO',
    'AT', 'AZ', 'BL', 'CA', 'CB', 'CG', 'CR', 'CS', 'CT', 'DA', 'ER', 'ET', 'FB',
    'FC', 'FS', 'FX', 'FY', 'G7', 'GC', 'GE', 'GG', 'GJ', 'GU', 'J1', 'J2', 'J3',
    'JC', 'JA', 'JB', 'JD', 'JG', 'JW', 'JZ', 'KX', 'L1', 'LU', 'M2', 'PD', 'PI',
    'PO', 'PN', 'PS', 'PT', 'Q0', 'Q1', 'Q3', 'Q4', 'Q5', 'Q6', 'QJ', 'QQ', 'RD',
    'RE', 'SC', 'SF', 'SS', 'SW', 'TB', 'TS', 'UJ', 'UN', 'UP', 'UQ', 'UR', 'US',
    'X1', 'X2', 'X3', 'X4', 'X5', 'GA', 'GX', 'GY', 'GZ', 'MA', 'MB', 'MC', 'MD',
    'ME', 'MF', 'MG', 'MH', 'D', 'E', 'G', 'H', 'I', 'J', 'N', 'P', 'R', 'S', 'X',
    'GM', 'QL', 'QM', 'QN', 'E1', 'E2', 'E3', 'E4', 'FA', 'F1', 'F2', 'F4', 'F5',
    'F6', 'F7', 'F8', 'F9', 'LC', 'LD', 'LM', 'LT', 'RC', 'RI', 'RT', 'TA', 'T1',
    'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'G8', 'G9', 'P1', 'P2', 'P3',
    'P4', 'P5', 'P6', 'QX', 'QZ', '23', '33', 'AX', 'EA', 'EB', 'EC', 'AY', 'ED',
    'EE', 'EJ', 'EM', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'GS', 'JE', 'V5', 'V6',
    'V7', 'V8', 'V9', '24', '25', '57', 'FT', 'GV', 'GW', '90', '92', 'LR', 'Q7',
    'Q8', 'Q9', '1P', '2P', '3P', '8P', 'AQ', 'AR', '73', '74', 'PA', 'PB', 'PC',
    '93', '95', 'FQ', 'GQ', 'GT', 'G0', 'GN', 'GO', 'GP', 'CO', 'CQ'
}

CHAPTER_RE  = re.compile(r'^Chapter\s+(\d+|[IVXLCDM]+)\s*[–-]\s*(.+)', re.I)
NUMSEC_RE   = re.compile(r'^(\d{1,3}(?:\.\d+)*)\s*[–-]\s*(.+)')
ALPHASEC_RE = re.compile(r'^([A-Z])\.\s+(.+)')
PAGE_LBL_RE = re.compile(r'^(Intro|[IVXLCDM]+|\w+)-\d+', re.I)
SENT_RE = re.compile(r"(?<=[.!?])\s+")

# ======== Clinical-code patterns ========
RE_MOD_GROUPS   = re.compile(  # keep names UPPER_SNAKE or Camel
    r"(?:MODIFIER[S]?|WITH),?\s+((?:[A-Z0-9]{2}(?:,\s*)?)+)"
    r"(?:\s+(?:AND|OR)\s+([A-Z0-9]{2}))?",
    re.I,
)
RE_ICD9_RANGE   = re.compile(r'\b(\d{3}(?:\.\d{1,2})?)\s*[-–]\s*(\d{3}(?:\.\d{1,2})?)\b')
RE_ICD9_SINGLE  = re.compile(r'\b\d{3}(?:\.\d{1,2})?\b')
RE_ICD10_RANGE  = re.compile(
    r'\b([A-TV-Z])(\d{2})(?:\.(\w{1,4}))?\s*[-–]\s*([A-TV-Z])(\d{2})(?:\.(\w{1,4}))?\b'
)
RE_ICD10_SINGLE = re.compile(r'\b[A-TV-Z][0-9]{2}(?:\.[A-Z0-9]{1,4})?\b')
RE_HCPCS_SINGLE = re.compile(r'\b[A-Z]\d{4}\b')
RE_HCPCS_RANGE  = re.compile(r'\b([A-Z])(\d{4})\s*[-–]\s*([A-Z])(\d{4})\b')
RE_CPT_SINGLE   = re.compile(r'\b\d{5}\b')
RE_CPT_RANGE    = re.compile(r'\b(\d{5})\s*[-–]\s*(\d{5})\b')