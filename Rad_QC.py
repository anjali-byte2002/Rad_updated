"""
pattern_review.py
------------------
Reads DISTINCT study names from rgd_udm_silver.radiology, classifies each
against modality + body part ground truth patterns, and writes any unresolved
names to unmapped_study_names.csv.

KEY BEHAVIOURS:
  * Case-insensitive matching (re.IGNORECASE) — mirrors MySQL REGEXP
    default behaviour with utf8mb4_*_ci collations.
  * CPT-mapped studies are excluded from the unmapped CSV. A study whose
    embedded 5-digit code resolves in tncpa.PROCEDURECODEREFERENCE, or whose
    HCPCS-shaped token resolves in semantics.hcpcs, is considered handled by
    the SQL standardisation pipeline regardless of its modality / body-part
    text classification.

Setup:
  1. Fill in your .env file (see below)
  2. pip3 install sqlalchemy pymysql python-dotenv pandas
  3. python3 pattern_review.py

.env file should contain:
  DB_HOST=...
  DB_PORT=3306
  DB_USER=...
  DB_PASSWORD=...
  DB_NAME=...
"""

import os, re, sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Load ground truth from the ground_truth folder ────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "ground_truth"))
from modality_patterns  import MODALITY_PATTERNS
from body_part_patterns import BODY_PART_PATTERNS

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
SOURCE_TABLE    = "rgd_udm_silver.radiology"
CPT_REF_TABLE   = "tncpa.PROCEDURECODEREFERENCE"   # column: PROCEDURECODE
HCPCS_REF_TABLE = "semantics.hcpcs"                # column: HCPC
OUTPUT_CSV      = "unmapped_study_names.csv"
MIN_COUNT       = 10   # skip study names that appear fewer than N times

# ── Confirmed imaging keywords ────────────────────────────────────────────────
# Used to decide if a study name with body_part=NS is still an imaging study.
# If body part resolved → it's imaging (lab tests don't have body parts).
# If modality resolved but body part=NS → check CONFIRMED_IMAGING to distinguish
#   real imaging studies (body part just unknown) from lab test false positives.
# Compiled with IGNORECASE so 'Echo', 'echo', 'Fluoro' etc all match.
CONFIRMED_IMAGING = re.compile(
    r'\bMRI\b|\bMRV\b|\bMRCP\b|\bMRA\b|\b3TMRI\b|\bTMRI\b|\b3TMRA\b'
    r'|\bCT\b|\bCTA\b|\bCTAC\b|\bCTC\b|\bCTP\b|\bCTV\b|\bLDCT\b|\bNCT\b'
    r'|\bPET\b|\bNM\b|\bSPECT\b'
    r'|\bECHO\b|\bECHOCARDIOGRAM\b|\bECHOCARDIOGRAPHY\b'
    r'|\bEEG\b|\bEKG[0-9]*\b|\bECG[0-9]*\b'
    r'|\bELECTROCARDIOGRAM\b|\bELECTROCARDIOGRAPH\b|\bELECTROCARDIOGRAPHY\b'
    r'|\bELECTROENCEPHALOGRAM\b|\bELECTROENCEPHALOGRAPHY\b'
    r'|\bXA\b|\bXR\b|\bXRAY\b|\bX-RAY\b|\bXRY\b'
    r'|\bULTRASOUND\b|\bUSV\b|\bUS\b'
    r'|\bMAM\b|\bMAMM\b|\bMAMMO\b|\bMAMMOGRAM\b|\bMAMMOGRAPHY\b|\bMG\b'
    r'|\bFLUORO\b|\bFL\b|\bFLU\b|\bFLUOROSCOPY\b|\bFLUOROSCOPIC\b'
    r'|\bDEXA\b|\bDXA\b|\bDEXASCAN\b'
    r'|\bTCD\b|\bDUPLEX\b|\bDOPPLER\b'
    r'|\bANGIO\b|\bANG\b'
    r'|\bIR\b|\bRAD\b|\bDX\b|\bRT\b'
    r'|\bRP\b'
    r'|\bBIOPSY\b|\bBX\b'
    r'|\bAUDIOGRAM\b|\bAUDIOMETRY\b|\bAUDITORY\b|\bHEARING\b|\bACOUSTIC\b'
    r'|\bENDOSCOPY\b|\bEGD\b'
    r'|\bOB US\b'
    r'|\bMR\b',
    flags=re.IGNORECASE,
)


# ── Classifier ────────────────────────────────────────────────────────────────
# Mirrors SQL CASE WHEN: evaluates patterns in order, returns first match.
# Returns None if nothing matches (= ELSE 'NS' in SQL).
#
# IGNORECASE is set so behaviour matches MySQL REGEXP under default
# case-insensitive collations (utf8mb4_*_ci, latin1_swedish_ci, etc).
#
# Supports two tuple formats:
#   (match_pattern, label)                  — standard match
#   (match_pattern, exclude_pattern, label) — match AND NOT exclude
#     exclude_pattern = None means no exclusion (treated same as 2-tuple)
def classify(value: str, patterns: list):
    for entry in patterns:
        if len(entry) == 3:
            pattern, exclude, label = entry
        else:
            pattern, label = entry
            exclude = None
        try:
            if re.search(pattern, value, flags=re.IGNORECASE):
                if exclude and re.search(exclude, value, flags=re.IGNORECASE):
                    continue   # exclusion fired — skip this rule
                return label
        except re.error:
            pass
    return None


# ── DB connection ─────────────────────────────────────────────────────────────
def connect():
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url).connect()


# ── Source query ──────────────────────────────────────────────────────────────
# Mirrors the CPT/HCPCS extraction logic in the standardisation SQL so we
# know which study names are "handled" via code lookup rather than text patterns.
#
# A study is `cpt_mapped = 1` when:
#   * its 5-digit token resolves in tncpa.PROCEDURECODEREFERENCE, OR
#   * its [letter][4-digit] token resolves in semantics.hcpcs.
SOURCE_QUERY = f"""
WITH studies AS (
    SELECT study_name, COUNT(*) AS cnt
    FROM {SOURCE_TABLE}
    WHERE study_name IS NOT NULL
      AND study_name != ''
    GROUP BY study_name
    HAVING COUNT(*) >= {MIN_COUNT}
),
extracted AS (
    SELECT
        study_name,
        cnt,
        -- first 5-digit token (CPT shape)
        REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)'),
                       '[^0-9]', '') AS code5,
        -- [letter][4-digit] token (HCPCS shape)
        REGEXP_SUBSTR(study_name, '\\\\b[A-Za-z][0-9]{{4}}\\\\b')   AS code_alpha4
    FROM studies
)
SELECT
    e.study_name,
    e.cnt,
    CASE
        WHEN cpt.PROCEDURECODE IS NOT NULL OR hcpcs.HCPC IS NOT NULL THEN 1
        ELSE 0
    END AS cpt_mapped
FROM extracted e
LEFT JOIN {CPT_REF_TABLE}   cpt   ON e.code5       = cpt.PROCEDURECODE
LEFT JOIN {HCPCS_REF_TABLE} hcpcs ON e.code_alpha4 = hcpcs.HCPC
GROUP BY e.study_name, e.cnt, cpt_mapped
ORDER BY e.cnt DESC
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    print("Connecting to DB...")
    conn = connect()

    print(f"Fetching DISTINCT study names from {SOURCE_TABLE} (count >= {MIN_COUNT})...")
    rows = conn.execute(text(SOURCE_QUERY)).fetchall()
    conn.close()

    print(f"Distinct study names fetched: {len(rows):,}")

    total             = len(rows)
    fully_mapped      = 0
    confirmed_no_mod  = 0
    confirmed_no_bp   = 0
    cpt_only          = 0     # handled by CPT/HCPCS lookup only
    unmapped          = []

    for row in rows:
        study_name = row[0]
        cnt        = row[1]
        cpt_mapped = bool(row[2])

        # Modality  → SQL: c.study_name (case-insensitive collation)
        mod = classify(study_name, MODALITY_PATTERNS)

        # Body part → SQL: UPPER(c.study_name); classify() uses IGNORECASE so
        # explicit upper() is no longer required — kept logic consistent with mod.
        bp  = classify(study_name, BODY_PART_PATTERNS)

        # Both mapped → fully handled by SQL pipeline
        if mod is not None and bp is not None:
            fully_mapped += 1
            continue

        # Modality mapped, body part null:
        # CONFIRMED_IMAGING present → real imaging study, body part just unknown
        if mod is not None and bp is None:
            if CONFIRMED_IMAGING.search(study_name):
                confirmed_no_bp += 1
                continue

        # Body part mapped, modality null:
        # Lab tests never have body parts → if body part resolved it's imaging.
        if mod is None and bp is not None:
            confirmed_no_mod += 1
            continue

        # Neither text classifier resolved BUT CPT/HCPCS lookup succeeded
        # → SQL standardisation pipeline has it covered via proc_code_std
        if cpt_mapped:
            cpt_only += 1
            continue

        unmapped.append({
            "study_name":         study_name,
            "count":              cnt,
            "probable_modality":  mod,   # None → blank/null in CSV
            "probable_body_part": bp,    # None → blank/null in CSV
        })

    pd.DataFrame(
        unmapped,
        columns=["study_name", "count", "probable_modality", "probable_body_part"]
    ).to_csv(OUTPUT_CSV, index=False)

    print(f"\n── Summary ───────────────────────────────────────────────")
    print(f"  Total distinct study names       : {total:,}")
    print(f"  Fully mapped (mod + body part)   : {fully_mapped:,}")
    print(f"  Imaging, modality unknown        : {confirmed_no_mod:,}")
    print(f"  Imaging, body part unknown       : {confirmed_no_bp:,}")
    print(f"  Mapped via CPT/HCPCS lookup only : {cpt_only:,}")
    print(f"  Written to CSV (needs review)    : {len(unmapped):,}")
    print(f"  Output                           : {OUTPUT_CSV}")
    mod_null = sum(1 for r in unmapped if r["probable_modality"]  is None)
    bp_null  = sum(1 for r in unmapped if r["probable_body_part"] is None)
    print(f"    └─ modality null               : {mod_null:,}")
    print(f"    └─ body part null              : {bp_null:,}")


if __name__ == "__main__":
    run()
