"""
Rad_QC.py
---------
Bidirectional data-quality scanner for cross-contamination between the
Radiology and Labs tables.

Detects:
  1. Lab test names / blood-report metrics leaking into Radiology target
     columns (e.g. study_name).
  2. Radiology / imaging terms leaking into Labs target columns
     (e.g. test_parameter).

For every flagged value, captures the ndid/psid of every row it appears in.

Outputs two files:
  - leaked_values_log.xlsx     : unique anomalous values + why they were flagged
  - impacted_patients_log.xlsx : ndid/psid rows tied to each leaked value

Setup:
  pip3 install sqlalchemy pymysql python-dotenv pandas openpyxl
  .env file:
    DB_HOST=...
    DB_PORT=3306
    DB_USER=...
    DB_PASSWORD=...
    DB_NAME=...

NOTE:
  LABS_VOCAB_PATTERNS (imported below) is a STARTER vocabulary — see
  ground_truth/labs_vocab_patterns.py. Replace/extend with your
  authoritative lab dictionary for better precision.
"""

import os, re, sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# ── Load ground truth reference vocabularies ────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "ground_truth"))
from modality_patterns import MODALITY_PATTERNS
from body_part_patterns import BODY_PART_PATTERNS
from labs_vocab_patterns import LABS_VOCAB_PATTERNS   # rename/relocate per note above

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
RADIOLOGY_TABLE = "rgd_gold_ad.radiology"
LABS_TABLE      = "rgd_gold_ad.labs"

RADIOLOGY_TARGET_COLUMNS = ["study_name"]        # leaking Lab values
LABS_TARGET_COLUMNS      = ["test_parameter"]    # leaking Radiology values

ID_COLUMNS = ["ndid", "psid"]   # confirmed present directly on both tables

MIN_COUNT = 1   # raise to skip one-off/rare values if noise becomes an issue

LEAKED_VALUES_OUTPUT     = "leaked_values_log.xlsx"
IMPACTED_PATIENTS_OUTPUT = "impacted_patients_log.xlsx"

# Chunk size for IN(...) queries, to avoid overly large parameter lists
# when pulling ndid/psid rows for a large set of flagged values.
IN_CLAUSE_CHUNK_SIZE = 500

# ── Radiology vocabulary ─────────────────────────────────────────────────────
# Anything matching this = confidently imaging/radiology language.
# (Same keyword set as CONFIRMED_IMAGING in the original pattern-review logic.)
RADIOLOGY_VOCAB = re.compile(
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
    r'|\bMR\b|\bIMAGING\b|\bSCAN\b|\bRADIOGRAPH\b',
    flags=re.IGNORECASE,
)


def classify(value: str, patterns: list):
    """Generic first-match classifier. Supports 2-tuple (pattern, label) and
    3-tuple (pattern, exclude, label) entries, same convention as the
    modality/body-part ground truth files."""
    for entry in patterns:
        if len(entry) == 3:
            pattern, exclude, label = entry
        else:
            pattern, label = entry
            exclude = None
        try:
            if re.search(pattern, value, flags=re.IGNORECASE):
                if exclude and re.search(exclude, value, flags=re.IGNORECASE):
                    continue
                return label
        except re.error:
            pass
    return None


def classify_radiology_value(value: str):
    """Flag a Radiology-column value that looks like it's actually Lab data."""
    lab_hit  = classify(value, LABS_VOCAB_PATTERNS)
    mod      = classify(value, MODALITY_PATTERNS)
    bp       = classify(value, BODY_PART_PATTERNS)
    rad_hit  = bool(RADIOLOGY_VOCAB.search(value)) or mod is not None or bp is not None

    if lab_hit and not rad_hit:
        return f"Lab value leaked into Radiology (matched: {lab_hit})"
    if lab_hit and rad_hit:
        return f"Ambiguous — matches both Lab ({lab_hit}) and Radiology vocab"
    if not lab_hit and not rad_hit:
        return "Unclassified — matches neither Radiology nor Lab vocabulary"
    return None  # confidently Radiology-only -> not a leak


def classify_labs_value(value: str):
    """Flag a Labs-column value that looks like it's actually Radiology data."""
    rad_hit = bool(RADIOLOGY_VOCAB.search(value))
    lab_hit = classify(value, LABS_VOCAB_PATTERNS)

    if rad_hit and not lab_hit:
        return "Radiology/imaging term leaked into Labs"
    if rad_hit and lab_hit:
        return f"Ambiguous — matches both Radiology and Lab ({lab_hit}) vocab"
    return None  # confidently Lab-only, or unclassified -> not treated as a leak here


def connect():
    # Built with URL.create() instead of an f-string so that special
    # characters in DB_USER/DB_PASSWORD (@, :, /, etc. — common in
    # RDS-generated passwords) get URL-encoded automatically. An f-string
    # here silently mis-parses the host whenever the password contains "@".
    db_port = os.getenv("DB_PORT")
    url = URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(db_port) if db_port else None,
        database=os.getenv("DB_NAME"),
    )
    print(f"  -> connecting to {url.host}:{url.port}/{url.database} as {url.username}")
    return create_engine(url).connect()


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def scan_table(conn, table: str, target_columns: list, classifier_fn):
    """
    For each target column:
      1. Pull DISTINCT values + counts (cheap — classify once per unique value).
      2. Classify each distinct value; keep only flagged ones.
      3. Re-query the table (chunked IN clause) to pull every ndid/psid row
         where a flagged value occurs.
    Returns a single row-level DataFrame across all target columns.
    """
    flagged_frames = []

    for col in target_columns:
        distinct_sql = text(f"""
            SELECT {col} AS value, COUNT(*) AS cnt
            FROM {table}
            WHERE {col} IS NOT NULL AND {col} != ''
            GROUP BY {col}
            HAVING COUNT(*) >= :min_count
        """)
        distinct_df = pd.read_sql(distinct_sql, conn, params={"min_count": MIN_COUNT})
        if distinct_df.empty:
            continue

        distinct_df["flag_reason"] = distinct_df["value"].astype(str).apply(classifier_fn)
        flagged = distinct_df.dropna(subset=["flag_reason"]).copy()
        if flagged.empty:
            continue

        value_list = flagged["value"].tolist()
        row_chunks = []
        for chunk in chunked(value_list, IN_CLAUSE_CHUNK_SIZE):
            params = {f"v{i}": v for i, v in enumerate(chunk)}
            placeholders = ", ".join(f":{k}" for k in params)
            row_sql = text(f"""
                SELECT {col} AS leaked_value, ndid, psid
                FROM {table}
                WHERE {col} IN ({placeholders})
            """)
            row_chunks.append(pd.read_sql(row_sql, conn, params=params))

        rows_df = pd.concat(row_chunks, ignore_index=True)
        rows_df = rows_df.merge(
            flagged.rename(columns={"value": "leaked_value"})[["leaked_value", "flag_reason", "cnt"]],
            on="leaked_value", how="left",
        )
        rows_df["source_table"] = table
        rows_df["source_column"] = col
        flagged_frames.append(rows_df)

    if not flagged_frames:
        return pd.DataFrame(columns=[
            "source_table", "source_column", "leaked_value", "flag_reason", "ndid", "psid", "cnt"
        ])
    return pd.concat(flagged_frames, ignore_index=True)


def run():
    print("Connecting to DB...")
    conn = connect()

    print(f"Scanning {RADIOLOGY_TABLE} ({RADIOLOGY_TARGET_COLUMNS}) for leaked Lab values...")
    rad_flags = scan_table(conn, RADIOLOGY_TABLE, RADIOLOGY_TARGET_COLUMNS, classify_radiology_value)

    print(f"Scanning {LABS_TABLE} ({LABS_TARGET_COLUMNS}) for leaked Radiology values...")
    labs_flags = scan_table(conn, LABS_TABLE, LABS_TARGET_COLUMNS, classify_labs_value)

    conn.close()

    all_flags = pd.concat([rad_flags, labs_flags], ignore_index=True)

    if all_flags.empty:
        print("No cross-contamination detected.")
        return

    # ── File 1: Leaked Values Log — one row per unique (table, column, value) ──
    leaked_values = (
        all_flags.groupby(["source_table", "source_column", "leaked_value", "flag_reason"], as_index=False)
        .agg(occurrence_count=("ndid", "count"), unique_patients=("ndid", "nunique"))
        .sort_values("occurrence_count", ascending=False)
    )
    leaked_values.to_excel(LEAKED_VALUES_OUTPUT, index=False)

    # ── File 2: Impacted Patients Log — every ndid/psid tied to a leaked value ──
    impacted_patients = (
        all_flags[["source_table", "source_column", "leaked_value", "flag_reason", "ndid", "psid"]]
        .drop_duplicates()
        .sort_values(["source_table", "leaked_value"])
    )
    impacted_patients.to_excel(IMPACTED_PATIENTS_OUTPUT, index=False)

    print("\n── Summary ───────────────────────────────────────────────")
    print(f"  Total flagged rows           : {len(all_flags):,}")
    print(f"  Unique leaked values         : {len(leaked_values):,}")
    print(f"  Impacted ndid/psid/value rows: {len(impacted_patients):,}")
    print(f"  Unique patients impacted     : {all_flags['ndid'].nunique():,}")
    print(f"  Output 1 (values)   -> {LEAKED_VALUES_OUTPUT}")
    print(f"  Output 2 (patients) -> {IMPACTED_PATIENTS_OUTPUT}")


if __name__ == "__main__":
    run()
