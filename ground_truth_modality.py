# ground_truth/modality_patterns.py
# -----------------------------------
# Exact translation of the SQL modality CASE block.
# ORDER MATTERS — first match wins, just like SQL CASE WHEN.
# Combined modalities must stay above single modalities.
# SQL matches against c.study_name (original case) — we do the same.
#
# Format: (match_pattern, exclude_pattern_or_None, label)
# If exclude_pattern is set, the row only matches when:
#   match_pattern fires AND exclude_pattern does NOT fire.
# This mirrors SQL:  WHEN ... REGEXP '...' AND ... NOT REGEXP '...'

MODALITY_PATTERNS = [

    # ── Combined modalities (must come first) ──────────────────────────────────
    (r'\bPET/CT\b|\bPET CT\b',          None,   'Positron Emission Tomography (PET) / Computed Tomography'),
    (r'\bXR/RF\b',                       None,   'Digital Radiography / Radio Fluoroscopy'),
    (r'\bUS DOPPLER\b|\bUS DUPLEX\b',    None,   'Ultrasound / Duplex Doppler'),
    (r'\bUS ECHOCARDIOGRAM\b',           None,   'Ultrasound / Echocardiography'),
    (r'\bXA US\b',                       None,   'X-Ray Angiography / Ultrasound'),

    # ── Single modalities ──────────────────────────────────────────────────────
    (r'\bCT\b|\bCAT\b|\bNCT\b|\bLDCT\b|\bCTA\b|\bCTV\b|\bCTAC\b|\bCTC\b|\bCTP\b', None, 'Computed Tomography'),
    # \bPET\b first (no exclusion needed), then \bPT\b with exclusion for lab coagulation tests and E&M billing codes
    (r'\bPET\b',                         None,   'Positron Emission Tomography (PET)'),
    (r'\bPT\b',                          r'\bPT/|/PT\b|\bPT\s+(PANEL|COAGULATION)|\bPROTIME\b|\bINR\b|\bPTT\b|\bESTAB\s+PT\b|\bNEW\s+PT\b|\bMED\s+DECISION\b', 'Positron Emission Tomography (PET)'),
    # MRA before MRI/MR
    (r'\bMRA\b|\bzzMRA\b',               None,   'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'),
    (r'\bMRI\b|\bMRV\b|\bMRCP\b|\b3TMRI\b|\bTMRI\b|\b3TMRA\b|\bMR\b', None, 'Magnetic Resonance'),
    # DEXASCAN added — \bDEXA\b alone fails on DEXASCAN because S after DEXA breaks the word boundary
    (r'\bDEXA\b|\bDXA\b|\bDEXASCAN\b',   None,   'Bone Densitometry (X-Ray)'),
    # Full mammography keywords first, then \bMG\b with exclusion for Myasthenia Gravis
    (r'\bMAM\b|\bMAMM\b|\bMAMMO\b|\bMMAMMO\b|\bMAMMOGRAM\b|\bMAMMOGRAPHY\b', None, 'Mammography'),
    (r'\bMG\b',                          r'\bMYASTHENIA\b|\bGRAVIS\b|\bEVALUATION\b', 'Mammography'),
    (r'\bUS\b|\bULTRASOUND\b|\bUSV\b|\bBI US\b|\bOB US\b', None, 'Ultrasound'),
    # \bXA\b with exclusion for anti-Xa lab context
    (r'\bXA\b',                          r'\bANTI-XA\b|\bANTI XA\b|\bHEPARIN\b', 'X-Ray Angiography'),
    (r'\bANG\b|\bANGIO\b',               None,   'X-Ray Angiography'),
    (r'\bCR\b',                          None,   'Computed Radiography'),
    (r'\bDX\b|\bXR\b|\bX-RAY\b|\bXRAY\b|\bXRY\b', None, 'Digital Radiography'),
    # \bDR\b with exclusion for HLA typing context
    (r'\bDR\b',                          r'\bHLA\b|\bTYPING\b|\bDQ\b|\bDP\b', 'Digital Radiography'),
    # \bRF\b with exclusion for Rheumatoid Factor lab context
    (r'\bRF\b',                          r'\bRHEUMATOID\b|\bFACTOR\b|\bANTI-CCP\b|\bANA\b|\bSERUM\b|\bTITER\b', 'Radio Fluoroscopy'),
    (r'\bFL\b|\bFLUORO\b|\bFLU\b|\bFLUOROSCOPY\b|\bFLUOROSCOPIC\b', None, 'Radio Fluoroscopy'),
    (r'\bFS\b',                          None,   'Fundoscopy (FS - Retired) / Ophthalmic Photography'),
    (r'\bNM\b',                          None,   'Nuclear Medicine'),
    (r'\bECHO\b|\bECHOCARDIOGRAM\b|\bECHOCARDIOGRAPHY\b', None, 'Echocardiography (EC - Retired) / Ultrasound'),
    # \d* trailing pattern — handles ECG1, EKG12 etc. \bECG\b alone fails on ECG1 because digit breaks word boundary
    (r'\bECG[0-9]*\b|\bEKG[0-9]*\b|\bELECTROCARDIOGRAM\b|\bELECTROCARDIOGRAPH\b|\bELECTROCARDIOGRAPHY\b', None, 'Electrocardiography'),
    (r'\bEEG\d*\b|\bELECTROCEPHANLOGRAM\b|\bELECTROENCEPHALOGRAM\b', None, 'Electroencephalography'),
    (r'\bENDOSCOPY\b|\bEGD\b',           None,   'Endoscopy'),
    (r'\bCD\b',                          None,   'Color Flow Doppler (CD - Retired) / Ultrasound'),
    (r'\bTCD\b|\bDUPLEX\b|\bDOPPLER\b',  None,   'Duplex Doppler (DD - Retired) / Ultrasound'),
    (r'\bAUDIO\b|\bAUDIOMETRY\b|\bAUDITORY\b|\bHEARING\b|\bAUDIOGRAM\b|\bACOUSTIC\b', None, 'Audio'),
    (r'\bRP\b',                          None,   'Radiotherapy Plan'),
    # \bRT\b with exclusion for renal/lab context
    (r'\bRT\b',                          r'\bCREATININE\b|\bCREAT\b|\bRENAL\b|\bKIDNEY\b', 'Radiographic Imaging (RG) / Interventional Radiology'),
    (r'\bRAD\b|\bIR\b|\bINTERVENTIONAL RADIOLOGY\b', None, 'Radiographic Imaging (RG) / Interventional Radiology'),
    (r'\bSPECT\b',                       None,   'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine'),
    (r'\bBX\b|\bBIOPSY\b|\bVL\b|\bOHS\b|\bI-123\b|\b1-131\b|\bMPI\b', None, 'Other'),

    # ELSE 'NS' → returned as None by classify(), written as null in CSV
]
