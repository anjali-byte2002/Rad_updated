# ground_truth/labs_vocab_patterns.py
# -------------------------------------
# STARTER master vocabulary for Lab test/component language.
# Mirrors the structure of modality_patterns.py / body_part_patterns.py so it
# can be dropped into the same ground_truth/ folder and imported the same way.
#
# Purpose: give Rad_QC.py a reference list of terms that are unambiguously
# "Labs" language, so a value found in a Radiology column (e.g. study_name)
# that matches one of these patterns is very likely a leaked lab value.
#
# THIS IS A PLACEHOLDER. Replace/extend with your authoritative lab
# dictionary — ideally sourced from a LOINC component list, your lab
# reference/master table, or an export of distinct test_parameter values
# that a lab SME has confirmed. Order matters (first match wins) only if
# you add exclusion patterns like the modality file does.

LABS_VOCAB_PATTERNS = [

    # ── Panels / orders ───────────────────────────────────────────────────
    (r'\bCBC\b|\bCOMPLETE BLOOD COUNT\b',                         'CBC Panel'),
    (r'\bCMP\b|\bCOMPREHENSIVE METABOLIC PANEL\b',                'CMP Panel'),
    (r'\bBMP\b|\bBASIC METABOLIC PANEL\b',                        'BMP Panel'),
    (r'\bLIPID PANEL\b|\bLIPID PROFILE\b',                        'Lipid Panel'),
    (r'\bLFT\b|\bLIVER FUNCTION\b|\bHEPATIC FUNCTION\b',          'Liver Function Panel'),
    (r'\bBMP\b|\bRENAL PANEL\b|\bRENAL FUNCTION\b',               'Renal Panel'),
    (r'\bCOAG(ULATION)?\s*PANEL\b|\bPROTIME\b|\bPT/INR\b|\bPTT\b', 'Coagulation Panel'),
    (r'\bURINALYSIS\b|\bUA\b|\bURIC ACID\b',                      'Urinalysis'),
    (r'\bCULTURE\b|\bSENSITIVITY\b|\bC&S\b|\bGRAM STAIN\b',       'Microbiology / Culture'),
    (r'\bSEROLOGY\b|\bANTIBODY\b|\bANTIGEN\b|\bTITER\b',          'Serology'),

    # ── Hematology ────────────────────────────────────────────────────────
    (r'\bHGB\b|\bHEMOGLOBIN\b',                                    'Hemoglobin'),
    (r'\bHCT\b|\bHEMATOCRIT\b',                                    'Hematocrit'),
    (r'\bWBC\b|\bWHITE BLOOD CELL\b|\bLEUKOCYTE\b',               'WBC'),
    (r'\bRBC\b|\bRED BLOOD CELL\b|\bERYTHROCYTE\b',               'RBC'),
    (r'\bPLT\b|\bPLATELET\b',                                     'Platelets'),
    (r'\bMCV\b|\bMCH\b|\bMCHC\b|\bRDW\b',                         'RBC Indices'),
    (r'\bESR\b|\bSED(IMENTATION)? RATE\b',                        'ESR'),

    # ── Chemistry / metabolic ─────────────────────────────────────────────
    (r'\bGLUCOSE\b|\bBLOOD SUGAR\b|\bFASTING GLUCOSE\b',          'Glucose'),
    (r'\bSODIUM\b|\bNA\+?\b',                                     'Sodium'),
    (r'\bPOTASSIUM\b|\bK\+?\b',                                   'Potassium'),
    (r'\bCHLORIDE\b|\bCL-?\b',                                    'Chloride'),
    (r'\bBICARBONATE\b|\bCO2\b',                                  'Bicarbonate'),
    (r'\bCREATININE\b|\bCREAT\b|\bEGFR\b|\bGFR\b',                'Creatinine / Renal Function'),
    (r'\bBUN\b|\bUREA NITROGEN\b',                                'BUN'),
    (r'\bALT\b|\bAST\b|\bSGOT\b|\bSGPT\b|\bBILIRUBIN\b|\bALK(ALINE)? PHOS(PHATASE)?\b', 'Liver Enzymes'),
    (r'\bTSH\b|\bT3\b|\bT4\b|\bFREE T4\b|\bTHYROID PANEL\b',      'Thyroid Function'),
    (r'\bHDL\b|\bLDL\b|\bTRIGLYCERIDE\b|\bCHOLESTEROL\b',         'Lipids'),
    (r'\bA1C\b|\bHBA1C\b|\bHEMOGLOBIN A1C\b',                     'A1C'),
    (r'\bPSA\b',                                                  'PSA'),
    (r'\bTROPONIN\b|\bBNP\b|\bNT-?PROBNP\b',                      'Cardiac Markers'),
    (r'\bCRP\b|\bC-?REACTIVE PROTEIN\b',                          'CRP'),
    (r'\bVITAMIN D\b|\bVITAMIN B12\b|\bFOLATE\b|\bFERRITIN\b|\bIRON\b', 'Nutrient / Iron Studies'),

    # ── Specimen / units / lab-only language ──────────────────────────────
    (r'\bSPECIMEN\b|\bSERUM\b|\bPLASMA\b|\bWHOLE BLOOD\b|\bVENIPUNCTURE\b', 'Specimen Type'),
    (r'\bMG/DL\b|\bMMOL/L\b|\bU/L\b|\bNG/ML\b|\bMEQ/L\b',         'Lab Units'),
    (r'\bREFERENCE RANGE\b|\bCRITICAL VALUE\b|\bPANEL\b|\bLAB(ORATORY)? RESULT\b', 'Lab Reporting Language'),

    # ELSE -> None (no match) — treated as "not confidently Lab vocabulary"
]
