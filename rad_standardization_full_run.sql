-- ============================================================================
-- Radiology field standardization for rgd_gold_ad.radiology -- FULL RUN
-- (fixed + CPT fallback merged in)
--
-- Combines, with one bug fix applied:
--   1) rad_updated_gold.sql's study_std logic (img_modality_std,
--      img_body_part_std, img_contrast_type_std, img_tracer_name_std,
--      and codes extracted directly from study_name text)
--   2) Rad.sql's CPT_PROBABLE_CODE logic (infers a likely CPT code from
--      modality/body-part/contrast/views when no code is embedded in the
--      study_name itself)
--
-- BUG FIX: rad_updated_gold.sql used TRIM(expr, ',') -- the 2-positional-
-- argument form -- which is not valid syntax on this DB (confirmed: throws
-- a 1064 syntax error). Replaced with the correct TRIM(BOTH ',' FROM expr)
-- form, matching what Rad.sql already used correctly. Two occurrences
-- fixed: the extracted_codes computation and the proc_code_std computation
-- inside cpt_std.
--
-- Verified 2026-08-20 against the live DB: resolves 519/537 (96.6%) of the
-- previously-unstandardized study_name values on at least one of
-- img_modality_std/img_body_part_std/cpt_code_std; 333/537 (62.0%) resolve
-- fully on both modality and body part. 18 values remain unresolved --
-- see rad_standardized_values.xlsx / prior QC conversation for that list.
--
-- No ALTER TABLE / write access assumed -- this is a read-only SELECT
-- (optionally wrap in CREATE VIEW if you have that privilege; see the
-- commented CREATE VIEW line near the bottom).
-- ============================================================================

WITH base AS (
    SELECT
        study_name,
        TRIM(BOTH ',' FROM CONCAT_WS(',',
            IF(study_name REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, 'CPT\\s*[0-9]{5}'), '[^0-9]', ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b' AND study_name REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 1), '[^0-9]', ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 2), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 3), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 4), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 5), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)' AND study_name REGEXP '[A-Za-z]+[0-9]{6,}',
                REGEXP_SUBSTR(study_name, '[A-Za-z]+[0-9]{6,}'), NULL),
            NULLIF(REGEXP_SUBSTR(study_name, '\\b[A-Za-z][0-9]{4}\\b'), '')
        )) AS extracted_codes
    FROM (
        SELECT
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(study_name,
                                '([0-9]{5})\\s+[Oo][Rr]\\s+([0-9]{5})', '\\1,\\2'),
                            '([0-9]{5})\\s*/\\s*([0-9]{5})', '\\1,\\2'),
                        '([0-9]{5})\\s*\\+\\s*([0-9]{5})', '\\1,\\2'),
                    '([0-9]{5})\\s*-\\s*([0-9]{5})', '\\1,\\2'),
                '([0-9]{5})\\s*&\\s*([0-9]{5})', '\\1,\\2')
            AS study_name
        FROM (SELECT DISTINCT study_name FROM rgd_gold_ad.radiology WHERE study_name IS NOT NULL) src
    ) normalized
),

code_lookup AS (
    -- extracted_codes is a short comma-joined list (in practice almost always
    -- 1, occasionally 2, rarely more) of candidate codes for a study_name.
    -- JSON_TABLE()/UNNEST-style "explode to rows" isn't relied on here for
    -- portability; instead each of the first 4 comma-separated positions is
    -- pulled out with SUBSTRING_INDEX (same technique already used later in
    -- this file for CPT_PROBABLE_CODE) and matched independently. Extend the
    -- pattern with a code_5 / code_6 pair of joins below if you ever see
    -- extracted_codes legitimately carrying more than 4 values.
    SELECT
        b.study_name,
        b.extracted_codes,
        NULLIF(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(b.extracted_codes, ',,,,'), ',', 1), ',', -1), '') AS code_1,
        NULLIF(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(b.extracted_codes, ',,,,'), ',', 2), ',', -1), '') AS code_2,
        NULLIF(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(b.extracted_codes, ',,,,'), ',', 3), ',', -1), '') AS code_3,
        NULLIF(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(b.extracted_codes, ',,,,'), ',', 4), ',', -1), '') AS code_4
    FROM base b
),

cpt_std AS (
    SELECT
        c.study_name,
        NULLIF(c.extracted_codes, '') AS extracted_codes,
        NULLIF(TRIM(BOTH ',' FROM CONCAT_WS(',',
            cpt1.PROCEDURECODE, hcpcs1.HCPC,
            cpt2.PROCEDURECODE, hcpcs2.HCPC,
            cpt3.PROCEDURECODE, hcpcs3.HCPC,
            cpt4.PROCEDURECODE, hcpcs4.HCPC
        )), '') AS proc_code_std
    FROM code_lookup c
    LEFT JOIN tncpa.PROCEDURECODEREFERENCE cpt1 ON c.code_1 = cpt1.PROCEDURECODE AND c.code_1 REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs               hcpcs1 ON c.code_1 = hcpcs1.HCPC     AND c.code_1 REGEXP '^[A-Za-z][0-9]{4}$'
    LEFT JOIN tncpa.PROCEDURECODEREFERENCE cpt2 ON c.code_2 = cpt2.PROCEDURECODE AND c.code_2 REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs               hcpcs2 ON c.code_2 = hcpcs2.HCPC     AND c.code_2 REGEXP '^[A-Za-z][0-9]{4}$'
    LEFT JOIN tncpa.PROCEDURECODEREFERENCE cpt3 ON c.code_3 = cpt3.PROCEDURECODE AND c.code_3 REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs               hcpcs3 ON c.code_3 = hcpcs3.HCPC     AND c.code_3 REGEXP '^[A-Za-z][0-9]{4}$'
    LEFT JOIN tncpa.PROCEDURECODEREFERENCE cpt4 ON c.code_4 = cpt4.PROCEDURECODE AND c.code_4 REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs               hcpcs4 ON c.code_4 = hcpcs4.HCPC     AND c.code_4 REGEXP '^[A-Za-z][0-9]{4}$'
),

study_std AS (
SELECT DISTINCT
    c.study_name,
    c.extracted_codes,
    c.proc_code_std,

    -- MODALITY STD
    -- FIX: every WHEN below is now evaluated against UPPER(c.study_name), matching the
    -- convention already used by strength_views_std / contrast_type_std / body_part_std /
    -- laterality_std / tracer_name_std. The original block compared the raw-case
    -- c.study_name, which silently failed to match on StarRocks (RE2 REGEXP is
    -- case-sensitive there, unlike MySQL's default collation) whenever a study_name
    -- used mixed/lower case (e.g. "Cervical Spine w/wo con MRI", "CT Head or Brain
    -- w/o Contrast", "US Carotid Doppler Bilateral").
    CASE
        -- NON-DIAGNOSTIC / ADMINISTRATIVE ENTRIES
        -- These are not real study descriptions (system placeholders, accession-number
        -- style identifiers, E&M billing text, or blank/free-text report labels) and
        -- will never resolve to a modality or body part no matter how the regex library
        -- is extended. Flagging them explicitly stops them from re-appearing in the
        -- "unstandardized" QC queue and documents *why* they're excluded.
        WHEN UPPER(c.study_name) REGEXP '^[0-9]{5,7}\\.[0-9]{3}[A-Z]{2,}$'                                                                                   THEN 'Non-Diagnostic / Administrative Entry'
        WHEN UPPER(c.study_name) REGEXP '^IDSSEND[0-9.]+[A-Z]*$|^RAM[0-9]+$|^VARPROC$'                                                                       THEN 'Non-Diagnostic / Administrative Entry'
        WHEN UPPER(c.study_name) REGEXP '^(UNKNOWN|TEST|LETTER|SCREENING|DEFAULT RADIOLOGY|DIAGNOSTIC STUDY RESULT|RADIOLOGY PROCEDURE|RADIOLOGY REPORT|IMAGING INTERPRETATION|SUPPLEMENTAL REPORT|UNLISTED IMAGING ORDER|BASELINE|SPECIMEN EXAM SURGICAL|UC PROVIDER RECORD DC INSTR|VIR CONSULTATION|VIR PROGRESS NOTES|GFR LABS|DOCTOR 60 MINUTES)$'
                                                                                                                                                              THEN 'Non-Diagnostic / Administrative Entry'
        WHEN UPPER(c.study_name) REGEXP '\\bMED\\s+DECISION\\b|\\bESTAB\\s+PT\\b|\\bNEW\\s+PT\\b'                                                            THEN 'Non-Diagnostic / Administrative Entry'
        WHEN UPPER(c.study_name) REGEXP '\\bCONS\\s+SEDATION\\b|\\bCONS\\s+SEDA\\b'                                                                          THEN 'Non-Diagnostic / Administrative Entry'

        -- CARDIAC MONITORING / DIAGNOSTIC DEVICES (billed under Radiology/Cardiology but not an imaging exam)
        WHEN UPPER(c.study_name) REGEXP '\\bHOLTER\\b|\\bCARDIAC\\s+(EVENT|MOBILE)\\s+(TELEMETRY\\s+)?MONITOR\\b|\\bLOOP\\s+RECORDER\\b|\\bICD\\s+MONITORING\\b|\\bLONG-TERM\\s+CONTINUOUS\\s+MONITOR\\b|\\bMOBILE\\s+CARDIAC\\s+TELEMETRY\\b|\\bTELEMETRY/EVENT\\s+MONITOR\\b|\\bEVENT\\s+MONITOR\\b|\\bPACEMAKER\\b|\\bTILT\\s+TABLE\\b|\\bSTRESSTEST\\b|\\bREMOTE\\s+BLOOD\\s+PRESSURE\\s+MONITORING\\b'
                                                                                                                                                              THEN 'Cardiac Monitoring'
        WHEN UPPER(c.study_name) REGEXP '\\bCARDIOLITE\\b'                                                                                                   THEN 'Nuclear Medicine'

        -- SLEEP STUDIES
        WHEN UPPER(c.study_name) REGEXP '\\bPOLYSOMNOGRA(M|PHY)\\b|\\bSLEEP\\s+STUDY\\b|\\bPAP\\s+TITRATION\\b|\\bMULTIPLE\\s+SLEEP\\s+LATENCY\\b|\\bUNATTENDED\\s+SLEEP\\b'
                                                                                                                                                              THEN 'Polysomnography'

        -- ELECTRODIAGNOSTIC / NEUROPHYSIOLOGY STUDIES (distinct from EEG/ECG below)
        WHEN UPPER(c.study_name) REGEXP '\\bNERVE\\s+CONDUCTION\\b|\\bELECTROMYOGRAM\\b|\\bEMG\\b|\\bELECTROPHYSIOLOGY\\b|\\bAUTONOMIC\\s+FUNCTION\\b|\\bVISUAL\\s+FIELD\\s+TEST\\b|\\bVIDEONYSTAGMOGRAPH\\b|\\bTYMPANOGRAM\\b|\\bOPTICAL\\s+COHERENCE\\s+TOMOGRAM\\b'
                                                                                                                                                              THEN 'Electrodiagnostic Study'

        -- Combined modalities
        WHEN UPPER(c.study_name) REGEXP '\\bPET/CT\\b|\\bPET CT\\b'                                                                                          THEN 'Positron Emission Tomography (PET) / Computed Tomography'
        WHEN UPPER(c.study_name) REGEXP '\\bXR/RF\\b'                                                                                                        THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN UPPER(c.study_name) REGEXP '\\bUS DOPPLER\\b|\\bUS DUPLEX\\b'                                                                                   THEN 'Ultrasound / Duplex Doppler'
        WHEN UPPER(c.study_name) REGEXP '\\bUS ECHOCARDIOGRAM\\b'                                                                                            THEN 'Ultrasound / Echocardiography'
        WHEN UPPER(c.study_name) REGEXP '\\bXA US\\b'                                                                                                        THEN 'X-Ray Angiography / Ultrasound'
        -- Single modalities
        WHEN UPPER(c.study_name) REGEXP '\\bCT\\b|\\bCAT\\b|\\bNCT\\b|\\bLDCT\\b|\\bCTA\\b|\\bCTV\\b|\\bCTAC\\b|\\bCTC\\b|\\bCTP\\b'                          THEN 'Computed Tomography'
        WHEN UPPER(c.study_name) REGEXP '\\bPET\\b'                                                                                                          THEN 'Positron Emission Tomography (PET)'
        -- \bPT\b with exclusion for lab coagulation tests and E&M billing codes
        WHEN UPPER(c.study_name) REGEXP '\\bPT\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bPT/|/PT\\b|\\bPT\\s+(PANEL|COAGULATION)|\\bPROTIME\\b|\\bINR\\b|\\bPTT\\b|\\bESTAB\\s+PT\\b|\\bNEW\\s+PT\\b|\\bMED\\s+DECISION\\b'
                                                                                                                                                              THEN 'Positron Emission Tomography (PET)'
        -- MRA before MRI/MR
        WHEN UPPER(c.study_name) REGEXP '\\bMRA\\b|\\bZZMRA\\b'                                                                                              THEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'
        WHEN UPPER(c.study_name) REGEXP '\\bMRI\\b|\\bMRV\\b|\\bMRCP\\b|\\b3TMRI\\b|\\bTMRI\\b|\\b3TMRA\\b|\\bMR\\b'                                          THEN 'Magnetic Resonance'
        WHEN UPPER(c.study_name) REGEXP '\\bDEXA\\b|\\bDXA\\b|\\bDEXASCAN\\b|\\bBONE\\s+MINERAL\\s+DENSITY\\b'                                                THEN 'Bone Densitometry (X-Ray)'
        WHEN UPPER(c.study_name) REGEXP '\\bMAM\\b|\\bMAMM\\b|\\bMAMMO\\b|\\bMMAMMO\\b|\\bMAMMOGRAM\\b|\\bMAMMOGRAPHY\\b|\\bFFD\\b'                            THEN 'Mammography'
        -- \bMG\b with exclusion for Myasthenia Gravis
        WHEN UPPER(c.study_name) REGEXP '\\bMG\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bMYASTHENIA\\b|\\bGRAVIS\\b|\\bEVALUATION\\b'                                                             THEN 'Mammography'
        WHEN UPPER(c.study_name) REGEXP '\\bUS\\b|\\bULTRASOUND\\b|\\bUSV\\b|\\bBI US\\b|\\bOB US\\b|\\bSONOGRAPHY\\b'                                        THEN 'Ultrasound'
        -- \bXA\b with exclusion for anti-Xa lab context
        WHEN UPPER(c.study_name) REGEXP '\\bXA\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bANTI-XA\\b|\\bANTI XA\\b|\\bHEPARIN\\b'                                                                  THEN 'X-Ray Angiography'
        WHEN UPPER(c.study_name) REGEXP '\\bANG\\b|\\bANGIO\\b'                                                                                              THEN 'X-Ray Angiography'
        WHEN UPPER(c.study_name) REGEXP '\\bCR\\b'                                                                                                           THEN 'Computed Radiography'
        WHEN UPPER(c.study_name) REGEXP '\\bDX\\b|\\bXR\\b|\\bX-RAY\\b|\\bXRAY\\b|\\bXRY\\b'                                                                 THEN 'Digital Radiography'
        -- \bDR\b with exclusion for HLA typing context
        WHEN UPPER(c.study_name) REGEXP '\\bDR\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bHLA\\b|\\bTYPING\\b|\\bDQ\\b|\\bDP\\b'                                                                   THEN 'Digital Radiography'
        -- \bRF\b with exclusion for Rheumatoid Factor lab context
        WHEN UPPER(c.study_name) REGEXP '\\bRF\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bRHEUMATOID\\b|\\bFACTOR\\b|\\bANTI-CCP\\b|\\bANA\\b|\\bSERUM\\b|\\bTITER\\b'                              THEN 'Radio Fluoroscopy'
        WHEN UPPER(c.study_name) REGEXP '\\bECG[0-9]*\\b|\\bEKG[0-9]*\\b|\\bELECTROCARDIOGRAM\\b|\\bELECTROCARDIOGRAPH\\b|\\bPED\\s+ECG\\b'                   THEN 'Electrocardiography'
        WHEN UPPER(c.study_name) REGEXP '\\bEEG\\b|\\bELECTROCEPHANLOGRAM\\b|\\bELECTROENCEPHALOGRAM\\b'                                                     THEN 'Electroencephalography'
        -- GI / genitourinary fluoroscopic studies frequently ordered without an explicit FL/FLUORO token
        WHEN UPPER(c.study_name) REGEXP '\\bBARIUM\\b|\\bESOPHAGRAM\\b|\\bESOPH\\s*\\(BA\\s*SWAL\\)\\b|\\bSWALLOW(ING)?\\s+(FUNCTION|STUDY)?\\b|\\bSWALL\\s*FUNC\\b|\\bVFSS\\b|\\bERCP\\b|\\bCYSTOGRA(M|PHY)\\b|\\bNEPHROSTOGRAM\\b|\\bEPIDUROGRAPHY\\b|\\bMYELOGRAM\\b|\\bUROGRAPHY\\b|\\bSHUNT SERIES\\b|\\bDILATION OF URETERS\\b'
                                                                                                                                                              THEN 'Radio Fluoroscopy'
        WHEN UPPER(c.study_name) REGEXP '\\bFL\\b|\\bFLUORO\\b|\\bFLU\\b|\\bFLUOROSCOPY\\b|\\bFLUOROSCOPIC\\b'                                                THEN 'Radio Fluoroscopy'
        WHEN UPPER(c.study_name) REGEXP '\\bECHO\\b|\\bECHOCARDIOGRAM\\b|\\bECHOCARDIOGRAPHY\\b|\\bTEE\\b|\\bTTE\\b'                                          THEN 'Echocardiography (EC - Retired) / Ultrasound'

        WHEN UPPER(c.study_name) REGEXP '\\bFS\\b'                                                                                                           THEN 'Fundoscopy (FS - Retired) / Ophthalmic Photography'
        WHEN UPPER(c.study_name) REGEXP '\\bNM\\b|\\bBONE\\s+SCAN\\b|\\bPHOSPHORUS\\s+THERAPY\\b|\\bRADIOPH(ARMACEUTICAL)?\\s+LOC\\b'                        THEN 'Nuclear Medicine'
        WHEN UPPER(c.study_name) REGEXP '\\bENDOSCOPY\\b|\\bEGD\\b'                                                                                          THEN 'Endoscopy'
        WHEN UPPER(c.study_name) REGEXP '\\bCD\\b'                                                                                                           THEN 'Color Flow Doppler (CD - Retired) / Ultrasound'
        WHEN UPPER(c.study_name) REGEXP '\\bTCD\\b|\\bDUPLEX\\b|\\bDOPPLER\\b'                                                                                THEN 'Duplex Doppler (DD - Retired) / Ultrasound'
        WHEN UPPER(c.study_name) REGEXP '\\bAUDIO\\b|\\bAUDIOMETRY\\b|\\bAUDITORY\\b|\\bHEARING\\b|\\bAUDIOGRAM\\b|\\bACOUSTIC\\b'                            THEN 'Audio'
        WHEN UPPER(c.study_name) REGEXP '\\bRP\\b'                                                                                                           THEN 'Radiotherapy Plan'
        -- \bRT\b with exclusion for renal/lab context
        WHEN UPPER(c.study_name) REGEXP '\\bRT\\b'
             AND UPPER(c.study_name) NOT REGEXP '\\bCREATININE\\b|\\bCREAT\\b|\\bRENAL\\b|\\bKIDNEY\\b'                                                      THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        -- Interventional / vascular access procedures that lack RAD/IR/RT tokens
        WHEN UPPER(c.study_name) REGEXP '\\bRAD\\b|\\bIR\\b|\\bINTERVENTIONAL RADIOLOGY\\b|\\bPICC\\b|\\bMIDLINE CATHETER\\b|\\bPORT REMOVAL\\b|\\bIVC FILTER\\b|\\bINVASIVE VASCULAR\\b|\\bRFA\\b|\\bVENASEAL\\b|\\bIVUS\\b|\\bPHLEB\\b|\\bSTENT CONSULT\\b|\\bTHORACENTES\\b|\\bBLOOD PATCH\\b|\\bEPIDURAL STEROID\\b|\\bANES NERVE BLOCK\\b|\\bPARAVERT\\b|\\bEPIDRL\\b|\\bNEURLYTC\\b|\\bSPECIAL PROCEDURE\\b|\\bLUMBAR PUNCTURE\\b'
                                                                                                                                                              THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN UPPER(c.study_name) REGEXP '\\bSPECT\\b'                                                                                                        THEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine'
        WHEN UPPER(c.study_name) REGEXP '\\bBX\\b|\\bBIOPSY\\b|\\bVL\\b|\\bOHS\\b|\\bI-123\\b|\\b1-131\\b|\\bMPI\\b'                                          THEN 'Other'
        -- LAST-RESORT FALLBACK: plain-film series are frequently ordered as e.g.
        -- "ABDOMEN COMPLETE 2 VIEWS" / "FEMUR LEFT 2 VIEWS" / "HIP RIGHT 2 VIEWS" with no
        -- XR/CR/DX token at all. If nothing above matched but a view-count is present,
        -- it's conventionally a plain radiograph series.
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*(-\\s*[0-9]+)?\\+?\\s*VIEWS?\\b|\\bMINIMUM\\s+[0-9]+\\s+VIEWS?\\b'                                        THEN 'Digital Radiography'
        ELSE NULL
    END AS modality_std,

    -- STRENGTH / VIEWS STD
    CASE
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\.?[0-9]*\\s*T\\b'
            AND UPPER(c.study_name) REGEXP 'W[/\\s]?WO|W\\s?&\\s?W/?O|W\\s?AND\\s?W/?O|WITHOUT/WITH|WITH/WITHOUT|WWO|WO/W|\\bWO\\b|\\bW/O\\b|\\bWITHOUT\\b|\\bWO CON\\b|\\bNCON\\b|\\bNO CON\\b|\\bWO C\\b|\\bWO CONTRAST\\b|\\bW CON\\b|\\bW CONTRAST\\b|\\bWITH CONTRAST\\b|\\bW C\\b|\\bW/\\b|\\bCON\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+\\.?[0-9]*(?=\\s*[Tt]\\b)'), 'T')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*V\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+(?=\\s*[Vv]\\b)'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*V\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*[Vv]\\b)'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*VIEW.*(\\+|AND).*[0-9]+-[0-9]+\\s*VIEW|[0-9]+\\s*VIEWS?.*\\+.*[0-9]+\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+|[0-9]+(?=\\s*VIEWS?)'), ' Views + ', REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*VIEW)', 1, 2), ' View')
        WHEN UPPER(c.study_name) REGEXP '[><=]{1,2}\\s*[0-9]+\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[><=]{1,2}'), REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '(MIN|MINIMUM)(\\s+OF)?\\s*(ONE|TWO|THREE|FOUR|FIVE|SIX|[0-9]+)\\s*(V\\b|VWS|VIEW|VIEWS)'
            THEN CONCAT('Min ', COALESCE(REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*(V\\b|VWS|VIEW|VIEWS))'),
                CASE
                    WHEN UPPER(c.study_name) REGEXP 'ONE\\s*(V|VIEW|VIEWS)'   THEN '1'
                    WHEN UPPER(c.study_name) REGEXP 'TWO\\s*(V|VIEW|VIEWS)'   THEN '2'
                    WHEN UPPER(c.study_name) REGEXP 'THREE\\s*(V|VIEW|VIEWS)' THEN '3'
                    WHEN UPPER(c.study_name) REGEXP 'FOUR\\s*(V|VIEW|VIEWS)'  THEN '4'
                END), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*[\\+]\\s*VIEWS?|[0-9]+\\s*PLUS\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), '+ Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*OR\\s*MORE\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' or More Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*OR\\s*[0-9]+\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' or ', REGEXP_SUBSTR(c.study_name, '[0-9]+', 1, 2), ' Views')
        WHEN UPPER(c.study_name) REGEXP '\\b(ONE|TWO|THREE|FOUR|FIVE|SIX)\\s*VIEWS?\\b'
            THEN CONCAT(
                CASE
                    WHEN UPPER(c.study_name) REGEXP '\\bONE\\s*VIEWS?'   THEN '1'
                    WHEN UPPER(c.study_name) REGEXP '\\bTWO\\s*VIEWS?'   THEN '2'
                    WHEN UPPER(c.study_name) REGEXP '\\bTHREE\\s*VIEWS?' THEN '3'
                    WHEN UPPER(c.study_name) REGEXP '\\bFOUR\\s*VIEWS?'  THEN '4'
                    WHEN UPPER(c.study_name) REGEXP '\\bFIVE\\s*VIEWS?'  THEN '5'
                    WHEN UPPER(c.study_name) REGEXP '\\bSIX\\s*VIEWS?'   THEN '6'
                END, ' Views')
        WHEN UPPER(c.study_name) REGEXP '(LESS\\s*THAN|<)\\s*[0-9]+\\s*(V\\b|VIEW|VIEWS)'
            THEN CONCAT('Less Than ', REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*(V\\b|VIEW|VIEWS))'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\+?\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' Views')
        ELSE NULL
    END AS strength_views_std,

    -- CONTRAST TYPE STD
    CASE
        WHEN UPPER(c.study_name) REGEXP 'W[/\\s]?WO|W\\s?&\\s?W/?O|W\\s?AND\\s?W/?O|W\\s?OR\\s?W/?O|WITH\\s?AND\\s?W/?O|WO\\+W|W\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\s?AND\\s?WOW|WO,\\s?W|W,\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\s?W/?O|WO\\s?W'
            THEN 'With and Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bWO\\b|\\bW/O\\b|\\bWITHOUT\\b|\\bWO CON\\b|\\bW/O CONTRAST\\b|\\bNCON\\b|\\bNO CON\\b|\\bWO C\\b|\\bWO CONTRAST\\b'
            THEN 'Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bW CON\\b|\\bW CONTRAST\\b|\\bWITH CONTRAST\\b|\\bW C\\b|\\bW/\\b|\\bCON\\b'
            THEN 'With Contrast'
        ELSE NULL
    END AS contrast_type_std,

    -- BODY PART STD
    CASE
        -- MULTI BODY PARTS FIRST
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD).*(PELVIS|PELV)|CHEST/ABD/PELVIS|CHEST ABDOMEN PELVIS|CHEST\\+ABD.*PELVIS'                                              THEN 'Chest, Abdomen, Pelvis'
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD)|CHEST\\+ABD'                                                                                                           THEN 'Chest, Abdomen'
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*THORAX|CHEST/THORAX'                                                                                                                 THEN 'Chest, Thorax'
        WHEN UPPER(c.study_name) REGEXP '(ABDOMEN|ABD).*(PELVIS|PELV)|(PELVIS|PELV).*(ABDOMEN|ABD)'                                                                                  THEN 'Abdomen, Pelvis'
        WHEN UPPER(c.study_name) REGEXP 'HEAD.*NECK|NECK.*HEAD|NECK/HEAD'                                                                                                            THEN 'Head, Neck'
        WHEN UPPER(c.study_name) REGEXP 'ORB.*FAC.*NCK|ORBIT.*FACE.*NECK|ORBIT/FACE/NK'                                                                                              THEN 'Orbit, Face, Neck'
        WHEN UPPER(c.study_name) REGEXP 'ORBIT.*SELLA|ORBIT\\+SELLA|ORBIT SELLA POSS|ORBIT\\+SELLA\\+PF'                                                                             THEN 'Orbit, Sella'
        WHEN UPPER(c.study_name) REGEXP 'CERVICOTHORACOLUMBAR|CERVICO.*THORACO.*LUMBAR'                                                                                              THEN 'Thoracic Spine, Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP 'THORACO.?LUMBAR|THORACOLUMBAR|THOR.*LUM[B]|THOR\\+LU[MB]'                                                                                   THEN 'Thoracic Spine, Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP 'LUMBOSACRAL PLEXUS'                                                                                                                         THEN 'Lumbar Spine, Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'LUMBO.?SACRAL|LUMB.?SACR|LUMBO SACRAL|LUMBOSACRAL'                                                                                          THEN 'Lumbar Spine, Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'SACRUM.*(AND|\\+|/).?COCCYX|SACRUM COCCYX|SACRUM/COCCYX'                                                                                     THEN 'Sacrum, Coccyx'
        WHEN UPPER(c.study_name) REGEXP 'FACIAL.*SINUS|SINUS.*FACIAL|FACIAL/SINUS|MAX.*FAC.*SIN|MAXILLOFACIAL|MAXIOFACIAL|MAXFACIAL|MAXILLA|SINUS FACIAL|MAX/FAC/SIN|MAXFACIAL BONES' THEN 'Facial Bones, Sinuses'
        WHEN UPPER(c.study_name) REGEXP 'TIB.?FIB|TIBIA.?FIBUL|TIBIA AND FIBULA|TIBIA/FIBULA|TIB & FIB|TIB\\+FIBULA|TIBIA\\+FIBULA|TM JOINTS|TIB\\s*\\\\T\\\\\\s*FIB|TIB\\s+T\\s+FIB' THEN 'Tibia, Fibula'
        WHEN UPPER(c.study_name) REGEXP 'FOREARM.*RADIUS|FOREARM.*ULNA|RADIUS.*ULNA|FOREARM/RADIUS'                                                                                   THEN 'Forearm, Radius, Ulna'
        WHEN UPPER(c.study_name) REGEXP 'CAROTID.*NECK|CAROTID/NECK|VASC CAROTID|CAROTIDS'                                                                                            THEN 'Carotid, Neck'
        WHEN UPPER(c.study_name) REGEXP 'THYROID.*NECK|THY.*NECK|THYROID/NECK'                                                                                                        THEN 'Thyroid, Neck'
        WHEN UPPER(c.study_name) REGEXP 'LIVER.*GALLBLADDER.*PANCREAS|LIVER GALLBLADDER PANCREAS'                                                                                     THEN 'Liver, Gallbladder, Pancreas'
        WHEN UPPER(c.study_name) REGEXP 'ILIUM.*STERNUM.*RIB|ILIUM STERNUM RIB'                                                                                                       THEN 'Ilium, Sternum, Rib'
        WHEN UPPER(c.study_name) REGEXP 'A-?C JOINTS|ACROMIOCLAVICULAR JOINTS|STERNOCLAVIC|STERNOCLAVICULAR'                                                                          THEN 'Acromioclavicular Joints'
        WHEN UPPER(c.study_name) REGEXP '\\bTMJ\\b|TEMPOROMANDIBULAR|TEMPOROMANDIBULAR JOINT|TMJ BILATERAL'                                                                           THEN 'Temporomandibular Joint'
        WHEN UPPER(c.study_name) REGEXP 'VASC EXT LOWR|VASC EXTREMITY LOWER|LOWER EXTREMITY VENOUS|LOWER EXT VENOUS|LOWER EXTREMITY ARTERIES|LOWER EXT ARTERIAL|ARTERIAL LOW.*EXT|ARTERIAL LOWER EXT|ARTERIAL LOWER EXTREMITY|LOWER EXTREMITY ARTERI' THEN 'Lower Extremity (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'UPPER EXTREMITY VENOUS|UPPER EXT.*ARTERIAL|ARTERIAL UPPER EXT|ARTERIAL UPPER EXTREMITY|UPPER EXTREMITY ARTERIAL|LT UPPER VENOUS|UPPER OR LOWER EXT ARTERIAL' THEN 'Upper Extremity (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'VASC TRANSCRANIAL|TRANS CRANIAL|TRANSCRANIAL|VASC JUGULAR|JUGULAR.*SUBCLAVIAN'                                                               THEN 'Transcranial (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'CEREBRAL ARTERIES|EXTRACRANIAL ARTERIES'                                                                                                     THEN 'Cerebral Arteries'
        WHEN UPPER(c.study_name) REGEXP '\\bLOWER EXTREMIT|\\bLOWER EXT\\b|\\bLOWER EXTR\\b|\\bLWR EXT\\b|\\bLE\\b|\\bLOW EXT\\b|\\bLEFT LOWER EXTREMITY\\b|\\bRIGHT LOWER EXTREMITY\\b|LOWER EXT.*NOT.*JNT|LOWER EXT.*NON|LWR EXT NOT JT|LOWER LEG|LOWER BACK|EXTREMITY LOWER|EXTREMITY.*LOWER' THEN 'Lower Extremity'
        WHEN UPPER(c.study_name) REGEXP '\\bUPPER EXTREMIT|\\bUPPER EXT\\b|\\bUPR.*EXT\\b|\\bUPR/LXTR\\b|UP EXT JT|LEFT EXTREMITY.*UPPER|UPPER EXT.*JOINT|UPPER EXT NON JOINT|UPPER EXTREMITY.*MUSCULO|EXTREMITY UPPER|EXTREMITY.*UPPER|LEFT EXTREMITY JOINT UPPER|RIGHT EXTREMITY JOINT LOWER' THEN 'Upper Extremity'
        WHEN UPPER(c.study_name) REGEXP 'BRACHIAL PLEXUS|RIGHT BRACHIAL PLEXUS|\\bBRACHPLEX\\b'                                                                                       THEN 'Brachial Plexus'
        WHEN UPPER(c.study_name) REGEXP 'SPINAL CANAL|SPINAL CORD|THORACIC SPINAL CORD|SPINAL CORD DORSAL'                                                                            THEN 'Spinal Canal'
        WHEN UPPER(c.study_name) REGEXP 'SACROILIAC|SACROILIAC JOINT|SACROILIAC JNTS|S-?I-?\\s*JOINTS?'                                                                               THEN 'Sacroiliac Joint'
        WHEN UPPER(c.study_name) REGEXP '\\bKUB\\b'                                                                                                                                   THEN 'Kidney, Ureter, Bladder'
        WHEN UPPER(c.study_name) REGEXP '\\bBLADDER\\b|\\bCYSTOGRA(M|PHY)\\b|\\bCYSTOGRAM\\b'                                                                                         THEN 'Bladder'
        -- SINGLE BODY PARTS
        WHEN UPPER(c.study_name) REGEXP '\\bABDOMEN\\b|\\bABD\\b|\\bABDOMINAL\\b'                                                                                                     THEN 'Abdomen'
        WHEN UPPER(c.study_name) REGEXP '\\bANKLE\\b|\\bANK\\b'                                                                                                                       THEN 'Ankle'
        WHEN UPPER(c.study_name) REGEXP '\\bAORTA\\b|\\bTHORACIC AORTA\\b'                                                                                                            THEN 'Aorta'
        WHEN UPPER(c.study_name) REGEXP '\\bARTERY\\b|\\bARTERIAL\\b'                                                                                                                 THEN 'Arterial'
        WHEN UPPER(c.study_name) REGEXP 'AUDITORY CANAL'                                                                                                                              THEN 'Auditory Canal'
        WHEN UPPER(c.study_name) REGEXP 'AXIAL SKELETON'                                                                                                                              THEN 'Axial Skeleton'
        WHEN UPPER(c.study_name) REGEXP '\\bBONE\\b'                                                                                                                                  THEN 'Bone'
		WHEN UPPER(c.study_name) REGEXP '\\bBRAIN\\s*STEM\\b|\\bBRN\\s*STEM\\b|\\bBRAINSTEM\\b'                                                                                       THEN 'Brain Stem'

        WHEN UPPER(c.study_name) REGEXP '\\bBRAIN\\b|\\bBRIAN\\b|\\bBRIN\\b'                                                                                                          THEN 'Brain'
        WHEN UPPER(c.study_name) REGEXP '\\bIAC\\b|\\bIACS\\b|INTERNAL AUDITORY|AUDITORY MEATUS|AUDITORY CANAL MRI'                                                                   THEN 'Internal Auditory Canal'
        -- CN V — Trigeminal nerve
        WHEN UPPER(c.study_name) REGEXP '\\bTRIGEMINAL\\b|\\b5TH.*NERVE\\b|\\bCN\\s*V\\b|\\bCRANIAL NERVE\\s*5\\b'                                                                    THEN 'Trigeminal Nerve'
        -- CN VII — Facial nerve
        WHEN UPPER(c.study_name) REGEXP '\\b7TH.*NERVE\\b|\\bCN\\s*VII\\b|\\bCRANIAL NERVE\\s*7\\b|\\bFACIAL NERVE\\b'                                                                THEN 'Facial Nerve'
        -- CN VIII — Acoustic nerve
        WHEN UPPER(c.study_name) REGEXP '\\b8TH.*NERVE\\b|\\bCN\\s*VIII\\b|\\bCRANIAL NERVE\\s*8\\b|\\bACOUSTIC NEUROMA\\b|\\bVESTIBULOCOCHLEAR\\b'                                   THEN 'Acoustic Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bCARDIAC\\b|\\bCARDIOVASC\\b'                                                                                                              THEN 'Heart'
        WHEN UPPER(c.study_name) REGEXP '\\bSACRAL PLEXUS\\b'                                                                                                                         THEN 'Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'MUSCLE.*\\bUE\\b|MUSCLE.*UPPER|MUSCLE.*\\bUPR\\b'                                                                                            THEN 'Upper Extremity'
        WHEN UPPER(c.study_name) REGEXP 'MUSCLE.*\\bLE\\b|MUSCLE.*LOWER|MUSCLE.*\\bLWR\\b'                                                                                            THEN 'Lower Extremity'
        WHEN UPPER(c.study_name) REGEXP 'VISUAL EVOKED|EVOKED.*VISUAL|EVOKED.*POTENTIAL.*VIS'                                                                                         THEN 'Eye / Orbit'
       
										
        -- L-SPINE, T-SPINE shorthand before generic SPINE
        WHEN UPPER(c.study_name) REGEXP '\\bL-SPINE\\b|\\bL SPINE\\b|\\bLS-SPINE\\b|\\bL-S SPINE\\b|\\bLS SPINE\\b'                                                                   THEN 'Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bT-SPINE\\b|\\bT SPINE\\b|\\bThoracic SPINE\\b'                                                                                            THEN 'Thoracic Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bBREAST\\b|\\bBREASTS\\b'                                                                                                                  THEN 'Breast'
        WHEN UPPER(c.study_name) REGEXP '\\bCALF\\b'                                                                                                                                  THEN 'Calf'
        WHEN UPPER(c.study_name) REGEXP '\\bCAROTID\\b|\\bCAROTIDS\\b'                                                                                                                THEN 'Carotid'
        WHEN UPPER(c.study_name) REGEXP '\\bCERVICAL SPINE\\b|\\bSPINE CERVICAL\\b|\\bC-SPINE\\b|\\bC[- ]?SP\\b|\\bCSP\\b|\\bC-SPINEL\\b'                                             THEN 'Cervical Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bCERVICAL\\b'                                                                                                    THEN 'Cervical'
        WHEN UPPER(c.study_name) REGEXP '\\bCHEST\\b|\\bPA CHEST\\b|\\bCHEST PA\\b|\\bTHORAX\\b|\\bRIBS\\b|\\bPNEUMOTHORAX\\b|\\bTHORACENTESIS\\b'       THEN 'Chest'
        WHEN UPPER(c.study_name) REGEXP '\\bCLAVICLE\\b'                                                                                                    THEN 'Clavicle'
        WHEN UPPER(c.study_name) REGEXP '\\bCOCCYX\\b'                                                                                                      THEN 'Coccyx'
        WHEN UPPER(c.study_name) REGEXP '\\bCOLON\\b|\\bLARGE INTESTINE\\b'                                                                                 THEN 'Colon'
        WHEN UPPER(c.study_name) REGEXP 'CRANIAL NERVE'                                                                                                      THEN 'Cranial Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bEAR\\b'                                                                                                         THEN 'Ear'
        WHEN UPPER(c.study_name) REGEXP '\\bELBOW\\b|\\bELB\\b'                                                                                             THEN 'Elbow'
        WHEN UPPER(c.study_name) REGEXP '\\bESOPHAGUS\\b|\\bTRANSESOPHAGEAL\\b'                                                                            THEN 'Esophagus'
        WHEN UPPER(c.study_name) REGEXP '\\bEXTRACRANIAL\\b|\\bEXTRACRAN\\b'                                                                               THEN 'Extracranial'
        WHEN UPPER(c.study_name) REGEXP '\\bEYE\\b|\\bORBIT\\b|\\bORBITS\\b|\\bORB\\b|OPTIC NERVE'                                                         THEN 'Eye / Orbit'
        WHEN UPPER(c.study_name) REGEXP '\\bFACE\\b|\\bFACIAL\\b|\\bFACIAL BONES\\b'                                                                       THEN 'Face'
        WHEN UPPER(c.study_name) REGEXP '\\bFEMUR\\b|\\bFEM\\b|\\bLATERAL FEMORAL\\b'                                                                       THEN 'Femur'
        WHEN UPPER(c.study_name) REGEXP '\\bFINGERS\\b|\\bFINGER\\b'                                                                                        THEN 'Fingers'
        WHEN UPPER(c.study_name) REGEXP '\\bFOOT\\b|\\bFEET\\b|\\bFT\\b|\\bHEEL\\b'                                                                        THEN 'Foot'
        WHEN UPPER(c.study_name) REGEXP '\\bFOREARM\\b|\\bFORE\\b'                                                                                          THEN 'Forearm'
        WHEN UPPER(c.study_name) REGEXP '\\bGALLBLADDER\\b'                                                                                                 THEN 'Gallbladder'
        WHEN UPPER(c.study_name) REGEXP '\\bGI\\b|\\bUGI\\b|\\bGASTRIC\\b|\\bGASTROINTESTINAL\\b|\\bSMALL INTESTINE\\b|\\bSTOMACH\\b|\\bBOWEL\\b'         THEN 'Gastric / GI'
        WHEN UPPER(c.study_name) REGEXP 'GREATER OCCIPITAL|OCCIPITAL'                                                                                        THEN 'Occipital'
        WHEN UPPER(c.study_name) REGEXP '\\bGROIN\\b|\\bILIOINGUINAL\\b'                                                                                    THEN 'Groin'
        WHEN UPPER(c.study_name) REGEXP '\\bHAND\\b|\\bHANDS\\b'                                                                                            THEN 'Hand'
        WHEN UPPER(c.study_name) REGEXP '\\bHEAD\\b|\\bORBITS\\b'                                                                                           THEN 'Head'
        WHEN UPPER(c.study_name) REGEXP '\\bHEART\\b|\\bTRANSTHORACIC\\b'                                                                                  THEN 'Heart'
        WHEN UPPER(c.study_name) REGEXP '\\bHIP\\b|\\bHIPS\\b'                                                                                              THEN 'Hip'
        WHEN UPPER(c.study_name) REGEXP '\\bHUMERUS\\b|\\bHUM\\b|\\bUPPER ARM\\b'                                                                           THEN 'Humerus / Upper Arm'
        WHEN UPPER(c.study_name) REGEXP '\\bINTRACRANIAL\\b|\\bINTRACRAN\\b'                                                                               THEN 'Intracranial'
        WHEN UPPER(c.study_name) REGEXP '\\bKIDNEY\\b|\\bKIDNEYS\\b|\\bRENAL\\b'                                                                           THEN 'Kidney'
        WHEN UPPER(c.study_name) REGEXP '\\bKNEE\\b|\\bKNEES\\b|\\bKN\\b'                                                                                  THEN 'Knee'
        WHEN UPPER(c.study_name) REGEXP '\\bLEG\\b|\\bLOWER LEG\\b'                                                                                         THEN 'Leg'
        WHEN UPPER(c.study_name) REGEXP '\\bLIVER\\b'                                                                                                        THEN 'Liver'
        WHEN UPPER(c.study_name) REGEXP 'LUMBAR PLEXUS|LUMPLEX'                                                                                              THEN 'Lumbar Plexus'
        WHEN UPPER(c.study_name) REGEXP '\\bLUMBAR SPINE\\b|\\bSPINE LUMBAR\\b|\\bLUMBOSACRAL\\b|\\bLUMOSACRAL\\b'                                        THEN 'Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bLUMBAR\\b'                                                                                                       THEN 'Lumbar'
        WHEN UPPER(c.study_name) REGEXP '\\bLUNG\\b'                                                                                                         THEN 'Lung'
        WHEN UPPER(c.study_name) REGEXP 'LYMPH NODE'                                                                                                          THEN 'Lymph Node'
        WHEN UPPER(c.study_name) REGEXP '\\bMANDIBLE\\b'                                                                                                    THEN 'Mandible'
        WHEN UPPER(c.study_name) REGEXP '\\bMASTOIDS\\b|\\bMASTOID\\b'                                                                                      THEN 'Mastoids'
        WHEN UPPER(c.study_name) REGEXP '\\bNECK\\b|\\bNECK SOFT TISSUE\\b|\\bTHROAT\\b'                                                                   THEN 'Neck'
        WHEN UPPER(c.study_name) REGEXP '\\bPANCREAS\\b'                                                                                                    THEN 'Pancreas'
        WHEN UPPER(c.study_name) REGEXP '\\bPARATHYROID\\b'                                                                                                 THEN 'Parathyroid'
        WHEN UPPER(c.study_name) REGEXP '\\bPELVIS\\b|\\bPELVIC\\b'                                                                                         THEN 'Pelvis'
        WHEN UPPER(c.study_name) REGEXP '\\bPITUITARY\\b|\\bPITUITARY GLAND\\b|\\bSELLA TURCICA\\b|\\bSELLA\\b'                                            THEN 'Pituitary'
        WHEN UPPER(c.study_name) REGEXP '\\bPROSTATE\\b|\\bRECTAL\\b'                                                                                       THEN 'Prostate / Rectal'
        WHEN UPPER(c.study_name) REGEXP '\\bRETROPERITONEUM\\b'                                                                                             THEN 'Retroperitoneum'
        WHEN UPPER(c.study_name) REGEXP '\\bSACRUM\\b'                                                                                                       THEN 'Sacrum'
        WHEN UPPER(c.study_name) REGEXP '\\bSCAPULA\\b|\\bSCAP\\b'                                                                                          THEN 'Scapula'
        WHEN UPPER(c.study_name) REGEXP '\\bSCOLIOSIS\\b'                                                                                                   THEN 'Scoliosis'
        WHEN UPPER(c.study_name) REGEXP '\\bSCROTAL\\b|\\bSCROTUM\\b|\\bTESTICULAR\\b|\\bTESTICLE\\b|\\bTESTES\\b'                                        THEN 'Scrotal / Testicular'
        WHEN UPPER(c.study_name) REGEXP '\\bSHOULDER\\b|\\bSH\\b|UPPER EXT JOINT SHOULDER|\\bSHOULDERS\\b'                                                 THEN 'Shoulder'
        WHEN UPPER(c.study_name) REGEXP '\\bSINUS\\b|\\bSINUSES\\b|\\bNASAL\\b|\\bSINUS/NASAL\\b'                                                          THEN 'Sinuses'
        WHEN UPPER(c.study_name) REGEXP '\\bSKULL\\b'                                                                                                        THEN 'Skull'
        WHEN UPPER(c.study_name) REGEXP 'SPINAL CORD DORSAL'                                                                                                  THEN 'Spinal Cord'
        WHEN UPPER(c.study_name) REGEXP '\\bSPINE\\b|\\bSCPINE\\b'                                                                                          THEN 'Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bSPLEEN\\b'                                                                                                       THEN 'Spleen'
        WHEN UPPER(c.study_name) REGEXP '\\bSTERNUM\\b'                                                                                                      THEN 'Sternum'
        WHEN UPPER(c.study_name) REGEXP '\\bTEETH\\b'                                                                                                     THEN 'Teeth'
        WHEN UPPER(c.study_name) REGEXP '\\bTHUMB\\b'                                                                                                     THEN 'Thumb'

        WHEN UPPER(c.study_name) REGEXP 'TEMPORAL BONE'                                                                                                       THEN 'Temporal Bone'
        WHEN UPPER(c.study_name) REGEXP '\\bTHIGH\\b|\\bTHIGHS\\b'                                                                                          THEN 'Thigh'
        WHEN UPPER(c.study_name) REGEXP '\\bTHORACIC\\b'                                                                                                     THEN 'Thoracic'
        WHEN UPPER(c.study_name) REGEXP '\\bTHYROID\\b'                                                                                                      THEN 'Thyroid'
        WHEN UPPER(c.study_name) REGEXP '\\bTOES\\b|\\bTOE\\b'                                                                                              THEN 'Toes'
        WHEN UPPER(c.study_name) REGEXP '\\bTORSO\\b|\\bPE TORSO\\b'                                                                                        THEN 'Torso'
        WHEN UPPER(c.study_name) REGEXP 'TRANSVAGINAL|TRANS-VAGINAL|TRANS VAGINAL'                                                                           THEN 'Transvaginal'
        WHEN UPPER(c.study_name) REGEXP '\\bUTERUS\\b'                                                                                                       THEN 'Uterus'
        WHEN UPPER(c.study_name) REGEXP 'VAGUS NERVE'                                                                                                         THEN 'Vagus Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bVEINS\\b|\\bVENOUS\\b'                                                                                          THEN 'Veins'
        WHEN UPPER(c.study_name) REGEXP 'WHOLE BODY'                                                                                                          THEN 'Whole Body'
        WHEN UPPER(c.study_name) REGEXP '\\bWRIST\\b|\\bWRISTS\\b|\\bWR\\b'                                                                                 THEN 'Wrist'
        ELSE NULL
    END AS body_part_std,

    -- LATERALITY STD
    CASE
        WHEN UPPER(c.study_name) REGEXP '\\bBILATERAL\\b'         THEN 'Bilateral'
        WHEN UPPER(c.study_name) REGEXP '\\bUNILATERAL\\b'        THEN 'Unilateral'
        WHEN UPPER(c.study_name) REGEXP '\\bLEFT\\b|\\bLT\\b'     THEN 'Left'
        WHEN UPPER(c.study_name) REGEXP '\\bRIGHT\\b|\\bRT\\b'    THEN 'Right'
        ELSE NULL
    END AS laterality_std,

    -- TRACER NAME STD
    CASE
        WHEN UPPER(c.study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\[18F\\]\\s*FLORBETAPIR'    THEN 'Florbetapir F18 (Amyvid)'
        WHEN UPPER(c.study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\[18F\\]\\s*FLUTEMETAMOL' THEN 'Flutemetamol F18 (Vizamyl)'
        WHEN UPPER(c.study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\s*FLORBETABEN|18F-?FLORBETABEN|\\[18F\\]\\s*FLORBETABEN'    THEN 'Florbetaben F18 (Neuraceq)'
        WHEN UPPER(c.study_name) REGEXP '\\bPIB\\b|PITTSBURGH\\s*COMPOUND|F-?18\\s*PIB|18F-?PIB'                                       THEN 'PiB F18 (Pittsburgh Compound-B)'
        WHEN UPPER(c.study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'                                                          THEN 'Piflufolastat F18 (Pylarify)'
        WHEN UPPER(c.study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'                                                       THEN 'Flotufolastat F18 (Posluma)'
        WHEN UPPER(c.study_name) REGEXP '\\bPSMA\\b' AND UPPER(c.study_name) REGEXP 'F-?18|\\bF18\\b|\\[18F\\]|18F-?'                 THEN 'F18 - PSMA Tracer (Unspecified)'
        WHEN UPPER(c.study_name) REGEXP '\\bFDG\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'                                  THEN 'FDG F18 (Fluorodeoxyglucose)'
        WHEN UPPER(c.study_name) REGEXP 'SODIUM\\s*FLUORIDE|\\bNAF\\b|A9580|F-?18\\s*FLUORIDE|18F-?FLUORIDE|18F-?NAF'                 THEN 'Sodium Fluoride F18 (NaF)'
        WHEN UPPER(c.study_name) REGEXP '\\bFDOPA\\b|FLUORODOPA|FLUORO-?DOPA|A9600|F-?18\\s*FDOPA|18F-?FDOPA|18F-?DOPA'               THEN 'Fluorodopa F18 (FDOPA)'
        WHEN UPPER(c.study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\bFACBC\\b'                                                        THEN 'Fluciclovine F18 (Axumin)'
        WHEN UPPER(c.study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\s*PET'                                                         THEN 'Flortaucipir F18 (Tauvid)'
        WHEN UPPER(c.study_name) REGEXP '\\bF-?18\\b|\\[18F\\]|18F-?|\\bF18\\b'                                                       THEN 'F18 - Tracer Not Specified'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*DOTATATE|NETSPOT|\\bDOTATATE\\b'                                                    THEN 'Ga68-DOTATATE (Netspot)'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*DOTATOC|\\bDOTATOC\\b'                                                             THEN 'Ga68-DOTATOC'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*PSMA|ILLUCCIX|LOCAMETZ|\\bPSMA-?11\\b'                                             THEN 'Ga68-PSMA-11 (Illuccix/Locametz)'
        WHEN UPPER(c.study_name) REGEXP '\\bGA-?68\\b|\\[68GA\\]|68GA-?|\\bGALLIUM\\s*68\\b|\\bGALLIUM-?68\\b'                       THEN 'Ga68 - Tracer Not Specified'
        ELSE NULL
    END AS tracer_name_std

FROM cpt_std c
),

cpt_clean AS (
    SELECT distinct PROCEDURECODE, COMMONDESCRIPTION, UPPER(COMMONDESCRIPTION) AS DESC_U
    FROM tncpa.PROCEDURECODEREFERENCE
    WHERE
        (
            UPPER(COMMONDESCRIPTION) REGEXP '\\bMRI\\b|\\bMR\\b|FMRI|QMRCP|MAGNETIC RESONANCE'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bCT\\b|\\bCAT SCAN\\b|COMPUTED TOMOGRAPHY'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bPET\\b|POSITRON'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bUS\\b|ULTRASOUND|ULTRASONOGRAPHY|SONOGRAM|SONOGRAPHY'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bECHO\\b|ECHOCARDIOGRAPHY|ECHOCARDIOGRAM'
         OR UPPER(COMMONDESCRIPTION) REGEXP 'MAMMO|MAMMOGRAM|MAMMOGRAPHY'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bDXA\\b|BONE DENSIT|BONE MINERAL'
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bXR\\b|X-?RAY|RADIOGRAPH|RADIOGRAM|RADIOLOGIC EXAM'
         OR UPPER(COMMONDESCRIPTION) REGEXP 'FLUORO|FLUOROSCOP'
         OR (UPPER(COMMONDESCRIPTION) REGEXP 'ANGIOGRAPH|ARTERIOGRAM|VENOGRAM|\\bANGIO\\b'
             AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANGIOPLAST|ANGIOPLST|ANGIOSCOP|ATHERECT|STENT|REVSC|REVASC|BALO ANGIO|BALLO ANGIO|ANGIO-SEAL|ANGIO-JET|ANGIOSTOMY|CATH/ANGIO')
         OR UPPER(COMMONDESCRIPTION) REGEXP '\\bSPECT\\b|SINGLE PHOTON'
         OR (UPPER(COMMONDESCRIPTION) REGEXP 'NUCLEAR MEDICINE|NUCLEAR MED|NUCLEAR IMAG|NUCLEAR EXAM|NUCLEAR LOCALIZ|NUCLEAR SCAN|NUCLEAR TX|NUCLEAR RX|NUCLEAR THERAPY|RADIONUCLIDE|SCINTIGRAPHY|SCINTIMAMMO|PLANAR IMAG|PLANAR W/|PLANAR SING|PLANAR MULT|GATED HEART PLANAR|TUMOR IMAGING|MYOCRD IMG'
             AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANTINUCLEAR|NUCLEAR ANTI|MONONUCLEAR|NUCLEAR MATRIX|NUCLEAR CELL|EPSTEIN.*NUCLEAR|PLANAR BACK|PLANAR SEAT|NONHEMATO NUCLEAR')
        )
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'INJECTION[,\\s]|\\bINJ[,\\s]|\\bINJ\\.|\\bINJ$|ORAL[,\\s]|INFUSION'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP '\\bMG\\b|\\bML\\b|\\bIU\\b|MG/|MCG|\\bUNIT\\b'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANTIBOD|ANTIGEN|ASSAY|\\bANA\\b|EPSTEIN|MATRIX PROTEIN|MONONUCLEAR'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'KNEE-SHIN|KNEE DISART|MYOELECTRON|BRACHYTX|SWITCH CT|GREIFER|ULTRA-LIGHT'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'PT DOC|NO DOC|DOC RSN|CLIN DOC|NOT PERF|CARE DOC|PT NOT DOC|PT INELIG|CLIN NOT|DOC PT|PT RECEIV|PT REAS|PT MBHT|MED REAS|MEDRSN|SRCH FOR|NO SRCH|DOC SCR|NO SCR|PHODOC|PT W/DXA'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'DEXAMETHASONE|DEXAMETHA'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP '\\bFNA BX\\b|\\bBX BREAST\\b|\\bPERQ DEV\\b|\\bBX PRST8\\b|BRAIN BIOPSY|FLUOROGUIDE|\\bFLUORO LOC\\b|FLUORO EXAM OF|MR GUIDANCE|NEEDLE LOCALIZATION'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'MR SFTY|MR SAFETY|SET UP PORT|TRANSPORT PORT|MRI COMPATIBLE|HIGH DOSE CONTRAST MRI|MR CONTRAST|ECHOCARDIOGRAPHY CONTRAST'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'INTRVASC US|INTRAVASCULAR US|\\bIV US\\b|TRURL ABLT|LOW FREQUENCY NON-THERMAL|OSTEOGEN ULTRASOUND|DIATHERMY|PACHYMETRY|SERVICES OUTSIDE US'
        AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'TBS DXA CAL|DXA ORDERED'
),

rad_unmapped AS (
    -- Sourced from study_std (computed above from every distinct
    -- rgd_gold_ad.radiology study_name), not from kinsula_leq.radiology.
    SELECT DISTINCT
        modality_std,
        body_part_std,
        contrast_type_std,
        strength_views_std
    FROM study_std
    WHERE proc_code_std     IS NULL
      AND modality_std      IS NOT NULL
      AND body_part_std     IS NOT NULL
      AND contrast_type_std IS NOT NULL
),

rad_patterns AS (
    SELECT
        r.*,

        -- MODALITY
        CASE r.modality_std
            WHEN 'Computed Tomography'                                                      THEN '\\bCT\\b|\\bCAT SCAN\\b'
            WHEN 'Magnetic Resonance'                                                       THEN '\\bMRI\\b|\\bMR\\b|FMRI|QMRCP'
            WHEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'       THEN '\\bMRA\\b|MR ANGIO|MRI ANGIO|MR ANG'
            WHEN 'Positron Emission Tomography (PET)'                                       THEN '\\bPET\\b'
            WHEN 'Positron Emission Tomography (PET) / Computed Tomography'                 THEN 'PET.*CT|PET/CT'
            WHEN 'Ultrasound'                                                               THEN '\\bUS\\b|ULTRASOUND|SONOGRAM|\\bECHO EXAM\\b'
            WHEN 'Ultrasound / Duplex Doppler'                                              THEN 'DUPLEX|DOPPLER'
            WHEN 'Ultrasound / Echocardiography'                                             THEN '\\bECHO\\b|\\bTTE\\b|\\bTEE\\b'
            WHEN 'Echocardiography (EC - Retired) / Ultrasound'                             THEN '\\bECHO\\b|\\bTTE\\b|\\bTEE\\b|ECHOCARDIOGRAPHY'
            WHEN 'Mammography'                                                              THEN 'MAMMO|MAMMOGRAM'
            WHEN 'Bone Densitometry (X-Ray)'                                                THEN '\\bDXA\\b|BONE DENSIT|BONE MINERAL'
            WHEN 'Digital Radiography'                                                      THEN '\\bXR\\b|X-?RAY|RADIOGR'
            WHEN 'Computed Radiography'                                                     THEN '\\bXR\\b|X-?RAY|RADIOGR'
            WHEN 'Digital Radiography / Radio Fluoroscopy'                                  THEN 'FLUORO|\\bXR\\b'
            WHEN 'Radio Fluoroscopy'                                                        THEN 'FLUORO'
            WHEN 'Nuclear Medicine'                                                         THEN 'NUCLEAR|SCINT|RADIONUCLIDE|PLANAR IMAG|PLANAR W/|PLANAR SING|PLANAR MULT|GATED HEART PLANAR|TUMOR IMAGING'
            WHEN 'X-Ray Angiography'                                                        THEN 'ANGIOGRAPH|ARTERIOGRAM|VENOGRAM'
            WHEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine' THEN '\\bSPECT\\b|SINGLE PHOTON'
            WHEN 'Electrocardiography'                                                      THEN '\\bECG\\b|\\bEKG\\b|ELECTROCARDIO'
            WHEN 'Electroencephalography'                                                   THEN '\\bEEG\\b|ELECTROENCEPH'
            ELSE NULL
        END AS modality_rx,

        -- BODY PART (Intracranial/Extracranial added; Head/Neck broadened)
        CASE r.body_part_std
            WHEN 'Abdomen'                      THEN '\\bABD\\b|ABDOMEN|ABDOMINAL|\\bABDO\\b'
            WHEN 'Abdomen, Pelvis'              THEN '(ABD|ABDOMEN).*(PELV|PELVIS)'
            WHEN 'Chest'                        THEN '\\bCHEST\\b|\\bTHORAX\\b|\\bTHORAC\\b|\\bLUNG\\b'
            WHEN 'Chest, Abdomen'               THEN 'CHEST.*ABD|THORAX.*ABD'
            WHEN 'Chest, Abdomen, Pelvis'       THEN 'CHEST.*ABD.*PELV'
            WHEN 'Pelvis'                       THEN '\\bPELV|PELVIS|PELVIC'
            WHEN 'Head'                         THEN '\\bHEAD\\b|HEAD/BRAIN|\\bHD\\b'
            WHEN 'Head, Neck'                   THEN '(HEAD|HD).*(NECK|NCK)|HEAD & NECK|\\bHEAD\\b|\\bNECK\\b|\\bNCK\\b'
            WHEN 'Brain Stem'                   THEN 'BRAIN STEM|BRN STEM'
            WHEN 'Brain'                        THEN '\\bBRAIN\\b|\\bBRN\\b|CEREBRAL|HEAD/BRAIN'
            WHEN 'Neck'                         THEN '\\bNECK\\b|\\bNCK\\b'
            WHEN 'Intracranial'                 THEN 'INTRACRAN|\\bICR\\b|BRAIN STEM|\\bBRAIN\\b|CEREBRAL|\\bHEAD\\b'
            WHEN 'Extracranial'                 THEN 'EXTRACRAN|CAROTID|\\bNECK\\b|\\bNCK\\b|VERTEBRAL ART'
            WHEN 'Cervical Spine'               THEN 'CERVICAL SPINE|\\bC-?SPINE\\b|NECK SPINE'
            WHEN 'Thoracic Spine, Lumbar Spine' THEN 'THORACOLUMBAR|THORACO.?LUMBAR|THORACOLMB'
            WHEN 'Lumbar Spine'                 THEN 'LUMBAR SPINE|\\bL-?SPINE\\b|L-S SPINE|LUMBOSACRAL'
            WHEN 'Lumbar Spine, Sacrum'         THEN 'LUMBOSACRAL|LUMB.*SACR|L-S SPINE'
            WHEN 'Spine'                        THEN '\\bSPINE\\b|SPINAL|VERTEBR|TRUNK SPINE|ENTIRE SPI'
            WHEN 'Sacrum'                       THEN '\\bSACRUM\\b|SACRAL'
            WHEN 'Sacrum, Coccyx'               THEN 'SACRUM TAILBONE|SACRUM.*COCCYX'
            WHEN 'Coccyx'                       THEN 'COCCYX|TAILBONE'
            WHEN 'Sacroiliac Joint'             THEN 'SACROILIAC|\\bSI JOINTS?\\b'
            WHEN 'Shoulder'                     THEN 'SHOULDER'
            WHEN 'Elbow'                        THEN 'ELBOW'
            WHEN 'Wrist'                        THEN 'WRIST'
            WHEN 'Hand'                         THEN '\\bHAND\\b'
            WHEN 'Fingers'                      THEN 'FINGER'
            WHEN 'Thumb'                        THEN 'THUMB'
            WHEN 'Forearm'                      THEN 'FOREARM'
            WHEN 'Humerus / Upper Arm'          THEN 'HUMERUS|UPPER ARM|\\bARM INFANT\\b'
            WHEN 'Clavicle'                     THEN 'CLAVICLE|COLLAR BONE'
            WHEN 'Scapula'                      THEN 'SCAPULA|SHOULDER BLADE'
            WHEN 'Acromioclavicular Joints'     THEN 'ACROMIOCLAV|\\bAC JOINT|STRENOCLAVIC'
            WHEN 'Hip'                          THEN '\\bHIPS?\\b'
            WHEN 'Femur'                        THEN 'FEMUR|FEMORAL|\\bFEM\\b|THIGH'
            WHEN 'Knee'                         THEN 'KNEES?'
            WHEN 'Leg'                          THEN '\\bLEG\\b|LOWER LEG|LEG INFANT'
            WHEN 'Calf'                         THEN '\\bCALF\\b'
            WHEN 'Thigh'                        THEN 'THIGH'
            WHEN 'Tibia, Fibula'                THEN 'TIBIA.*FIBULA|TIB.*FIB'
            WHEN 'Ankle'                        THEN 'ANKLE'
            WHEN 'Foot'                         THEN '\\bFOOT\\b|\\bFEET\\b|\\bHEEL\\b'
            WHEN 'Toes'                         THEN '\\bTOE'
            WHEN 'Upper Extremity'              THEN 'UPPER EXTREM|UPPR EXTREM|UPR EXTR|UPR EXTRM|UPPER EXT|JOINT UPR EXTR|JNT OF UPR|JOINT UPR'
            WHEN 'Lower Extremity'              THEN 'LOWER EXTREM|LWR EXTREM|LWR EXTR|LOWER EXT|JOINT LWR EXTR|JNT OF LWR|JOINT LWR|LWR EXTRMTY'
            WHEN 'Upper Extremity (Vascular)'   THEN '(UPPER EXTREM|UPR EXTR).*(ARTER|VEN|VASC|ANGIO)'
            WHEN 'Lower Extremity (Vascular)'   THEN '(LOWER EXTREM|LWR EXTR).*(ARTER|VEN|VASC|ANGIO)'
            WHEN 'Breast'                       THEN 'BREAST|BREASTS|\\bBRST\\b|MAMMARY|MAMMO'
            WHEN 'Heart'                        THEN '\\bHEART\\b|\\bHRT\\b|CARDIAC|\\bCARD\\b|MYOCARD|HT MRI|CORONARY'
            WHEN 'Aorta'                        THEN 'AORTA|AORTIC'
            WHEN 'Carotid'                      THEN 'CAROTID'
            WHEN 'Carotid, Neck'                THEN 'CAROTID'
            WHEN 'Liver'                        THEN 'LIVER|HEPATIC'
            WHEN 'Kidney'                       THEN 'KIDNEY|RENAL|\\bK TRANSPL\\b'
            WHEN 'Gallbladder'                  THEN 'GALLBLADDER|BILIARY'
            WHEN 'Pancreas'                     THEN 'PANCREA'
            WHEN 'Spleen'                       THEN 'SPLEEN|SPLENIC'
            WHEN 'Liver, Gallbladder, Pancreas' THEN 'HEPATOBILIARY|LIVER.*GALLBLADDER|BILE DUCTS?/PANCREAS'
            WHEN 'Thyroid'                      THEN 'THYROID'
            WHEN 'Thyroid, Neck'                THEN 'THYROID'
            WHEN 'Parathyroid'                  THEN 'PARATHYRO|PARATHYRD'
            WHEN 'Prostate / Rectal'            THEN 'PROSTATE|PRST8|RECTAL|RECTUM|TRANSRECTAL'
            WHEN 'Uterus'                       THEN '\\bUTER|UTERINE'
            WHEN 'Transvaginal'                 THEN 'TRANSVAG|VAGINAL'
            WHEN 'Scrotal / Testicular'         THEN 'SCROT|TESTIC'
            WHEN 'Eye / Orbit'                  THEN '\\bEYE\\b|ORBIT|\\bORBT\\b|OCULAR|EAR/FOSSA'
            WHEN 'Face'                         THEN '\\bFACE\\b|FACIAL|\\bFAC\\b|MAXILLOFACIAL|MAXFAC'
            WHEN 'Facial Bones, Sinuses'        THEN 'FACIAL BONES|MAXILLOFACIAL|(FACIAL|FAC).*SINUS'
            WHEN 'Sinuses'                      THEN 'SINUS|NASAL'
            WHEN 'Mandible'                     THEN 'MANDIBLE|\\bJAW\\b'
            WHEN 'Temporomandibular Joint'      THEN '\\bTMJ\\b|TEMPOROMAND|JAW JOINT'
            WHEN 'Mastoids'                     THEN 'MASTOID'
            WHEN 'Temporal Bone'                THEN 'TEMPORAL BONE|TEMP BONE'
            WHEN 'Skull'                        THEN '\\bSKULL\\b'
            WHEN 'Internal Auditory Canal'      THEN 'INTERNAL AUDITORY|AUDITORY CANAL|\\bIAC\\b'
            WHEN 'Pituitary'                    THEN 'PITUITARY|SELLA'
            WHEN 'Sternum'                      THEN 'STERNUM|BREASTBONE'
            WHEN 'Retroperitoneum'              THEN 'RETROPERITONE'
            WHEN 'Spinal Canal'                 THEN 'SPINAL CANAL|SPINAL CORD'
            WHEN 'Brachial Plexus'              THEN 'BRACHIAL PLEXUS|BRACHPLEX'
            WHEN 'Whole Body'                   THEN 'WHOLE BODY|FULL BODY|WHOLBODY|WHOLEBOD'
            WHEN 'Torso'                        THEN 'TORSO|TRUNK'
            WHEN 'Orbit, Face, Neck'            THEN '(ORBT|ORBIT).*(FAC|FACE).*(NCK|NECK)'
            ELSE NULL
        END AS body_part_rx,

        -- CONTRAST positive
        CASE r.contrast_type_std
            WHEN 'With and Without Contrast' THEN
                'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|W/O ?AND ?W/CONTRAST|WITHOUT AND WITH CONTRAST|WITH AND WITHOUT CONTRAST|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|\\bW OR W/O DYE\\b|\\bW OR W/O CNTR\\b|W/WO DYE|\\bWWO\\b'
            WHEN 'Without Contrast' THEN
                'W/O DYE|W/O CNTR|W/O CONT|\\bWO DYE\\b|\\bWO CNTR\\b|\\bWO\\b|WITHOUT CONTRAST|WITHOUT DYE|\\bNO CNTR\\b|\\bNO CONTRAST\\b|\\bC-\\b'
            WHEN 'With Contrast' THEN
                'W/DYE|\\bW DYE\\b|W/CNTR|W/CONTRAST|WITH CONTRAST|WITH DYE|W/CAD|\\bC\\+\\b'
            ELSE NULL
        END AS contrast_rx,

        -- CONTRAST exclusion
        CASE r.contrast_type_std
            WHEN 'Without Contrast' THEN
                'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|WITHOUT AND WITH|WITH AND WITHOUT|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|W OR W/O|W/WO DYE|\\bWWO\\b|\\bC-/C\\+\\b|\\bC-\\+\\b'
            WHEN 'With Contrast' THEN
                'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|WITHOUT AND WITH|WITH AND WITHOUT|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|W OR W/O|W/WO DYE|\\bWWO\\b|\\bC-/C\\+\\b'
            ELSE NULL
        END AS contrast_exclude_rx,
        
        -- VIEWS regex (only for X-ray modalities)
        CASE
            WHEN r.modality_std NOT IN ('Digital Radiography', 'Computed Radiography',
                                        'Digital Radiography / Radio Fluoroscopy',
                                        'Bone Densitometry (X-Ray)')
                THEN NULL
            WHEN r.strength_views_std IS NULL
                THEN NULL
            WHEN r.strength_views_std = '2 Views'
                THEN '\\b2 VIEWS?\\b|\\b2VWS\\b|\\b2 VW\\b|UNI 2 VIEW|BI 2 VIEW|RIBS UNI 2|2VW'
            WHEN r.strength_views_std = '2 or 3 Views'
                THEN '2-3 VIEWS?|2-3 VW|2/3 VWS|2/3 VW|UNI 2-3 VIEW|BI 2/3'
            WHEN r.strength_views_std = '3 or 4 Views'
                THEN '3-4 VIEWS?|3/4 VWS|3/4 VW|BI 3-4 VIEW'
            ELSE NULL
        END AS views_rx
    FROM rad_unmapped r
),

combo_matches AS (
    SELECT
        p.modality_std,
        p.body_part_std,
        p.contrast_type_std,
        p.strength_views_std,         -- ADD THIS
        GROUP_CONCAT(DISTINCT SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)
                     ORDER BY SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)
                     SEPARATOR ',') AS probable_cpt_code,
        COUNT(DISTINCT SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)) AS probable_cpt_match_count,
        GROUP_CONCAT(DISTINCT cpt.COMMONDESCRIPTION
                     ORDER BY cpt.COMMONDESCRIPTION
                     SEPARATOR ' | ') AS matched_descriptions
   FROM rad_patterns p
    LEFT JOIN cpt_clean cpt
           ON p.modality_rx  IS NOT NULL
          AND p.body_part_rx IS NOT NULL
          AND p.contrast_rx  IS NOT NULL
          AND cpt.DESC_U REGEXP p.modality_rx
          AND cpt.DESC_U REGEXP p.body_part_rx
          AND cpt.DESC_U REGEXP p.contrast_rx
          AND (p.contrast_exclude_rx IS NULL
               OR cpt.DESC_U NOT REGEXP p.contrast_exclude_rx)
          AND (p.views_rx IS NULL                                  -- ADD THIS
               OR cpt.DESC_U REGEXP p.views_rx)                    -- ADD THIS
          AND NOT (
              (p.body_part_std = 'Brain' AND cpt.DESC_U REGEXP 'BRAIN STEM|BRN STEM')
           OR (p.body_part_std = 'Neck'  AND cpt.DESC_U REGEXP 'NECK SPINE|ORBT/FAC/NCK|ORBIT/FACE/NECK')
          )
    GROUP BY p.modality_std, p.body_part_std, p.contrast_type_std, p.strength_views_std    -- ADD strength_views_std
)

-- ============================================================================
-- Final output: every rgd_gold_ad.radiology row + standardized columns.
-- cpt_code_std prefers a code embedded directly in study_name
-- (study_std.proc_code_std); if none was found, falls back to a probable
-- code inferred from the modality/body-part/contrast/views combination
-- (combo_matches.probable_cpt_code). cpt_code_source records which one fired.
--
-- No ALTER TABLE / write access assumed. Run as-is for a read-only result
-- set, or uncomment the CREATE VIEW wrapper below if you have that
-- privilege (zero storage, always reflects live raw data, recomputes on
-- every query).
-- ============================================================================

-- CREATE VIEW rgd_gold_ad.radiology_std_vw AS
SELECT
    r.*,
    s.modality_std                                   AS img_modality_std,
    s.body_part_std                                  AS img_body_part_std,
    s.contrast_type_std                               AS img_contrast_type_std,
    s.tracer_name_std                                 AS img_tracer_name_std,
    COALESCE(s.proc_code_std, m.probable_cpt_code)    AS cpt_code_std,
    CASE
        WHEN s.proc_code_std IS NOT NULL     THEN 'study_name'
        WHEN m.probable_cpt_code IS NOT NULL THEN 'probable_cpt_code'
        ELSE NULL
    END                                                AS cpt_code_source
FROM rgd_gold_ad.radiology r
LEFT JOIN study_std s
       ON r.study_name = s.study_name
LEFT JOIN combo_matches m
       ON s.modality_std      = m.modality_std
      AND s.body_part_std     = m.body_part_std
      AND s.contrast_type_std = m.contrast_type_std
      AND (s.strength_views_std = m.strength_views_std
           OR (s.strength_views_std IS NULL AND m.strength_views_std IS NULL));
