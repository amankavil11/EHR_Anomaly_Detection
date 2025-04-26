// =======================================================
// Step 1: Constraint Creation and Parameter Instantiation
// =======================================================

// Unique Patient ID
CREATE CONSTRAINT patient_id_unique IF NOT EXISTS
FOR (p:Patient)
REQUIRE p.ID IS UNIQUE;

// Unique Admission
CREATE CONSTRAINT admission_key_unique IF NOT EXISTS
FOR (a:Admission)
REQUIRE a.HADM_ID IS UNIQUE;

// Unique LabResult
CREATE CONSTRAINT lab_key_unique IF NOT EXISTS
FOR (l:LabResult)
REQUIRE (l.HADM_ID, l.TEST_NAME, l.TIME) IS UNIQUE;

// Unique Medication
CREATE CONSTRAINT medication_key_unique IF NOT EXISTS
FOR (m:Medication)
REQUIRE (m.HADM_ID, m.DRUG) IS UNIQUE;

:param file_path_root => 'file:///'
:param file_1 => 'patients.csv'
:param file_2 => 'admissions.csv'
:param file_3 => 'diagnoses.csv'
:param file_4 => 'prescriptions.csv'
:param file_5 => 'labevents.csv'

// ==========================
// Step 2: Load Patient Nodes
// ==========================

LOAD CSV WITH HEADERS FROM $file_path_root  + $file_1 AS row
WITH row where row.SUBJECT_ID IS NOT NULL
CALL (row) {
    MERGE (p: Patient {ID: row.SUBJECT_ID})
    SET p.GENDER = row.GENDER,
        p.DOB = date(row.DOB),
        p.DOD = CASE row.DOD WHEN '' THEN NULL ELSE date(row.DOD) END,
        p.AGE_AT_DEATH = CASE 
                        WHEN p.DOB IS NOT NULL 
                        AND p.DOD IS NOT NULL 
                        AND p.DOB < p.DOD 
                        THEN duration.between(p.DOB, p.DOD).years 
                        ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM $file_path_root + $file_2 AS row
WITH row WHERE row.HADM_ID IS NOT NULL
CALL (row) {
    MERGE (a:Admission {HADM_ID: row.HADM_ID})
    SET a.admit_time = datetime(replace(row.ADMITTIME, ' ', 'T')),
        a.discharge_time = datetime(replace(row.DISCHTIME, ' ', 'T')),
        a.admission_type = row.ADMISSION_TYPE,
        a.ethnicity = row.ETHNICITY,
        a.diagnosis_free_text = row.DIAGNOSIS,
        a.hospital_expire_flag = CASE row.HOSPITAL_EXPIRE_FLAG WHEN 1 THEN true ELSE false END,
        a.readmission_within_30days = CASE row.READMIT30D WHEN 1 THEN true ELSE false END,
        a.age = row.AGE
    WITH row, a
    MATCH (p:Patient {ID: row.SUBJECT_ID})
    MERGE (p)-[:HAS_ADMISSION]->(a)
} IN TRANSACTIONS OF 10000 ROWS;
// ==========================
// Step 3: Load Diagnosis Nodes
// ==========================

LOAD CSV WITH HEADERS FROM $file_path_root + $file_3 AS row
WITH row WHERE row.hadm_id IS NOT NULL AND row.icd9_code IS NOT NULL
CALL (row){
  MERGE (c:Diagnosis {CODE: row.icd9_code})
  SET c.name = row.diagnosis

  WITH row, c
  MATCH (a:Admission {HADM_ID: row.hadm_id})
  MERGE (a)-[:HAS_DIAGNOSIS]->(c)
} IN TRANSACTIONS OF 10000 ROWS;

// ==========================
// Step 4: Load Medication Nodes
// ==========================

LOAD CSV WITH HEADERS FROM $file_path_root + $file_4 AS row
WITH row WHERE row.hadm_id IS NOT NULL AND row.drug IS NOT NULL
CALL (row) {
  MERGE (m:Medication {
    HADM_ID: row.hadm_id,
    DRUG: row.drug
  })
  SET m.route = row.route,
      m.dose = row.dose_val_rx,
      m.unit = row.dose_unit_rx,
      m.start = datetime(row.startdate),
      m.end = datetime(row.enddate)

  WITH row, m
  MATCH (a:Admission {HADM_ID: row.hadm_id})
  MERGE (a)-[:HAS_MEDICATION]->(m)
} IN TRANSACTIONS OF 10000 ROWS;

// ============================
// Step 5: Load Lab Event Nodes
// ============================

LOAD CSV WITH HEADERS FROM $file_path_root + $file_5 AS row
WITH row WHERE row.hadm_id IS NOT NULL AND row.lab_name IS NOT NULL AND row.charttime IS NOT NULL
CALL (row) {
  MERGE (l:LabResult {
    HADM_ID: row.hadm_id,
    TEST_NAME: row.lab_name,
    TIME: datetime(replace(row.charttime, ' ', 'T'))
  })
  SET l.value = toFloat(row.valuenum),
      l.unit = row.valueuom

  WITH row, l
  MATCH (a:Admission {HADM_ID: row.hadm_id})
  MERGE (a)-[:HAS_LAB]->(l)
} IN TRANSACTIONS OF 10000 ROWS;

