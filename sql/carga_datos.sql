-- 1. Habilitar extension
CREATE EXTENSION IF NOT EXISTS azure_storage;
-- 2. Crear tabla cruda
DROP TABLE IF EXISTS raw_loans;
CREATE TABLE raw_loans (
id TEXT, loan_amnt TEXT, term TEXT, int_rate TEXT, installment TEXT,
    grade TEXT, emp_length TEXT, home_ownership TEXT, annual_inc TEXT,
    verification_status TEXT, issue_d TEXT, loan_status TEXT, purpose TEXT,
    dti TEXT, revol_util TEXT, mort_acc TEXT, pub_rec_bankruptcies TEXT, zip_code TEXT
);
-- 3. Carga masiva (Pega tu SAS URL completo)
COPY raw_loans FROM 'TU_URL_DE_SAS_AQUI'
WITH (FORMAT csv, HEADER true, DELIMITER ',');