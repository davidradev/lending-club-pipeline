{{ config(materialized='view') }}

with source as (
    select * from {{ source('public', 'raw_loans') }}
)

select
    -- Casting de tipos de datos de TEXT a numericos/fechas
    id as loan_id,
    cast(loan_amnt as numeric) as loan_amnt,
    term,
    -- Limpieza de la tasa de interes (quitar % y convertir a numero)
    cast(replace(int_rate, '%', '') as numeric) as int_rate,
    cast(installment as numeric) as installment,
    grade,
    emp_length,
    home_ownership,
    cast(annual_inc as numeric) as annual_inc,
    verification_status,
    issue_d,
    loan_status,
    purpose,
    cast(dti as numeric) as dti,
    revol_util,
    cast(mort_acc as numeric) as mort_acc,
    cast(pub_rec_bankruptcies as numeric) as pub_rec_bankruptcies,
    zip_code
from source
where loan_amnt is not null 
  and loan_status is not null
