{{ config(materialized='table') }}

select distinct
    loan_id,
    loan_amnt,
    term,
    int_rate,
    installment,
    grade,
    purpose
from {{ ref('stg_loans') }}
