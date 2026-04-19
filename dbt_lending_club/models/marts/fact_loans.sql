{{ config(materialized='table') }}

select 
    loan_id,
    loan_id as borrower_id,
    issue_d,
    loan_status,
    dti,
    revol_util,
    mort_acc,
    zip_code
from {{ ref('stg_loans') }}
