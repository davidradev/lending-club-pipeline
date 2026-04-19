{{ config(materialized='table') }}

select distinct
    loan_id as borrower_id,
    emp_length,
    home_ownership,
    annual_inc,
    verification_status,
    pub_rec_bankruptcies
from {{ ref('stg_loans') }}
