{{ config(materialized='view') }}

with fact as ( select * from {{ ref('fact_loans') }} ),
borrower as ( select * from {{ ref('dim_borrower') }} ),
details as ( select * from {{ ref('dim_loan_details') }} )

select 
    f.loan_id,
    d.loan_amnt,
    d.int_rate,
    b.annual_inc,
    f.dti,
    d.installment,
    f.loan_status -- Incluimos esto para saber cuales son Current en el dashboard
from fact f
join borrower b on f.borrower_id = b.borrower_id
join details d on f.loan_id = d.loan_id
-- NOTA: Aqui NO filtramos por loan_status para poder predecir sobre TODOS
