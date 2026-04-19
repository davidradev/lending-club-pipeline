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
    -- Definicion del Target para el entrenamiento (1 = Riesgo, 0 = Pagado)
    case 
        when f.loan_status in ('Charged Off', 'Default', 'Late (31-120 days)') then 1
        else 0 
    end as target
from fact f
join borrower b on f.borrower_id = b.borrower_id
join details d on f.loan_id = d.loan_id
-- Solo incluimos creditos con desenlace final (no "Current")
where f.loan_status in ('Fully Paid', 'Charged Off', 'Default')
