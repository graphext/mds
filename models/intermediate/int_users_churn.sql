with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

users_churn as (
    select
        user_id,
        countif(page = 'Cancellation Confirmation') as churn
    from log
    group by 1
    order by 1
)

select * from users_churn