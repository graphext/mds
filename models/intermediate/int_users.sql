with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

users as (
    select 
        user_id,
        max(first_name) as first_name,
        max(last_name) as last_name,
        max(gender) as gender,
        min(registered_at) as registered_at
    from log
    where user_id is not null
    group by 1
    order by 1
)

select * from users