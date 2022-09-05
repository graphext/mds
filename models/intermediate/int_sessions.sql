with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

sessions as (
    select 
        session_id,
        max(user_id) as user_id,
        max(num_session_items) as num_session_items
    from log
    group by 1
    order by 1
)

select * from sessions