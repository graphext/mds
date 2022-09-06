with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

sessions as (
    select 
        session_id,
        max(user_id) as user_id,
        min(created_at) as started_at,
        max(created_at) as ended_at,
        timestamp_diff(max(created_at), min(created_at), second) as duration_sec,
        count(*) as num_events,
        max(num_session_items) as num_session_items
    from log
    group by 1
    order by 1
)

select * from sessions