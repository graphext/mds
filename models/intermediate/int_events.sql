with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

events as (
    select 
        -- BigQuery-specific, there are other ways to generate a
        -- surrogate key, including dbt-utils' surrogate_key macro
        generate_uuid() as event_id,
        user_id,
        session_id,
        created_at,
        page,
        auth,
        method,
        status,
        level,
        location,
        user_agent
    from log
)

select * from events