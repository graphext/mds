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
        user_agent,
        -- Extract the device from the user agent
        regexp_extract(regexp_extract(user_agent, r'^(.*?);'), r' \((.*)') as device,
        location,
        split(location)[SAFE_OFFSET (0)] as city,
        split(location)[SAFE_OFFSET (1)] as state
    from log
    order by created_at desc
)

select * from events