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
        min(registered_at) as registered_at,
        -- has been a paying customer once?
        countif(level='paid') > 0 as was_paying,
        -- is currently a paying customer?
        array_agg(log.level ORDER BY created_at desc limit 1)[offset(0)] = 'paid' as is_paying
    from log
    where user_id is not null
    group by 1
    order by 1
)

select * from users