with 

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

events as (
    select * from {{ ref('int_events') }}
)

select
    users.user_id,
    count(events.event_id) as num_events,
    count(distinct events.page) as num_unique_events
from users
left join events using (user_id)
group by 1
order by 1