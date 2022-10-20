with 

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

events_no_cancel as (
    select * from {{ ref('int_events') }} where lower(page) not like '%cancel%'
)

select
    users.user_id,
    count(distinct events_no_cancel.page) as num_unique_events_no_cancel
from users
left join events_no_cancel using (user_id)
group by 1
order by 1