with 

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

sessions as (
    select * from {{ ref('int_sessions') }}
)

select
    users.user_id,
    count(sessions.session_id) as num_sessions,
    round(avg(duration_sec / 60), 2) as avg_session_duration_minutes,
    round(stddev(duration_sec / 60), 2) as std_session_duration_minutes
from users
left join sessions using (user_id)
group by 1
order by 1

