with

events as (
    select * from {{ ref('int_events') }}
)

select
    user_id,
    round(avg(num_songs), 2) as avg_songs_per_session,
    round(stddev(num_songs), 2) as std_songs_per_session
from(
    select
        user_id,
        session_id,
        count(*) as num_songs
    from events
    where page = 'NextSong'
    group by user_id, session_id
    order by 1
)
group by user_id
order by 1