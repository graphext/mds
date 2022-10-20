with

artists as (
    select * from {{ ref('int_artists') }}
),

played_tracks as (
    select * from {{ ref('int_played_tracks') }}
)

select
    user_id,
    round(avg(num_songs), 2) as avg_songs_per_artist,
    round(stddev(num_songs), 2) as std_songs_per_artist,
    round(avg(familiarity), 2) as avg_familiarity_artist,
    round(stddev(familiarity), 2) as std_familiarity_artist,
    round(avg(hotness), 2) as avg_hotness_artist,
    round(stddev(hotness), 2) as std_hotness_artist,
from(
    select
        user_id,
        artist_name,
        max(familiarity) as familiarity,
        max(hotness) as hotness,
        count(*) as num_songs
    from played_tracks
    left join artists on played_tracks.artist_name = artists.name
    where page = 'NextSong'
    group by user_id, artist_name
    order by 1
)
group by user_id
order by 1