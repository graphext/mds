with

played_tracks as (
    select * from {{ ref('int_played_tracks') }}
)

select
    played_tracks.user_id,
    count(distinct artist_name) as num_unique_artists,
    count(distinct song_title) as num_unique_played_songs,
    round(sum(song_length), 2) as sum_all_played_song_length,
    round(avg(song_length), 2) as avg_song_length,
    round(stddev(song_length), 2) as std_song_length
from played_tracks
group by 1
order by 1