with 

songs_artists as (
    select * from {{ ref('stg_gratify__songs') }}
),

tracks_normalized as (
    select
        track_id,
        song_id,
        track_7digitalid,
        artist_id,
        title,
        release,
        duration,
        year,
        shs_perf,
        shs_work
    from songs_artists
)

select * from tracks_normalized