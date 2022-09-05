with 

songs_artists as (
    select * from {{ ref('stg_gratify__songs') }}
),

artists_normalized as (
    select
        artist_id,
        max(artist_mbid) as artist_mbid,
        max(artist_name) as name,
        max(artist_familiarity) as familiarity,
        max(artist_hotttnesss) as hotness
    from songs_artists
    group by 1
)

select * from artists_normalized