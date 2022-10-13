with 

log as (
    select * from {{ ref('stg_gratify__log') }}
),

played_tracks as(
    select
        *
    from log
    where page = "NextSong"
)

select * from played_tracks