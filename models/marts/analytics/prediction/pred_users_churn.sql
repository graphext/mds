{{ config(
    materialized='table',
)}}

with

users as (
    select * from {{ ref('int_users') }}
),

users_enriched as (
    select * from {{ ref('users_enriched') }}
),

users_churn as (
    select * from {{ ref('users_churn') }}
),

final as (
    select
        user_id,
        registered_at,
        num_events,
        num_unique_events_no_cancel,
        num_sessions,
        device,
        city,
        state,
        days_since_registration,
        days_since_registration_last_event,
        is_paying,
        num_played_songs,
        num_unique_played_songs,
        num_unique_artists,
        avg_songs_per_artist,
        std_songs_per_artist,
        avg_songs_per_session,
        std_songs_per_session,
        avg_session_duration_minutes,
        std_session_duration_minutes,
        avg_song_length,
        std_song_length,
        num_roll_advert_page,
        num_thumbs_down_page,
        num_thumbs_up_page,
        num_downgrade_page,
        num_upgrade_page,
        ratio_add_to_playlist_page,
        ratio_help_page,
        ratio_roll_advert_page,
        ratio_thumbs_down_page,
        ratio_thumbs_up_page,
        ratio_downgrade_page,
        churn
    from users
    left join users_enriched using (user_id)
    left join users_churn using (user_id)
    order by 1
)

select * from final