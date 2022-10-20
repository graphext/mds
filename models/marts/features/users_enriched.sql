with

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

users_events_enriched as (
     select * from {{ ref('users_events_enriched') }}
),

users_sessions_enriched as (
     select * from {{ ref('users_sessions_enriched') }}
),

users_played_tracks_enriched as (
     select * from {{ ref('users_played_tracks_enriched') }}
),

users_songs_per_artist as (
     select * from {{ ref('users_songs_per_artist') }}
),

users_songs_per_session as (
     select * from {{ ref('users_songs_per_session') }}
),

users_unique_events_no_cancel as (
     select * from {{ ref('users_unique_events_no_cancel') }}
),

final as (
    select
        user_id,
        num_events,
        num_unique_events,
        num_unique_events_no_cancel,
        num_sessions,
        device,
        city,
        state,
        days_since_registration,
        days_since_registration_last_event,
        num_played_songs,
        num_unique_played_songs,
        num_unique_artists,
        avg_familiarity_artist,
        std_familiarity_artist,
        avg_hotness_artist,
        std_hotness_artist,
        avg_songs_per_artist,
        std_songs_per_artist,
        avg_songs_per_session,
        std_songs_per_session,
        avg_session_duration_minutes,
        std_session_duration_minutes,
        sum_all_played_song_length,
        avg_song_length,
        std_song_length,
        -- count events types
        num_about_page,
        num_add_friend_page,
        num_add_to_playlist_page,
        num_error_page,
        num_help_page,
        num_home_page,
        num_logout_page,
        num_roll_advert_page,
        num_save_settings_page,
        num_settings_page,
        num_thumbs_down_page,
        num_thumbs_up_page,
        num_downgrade_page,
        num_upgrade_page,
        -- Ratios of each events w.r.t to all events
        ratio_played_songs,
        ratio_about_page,
        ratio_add_friend_page,
        ratio_add_to_playlist_page,
        ratio_error_page,
        ratio_help_page,
        ratio_home_page,
        ratio_logout_page,
        ratio_roll_advert_page,
        ratio_save_settings_page,
        ratio_settings_page,
        ratio_thumbs_down_page,
        ratio_thumbs_up_page,
        ratio_downgrade_page,
        ratio_upgrade_page
    from users
    left join users_events_enriched using (user_id)
    left join users_sessions_enriched using (user_id)
    left join users_played_tracks_enriched using (user_id)
    left join users_songs_per_artist using (user_id)
    left join users_songs_per_session using (user_id)
    left join users_unique_events_no_cancel using (user_id)
    order by 1
)

select * from final