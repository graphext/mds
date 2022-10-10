with

users as (
    select * from {{ ref('int_users') }}
),

users_enriched as (
    select * from {{ ref('int_users__enriched') }}
),

users_churn as (
    select * from {{ ref('int_users_churn') }}
),

final as (
    select
        users.user_id,
        gender,
        -- Show how to extract day parts in Graphext to obtain more features
        registered_at,
        num_events,
        num_unique_events,
        num_unique_events_no_cancel,
        num_sessions,
        device,
        city,
        state,
        days_since_registration,
        days_since_registration_last_event,
        was_paying,
        is_paying,
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
        ratio_upgrade_page,
        churn
    from users
    left join users_enriched using (user_id)
    left join users_churn using (user_id)
    order by 1
)

select * from final