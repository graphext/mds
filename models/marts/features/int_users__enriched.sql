with 

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

events as (
    select * from {{ ref('int_events') }}
),

events_no_cancel as (
    select * from {{ ref('int_events') }} where lower(page) not like '%cancel%'
),

sessions as (
    select * from {{ ref('int_sessions') }}
),

artists as (
    select * from {{ ref('int_artists') }}
),

log as (
    select * from {{ ref('stg_gratify__log') }}
),

songs_per_artist as (
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
        from log
        left join artists on log.artist_name = artists.name
        where page = 'NextSong'
        group by user_id, artist_name
        order by 1
    )
    group by user_id
    order by 1
),

songs_per_session as (
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
),

users_log_enriched as (
    select
        log.user_id,
        count(distinct artist_name) as num_unique_artists,
        count(distinct song_title) as num_unique_played_songs,
        round(sum(song_length), 2) as sum_all_played_song_length,
        round(avg(song_length), 2) as avg_song_length,
        round(stddev(song_length), 2) as std_song_length
    from log
    group by 1
    order by 1
),

users_events_no_cancel as (
    select
        users.user_id,
        count(distinct events_no_cancel.page) as num_unique_events_no_cancel
    from users
    left join events_no_cancel using (user_id)
    group by 1
    order by 1
),

users_events_enriched as (
    select
        users.user_id,
        count(events.event_id) as num_events,
        count(distinct events.page) as num_unique_events,
        ifnull(sum(case when page = 'NextSong' then 1 end), 0) as num_played_songs,
        ifnull(sum(case when page = 'About' then 1 end), 0) as num_about_page,
        ifnull(sum(case when page = 'Add Friend' then 1 end), 0) as num_add_friend_page,
        ifnull(sum(case when page = 'Add to Playlist' then 1 end), 0) as num_add_to_playlist_page,
        ifnull(sum(case when page = 'Error' then 1 end), 0) as num_error_page,
        ifnull(sum(case when page = 'Help' then 1 end), 0) as num_help_page,
        ifnull(sum(case when page = 'Home' then 1 end), 0) as num_home_page,
        ifnull(sum(case when page = 'Logout' then 1 end), 0) as num_logout_page,
        ifnull(sum(case when page = 'Roll Advert' then 1 end), 0) as num_roll_advert_page,
        ifnull(sum(case when page = 'Save Settings' then 1 end), 0) as num_save_settings_page,
        ifnull(sum(case when page = 'Settings' then 1 end), 0) as num_settings_page,
        ifnull(sum(case when page = 'Thumbs Down' then 1 end), 0) as num_thumbs_down_page,
        ifnull(sum(case when page = 'Thumbs Up' then 1 end), 0) as num_thumbs_up_page,
        ifnull(sum(case when page = 'Downgrade' then 1 end), 0) as num_downgrade_page,
        ifnull(sum(case when page = 'Upgrade' then 1 end), 0) as num_upgrade_page,
        -- Ratios of each events w.r.t to all events
        round(ifnull(sum(case when page = 'NextSong' then 1 end), 0) / count(events.event_id), 3) as ratio_played_songs,
        round(ifnull(sum(case when page = 'About' then 1 end), 0) / count(events.event_id), 3) as ratio_about_page,
        round(ifnull(sum(case when page = 'Add Friend' then 1 end), 0) / count(events.event_id), 3) as ratio_add_friend_page,
        round(ifnull(sum(case when page = 'Add to Playlist' then 1 end), 0) / count(events.event_id), 3) as ratio_add_to_playlist_page,
        round(ifnull(sum(case when page = 'Error' then 1 end), 0) / count(events.event_id), 3) as ratio_error_page,
        round(ifnull(sum(case when page = 'Help' then 1 end), 0) / count(events.event_id), 3) as ratio_help_page,
        round(ifnull(sum(case when page = 'Home' then 1 end), 0) / count(events.event_id), 3) as ratio_home_page,
        round(ifnull(sum(case when page = 'Logout' then 1 end), 0) / count(events.event_id), 3) as ratio_logout_page,
        round(ifnull(sum(case when page = 'Roll Advert' then 1 end), 0) / count(events.event_id), 3) as ratio_roll_advert_page,
        round(ifnull(sum(case when page = 'Save Settings' then 1 end), 0) / count(events.event_id), 3) as ratio_save_settings_page,
        round(ifnull(sum(case when page = 'Settings' then 1 end), 0) / count(events.event_id), 3) as ratio_settings_page,
        round(ifnull(sum(case when page = 'Thumbs Down' then 1 end), 0) / count(events.event_id), 3) as ratio_thumbs_down_page,
        round(ifnull(sum(case when page = 'Thumbs Up' then 1 end), 0) / count(events.event_id), 3) as ratio_thumbs_up_page,
        round(ifnull(sum(case when page = 'Downgrade' then 1 end), 0) / count(events.event_id), 3) as ratio_downgrade_page,
        round(ifnull(sum(case when page = 'Upgrade' then 1 end), 0) / count(events.event_id), 3) as ratio_upgrade_page,
        -- For the same user it is always the same device, city and state
        max(device) as device,
        max(city) as city,
        max(state) as state,
        max(timestamp_diff(created_at, registered_at, day)) as days_since_registration_last_event,
        max(timestamp_diff(current_timestamp(), registered_at, day)) as days_since_registration
    from users
    left join events using (user_id)
    group by 1
    order by 1
),

users_sessions_enriched as (
    select
        users.user_id,
        count(sessions.session_id) as num_sessions,
        round(avg(duration_sec / 60), 2) as avg_session_duration_minutes,
        round(stddev(duration_sec / 60), 2) as std_session_duration_minutes
    from users
    left join sessions using (user_id)
    group by 1
    order by 1
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
    left join users_log_enriched using (user_id)
    left join songs_per_artist using (user_id)
    left join songs_per_session using (user_id)
    left join users_events_no_cancel using (user_id)
    order by 1
)

select * from final