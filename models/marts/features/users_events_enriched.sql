with 

users as (
    select user_id, registered_at from {{ ref('int_users') }}
),

events as (
    select * from {{ ref('int_events') }}
)

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