with 

source as (
    select * from {{ source('gratify', 'log') }}
),

final as (
    select 
        ts as created_at,
        userId as user_id,
        sessionId as session_id,
        page,
        auth,
        method,
        status,
        level,
        itemInSession as num_session_items,
        location,
        userAgent as user_agent,
        lastName as last_name,
        firstName as first_name,
        registration as registered_at,
        gender,
        artist as artist_name,
        song as song_title,
        length as song_length
    from source
)

select * from final