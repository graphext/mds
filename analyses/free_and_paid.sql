-- Events of users who have been at both free and paid levels
with

free_and_paid as (
    select
        user_id
    from {{ ref('int_events')}}
    group by user_id
    having countif(level='free') > 0 and countif(level = 'paid') > 0
)

select
    *
from {{ ref('int_events') }}
where user_id in (select user_id from free_and_paid)
order by user_id, created_at