with 

source as (
    select * from {{ source('gratify', 'songs') }}
)

select * from source