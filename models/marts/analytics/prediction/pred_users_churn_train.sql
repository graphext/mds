with

pred_users_churn as (
    select * from {{ ref('pred_users_churn') }}
),

sample_train_data as (
    select 
        * 
    from pred_users_churn 
    where abs(mod(farm_fingerprint(cast(user_id as string)), 20)) < 17
)

select * from sample_train_data