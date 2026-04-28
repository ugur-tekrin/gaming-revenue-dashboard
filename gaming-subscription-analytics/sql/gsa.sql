with monthly_user_revenue as (

select
    user_id,
    game_name,
    date_trunc('month', payment_date)::date as payment_month,
    sum(revenue_amount_usd) as mrr
from project.games_payments
group by
    user_id,
    game_name,
    date_trunc('month', payment_date)

),

revenue_with_windows as (

select
    mur.*,

    lag(payment_month) over (
        partition by user_id, game_name
        order by payment_month
    ) as prev_payment_month,

    lead(payment_month) over (
        partition by user_id, game_name
        order by payment_month
    ) as next_payment_month,

    lag(mrr) over (
        partition by user_id, game_name
        order by payment_month
    ) as prev_mrr

from monthly_user_revenue mur

),

revenue_metrics as (

select

    user_id,
    game_name,
    payment_month,
    mrr,

case
when prev_payment_month is null
then mrr
else 0
end as new_mrr,

case
when prev_mrr is not null
and mrr > prev_mrr
and payment_month = prev_payment_month + interval '1 month'
then mrr - prev_mrr
else 0
end as expansion_revenue,

case
when prev_mrr is not null
and mrr < prev_mrr
and payment_month = prev_payment_month + interval '1 month'
then prev_mrr - mrr
else 0
end as contraction_revenue,

case
when prev_payment_month is not null
and payment_month > prev_payment_month + interval '1 month'
then mrr
else 0
end as back_from_churn_revenue,

case
when next_payment_month is null
or next_payment_month > payment_month + interval '1 month'
then payment_month + interval '1 month'
else null
end as churn_month,

case
when next_payment_month is null
or next_payment_month > payment_month + interval '1 month'
then mrr
else 0
end as churned_revenue

from revenue_with_windows

)

select

rm.user_id,
rm.game_name,
rm.payment_month,
rm.mrr,

rm.new_mrr,
rm.expansion_revenue,
rm.contraction_revenue,
rm.back_from_churn_revenue,
rm.churned_revenue,
rm.churn_month,

gpu.language,
gpu.age,
gpu.has_older_device_model

from revenue_metrics rm

left join project.games_paid_users gpu
on rm.user_id = gpu.user_id
and rm.game_name = gpu.game_name

order by payment_month;