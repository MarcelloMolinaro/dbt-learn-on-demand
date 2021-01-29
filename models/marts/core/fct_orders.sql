with orders as (select * from {{ ref('stg_orders') }}),

payments as (select * from {{ ref('stg_payments') }})

select
    o.order_id,
    o.customer_id,
    o.order_date,
    sum(case when p.status = 'success' then p.amount end) as amount
from orders as o
left join payments as p on o.order_id = p.orderid
group by 1,2,3


