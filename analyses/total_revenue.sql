with pay as (
select * from {{ref('stg_payments')}}
)

select sum(amount) from pay
where status = 'success'