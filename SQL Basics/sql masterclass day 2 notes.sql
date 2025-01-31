select o.* , r.*
from orders o
inner join order_returns r on o.order_id=r.order_id
;

select o.* , r.* 
from orders o
left join order_returns r on o.order_id=r.order_id
where r.return_reason ='Bad Quality' and category='Furniture'

select r.return_reason , sum(o.sales) as return_sales
from orders o
inner join order_returns r on o.order_id=r.order_id
group by r.return_reason
having sum(o.sales) > 3000;

select o.order_id,o.order_date,r.return_reason
from orders o
left join order_returns r on o.order_id=r.order_id;

select order_id,order_date,profit,sales,profit/sales as ratio, sales-profit as cost  
from orders

select order_id,order_date,profit,sales,category,customer_name
,len(customer_name) as l_cn
,left(customer_name,4) as left4_cn
,right(customer_name,3) as right3_cn
,SUBSTRING(customer_name,3,4) as mid_cn
,lower(category) as l_category
,upper(category) as u_category
,cast(len(customer_name) as varchar(10)) as l_cn
,category +' ' +customer_name as cat_cust
,sales+profit  as sales_profit
,cast(order_date as varchar(10))+' ' +customer_name as cat_cust
,CONCAT(customer_name,' ',category) as c_cc
from orders;

select order_id,order_date,ship_date, DATEPART(year,order_date) as year_order
from orders
where  DATEPART(year,order_date) = 2021

select order_id,order_date,ship_date
, DATEPART(year,order_date) as year_order
, DATEPART(month,order_date) as month_order
, DATEPART(weekday,order_date) as month_order
from orders

select DATEPART(year,order_date) as year_order , sum(sales) 
from orders
group by  DATEPART(year,order_date) 
order by year_order;

select order_id,order_date,ship_date,len(customer_name) as l_cust
, DATEADD(day,len(customer_name),order_date) as order_date_add
, dateadd(month,2,DATEADD(year,1,order_date)) as order_date_add
, DATEADD(day,-2,order_date) as order_date_sub
, DATEADD(day,-1*len(customer_name),order_date) as order_date_sub
from orders;

select order_id,order_date,ship_date,len(customer_name) as l_cust
,DATEDIFF(day,order_date,ship_date) as lead_time
from orders;

select order_id,order_date,profit 
,case
when profit<=50 then 'low profit'
when profit<=100 then 'medium profit'
when profit<=150 then 'high profit'
else 'very high profit'
end as profit_category
from orders 
;

select order_id,order_date,profit 
,case
when profit between 51 and 100 then 'medium profit'
when profit<=50 then 'low profit'
when profit between 101 and 150 then 'high profit'
else 'very high profit'
end as profit_category
from orders 
;

select order_id,order_date,profit 
,case
when profit between 51 and 100 then 'medium profit'
when profit<=50 then 'low profit'
when profit between 101 and 150 then 'high profit'
else 'very high profit'
end as profit_category
from orders 
;
select order_id,order_date,profit 
, datediff(year,order_date,getdate())  as order_age
from orders 

select datediff(year,order_date,getdate())  as order_age, count(distinct order_id)
from orders 
group by datediff(year,order_date,getdate())

select  case when datediff(year,order_date,getdate())  between 2 and 3 then 'order_23'
 when datediff(year,order_date,getdate())  between 4 and 5 then 'order_45'
 end as order_groups, count(distinct order_id)
from orders 
group by case when datediff(year,order_date,getdate())  between 2 and 3 then 'order_23'
 when datediff(year,order_date,getdate())  between 4 and 5 then 'order_45'
 end 


select order_id , CHARINDEX('-',order_id)
,SUBSTRING(order_id, 1,CHARINDEX('-',order_id)-1)
, CHARINDEX('-',order_id,4)
from orders;

create view orders_furniture as
(
select order_id,order_date from orders 
where category='Furniture'
);

select * from orders_furniture;

select * from
(select category,region,sales from orders) t
pivot (sum(sales) for region in ([East],[West],[Central],[South])
)
as pivot_table;

select category
,sum(case when region='East' then sales end) as east
,sum(case when region='West' then sales end) as west
,sum(case when region='Central' then sales end) as central
,sum(case when region='South' and ship_mode='Standard Class' then sales end) as south_standard_mode
from orders
group by category;

select category,region,sum(sales)
from orders
group by category,region;
--207
select avg(sales), sum(sales)/count(*) from orders
--sub queries inner query, outer query
--398.040911708253

select max(order_sales) from
(select order_id,sum(sales) as order_sales
from orders
group by order_id) A

select order_id,sum(sales) as order_sales into #temp_orders
from orders
group by order_id

select avg(order_sales) from #temp_orders

select order_id,sum(sales) as order_sales into final_orders
from orders
group by order_id

select * 
from orders
where order_id  in (select order_id from order_returns)






select getdate()


;
category__manager
category, manager_name
--joins
--string/date functions
-- case when
--connecting from excel
--views
--pivot