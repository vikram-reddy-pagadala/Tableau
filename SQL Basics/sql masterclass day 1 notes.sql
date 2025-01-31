select top 100 * from orders;

select top 10 order_id,order_date 
from orders;

select  *
from orders
order by order_date 


select top 5 *
from orders
order by order_date desc , sales desc ;

--filtering using where clause
select top 5 order_id,order_date 
from orders
where category='Technology'
order by order_date

select * from orders
where order_date = '2021-11-11'

select * from orders
where quantity = 2

select * from orders
where quantity >= 2

select * from orders
where order_date > '2021-11-11'
order by order_date

select * from orders
where order_date between '2021-01-01' and '2021-01-31'
order by order_date


select * from orders
where quantity between 2 and 4


select *
from orders
where category >= 'Technology'

--'vikar gupta\'s dairy'


select *
from orders
where category in ('Technology','Furniture')


select *
from orders
where quantity in (1,3)

-----------------

select *
from orders
where order_date in ('2018-08-25','2019-03-02');


select * 
from orders
where customer_name like 'Ann%'



select * 
from orders
where customer_name like 'N%ge'

select * 
from orders
where customer_name like '%ge%' --escape '%'

select * from orders
where customer_name like '_e%';


select order_id,order_date , category, segment
from orders
where category = 'Furniture' and segment = 'Consumer';

select order_id,order_date , category, segment
from orders
where category = 'Furniture' or segment = 'Consumer'

select order_id,order_date , category, segment from orders;

/*How can we select all rows where 1. category is Furniture but segment is not Consumer2. Segment is consumer but category is not furniture*/

select * from orders
where category != 'Furniture'


select * from orders
where quantity != 2

select * from orders
where quantity not in (1,3)


select order_id,order_date , category, segment
from orders
where category = 'Furniture' and segment != 'Consumer';

select order_id,order_date , category, segment
from orders
where category != 'Furniture' and segment = 'Consumer';


select order_id,order_date , category, segment , quantity
from orders
where  not (category = 'Furniture' and quantity > 2 and segment = 'Consumer');


select order_id,order_date , category, segment , quantity
from orders
where category != 'Furniture' and quantity <= 2 and segment != 'Consumer';


select order_id,order_date , category, segment,quantity
from orders
where (category = 'Furniture' and segment = 'Consumer') or (quantity=2 and segment='Consumer');

---null is null

select * from orders where ship_mode='null'

update orders set ship_mode=null where row_id in (1370,1364)

select * from orders where ship_mode is null
select * from orders 
where ship_mode is not null and quantity=2
 
 --select * from orders where ship_mode=''
 ---aggregation

select count(*) as no_of_records from orders
--select order_date,ship_mode as ship_category  from orders 

select sum(sales) as total_sales from orders

select max(sales) as max_sales from orders

select min(sales) as min_sales from orders

select avg(sales) as avg_sales from orders

select sum(sales)/count(*) as avg_sales from orders

select avg(sales) as avg_sales,sum(sales) as total_sales,sum(sales)/count(*) as avg_sales from orders;

select category,sub_category ,sum(sales) as cat_sales
from orders
group by category,sub_category


select category ,sub_category,sum(sales) as cat_sales
from orders
where quantity=2
group by category,sub_category
order by category,sub_category
;


select top 2 category ,sub_category,sum(sales) as cat_sales
from orders
where quantity=2
group by category,sub_category
order by cat_sales desc


select sub_category ,sum(sales) as scat_sales
from orders
group by sub_category
having sum(sales) > 20000
order by scat_sales desc

select category,sub_category ,sum(sales) as scat_sales,max(sales) as m_sales
from orders
group by category,sub_category
having max(sales) > 5000
order by scat_sales desc


select coalesce(ship_mode,'unknown') as ship_mode,sum(sales) from orders
group by coalesce(ship_mode,'unknown') 


select distinct segment from orders

select segment,sum(sales) from orders
group by segment