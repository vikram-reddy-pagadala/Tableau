1- write a sql to get all the orders where customers name has "a" as second character and "d" as fourth character 
select * from orders where customer_name like '_a_d%';

2- write a sql to get all the orders placed in the month of dec 2020 

select * from orders where  order_date between '2020-12-01' and '2020-12-31';

select * from orders where  datepart(year,order_date)=2020 and datepart(month,order_date)=12;

3- write a query to get all the orders where ship_mode is neither in 'Standard Class' nor in 'First Class' and ship_date is after nov 2020 

select * from orders where  ship_mode not in ('Standard Class','First Class')
and ship_date > '2020-11-30'

4- write a query to get all the orders where customer name neither start with "A" and nor ends with "n" 

select * from orders where customer_name not like 'A%n';

5- write a query to get all the orders where profit is negative 

select * from orders where profit<0

6- write a query to get all the orders where either quantity is less than 3 or profit is 0 

select * from orders where profit=0 or quantity<3

7- your manager handles the sales for South region and he wants you to create a report of all the orders in his region where some discount is provided to the customers 

select * from orders where region='South' and discount>0

8- write a query to find top 5 orders with highest sales in furniture category 

select top 5 * from orders where category='Furniture' order by sales desc 


9- write a query to find all the records in technology and furniture category for the orders placed in the year 2020 only 

select   * from orders 
where category in ('Furniture','Technology') 
and order_date between '2020-01-01' and '2020-12-31'


select   * from orders 
where category in ('Furniture','Technology') 
and datepart(year,order_date)=2020 


10-write a query to find all the orders where order date is in year 2020 but ship date is in 2021 

select   * from orders where 
order_date between '2020-01-01' and '2020-12-31' and ship_date between '2021-01-01' and '2021-12-31'


select   * from orders where 
datepart(year,order_date)=2020  and datepart(year,ship_date)=2021 

11- write a query to get total profit, first order date and latest order date for each category

select category , sum(profit) as total_profit, min(order_date) as first_order_date
,max(order_date) as latest_order_date
from orders
group by category 



12- write a query to find total number of products in each category.

select category,count(distinct product_id) as no_of_products
from orders
group by category


13- write a query to find top 5 sub categories in west region by total quantity sold
select top 5  sub_category, sum(quantity) as total_quantity
from orders
where region='West'
group by sub_category
order by total_quantity desc


14- write a query to find total sales for each region and ship mode combination for orders in year 2020
select region,ship_mode ,sum(sales) as total_sales
from orders
where order_date between '2020-01-01' and '2020-12-31'
group by region,ship_mode

15- write a query to get region wise count of return orders

select region,count(distinct o.order_id) as no_of_return_orders
from orders o
inner join returns r on o.order_id=r.order_id
group by region

16- write a query to get category wise sales of orders that were not returned
select category,sum(o.sales) as total_sales
from orders o
left join returns r on o.order_id=r.order_id
where r.order_id is null
group by category



17 - write a query to print sub categories where we have all 3 kinds of returns (others,bad quality,wrong items)

select o.sub_category
from orders o
inner join returns r on o.order_id=r.order_id
group by o.sub_category
having count(distinct r.return_reason)=3


18- write a query to find top 3 subcategories by sales of returned orders in east region
select top 3 sub_category,sum(o.sales) as return_sales
from orders o
inner join returns r on o.order_id=r.order_id
where o.region='East'
group by sub_category
order by return_sales  desc

19-orders table can have multiple rows for a particular order_id when customers buys more than 1 product in an order.

write a query to find order ids where there is only 1 product bought by the customer.
select order_id
from orders 
group by order_id
having count(1)=1

20- write a query to print 3 columns : category, total_sales and (total sales of returned orders)
select o.category,sum(o.sales) as total_sales
,sum(case when r.order_id is not null then sales end) as return_orders_sales
from orders o
left join returns r on o.order_id=r.order_id
group by category


