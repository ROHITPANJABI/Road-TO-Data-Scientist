use practice;
show tables;



select *
from interviewquery;

with recursive cte as (
select numbers,1 as counter
from interviewquery
union all
select numbers,counter+1
from cte
where counter<numbers)
select *
from cte
order by numbers desc;


-- (comment histogram) https://www.interviewquery.com/questions/comments-histogram (5- mar-2023) -- 

-- attempt 1

with cte as 
(select c.user_id,
case when c.created_at between '2020-01-01' and '2020-01-31'
then count(c.user_id)
else 0
end as comment_count
from users u left join comments c
on u.id = c.user_id
group by c.user_id)
select comment_count, count(comment_count) as freq
from cte
group by comment_count;

-- attempt 2

with cte as 
(select c.user_id,count(c.user_id) as comment_count
from users u left join comments c
on u.id = c.user_id
where c.created_at between '2020-01-01' and '2020-01-31'
group by c.user_id
union all
select distinct c.user_id,0 as comment_count
from users u left join comments c
on u.id = c.user_id
where c.created_at not between '2020-01-01' and '2020-01-31'
)
select comment_count, count(comment_count) as frequency
from cte
group by comment_count;

-- (second longest flight )  -- https://www.interviewquery.com/questions/second-longest-flight

--attempt 1( the problem with this is this is not identifying the routes which only have one flight path )

with path_cte as (
select *, flight_end-flight_start as flight_time,
case when destination_location>source_location
then concat(destination_location,source_location)
else
concat(source_location,destination_location) end as flight_paths
from flights
),
rank_cte as (

    select *,
    Rank() over (partition by flight_paths order by flight_time desc) ranking
    from path_cte
)
select id,destination_location,source_location,flight_start,flight_end
from rank_cte
where ranking=2;

--https://www.interviewquery.com/questions/payments-received

with jan2020_users as (
    select *
    from users
    where created_at between '2020-01-01' and '2020-01-31'
),
success_payments as
(
    select *
    from payments 
    where payment_state='Success'
),

joined as 
(
    select *
    from jan2020_users j left join success_payments s
    on j.id=s.payment_id
)
select *
from joined


-- attempt 2 (working)

with combo as (select *,case when sender_id>recipient_id then concat(sender_id,"_",recipient_id)
else concat(recipient_id,"_",sender_id) end as combi
from payments
where payment_state='Success'
),jan_signups as 
(
    select *
    from users
    where month(created_at)='01' and year(created_at)='2020'
),unified_ids as (
select *,substring(combi,1,length(combi)-position("_" in combi)) as ids
from combo
) , avid_transactions as (
select ids,sum(amount_cents) total_transaction
from jan_signups left join unified_ids
on jan_signups.id=unified_ids.payment_id
group by ids 
having sum(amount_cents)>100
)
select count( distinct ids) as num_customers
from avid_transactions
group by ids

-- https://www.interviewquery.com/questions/average-commute-time

select commuter_id,
floor(avg(timestampdiff(minute,start_dt,end_dt))) as avg_commuter_time,
(select round(avg(timestampdiff(minute,start_dt,end_dt)),0) 
from rides where city="NY") as avg_time

from rides

where city = "NY"

group by commuter_id;

-- https://www.interviewquery.com/questions/completed-shipments

select s.shipment_id,s.ship_date,s.customer_id,
case when c.membership_end_date>s.ship_date 
then "Y"
else "N" 
end as "is_member",
s.quantity
from shipments s left join customers c
on s.customer_id=c.customer_id



-- https://www.interviewquery.com/questions/weighted-average-sales

with one_day_cte as (
SELECT *,
lead(sales_volume*0.3) over(partition by product_id order by date desc) as one_day
FROM SALES
),two_day_cte as (
SELECT *,
lead(one_day*0.2/0.3) over(partition by product_id order by date desc) as two_day
FROM one_day_cte
)
select date,product_id,
(sales_volume*0.5+one_day+two_day) as weighted_avg_sales
from two_day_cte
where one_day is not null and two_day is not null

-- https://www.interviewquery.com/questions/over-budget-projects

with cte as (
select project_id,sum(coalesce(salary,0)) as total_spend
from employees e left join employee_projects ep
on e.id= ep.employee_id
group by project_id
),cte2 as (
    select TIMESTAMPDIFF(day,start_date,end_date) as proj_length,title,budget,id as proj_id
    from projects
)
select cte2.title,
case when cte.total_spend*cte2.proj_length/365>cte2.budget
then "overbudget"
else "within budget" END AS project_forecast
from cte2 left join cte
on cte2.proj_id = cte.project_id
order by cte2.title

-- https://www.interviewquery.com/questions/order-addresses

select round(
(SELECT count(t.shipping_address) as primary_address
FROM users u left join transactions t
on u.id=t.user_id
where u.address=t.shipping_address
) 
/
(select count(*)
from transactions),2) as home_address_percent



--  https://www.interviewquery.com/questions/user-experience-percentage

with cte as (
    select *,case when position_name in ('Data Analyst','Data Scientist')
    then 1 
    else 0
    end as positions,
    lag(end_date) over(partition by user_id order by position_name,start_date) as lag_date
    from user_experiences
    order by user_id,start_date
)
,cte2 as (
    select count(distinct user_id) as cnt1 
    from cte
    where start_date=lag_date and positions=1
),cte3 as (
    select count(distinct user_id) as cnt2
    from user_experiences
)
select cnt1/cnt2 as percentage
from cte2 join cte3
on 1=1

-- https://www.interviewquery.com/questions/random-weighted-driver

with cte as (
select id,weighting,weighting/sum(weighting) over() as scaled_weights
from drivers
),cte2 as (
select id, scaled_weights,sum(scaled_weights) over(order by id) as cumm_weight
from cte
)

select id
from cte2
where cumm_weight >rand()
order by id
limit 1;

-- https://www.interviewquery.com/questions/random-sql-sample

with cte as (
select *,id/sum(id) over() as scaled_id
from big_table
), cte2 as 
(
    select *,sum(scaled_id) over(order by id) as cumm_id
    from cte
)
select id,name
from cte2
where cumm_id > rand()
order by id
limit 1;



-- https://www.interviewquery.com/questions/departmental-spend-by-quarter

with cte as (
select *,
case when transaction_date between '2023-01-01' and '2023-03-31'
then "Q1"
when transaction_date between '2023-04-01' and '2023-06-3'
then "Q2"
when transaction_date between '2023-07-01' and '2023-09-30'
then "Q3"
else 
"Q4" end as quarter
from transactions
),cte2 as (
select quarter,
sum(case when department ="IT" then amount else 0 end) as it_spending,
sum(case when department ="HR" then amount else 0 end) as hr_spending,
sum(case when department ="Marketing" then amount else 0 end) as marketing_spending,
sum(case when department not in ("IT","HR","Marketing") then amount else 0 end) as other_spending
from cte
group by quarter
)
select *
from cte2

-- https://www.interviewquery.com/questions/percentage-of-revenue-by-year

with yearlysales as (
    select year(created_at) as yr, sum(amount-amount_refunded) as total
    from annual_payments
    group by year(created_at)
)
,total_sales as 
(
    select sum(amount-amount_refunded) as total_sales
    from annual_payments
),first_year_sales as (
    select yr,total
    from yearlysales  
    order by yr
    limit 1  
)
,last_year_sales as (
    select yr,total
    from yearlysales
    order by yr desc
    limit 1
)
select (
select round(total*100/total_sales,2)
from total_sales join first_year_sales
on 1=1) as percent_first,
(select round(total*100/total_sales,2)
from total_sales join last_year_sales
on 1=1) as percent_last

-- https://www.interviewquery.com/questions/hr-salary-reporting

with cte as (
select * ,overtime_hours*overtime_rate as compensation
from employees
)
select job_title,sum(salary) as total_salaries,sum(compensation) as total_overtime_payments,
sum(salary+compensation) as total_compensation
from cte
group by job_title

 -- https://www.interviewquery.com/questions/categorize-sales

with cte as (
select *,
case when sale_amount<2000 and region <>'East' and month(sale_date)<>7
then 'standard Sales'
when sale_amount >=2000 and month(sale_date) <> 7
then 'Premium sales'
when month(sale_date)<>7 and region ='East'
then 'Premium sales'
when month(sale_date)=7 then 'Promotional Sales'
else 'other sales'
end as sale_type
from  sales
)

select region ,sum(sale_amount) as total_sales,
sum(case sale_type when 'Promotional Sales' then sale_amount else 0 end) as promotional_sales,
sum(case sale_type when 'Premium Sales' then sale_amount else 0 end)as premium_sales,
sum(case sale_type when 'standard Sales' then sale_amount else 0 end) as standard_sales
 
from cte 
group by region
order by region

-- https://www.interviewquery.com/questions/released-patients

with cte as (
select release_date,released_patients, (released_patients - lag(released_patients) over(order by release_date)) as day_prior
from released_patients
)
select release_date,released_patients
from cte
where day_prior > 0 


-- https://www.interviewquery.com/questions/closed-accounts

with cte as (
select count(*) as oc
from account_status a join account_status b
on a.account_id=b.account_id
where a.date='2019-12-31' and b.date='2020-01-01'
and a.status='open' and b.status='closed'
),cte2 as 
(
    select count(*) as o
    from account_status
    where date='2019-12-31' and status='open'
)
select oc/o as percentage_closed
from cte join cte2
on 1=1

-- https://www.interviewquery.com/questions/book-availability-update

create table book_df(
    book_id int,
    book_tilte varchar(100),
    copies_available int
)
insert into book_df
values (0,"Moby Dick",5),
(1,"1984",7),
(2,"To Kill a Mockingbird",3),
(3,"The Great Gatsby",8),
(4,"Pride and Prejudice",10)


select *
from book_df


--https://www.interviewquery.com/questions/rolling-average-steps

with cte as (
SELECT user_id,steps,date,row_number() over(partition by user_id order by date) as wro,
round(avg(steps) over(order by user_id,date rows between 2 preceding and current row)) as avg_steps
FROM daily_steps
)cte 2 as (
select user_id,date,avg_steps
from cte
where wro>2
order by user_id
)


--https://www.interviewquery.com/questions/lowest-paid

select ep.employee_id,e.salary,count(p.title) as completed_projects
from employee_projects ep left join projects p
on ep.project_id=p.id
join employees e
on ep.employee_id=e.id
where end_date is not null
group by 1
having count(p.title)>=2
order by salary
limit 3


-- https://www.interviewquery.com/questions/third-purchase

with cte as (
select * ,row_number() over(partition by user_id order by created_at) as row_nd
from transactions
)
select user_id,created_at,product_id,quantity
from cte
where row_nd=3

-- https://www.interviewquery.com/questions/user-experience-percentage

with cte1 as (
    select user_id,position_name,start_date,coalesce(end_date,date(now()))as enddate,
    row_number() over(partition by user_id order by start_date) as rnd
    from user_experiences
)
,cte as (
select ue.user_id,ue.position_name,ue2.position_name as p2,ue.rnd,ue2.rnd as rnd2
from cte1 ue join cte1 ue2
on ue.start_date>=ue2.enddate
and ue.user_id=ue2.user_id
and ue.rnd-1=ue2.rnd
),total_users as (
    select count(distinct user_id) as all_users
    from user_experiences
),ds_da as (
select count(distinct user_id) da_ds_users
from cte 
where position_name="Data Scientist" and p2="Data Analyst"
)
select da_ds_users/all_users as percentage
from ds_da join total_users
on 1=1

-- https://www.interviewquery.com/questions/rolling-bank-transactions

with cte as (
    select date_format (created_at,'%Y-%m-%d') as dt,sum(transaction_value) as trv
    from bank_transactions
    where transaction_value>0
    group by 1
)
select dt,
avg(trv) over(order by dt rows between 2 preceding and current row) as rolling_three_day
from cte

-- https://www.interviewquery.com/questions/atm-robbery

select distinct a.user_id
from bank_transactions a join bank_transactions b
on time_to_sec(a.created_at)=time_to_sec(b.created_at)+10
or time_to_sec(a.created_at)+10=time_to_sec(b.created_at)
order by a.user_id







