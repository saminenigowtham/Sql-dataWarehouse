if OBJECT_ID('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price bigint,
dwh_create_date datetime default getdate()
);
-- inserting the data in to table but we have to truncate the data if it exists
truncate table silver.crm_sales_details;
-- inserting the data
insert into silver.crm_sales_details(
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity ,
sls_price 
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
	when (sls_order_dt = 0) or len(sls_order_dt) != 8 then Null
	else cast(cast(sls_order_dt as nvarchar) as date)
end as sls_order_dt,
case 
	when (sls_ship_dt = 0) or len(sls_ship_dt) != 8 then Null
	else cast(cast(sls_ship_dt as nvarchar) as date)
end as sls_ship_dt,
case 
	when (sls_due_dt = 0) or len(sls_due_dt) != 8 then Null
	else cast(cast(sls_due_dt as nvarchar) as date)
end as sls_due_dt,
case 
	when sls_sales <= 0 or sls_sales is null or sls_sales != abs(sls_price) * sls_quantity
		then abs(sls_price) * sls_quantity
	else sls_sales
end as sls_sales,
sls_quantity,
case 
	when sls_price <= 0 or sls_price is null 
		then abs(sls_sales) / nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
;



select * from silver.crm_sales_details;
