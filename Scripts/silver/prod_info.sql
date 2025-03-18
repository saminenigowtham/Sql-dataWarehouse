
-- changeing the table header because we have the derived coloums 
if OBJECT_ID('silver.crm_prd_info','U') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost bigint,
	prd_line nvarchar(10),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime default getdate()
);

-- inserting into silver.crm_prd_info
-- truncate table then inserting 

truncate table silver.crm_cust_info;
insert into silver.crm_prd_info(
	prd_id ,
	cat_id ,
	prd_key ,
	prd_nm ,
	prd_cost,
	prd_line ,
	prd_start_dt,
	prd_end_dt 
)
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost , 0) as prd_cost,
case (trim(upper(prd_line)))
	when 'M' then 'Mountains'
	when 'R' then 'Road'
	when 'S' then 'Streets'
	when 'T' then 'Touring'
	else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info
;



-- data Quality chescks

 --   SILVER
 -- QUALLITY CHECKS FOR THE DATA in silver
 -- 1 . NO DUPLICATES OF THE CST_ID
 -- EXP : NO OUTPUT
 SELECT CST_ID,COUNT(*) FROM silver.CRM_CUST_INFO
 GROUP BY CST_ID
 HAVING COUNT(*) > 1 OR CST_ID IS NULL;

 -- 2 . NO EXTRA SPACES IN NAMES 
 -- EXP : NO OUTPUT
 SELECT cst_lastname FROM silver.CRM_CUST_INFO
 WHERE cst_lastname != TRIM(cst_lastname)

 -- 3 . data standization and consistency
 select distinct cst_gndr from silver.CRM_CUST_INFO;

  select * from silver.CRM_prd_INFO
