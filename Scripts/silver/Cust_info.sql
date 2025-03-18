-- loading the data to silver layer
truncate table silver.crm_cust_info;
insert into silver.crm_cust_info(
CST_ID,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
SELECT 
CST_ID,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case 
	when trim(upper(cst_marital_status)) = 'S' then 'Single'
	when trim(upper(cst_marital_status)) = 'M' then 'Married'
	else 'n/a'
end as cst_marital_status,
case 
	when trim(upper(cst_gndr)) = 'F' then 'Female'
	when trim(upper(cst_gndr)) = 'M' then 'Male'
	else 'n/a'
end as cst_gndr,
cst_create_date
FROM 
(
	Select *,
	ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS R
	from bronze.crm_cust_info
) AS T
WHERE t.R =1 AND T.CST_ID IS NOT NULL;




-- CHECK ing data quality 

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

  select * from silver.CRM_CUST_INFO
