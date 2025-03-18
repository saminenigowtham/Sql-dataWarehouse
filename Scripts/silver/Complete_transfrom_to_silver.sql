/*
  1. we have to check the quality checks in data and by undesrtanding the data and then we have to transform the coloums 
  2. make sure that we have the joining key it may concate with the diffent keys find them
  3. for sales follow the busses rule sales = quantity * price follow the rule and fill the values 
  4. normailization and standaization of the data eg : M,F --> Male,Female
  5. stored all this silver.load_silver procedure 
  6. And also used time to calcuate the time 

     to use the code run this
      exec silver.load_silver;
*/


-- Storing in stored procedure 
create or Alter procedure silver.load_silver as
begin 
	begin try
	  declare @start_time datetime,@end_time datetime,@start_batch datetime,@end_batch datetime 
		set @start_batch = getdate()
		-- crm Products
		-- changeing the table header because we have the derived coloums 
		set @start_time = GETDATE()
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
		print '>>> truncate table  products'
		-- truncate table then inserting 
		truncate table silver.crm_prd_info;
		print '>>> inserrting products'
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
		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for Product load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'
		-- crm cust
		-- loading the data to silver layer
		set @start_time = getdate()
		print '>>> truncate table  customer'
		truncate table silver.crm_cust_info;
		print '>>> inserrting customer'
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

		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for Customer_crm load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'

		-- crm sales
		set @start_time = getdate()
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
		print '>>> truncate table  sales'
		truncate table silver.crm_sales_details;
		-- inserting the data
		print '>>> inserrting sales'
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
		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for Sales_crm load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'
		-- ERP load's
		set @start_time = getdate()
		--truncate the data
		print '>>> truncate table  customer_erp'
		truncate table silver.erp_cust_az12;
		print '>>> inserrting customer_erp'
		insert into silver.erp_cust_az12(
		cid,BDATE,GEN
		)
		select 
		case 
			when cid like 'NAS%' then substring(cid,4,len(cid))
			else cid
		end as CID,
		case 
			when BDATE > getdate() then  null
			else BDATE
		end as BDATE,
		case 
			when trim(upper(GEN)) = 'M' then 'Male'
			when trim(upper(GEN)) = 'F' then 'Female'
			when trim(upper(GEN)) = ' ' or trim(upper(GEN)) = NULL then 'n/a'
			else trim(gen)
		end as GEN
		from bronze.erp_cust_az12;
		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for Cust_az12 load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'

		-- select distinct gen from bronze.erp_cust_az12;
		set @start_time = getdate()
		-- erp of country
		print '>>> truncate table  loc_erp'
		truncate table silver.erp_loc_a101;
		print '>>> inserrting loc_erp'
		insert into silver.erp_loc_a101(
		CID,
		CNTRY
		)
		select 
		replace(CID,'-','') as CID,
		case 
			when trim(CNTRY) in ('US','USA') then 'United States'
			when trim(CNTRY) = 'DE' then 'Germany'
			when trim(cntry) = '' or cntry is NULL then 'n/a'
			else CNTRY
		end as CNTRY
		from bronze.erp_loc_a101;

		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for loc_a101 load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'

		-- erp products
		set @start_time = getdate()
		print '>>> truncate table  px_cat'
		truncate table silver.erp_px_cat_g1v2;
		print '>>> inserrting px_cat'
		insert into silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		select * from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE()
		print '-------------------------------'
		print 'Time for px_cat load : '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+' Seconds'
		print '--------------------------------'
		set @end_batch = GETDATE()
		print '-------------------------------'
		print 'Total Time for Batch load : '+ cast(datediff(second,@start_batch,@end_batch) as nvarchar)+' Seconds'
		print '--------------------------------'
	end try
	begin catch
	print 'Error message'
	print 'Error message : ' + cast(error_number() as nvarchar)
	end catch
	
end
