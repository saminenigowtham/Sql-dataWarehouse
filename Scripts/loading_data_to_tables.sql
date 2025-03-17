-- loading data into tables uding bulk insert

-- Creating Stored Procedure 
create or Alter procedure bronze.load_bronze 
as
Begin
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);

	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);

	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);

	truncate table bronze.erp_cust_az12;
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);

	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);

	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\gouth\Desktop\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
end

  
-- seeeing the data 

select * from bronze.crm_cust_info;

select * from bronze.crm_prd_info;

select * from bronze.crm_sales_details;

select * from [bronze].[erp_cust_az12];

select * from [bronze].[erp_loc_a101];

select * from [bronze].[erp_px_cat_g1v2];
