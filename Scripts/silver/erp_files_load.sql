--truncate the data
truncate table silver.erp_cust_az12;
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

-- select distinct gen from bronze.erp_cust_az12;

-- erp of country
truncate table silver.erp_loc_a101;
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

select *
from silver.erp_loc_a101;

-- erp products
truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(
ID,
CAT,
SUBCAT,
MAINTENANCE
)
select * from bronze.erp_px_cat_g1v2
;
