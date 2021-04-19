select 
	year(TRANSACTION_DATE) as year,
	month(TRANSACTION_DATE) as month,
 	' '|| cast(sum(DOSAGE_UNIT) as bigint) as sum,
 	cast(BUYER_STATE as varchar(20)) as state,
 	cast(BUYER_COUNTY as varchar(20)) as county
 	
 from cse532.dea 
 group by grouping sets(
 	(year(TRANSACTION_DATE), month(TRANSACTION_DATE), BUYER_STATE, BUYER_COUNTY),
 	(year(TRANSACTION_DATE), BUYER_STATE, BUYER_COUNTY),
 	(year(TRANSACTION_DATE), month(TRANSACTION_DATE), BUYER_STATE),
 	(year(TRANSACTION_DATE), BUYER_STATE)
 )
 order by BUYER_COUNTY;
 
-- group by month(TRANSACTION_DATE), year(TRANSACTION_DATE), BUYER_COUNTY, BUYER_STATE
-- limit 0, 10;
