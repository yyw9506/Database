with dea_monthly as 
(select 
	cast(sum(DOSAGE_UNIT) as bigint) as monthly_sum_dosage_unit,
	--year(TRANSACTION_DATE) as year,
	--month(TRANSACTION_DATE) as month,
	year(TRANSACTION_DATE) || 
	case 
		when cast(month(TRANSACTION_DATE) as int) < 10
			then '0' || cast(month(TRANSACTION_DATE) as varchar(2))
		else cast(month(TRANSACTION_DATE) as varchar(2))
 	end as year_month
 	
from cse532.dea 
	where BUYER_STATE = 'KY' and BUYER_COUNTY = 'PIKE'
group by year(TRANSACTION_DATE), month(TRANSACTION_DATE))

select 
	cast('KY' as varchar(10)) as state,
	cast('PIKE' as varchar(10)) as county,
	year_month as year_month,
	monthly_sum_dosage_unit as monthly_sum,
	avg(monthly_sum_dosage_unit) over (order by year_month rows between 3 preceding and 1 preceding) as monthly_smooth
from dea_monthly;



