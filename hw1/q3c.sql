select
	zip_code as zip,
	cast(zip_county as varchar(25)) as county,
	SUMMME.mme_sum/(select distinct(pop) from cse532.zip where cse532.zip.zip = SUMMME.zip_code and cse532.zip.pop!=0) as avg_mme_by_pop,
	rank() over (order by SUMMME.mme_sum/(select distinct(pop) from cse532.zip where cse532.zip.zip = SUMMME.zip_code and cse532.zip.pop!=0) desc) as rank
	from 
		(select 
			 BUYER_ZIP as zip_code,
			 BUYER_COUNTY as zip_county,
			 cast(sum(MME) as int) as mme_sum
			 from cse532.dea, cse532.zip where cse532.dea.BUYER_ZIP = cse532.zip.zip
			 group by BUYER_ZIP, BUYER_COUNTY) as SUMMME
	where SUMMME.mme_sum / (select distinct(cse532.zip.pop) from cse532.zip where cse532.zip.zip = SUMMME.zip_code and cse532.zip.pop!=0) is not null
limit 0, 10;

--select * from cse532.zip where cse532.zip.zip = '00501';
--select * from cse532.zip limit 0, 5;