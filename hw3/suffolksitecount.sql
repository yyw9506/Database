select 
	count(*) as totalcount

from cse532.site as s, cse532.counties as c 

where 
	c.STATEFP = '36' and c.COUNTY_NAME  = 'Suffolk' 
	and db2gse.st_contains(c.SHAPE, db2gse.st_point(db2gse.st_y(s.geolocation), db2gse.st_x(s.geolocation), 1)) = 1;