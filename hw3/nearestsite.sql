select
	name as name,
	cast(db2gse.st_astext(geolocation) as VARCHAR(60)) as geolocation,
	cast(db2gse.st_distance(geolocation, db2gse.st_point(40.824369, -72.993983, 1), 'STATUTE MILE') as DECIMAL(5, 3)) as distance
from cse532.site 
	where db2gse.st_within(geolocation, db2gse.st_buffer(db2gse.st_point(40.824369, -72.993983, 1), 10, 'STATUTE MILE'))=1
order by distance limit 3;