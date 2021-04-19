drop table cse532.siteorginal;
drop table cse532.site;

-- cse532.siteorginal(Name, address1, address2, city, state, zipcode, country, latitude, longitude) 
-- cse532.site(Name, address1, address2, city, state, zipcode, country, geolocation)

create table cse532.siteorginal(
	name VARCHAR(100),
	address1 VARCHAR(50),
	address2 VARCHAR(10),
	city VARCHAR(30),
	state VARCHAR(10),
	zipcode VARCHAR(10),
	country VARCHAR(10),
	latitude DOUBLE,
	longitude DOUBLE
);

create table cse532.site(
	name VARCHAR(100),
	address1 VARCHAR(50),
	address2 VARCHAR(10),
	city VARCHAR(30),
	state VARCHAR(10),
	zipcode VARCHAR(10),
	country VARCHAR(10),
	geolocation DB2GSE.ST_POINT
);

!db2se register_spatial_column sample
-tableSchema      cse532
-tableName        site
-columnName       geolocation
-srsName          nad83_srs_1
;