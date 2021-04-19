drop index cse532.siteidx;

drop index cse532.countiesidx;

create index cse532.siteidx on cse532.site(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.countiesidx on cse532.counties(shape) extend using db2gse.spatial_index(0.85, 2, 5);

runstats on table cse532.site and indexes all;

runstats on table cse532.counties and indexes all;