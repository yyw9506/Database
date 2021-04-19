create index dos_unit_idx on cse532.dea(DOSAGE_UNIT);
create index buyer_zip_idx on cse532.dea(BUYER_ZIP);
create index year_month_idx on cse532.dea(year(TRANSACTION_DATE), month(TRANSACTION_DATE));