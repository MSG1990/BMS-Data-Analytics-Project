-- this script is to import chiller_supply_ return_temperature data then
-- cleaning it and transfering it to the silver layer

-- login into the boronze database
select database();
use bronze;

-- creating the Chillers_RLA% table
-- datetime info must be imported as a string then converted ti datetime
create table if not exists bz_chillers_Tin_Tout (
	date_time varchar(20),
	`CHILLER 1 - T out` varchar(20),
    date_time_1 varchar(20),
	`CHILLER 1 - T in` varchar(20),
	date_time_2 varchar(20),
    `CHILLER 2 - T in` varchar(20),
    date_time_3 varchar(20),
	`CHILLER 2 - T out` varchar(20),
    date_time_4 varchar(20),
    `CHILLER 3 - T in` varchar(20),
    date_time_5 varchar(20),
	`CHILLER 3 - T out` varchar(20),
	date_time_6 varchar(20),
    `CHILLER 4 - T in` varchar(20),
    date_time_7 varchar(20),
	`CHILLER 4 - T out` varchar(20)
    );
    
-- improrting the data bi wizard
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/basimyousef/Documents/BMS Data Analytics Project/BMS DATA - JAN 12/CHILLER 01 SUPPLY TEMPERATURE_2025-12-15_09-42-39_2026-01-12_09-42-39.csv'
INTO TABLE bz_chillers_Tin_Tout
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
	(date_time,
	`CHILLER 1 - T out`,
    date_time_1,
	`CHILLER 1 - T in`,
	date_time_2,
    `CHILLER 2 - T in`,
    date_time_3,
	`CHILLER 2 - T out`,
    date_time_4,
    `CHILLER 3 - T in`,
    date_time_5,
	`CHILLER 3 - T out`,
	date_time_6,
    `CHILLER 4 - T in`,
    date_time_7,
	`CHILLER 4 - T out`);
    
-- verification
select * from `bz_chillers_Tin_Tout` limit 1000;

-- deleting unnecessar columns/data
alter table `bz_chillers_Tin_Tout` 
drop column date_time_1, 
drop column date_time_2,
drop column date_time_3,
drop column date_time_4,
drop column date_time_5,
drop column date_time_6,
drop column date_time_7;

DELETE FROM bz_chillers_Tin_Tout WHERE date_time IS NULL OR date_time = '';

-- fixting data type:
CREATE TABLE silver.sv_chillers_Tin_Tout_R1 (
    date_time datetime,
	`CHILLER 1 - T out` float,
	`CHILLER 1 - T in` float,
    `CHILLER 2 - T in` float,
	`CHILLER 2 - T out` float,
    `CHILLER 3 - T in` float,
	`CHILLER 3 - T out` float,
    `CHILLER 4 - T in` float,
	`CHILLER 4 - T out` float
);

INSERT INTO silver.sv_chillers_Tin_Tout_R1
SELECT 
    STR_TO_DATE(trim(date_time), '%Y-%m-%d %H:%i:%s'),
    CAST(`CHILLER 1 - T out` AS float),
    CAST(`CHILLER 1 - T in` AS float),
    CAST(`CHILLER 2 - T in` AS float),
    CAST(`CHILLER 2 - T out` AS float),
    CAST(`CHILLER 3 - T in` AS float),
    CAST(`CHILLER 3 - T out` AS float),
    CAST(`CHILLER 4 - T in` AS float),
    CAST(`CHILLER 4 - T out` AS float)
FROM bz_chillers_Tin_Tout;

-- verification
select * from silver.sv_chillers_Tin_Tout_R1 limit 1000;

-- verification no null
select * from silver.sv_chillers_Tin_Tout_R1 where date_time is null or 
`CHILLER 1 - T out` is null or
`CHILLER 1 - T in` is null or
`CHILLER 2 - T in` is null or
`CHILLER 2 - T out` is null or
`CHILLER 3 - T in` is null or
`CHILLER 3 - T out` is null or
`CHILLER 4 - T in` is null or
`CHILLER 4 - T out` is null;

-- Unpoviting
create table silver.sv_chillers_Tin_Tout_stacking(
    date_time DATETIME,
    chiller_id VARCHAR(50),
    `T in` FLOAT,
    `T out` FLOAT
);
     
INSERT INTO silver.sv_chillers_Tin_Tout_stacking (date_time, chiller_id, `T in`, `T out`) 
	SELECT date_time, 'CHILLER 1',`CHILLER 1 - T in`,`CHILLER 1 - T out` FROM silver.sv_chillers_Tin_Tout_R1
	UNION ALL
	SELECT date_time, 'CHILLER 2',`CHILLER 2 - T in`,`CHILLER 2 - T out` FROM silver.sv_chillers_Tin_Tout_R1
	UNION ALL
	SELECT date_time, 'CHILLER 3',`CHILLER 3 - T in`,`CHILLER 3 - T out` FROM silver.sv_chillers_Tin_Tout_R1
	UNION ALL
	SELECT date_time, 'CHILLER 4',`CHILLER 4 - T in`,`CHILLER 4 - T out` FROM silver.sv_chillers_Tin_Tout_R1;

    
-- verification
select * from silver.sv_chillers_Tin_Tout_stacking;

-- renaming Tin/out table in silver layer
drop table if exists `silver`.`sv_chillers_Tin_Tout_R1`;
rename table `silver`.`sv_chillers_Tin_Tout_stacking` to `silver`.`sv_chillers_Tin_Tout`;

-- verification
select * from silver.sv_chillers_Tin_Tout limit 1000;
select * from silver.sv_chillers_Tin_Tout limit 1000 offset 2000;

-- fixing the readings of a malfunctioning sensor
update silver.sv_chillers_Tin_Tout
	set `T in` = 15 
    where chiller_id = 'CHILLER 1';
    
-- sensors's data time-sequencing & niose elimintation
create table gold.gd_chillers_Tin_Tout_finetuned as
select
    DATE_FORMAT(date_time, '%Y-%m-%d %H:%i:00') AS date_time,
    chiller_id,
    `T in` AS `T in`,
    `T out` AS `T out`
from silver.sv_chillers_Tin_Tout;

-- verification
select * from gold.gd_chillers_Tin_Tout_finetuned limit 1000;
select * from gold.gd_chillers_Tin_Tout_finetuned limit 1000 offset 2000;