-- this script is to import chiller_flow data then
-- cleaning it and transfering it to the silver layer

-- login into the boronze database
select database();
use bronze;

-- creating the chiller_flow table
-- data will be imported as a string then converted ti datetime
create table if not exists bz_chiller_flow (
	date_time varchar(20),
	`CHILLER 1 flow l/s` varchar(20),
    date_time_1 varchar(20),
	`CHILLER 2 flow l/s` varchar(20),
	date_time_2 varchar(20),
    `CHILLER 3 flow l/s` varchar(20),
    date_time_3 varchar(20),
	`CHILLER 4 flow l/s` varchar(20)
    );
    
-- improrting the data 
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/basimyousef/Documents/BMS Data Analytics Project/BMS DATA - JAN 12/CHILLER 1 FLOW_2025-12-14_23-20-41_2026-01-11_23-20-41.csv'
INTO TABLE bz_chiller_flow
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
	(date_time,
	`CHILLER 1 flow l/s`,
    date_time_1,
	`CHILLER 2 flow l/s`,
	date_time_2,
    `CHILLER 3 flow l/s`,
    date_time_3,
	`CHILLER 4 flow l/s`
   );
    
-- verification
select * from `bz_chiller_flow` limit 1000;

-- deleting unnecessar columns/data
alter table `bz_chiller_flow` 
drop column date_time_1, 
drop column date_time_2,
drop column date_time_3;

DELETE FROM bz_chiller_flow WHERE date_time IS NULL OR date_time = '';

-- fixting data type:

CREATE TABLE silver.sv_chiller_flow (
    `date_time` datetime,
	`CHILLER 1 flow l/s` INT,
	`CHILLER 2 flow l/s` INT,
    `CHILLER 3 flow l/s` INT,
	`CHILLER 4 flow l/s` INT
);

INSERT INTO silver.sv_chiller_flow
SELECT 
    STR_TO_DATE(trim(date_time), '%Y-%m-%d %H:%i:%s'),
	CAST(GREATEST(0,ROUND(`CHILLER 1 flow l/s`)) AS UNSIGNED),
	CAST(GREATEST(0,ROUND(`CHILLER 2 flow l/s`)) AS UNSIGNED),
	CAST(GREATEST(0,ROUND(`CHILLER 3 flow l/s`)) AS UNSIGNED),
	CAST(GREATEST(0,ROUND(`CHILLER 4 flow l/s`)) AS UNSIGNED)
FROM bz_chiller_flow;

-- verification
select * from silver.sv_chiller_flow limit 1000;

-- verification no null
select * from silver.sv_chiller_flow 
where date_time is null 
or `CHILLER 1 flow l/s` is null
or `CHILLER 2 flow l/s` is null 
or `CHILLER 3 flow l/s` is null
or `CHILLER 4 flow l/s` is null;

-- Unpoviting
create table silver.sv_chiller_flow_STACKING(
    date_time DATETIME,
    chiller_id VARCHAR(50),
    `flow l/s` int
);
     
INSERT INTO silver.sv_chiller_flow_STACKING (date_time, chiller_id, `flow l/s`) 
	SELECT date_time, 'CHILLER 1',`CHILLER 1 flow l/s` FROM silver.sv_chiller_flow
	UNION ALL
	SELECT date_time, 'CHILLER 2',`CHILLER 2 flow l/s` FROM silver.sv_chiller_flow
    UNION ALL
	SELECT date_time, 'CHILLER 3',`CHILLER 3 flow l/s` FROM silver.sv_chiller_flow
    UNION ALL
	SELECT date_time, 'CHILLER 4',`CHILLER 4 flow l/s` FROM silver.sv_chiller_flow;
    
-- verification
select * from silver.sv_chiller_flow_STACKING;

-- renaming the table in silver layer
drop table if exists `silver`.`sv_chiller_flow`;
rename table `silver`.`sv_chiller_flow_STACKING` to `silver`.`sv_chiller_flow`;

-- verification
select * from silver.sv_chiller_flow limit 1000;

-- sensors's data time-sequencing & niose elimintation
create table gold.gd_chiller_flow as
select
    DATE_FORMAT(date_time, '%Y-%m-%d %H:%i:00') AS date_time,
    chiller_id,
    `flow l/s` AS `flow_l/s`
from silver.sv_chiller_flow;

-- verification
select * from gold.gd_chiller_flow limit 1000;