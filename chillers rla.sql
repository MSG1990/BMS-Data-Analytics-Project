-- this script is to import chiller_RLA5 data then
-- cleaning it and transfering it to the silver layer

-- login into the boronze database
select database();
use bronze;

-- creating the Chillers_RLA% table
-- datetime info must be imported as a string then converted ti datetime
create table if not exists bz_chillers_RLA (
	date_time varchar(20),
	CHILLER_2_SYS1_RLA varchar(20),
    date_time_1 varchar(20),
	CHILLER_1_SYS1_RLA varchar(20),
    date_time_2 varchar(20),
	CHILLER_1_SYS2_RLA varchar(20),
    date_time_3 varchar(20),
	CHILLER_2_SYS2_RLA varchar(20),
    date_time_4 varchar(20),
	CHILLER_3_SYS1_RLA varchar(20),
    date_time_5 varchar(20),
	CHILLER_3_SYS2_RLA varchar(20),
    date_time_6 varchar(20),
	CHILLER_4_SYS1_RLA varchar(20),
    date_time_7 varchar(20),
	CHILLER_4_SYS2_RLA varchar(20)
    );
    
-- improrting the data bi wizard
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/basimyousef/Documents/BMS Data Analytics Project/BMS DATA - JAN 12/CHILLER 2 - SYS1 RLA %_2025-12-09_21-13-00_2026-01-06_21-13-00.csv'
INTO TABLE bz_chillers_RLA
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(date_time,
	CHILLER_2_SYS1_RLA,
    date_time_1,
	CHILLER_1_SYS1_RLA,
    date_time_2,
	CHILLER_1_SYS2_RLA,
    date_time_3,
	CHILLER_2_SYS2_RLA,
    date_time_4,
	CHILLER_3_SYS1_RLA,
    date_time_5,
	CHILLER_3_SYS2_RLA,
    date_time_6,
	CHILLER_4_SYS1_RLA,
    date_time_7,
	CHILLER_4_SYS2_RLA);
    
-- verification
select * from `bz_chillers_RLA` limit 1000 offset 20;

-- deleting unnecessary columns/data
alter table `bz_chillers_RLA` 
drop column date_time_1, 
drop column date_time_2,
drop column date_time_3,
drop column date_time_4,
drop column date_time_5,
drop column date_time_6,
drop column date_time_7;

DELETE FROM bz_chillers_RLA WHERE date_time IS NULL OR date_time = '';

-- fixting data type:

CREATE TABLE silver.sv_chillers_RLA (
    date_time DATETIME,
    CHILLER_1_SYS1_RLA FLOAT,
    CHILLER_1_SYS2_RLA FLOAT,
    CHILLER_2_SYS1_RLA FLOAT,
    CHILLER_2_SYS2_RLA FLOAT,
    CHILLER_3_SYS1_RLA FLOAT,
    CHILLER_3_SYS2_RLA FLOAT,
    CHILLER_4_SYS1_RLA FLOAT,
    CHILLER_4_SYS2_RLA FLOAT
);

INSERT INTO silver.sv_chillers_RLA
SELECT 
    STR_TO_DATE(trim(date_time), '%Y-%m-%d %H:%i:%s'),
    CAST(`CHILLER_1_SYS1_RLA` AS FLOAT),
    CAST(`CHILLER_1_SYS2_RLA` AS FLOAT),
    CAST(`CHILLER_2_SYS1_RLA` AS FLOAT),
    CAST(`CHILLER_2_SYS2_RLA` AS FLOAT),
    CAST(`CHILLER_3_SYS1_RLA` AS FLOAT),
    CAST(`CHILLER_3_SYS2_RLA` AS FLOAT),
    CAST(`CHILLER_4_SYS1_RLA` AS FLOAT),
    CAST(`CHILLER_4_SYS2_RLA` AS FLOAT)
FROM bz_chillers_RLA;

-- verification
select * from silver.sv_chillers_RLA limit 1000;

-- verification no null
select * from silver.sv_chillers_RLA where date_time is null or 
CHILLER_1_SYS1_RLA is null or
CHILLER_1_SYS2_RLA is null or
CHILLER_2_SYS1_RLA is null or
CHILLER_2_SYS2_RLA is null or
CHILLER_3_SYS1_RLA is null or
CHILLER_3_SYS2_RLA is null or
CHILLER_4_SYS1_RLA is null or
CHILLER_4_SYS2_RLA is null;

-- Unpoviting
create table silver.sv_chillers_RLA_stacking (
    date_time DATETIME,
    chiller_name VARCHAR(50),
    rla_value FLOAT
);
    
INSERT INTO silver.sv_chillers_RLA_stacking (date_time, chiller_name, rla_value) -- Explicitly name the 3 columns
	SELECT date_time, 'CHILLER 1_SYS1', CHILLER_1_SYS1_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 1_SYS2', CHILLER_1_SYS2_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 2_SYS1', CHILLER_2_SYS1_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 2_SYS2', CHILLER_2_SYS2_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 3_SYS1', CHILLER_3_SYS1_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 3_SYS2', CHILLER_3_SYS2_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 4_SYS1', CHILLER_4_SYS1_RLA FROM silver.sv_chillers_RLA
	UNION ALL
	SELECT date_time, 'CHILLER 4_SYS2', CHILLER_4_SYS2_RLA FROM silver.sv_chillers_RLA;
    
-- verification
select * from silver.sv_chillers_RLA_stacking limit 1000;

-- renaming RLA table in silver layer
drop table if exists `silver`.`sv_chillers_RLA`;

rename table `silver`.`sv_chillers_RLA_stacking` to `silver`.`sv_chillers_RLA`;

-- splitting chiller name and sys:
ALTER TABLE silver.sv_chillers_RLA 
ADD COLUMN chiller_id VARCHAR(10),
ADD COLUMN system_id VARCHAR(10);

UPDATE silver.sv_chillers_RLA
SET 
    chiller_id = SUBSTRING_INDEX(chiller_name, '_', 1),  -- Gets 'CHILLER 1'
    system_id = SUBSTRING_INDEX(chiller_name, '_', -1); -- Gets 'SYS1'
    
ALTER TABLE silver.sv_chillers_RLA 
DROP COLUMN chiller_name;

-- verification
select * from silver.sv_chillers_RLA limit 1000;

-- sensors's data time-sequencing & niose elimintation
create table silver.sv_chillers_RLA_finetuned as
select
    DATE_FORMAT(date_time, '%Y-%m-%d %H:%i:00') AS adjusted_date_time,
    chiller_id,
    system_id,
    round(avg(rla_value),2) AS rla_value
from silver.sv_chillers_RLA
group by adjusted_date_time, chiller_id,system_id;

-- verification
select * from silver.sv_chillers_RLA_finetuned limit 1000;
select * from silver.sv_chillers_RLA_finetuned limit 1000 offset 1000;
select * from silver.sv_chillers_RLA_finetuned
	order by adjusted_date_time asc, chiller_id asc;
    
-- capturing the RLA at the chiller level instead of system level
create table silver.sv_chillers_RLA_finetuned_summedRLA as
select
	adjusted_date_time,
	chiller_id,
	sum(rla_value) as total_rla
from silver.sv_chillers_RLA_finetuned 
group by adjusted_date_time, chiller_id;

-- verification
select * from silver.sv_chillers_RLA_finetuned_summedRLA limit 1000;

-- to the gold schema
create table gold.gd__chillers_RLA as 
select
	adjusted_date_time,
	chiller_id,
	total_rla
from silver.sv_chillers_RLA_finetuned_summedRLA;

-- maintaining standard naming convintion
alter table gold.gd__chillers_RLA
	rename column `adjusted_date_time` to `date_time`;
    
rename table gold.gd__chillers_RLA to gold.gd_chillers_RLA;

