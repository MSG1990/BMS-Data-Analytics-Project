-- this script is to import chiller_Run_status data then
-- cleaning it and transfer it to silver layer

use bronze;

set global local_infile = 1;
show variables like 'local_infile';

load data local infile '/Users/basimyousef/Documents/BMS Data Analytics Project/BMS DATA - JAN 12/CHILLER 01 SUPPLY TEMPERATURE_2025-12-15_09-42-39_2026-01-12_09-42-39.csv'
	into table bronze.BZ_CHILLERS_RUN_STATUS
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\r\n'
    ignore 1 lines;
select * from BZ_CHILLERS_RUN_STATUS;

-- Mysql couldn't read the csv file hence it wasaltered as below :
create table `BZ_CHILLERS_RUN_STATUS`(
`date_time` varchar (20),
`chiller 02 run status`varchar (20),
`Dt2` varchar (20),
`chiller 01 run status`varchar (20),
`Dt3` varchar (20),
`chiller 03 run status`varchar (20),
`Dt4` varchar (20),
`chiller 04 run status`varchar (20)
);

-- verification
select * from BZ_CHILLERS_RUN_STATUS;

-- data loaded via wizard import
select * from BZ_CHILLERS_RUN_STATUS;

-- deleting the repeated time stamps

alter table bz_chillers_run_status
drop column dt2,
drop column dt3,
drop column dt4;

DESCRIBE bz_chillers_run_status;

-- resetting date_ime to datetime data type
SET SQL_SAFE_UPDATES = 0;
update  BZ_CHILLERS_RUN_STATUS
	set date_time = str_to_date(TRIM(`date_time`), '%Y-%m-%d %H:%i:%s');
    
ALTER TABLE BZ_CHILLERS_RUN_STATUS 
MODIFY COLUMN `chiller 02 run status` TINYINT,
MODIFY COLUMN `chiller 01 run status` TINYINT,
MODIFY COLUMN `chiller 03 run status` TINYINT,
MODIFY COLUMN `chiller 04 run status` TINYINT;
    
-- verification of the previous step
describe bz_chillers_run_status;

-- unpivoting the table and saving the work as a table in silver data base
create table silver.sv_chillers_run_status as
select
	Date_time,
	'Chiller 1' as Chiller_id,
	`chiller 01 run status`  as RUN_STATUS
from `bz_chillers_run_status`
union all
select
	Date_time,
    'Chiller 2' as Chiller_id,
    `chiller 02 run status`  as RUN_STATUS
from `bz_chillers_run_status`
union all
select
	Date_time,
    'Chiller 3' as Chiller_id,
    `chiller 03 run status` as RUN_STATUS
from `bz_chillers_run_status`
union all
select
	Date_time,
    'Chiller 4' as Chiller_id,
    `chiller 04 run status`  as RUN_STATUS
from `bz_chillers_run_status`
;

select * from silver.sv_chillers_run_status limit 1000 offset 3000;


-- sensors's data time-sequencing & niose elimintation
create table `gold`.`gd_chillers_run_status` as
select
    DATE_FORMAT(date_time, '%Y-%m-%d %H:%i:00') AS date_time,
    chiller_id,
    RUN_STATUS AS run_status
from silver.sv_chillers_run_status
;

-- verification
select * from gold.gd_chillers_run_status limit 1000 offset 4000;
