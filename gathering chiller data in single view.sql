/*
The purpose of this script is to gather relvant data in one tabel
	- chiller run status
    - chiller RLA
    - chilled water flow
    - chilled water T in/out
    - chilled water pumps run status
    */
-- for mysql performance enhacement let's index the tables:
CREATE INDEX idx_ch_time ON gold.gd_chillers_run_status (chiller_id, date_time);
CREATE INDEX idx_rla_time ON gold.gd_chillers_RLA (chiller_id, date_time);
CREATE INDEX idx_flow_time ON gold.gd_chiller_flow (chiller_id, date_time);
CREATE INDEX idx_temp_time ON gold.gd_chillers_Tin_Tout_finetuned (chiller_id, date_time);

-- gathering all data in one view
CREATE TABLE gold.gd_master_chillers_report AS
SELECT
    DATE(a.date_time) AS `date`,
    TIME(a.date_time) AS `time`,
    a.chiller_id,
    a.run_status AS `chiller run status`,
    b.total_rla AS `chiller rla`,
    c.`flow_l/s` AS `flow rate l/s`,
    d.`T in`,
    d.`T out` 
FROM `gold`.`gd_chillers_run_status` a

-- 1. Match RLA (within 5 mins)
LEFT JOIN LATERAL (
    SELECT total_rla FROM `gold`.`gd_chillers_RLA` 
    WHERE chiller_id = a.chiller_id 
      AND date_time BETWEEN a.date_time - INTERVAL 5 MINUTE 
                        AND a.date_time + INTERVAL 5 MINUTE
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, a.date_time, date_time)) ASC 
    LIMIT 1
) AS b ON TRUE

-- 2. Match Flow (within 5 mins)
LEFT JOIN LATERAL (
    SELECT `flow_l/s` FROM `gold`.`gd_chiller_flow` 
    WHERE chiller_id = a.chiller_id 
      AND date_time BETWEEN a.date_time - INTERVAL 5 MINUTE 
                        AND a.date_time + INTERVAL 5 MINUTE
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, a.date_time, date_time)) ASC 
    LIMIT 1
) AS c ON TRUE

-- 3. Match Temperatures (within 5 mins)
LEFT JOIN LATERAL (
    SELECT `T in`, `T out` FROM `gold`.`gd_chillers_Tin_Tout_finetuned` 
    WHERE chiller_id = a.chiller_id 
      AND date_time BETWEEN a.date_time - INTERVAL 5 MINUTE 
                        AND a.date_time + INTERVAL 5 MINUTE
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, a.date_time, date_time)) ASC 
    LIMIT 1
) AS d ON TRUE;
	
    
-- verification
select * from gold.gd_master_chillers_report;
select * from `gold`.`gd_chillers_RLA`
where chiller_id = 'chiller 4';

-- Check the first 5 records in both tables to see if they align
SELECT 'Run Status' as Source, MIN(date_time), MAX(date_time) FROM gold.gd_chillers_run_status
UNION ALL
SELECT 'RLA Data' as Source, MIN(date_time), MAX(date_time) FROM gold.gd_chillers_RLA;
