use opcentre_db;
CREATE TABLE `PLATFORM` (
  `Id` int DEFAULT NULL,
  `name` varchar(250) DEFAULT NULL,
  KEY `fk_platform_idx` (`Id`)
);
CREATE TABLE `ACCOUNT` (
  `id` int NOT NULL AUTO_INCREMENT,
  `platform_id` int NOT NULL,
  `operator_id` int NOT NULL,
  `username` varchar(100) DEFAULT NULL,
  `password` varchar(45) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `name` varchar(45) DEFAULT NULL,
  `affiliate_login_url` varchar(150) DEFAULT NULL,
  `manager` varchar(255) DEFAULT NULL,
  `brt_account_id` int DEFAULT NULL,
  `views` varchar(45) DEFAULT NULL,
  `clicks` varchar(45) DEFAULT NULL,
  `signups` varchar(45) DEFAULT NULL,
  `deposits` varchar(45) DEFAULT NULL,
  `new_deposits` varchar(45) DEFAULT NULL,
  `postback` varchar(45) DEFAULT NULL,
  `brt_password` varchar(45) DEFAULT NULL,
  `password_check` varchar(45) DEFAULT NULL,
  `api_key` varchar(200) DEFAULT NULL,
  `start_date` varchar(45) DEFAULT NULL,
  `endpoint` varchar(150) DEFAULT NULL,
  `airbyte_source_id` varchar(45) DEFAULT NULL,
  `recovery_airbyte_source_id` varchar(45) DEFAULT NULL,
  `airbyte_connection_id` varchar(45) DEFAULT NULL,
  `recovery_airbyte_connection_id` varchar(45) DEFAULT NULL,
  `loopback_days` int DEFAULT '2',
  `validation_status` varchar(45) DEFAULT 'Unvalidated',
  `validation_message` varchar(450) DEFAULT NULL,
  `validation_date_time` datetime DEFAULT NULL,
  `account_status` varchar(45) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `tlog_deleted_date` datetime DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  `connection_status` varchar(45) DEFAULT NULL,
  `status_updated_at` datetime DEFAULT NULL,
  `tlog_deleted` tinyint(1) DEFAULT NULL,
  `shortcut_ticket_id` int DEFAULT NULL,
  `shortcut_ticket_url` varchar(45) DEFAULT NULL,
  `shortcut_ticket_status` varchar(45) DEFAULT NULL,
  `shortcut_ticket_created_on` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
CREATE TABLE `JOB` (
  `id` int NOT NULL AUTO_INCREMENT,
  `operator_id` int DEFAULT NULL,
  `job_id` int NOT NULL,
  `config_type` varchar(45) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `records_extracted` varchar(45) DEFAULT NULL,
  `records_loaded` varchar(45) DEFAULT NULL,
  `data_size` varchar(45) DEFAULT NULL,
  `execution_time_taken` varchar(45) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `attempt_started` datetime DEFAULT NULL,
  `attempt_ended` datetime DEFAULT NULL,
  `failure_origin` varchar(45) DEFAULT NULL,
  `error_message` varchar(5000) DEFAULT NULL,
  `environment` varchar(45) DEFAULT NULL,
  `shortcut_ticket_id` int DEFAULT NULL,
  `shortcut_ticket_url` varchar(45) DEFAULT NULL,
  `shortcut_ticket_status` varchar(45) DEFAULT NULL,
  `shortcut_ticket_created_on` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `job_id_UNIQUE` (`job_id`)
);

CREATE TABLE `JOB_DETAIL` (
  `id` int NOT NULL AUTO_INCREMENT,
  `job_id` int DEFAULT NULL,
  `attempt_id` int DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `stream_name` varchar(100) DEFAULT NULL,
  `records_extracted` varchar(45) DEFAULT NULL,
  `records_loaded` varchar(45) DEFAULT NULL,
  `data_size` varchar(45) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `ended_at` datetime DEFAULT NULL,
  `failure_summary` text,
  `sync_window` text,
  `recovery_dates` text,
  `is_recovery` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_JobID_idx` (`job_id`),
  CONSTRAINT `FK_JobID` FOREIGN KEY (`job_id`) REFERENCES `JOB` (`job_id`)
) ;

CREATE TABLE `DATA_QUALITY_DIMENSION` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);


CREATE TABLE `DATA_SOURCE` (
  `id` int NOT NULL AUTO_INCREMENT,
  `operator_id` int DEFAULT NULL,
  `platform_name` varchar(45) DEFAULT NULL,
  `source_name` varchar(45) DEFAULT NULL,
  `airbyte_connection_id` varchar(100) DEFAULT NULL,
  `recovery_airbyte_connection_id` varchar(100) DEFAULT NULL,
  `path` varchar(256) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `DATA_QUALITY_RULE` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_quality_dimension_id` int DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `expression` varchar(255) DEFAULT '5',
  `threshold` int DEFAULT NULL,
  `isenabled` varchar(45) DEFAULT NULL,
  `severity` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_DIM_ID` (`data_quality_dimension_id`),
  CONSTRAINT `FK_DIM_ID` FOREIGN KEY (`data_quality_dimension_id`) REFERENCES `DATA_QUALITY_DIMENSION` (`id`)
);


CREATE TABLE `DATA_SOURCE_DQ` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_source_id` int DEFAULT NULL,
  `data_quality_rule_id` int NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_DATA_SOURCE_ID` (`data_source_id`),
  KEY `FK_DQRULE_ID` (`data_quality_rule_id`),
  CONSTRAINT `FK_DATA_SOURCE_ID` FOREIGN KEY (`data_source_id`) REFERENCES `DATA_SOURCE` (`id`),
  CONSTRAINT `FK_DQRULE_ID` FOREIGN KEY (`data_quality_rule_id`) REFERENCES `DATA_QUALITY_RULE` (`id`)
);

CREATE TABLE `DATA_SOURCE_ITEM` (
  `id` int NOT NULL AUTO_INCREMENT,
  `job_id` int DEFAULT NULL,
  `job_detail_id` int DEFAULT NULL,
  `data_source_id` int DEFAULT NULL,
  `item_name` varchar(256) DEFAULT NULL,
  `path` varchar(256) DEFAULT NULL,
  `records_extracted` int DEFAULT NULL,
  `status` varchar(256) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `last_updated_at` datetime DEFAULT NULL,
  `transaction_date` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_DataSource_ID_idx` (`data_source_id`),
  KEY `FK_JobDetail_ID_idx` (`job_detail_id`),
  CONSTRAINT `FK_DataSource_ID` FOREIGN KEY (`data_source_id`) REFERENCES `DATA_SOURCE` (`id`),
  CONSTRAINT `FK_JobDetail_ID` FOREIGN KEY (`job_detail_id`) REFERENCES `JOB_DETAIL` (`id`)
);

CREATE TABLE `DATA_QUALITY_ISSUE` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_quality_rule_id` int DEFAULT NULL,
  `data_source_item_id` int DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `severity` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `validation_error` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_DS_ITEM_ID_idx` (`data_source_item_id`),
  KEY `FK_RULE_ID_idx` (`data_quality_rule_id`),
  CONSTRAINT `FK_DS_ITEM_ID` FOREIGN KEY (`data_source_item_id`) REFERENCES `DATA_SOURCE_ITEM` (`id`),
  CONSTRAINT `FK_RULE_ID` FOREIGN KEY (`data_quality_rule_id`) REFERENCES `DATA_QUALITY_RULE` (`id`)
);

CREATE TABLE `ACCOUNT_VALIDATION` (
  `id` int NOT NULL AUTO_INCREMENT,
  `account_id` int DEFAULT NULL,
  `validation_status` varchar(45) DEFAULT NULL,
  `validation_message` mediumtext,
  `validation_date_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
);
CREATE TABLE `SLACK_NOTIFICATION` (
  `id` int NOT NULL AUTO_INCREMENT,
  `category` varchar(45) DEFAULT NULL,
  `is_enabled` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`)
);
CREATE TABLE DAILY_AVERAGE_RECORD_COUNT (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `data_source_id` INT,
    `transaction_date` DATE,
    `avg_records_extracted` INT,
    PRIMARY KEY (`id`)
);

-- latest_records - get all records from `DATA_SOURCE_ITEM` table where `transaction_date` is less than current date
-- and assign each record a row_number per (`data_source_id`, `transaction_date`) ordered by descending `job_id`
-- distinct_dates - fetch all distinct (`data_source_id`, `transaction_date`) from `latest_records` and assign a row_number
-- based on transaction_date in descending order
-- Join `latest_records` and `distinct_dates` and keep only those rows which have records matching in the past 30 days 
-- and exclude rows where `records_extracted` is 0, we're then grouping by `data_source_id` 
-- and calculating average and rounding it to nearest integer
-- Remove `and (`latest_records`.`records_extracted` != 0)` from `WHERE` clause to include zeros in the average calculation

CREATE VIEW `opcentre_db`.`vw_average_record_count` AS with `latest_records` as (
  select `opcentre_db`.`DATA_SOURCE_ITEM`.`data_source_id` AS `data_source_id`,
    `opcentre_db`.`DATA_SOURCE_ITEM`.`job_id` AS `job_id`,
    `opcentre_db`.`DATA_SOURCE_ITEM`.`records_extracted` AS `records_extracted`,
    `opcentre_db`.`DATA_SOURCE_ITEM`.`path` AS `path`,
    `opcentre_db`.`DATA_SOURCE_ITEM`.`transaction_date` AS `transaction_date`,
    row_number() OVER (
        PARTITION BY `opcentre_db`.`DATA_SOURCE_ITEM`.`data_source_id`,
          `opcentre_db`.`DATA_SOURCE_ITEM`.`transaction_date` 
        ORDER BY `opcentre_db`.`DATA_SOURCE_ITEM`.`job_id` desc
    )  AS `row_num` 
  from `opcentre_db`.`DATA_SOURCE_ITEM` 
  where (`opcentre_db`.`DATA_SOURCE_ITEM`.`transaction_date` < curdate())
  ), 
  `distinct_dates` as (
    select `subquery`.`data_source_id` AS `data_source_id`,
      `subquery`.`transaction_date` AS `transaction_date`,
      row_number() OVER (
        PARTITION BY `subquery`.`data_source_id` 
        ORDER BY `subquery`.`transaction_date` desc 
      )  AS `date_rank` 
      from (
        select distinct `latest_records`.`data_source_id` AS `data_source_id`,
          `latest_records`.`transaction_date` AS `transaction_date` 
        from `latest_records`) `subquery`) 
  select `latest_records`.`data_source_id` AS `data_source_id`,
    round(avg(`latest_records`.`records_extracted`),0) AS `avg_records_extracted`,
    max(`latest_records`.`records_extracted`) AS `max_records_extracted`,
    min(`latest_records`.`records_extracted`) AS `min_records_extracted` 
  from (`latest_records` 
  join `distinct_dates` 
  on(((`latest_records`.`data_source_id` = `distinct_dates`.`data_source_id`) 
    and (`latest_records`.`transaction_date` = `distinct_dates`.`transaction_date`)))) 
  where ((`distinct_dates`.`date_rank` <= 30) 
    and (`latest_records`.`row_num` = 1)) 
    and (`latest_records`.`records_extracted` != 0)
  group by `latest_records`.`data_source_id`;


use opcentre_db;
INSERT INTO PLATFORM (`Id`, `name`) VALUES (1,'Mexos');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (2,'Cellxpert');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (3,'NetRefer');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (4,'MyAffiliates');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (5,'Inhouse');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (6,'Smartico');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (7,'Q');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (8,'Income Access');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (9,'SoftSwiss');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (10,'EGO');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (11,'Google Analytics');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (12,'BRC');
INSERT INTO PLATFORM (`Id`, `name`) VALUES (13,'Voluum');

INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('operator', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('jobs', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('dq', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('dq_zero', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('dq_high', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('dq_low', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('pk_val', '0');
INSERT INTO `opcentre_db`.`SLACK_NOTIFICATION` (`category`, `is_enabled`) VALUES ('schema_val', '0');

INSERT INTO `opcentre_db`.`DATA_QUALITY_DIMENSION` (`name`, `description`) VALUES ('COMPLETENESS', NULL);
INSERT INTO `opcentre_db`.`DATA_QUALITY_DIMENSION` (`name`, `description`) VALUES ('INTEGRITY', NULL);


update opcentre_db.DATA_QUALITY_RULE set severity = "High" where name in ('PRIMARY_KEY_VALIDATION','SCHEMA_VALIDATION');
update opcentre_db.DATA_QUALITY_RULE set severity = "Low" where name in ('LOW_RECORD_COUNT','HIGH_RECORD_COUNT','ZERO_RECORD_COUNT');

update opcentre_db.DATA_QUALITY_ISSUE set severity = "Low" where description in ('LOW RECORD COUNT','HIGH RECORD COUNT','ZERO RECORD COUNT');
update opcentre_db.DATA_QUALITY_ISSUE set severity = "High" where description in ('PRIMARY KEY VALIDATION','SCHEMA VALIDATION');
