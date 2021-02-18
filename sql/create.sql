-- drop table repo_masters;
-- drop table repo_transactions;
-- drop table repo_transaction_history;

CREATE TABLE `repo_masters` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `code` varchar(100) NOT NULL,
  `currency` varchar(5) NOT NULL,
  `date_count` varchar(100),
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_by` int(11) unsigned DEFAULT NULL,
  `updated_by` int(11) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx_repo_masters__code` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `repo_transactions` (
	`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
	`transaction_id` varchar(20) NOT NULL,
	`transaction_type` varchar(100),
	`portfolio` varchar(100),
	`custodian` varchar(100),
	`collateral_id_type` varchar(100),
	`collateral_id` varchar(100),
	`collateral_global_id` varchar(100),
	`trade_date` datetime NOT NULL,
	`settle_date` datetime NOT NULL,
	`is_open_repo` tinyint unsigned NOT NULL DEFAULT '0',
	`maturity_date` varchar(100),
	`quantity` decimal(18,6) NOT NULL,
	`currency` varchar(5) NOT NULL,
	`price` decimal(18,6) NOT NULL,
	`collateral_value` decimal(18,6) NOT NULL,
	`repo_code` varchar(100) NOT NULL,
	`interest_rate` decimal(18,6) NOT NULL,
	`loan_amount` decimal(18,6) NOT NULL,
	`broker` varchar(100),
	`haircut` decimal(18,6) NOT NULL,
	`status` varchar(10) NOT NULL,
	`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`created_by` int(11) unsigned DEFAULT NULL,
	`updated_by` int(11) unsigned DEFAULT NULL,
	PRIMARY KEY (`id`),
	KEY `idx_repo_transactions__repo_code` (`repo_code`),
	UNIQUE KEY `udx_repo_transactions__transaction_id` (`transaction_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `repo_transaction_history` (
	`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
	`transaction_id` varchar(20) NOT NULL,
	`action` varchar(10) NOT NULL,
	`date` datetime NOT NULL,
	`interest_rate` decimal(18,6) NOT NULL,
	`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`created_by` int(11) unsigned DEFAULT NULL,
	`updated_by` int(11) unsigned DEFAULT NULL,
	PRIMARY KEY (`id`),
	KEY `idx_repo_transaction_history__transaction_id` (`transaction_id`),
	KEY `idx_repo_transaction_history__action` (`action`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
