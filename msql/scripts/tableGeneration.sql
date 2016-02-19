#CREATE DATABASE futures_master;
#USE securities_master;

CREATE TABLE `exchange` (
  `id` int NOT NULL AUTO_INCREMENT,
  `abbrev` varchar(32) NOT NULL,
  `name` varchar(255) NOT NULL,
  `city` varchar(255) NULL,
  `country` varchar(255) NULL,
  `currency` varchar(64) NULL,
  `timezone_offset` time NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `data_vendor` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `website_url` varchar(255) NULL,
  `support_email` varchar(255) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `symbol` (
  `id` int NOT NULL AUTO_INCREMENT,
  `exchange_id` int NULL,
  `ticker` varchar(32) NOT NULL,
  `ticker_head` varchar(32) NOT NULL,
  `ticker_year` int NOT NULL,
  `ticker_month` int NOT NULL,
  `expiration_date` date NOT NULL,
  `instrument` varchar(64) NOT NULL,
  `name` varchar(255) NULL,
  `ticker_class` varchar(255) NULL,
  `currency` varchar(32) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `index_exchange_id` (`exchange_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

ALTER TABLE `symbol`
ADD UNIQUE INDEX `unique_ticker` (`ticker`,`instrument`);

CREATE TABLE `daily_price` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_vendor_id` int NOT NULL,
  `ticker_head` varchar(32) NOT NULL,
  `ticker_month` int NOT NULL,
  `symbol_id` int NOT NULL,
  `price_date` datetime NOT NULL,
  `cal_dte` int NOT NULL,
  `tr_dte` int NOT NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  `open_price` decimal(19,4) NULL,
  `high_price` decimal(19,4) NULL,
  `low_price` decimal(19,4) NULL,
  `close_price` decimal(19,4) NULL,
  `volume` bigint NULL,
  `open_interest` bigint NULL,
  PRIMARY KEY (`id`),
  KEY `index_data_vendor_id` (`data_vendor_id`),
  KEY `index_synbol_id` (`symbol_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

ALTER TABLE `daily_price`
ADD UNIQUE INDEX `ticker_day` (`symbol_id`,`price_date`);

CREATE TABLE `strategy` (
`id` int NOT NULL AUTO_INCREMENT,
`alias` varchar(255) NOT NULL,
`open_date` datetime NOT NULL,
`close_date` datetime NOT NULL,
`pnl` decimal(19,4) NULL,
`created_date` datetime NOT NULL,
`last_updated_date` datetime NOT NULL,
`description_string` varchar(1000) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY (`alias`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `trades` (
`id` int NOT NULL AUTO_INCREMENT,
`ticker` varchar(32) NOT NULL,
`option_type` varchar(32) NULL,
`strike_price` int NULL,
`strategy_id` int NOT NULL,
`trade_price` decimal(19,4) NULL,
`trade_quantity` int NOT NULL,
`trade_date` datetime NOT NULL,
`instrument` varchar(64) NOT NULL,
`real_tradeQ` tinyint(1) NOT NULL,
`created_date` datetime NOT NULL,
`last_updated_date` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `index_strategy_id` (`strategy_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;










