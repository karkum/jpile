DROP TABLE IF EXISTS `contact`;
CREATE TABLE `contact` (
  `customer_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `first_name` varchar(100) NOT NULL DEFAULT '',
  `last_name` varchar(100) NOT NULL DEFAULT '',
  `type` enum('PRIMARY', 'SECONDARY') NOT NULL,
  `street_number` varchar(24) NOT NULL,
  `street` varchar(48) NOT NULL,
  `city` varchar(48) NOT NULL,
  `state` char(2) NOT NULL,
  `zip_code` char(5) NOT NULL,
  PRIMARY KEY (`customer_id`, `first_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `contact_phone`;
CREATE TABLE `contact_phone` (
  `customer_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `phone` varchar(12) NOT NULL DEFAULT '',
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `last_seen_on` datetime NOT NULL,
  `type` tinyint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `customer_id` int(11) NOT NULL,
  `supplier_id` int(11) NOT NULL,
  `purchased_on` datetime NOT NULL,
  `title` varchar(100) NOT NULL DEFAULT '',
  `description` varchar(100) NOT NULL DEFAULT '',
  `price` double NOT NULL,
  `packaging` tinyint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `supplier`;
CREATE TABLE `supplier` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `street_number` varchar(24) NOT NULL,
  `street` varchar(48) NOT NULL,
  `city` varchar(48) NOT NULL,
  `state` char(2) NOT NULL,
  `zip_code` char(5) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `binary_data`;
CREATE TABLE `binary_data` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL DEFAULT '',
  `md5` binary(16) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
