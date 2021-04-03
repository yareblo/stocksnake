CREATE SCHEMA `stocksnake`;

CREATE USER 'stocksnakeusr'@'localhost' IDENTIFIED BY '*Secret*';
GRANT ALL PRIVILEGES ON `stocksnake`.* TO 'stocksnakeusr'@'localhost';





CREATE SCHEMA `stocksnake-test`;

CREATE USER 'stocktestusr'@'localhost' IDENTIFIED BY '4711PwD';
GRANT ALL PRIVILEGES ON `stocksnake-test`.* TO 'stocktestusr'@'localhost';
