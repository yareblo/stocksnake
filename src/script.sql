CREATE SCHEMA `stocksnake`;

CREATE USER 'stocksnakeusr'@'localhost' IDENTIFIED BY '*Secret*';
GRANT ALL PRIVILEGES ON `stocksnake`.* TO 'stocksnakeusr'@'localhost';



CREATE SCHEMA `stocksnake-test`;

CREATE USER 'stocktestusr'@'localhost' IDENTIFIED BY '4711PwD';
GRANT ALL PRIVILEGES ON `stocksnake-test`.* TO 'stocktestusr'@'localhost';



CREATE SCHEMA `stocksnake-dev`;

CREATE USER 'stockdevusr'@'localhost' IDENTIFIED BY '4711PwD-Dev';
GRANT ALL PRIVILEGES ON `stocksnake-dev`.* TO 'stockdevusr'@'localhost';