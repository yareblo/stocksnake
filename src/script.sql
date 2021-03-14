CREATE SCHEMA `stocksnake`;

CREATE USER 'stocksnakeusr'@'localhost' IDENTIFIED BY '*Secret*';
GRANT ALL PRIVILEGES ON `stocksnake`.* TO 'stocksnakeusr'@'localhost';



