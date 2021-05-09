# stocksnake

Note:
This instruction is just a mental note for myself. It might work for you, but does not have to


# Requirements:

- mysql or mariadb
- InfluxDB 2.0
- Python 3.7


# Installation on Linux:

MySQL:
- create a database
- create a user with all rights on this DB

InfluxDB:
- create a token with full permissions. At least you need to be able to create and delete buckets, read, write and delete database

Ubuntu:
-  create folders:
 /usr/bin/stocksnake    for the scripts
 /etc/stocksnake        for the config files
 /var/log/stocksnake    for the logfiles
 
## Install Python 3.8:


## Install pip3:
https://tech.serhatteker.com/post/2018-12/virtualenv/

sudo apt-get install python3-pip


## Install virtual environment

sudo pip3 install virtualenv


## Build, Use and activate Virtual Environment:
 
### Build:
cd $YOUR_PROJECT_DIRECTORY

Hint: Exchange .venv with own directory name

virtualenv .venv

For Python 3.8:
virtualenv -p /usr/bin/python3.8 .venv

### Activate:
source .venv/bin/activate

Install Packages after activating:
pip install <some-package>

### Deactivate:
deactivate

