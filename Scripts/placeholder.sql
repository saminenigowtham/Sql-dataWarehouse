/*
scripts :
	we are creating the database with named as datawarhouse and creating the schema as bronze,silver and gold



*/

use master;
-- creating datbase
create database DataWarhouse;
-- using the data base
use DataWarhouse;

-- createing schema in database

create schema bronze;
go
create schema silver;
go
create schema gold;

