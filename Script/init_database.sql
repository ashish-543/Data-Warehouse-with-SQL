/*
-------------------------------------------------------------------
-------------------------------------------------------------------
This script creates new database DataWarehouse after checking if that database exists and if it already exists, it drops the previous database and creates new
database. After creating the database, the schemas are also defined. Since, our datawarehouse contains three layers so, we create three schemas: bronze, silver and
gold.
---------------------------------------------------------------------
---------------------------------------------------------------------
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/
USE MASTER;
GO


/* CHECK IF THE DATABASE EXISTS BEFORE CREATING IT. WHILE CHECKING, YOU CAN SET SINGLE USER AND ALSO USE ROLLBACK IMMEDIATE WHICH roll backs the running transactions.
Forcefully kick out all users from the DataWarehouse database, roll back whatever they were doing, and lock the database so only a single user (me) can access it. */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
-- CREATE NEW DATABASE 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
