use master;
GO

IF EXISTS(SELECT 1 FROM sys.database where name = 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

-- create database 'DataWareHouse'
create database DataWareHouse;
GO

Use DataWareHouse;
GO

-- CREATE SCHEMAS
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO