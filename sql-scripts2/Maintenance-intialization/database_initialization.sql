/*
Goal: Creation of the datawarehouse structure
-------------------------------------------------------------------
1. Check for database existence and if it does exist drop and reinitialize
2. Create a procedure to reinitialize the schemas
3. Execute the procedure
Warning: Execution of this script will delete :
    1. Data base intially called DataWarehouse
    2. Initial schemas gold bronze and silver
*/

/*====================== Database Intialization============================*/
-- Checking if the database exists and dropping
USE master ;
GO
IF EXISTS(SELECT 1 FROM sys.databases WHERE name='BaraaDataWarehouse')
BEGIN 
     PRINT('>>> Database exists: reinitialization ...') ; 
     -- Setting the database to single user mode
     ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
     EXEC('DROP DATABASE BaraaDataWarehouse') ;
END
ELSE 
BEGIN 
     PRINT '>>> Database does not exist: initialization ....' ;
END
GO
-- Creating the Database  
CREATE DATABASE BaraaDataWarehouse ;
GO
/*===========================Schema Initialization===========================================*/
USE BaraaDataWarehouse ;
GO
-- Turning on Admin Priviledges
ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ;
GO 
-- Creating the schema to store bronze procedures
CREATE SCHEMA warehouse_procedures ;
GO
-- Creating the procedure to drop the existing database and schema
CREATE OR ALTER PROCEDURE warehouse_procedures.initialize_warehouse_schemas 
(
   @schema1 NVARCHAR(20) = 'bronze',
   @schema2 NVARCHAR(20) = 'silver',
   @schema3 NVARCHAR(20) = 'gold' 
)
AS
BEGIN TRY
    BEGIN
         DECLARE @sql1 NVARCHAR(MAX), @sql2 NVARCHAR(MAX), @sql3 NVARCHAR(MAX) ;
         -- bronze schema
         SET @sql1 = N'CREATE SCHEMA ' + QUOTENAME(@schema1) ;
         EXEC sp_executesql @sql1 ; 
         -- silver schema
         SET @sql2 = N'CREATE SCHEMA ' + QUOTENAME(@schema2) ; 
         EXEC sp_executesql @sql2 ;
         -- Gold schema
         SET @sql3 = N'CREATE SCHEMA ' + QUOTENAME(@schema3) ;
         EXEC sp_executesql @sql3 ;
         -- Confirming the schema existence
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema1)
            PRINT '>>> Schema ' + @schema1 + ' created.';
        ELSE
            PRINT '>>> Schema ' + @schema1 + ' missing.';
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema2)
            PRINT '>>> Schema ' + @schema2 + ' created.';
        ELSE
            PRINT '>>> Schema ' + @schema2 + ' missing.';

        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema3)
            PRINT '>>> Schema ' + @schema3 + ' created.';
        ELSE
            PRINT '>>> Schema ' + @schema3 + ' missing.';
    END 
END TRY 
BEGIN CATCH
    PRINT '1. Error Message' + ERROR_MESSAGE() ;
    PRINT '2. Error Line' + ERROR_LINE() ;
    PRINT '2. Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR(20)) ;
    PRINT '3. Error Procedure' + COALESCE(CAST(ERROR_PROCEDURE() AS NVARCHAR), 'N/A') ;
END CATCH
GO
--  Running the procedure to create the schema
EXEC warehouse_procedures.initialize_warehouse_schemas ;
GO
-- Turning off Admin Priviledges
ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ;
GO