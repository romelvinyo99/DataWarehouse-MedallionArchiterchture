/*
Goal: Bulk insertion of data from csv files into the tables in the bronze schema(layer)
---------------------------------------------------------------------------------------
*/

USE BaraaDataWarehouse ;
GO 
-- Switching on Admin Priviledges
ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ;
GO 
-- Creating a procedure for bulk insertion into bronze tables
CREATE OR ALTER PROCEDURE warehouse_procedures.bulk_insertion AS
BEGIN TRY
    BEGIN
	   -- crm source
	   -- cust info table
	   IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.crm_cust_info ;
	       BULK INSERT bronze.crm_cust_info
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   ROWTERMINATOR='\n',
			   TABLOCK
		   ) ;
		   PRINT '>>> cust info table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> cust info table does not exist' ;
	   END
	   -- prd info table
	   IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.crm_prd_info ;
	       BULK INSERT bronze.crm_prd_info
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   ROWTERMINATOR='\n',
			   TABLOCK
		   ) ;
		   PRINT '>>> prd info table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> prd info table does not exist' ;
	   END ;
	   -- Sales Details table
	   IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.crm_sales_details ;
	       BULK INSERT bronze.crm_sales_details
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   ROWTERMINATOR='\n',
			   TABLOCK
		   ) ;
		   PRINT '>>> sales details table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> sales details table does not exist' ;
	   END 

	   -- erp source
	   -- loc a101 table
	   IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.erp_loc_a101 ;
	       BULK INSERT bronze.erp_loc_a101
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   ROWTERMINATOR='\n',
			   TABLOCK
		   ) ;
		   PRINT '>>> loc a101 table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> loc a101 table does not exist' ;
	   END 
	   -- cust az12 table
	   IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.erp_cust_az12 ;
	       BULK INSERT bronze.erp_cust_az12
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   ROWTERMINATOR='\n',
			   TABLOCK
		   ) ;
		   PRINT '>>> cust az12 table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> cust az12 table does not exist' ;
	   END 

	   -- px cat g1v2 table
	   IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	   BEGIN 
	       TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;
	       BULK INSERT bronze.erp_px_cat_g1v2
		   FROM 'C:\Users\nyasa\Downloads\Coding-Files\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		   WITH (
		       FIRSTROW=2,
			   FIELDTERMINATOR=',',
			   TABLOCK
		   ) ;
		   PRINT '>>> px cat g1v2 table data inserted' ;
	   END
	   ELSE 
	   BEGIN 
	      PRINT '>>> px cat g1v2 table does not exist' ;
	   END 
	END
END TRY
BEGIN CATCH
    PRINT '1. Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(20)) ; 
	PRINT '2. Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(20)) ;
	PRINT '3. Error Message: ' + ERROR_MESSAGE() ;
	PRINT '4. Error Procedure ' + COALESCE(ERROR_PROCEDURE(), 'N/A') ; 
END CATCH 
GO 
-- Execution of the procedures
EXEC warehouse_procedures.bulk_insertion ; 
GO
-- Turning off Admin Priviledges
ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ;
GO 
-- Checking the tables and confirming the insertions - Using one example
SELECT COUNT(*) FROM bronze.crm_cust_info ; 
GO 
SELECT COUNT(*) FROM bronze.crm_prd_info ; 
GO 
SELECT COUNT(*) FROM bronze.crm_sales_details ; 
GO 
SELECT COUNT(*) FROM bronze.erp_cust_az12 ; 
GO
SELECT COUNT(*) FROM bronze.erp_loc_a101 ;
GO 
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2 ;  

