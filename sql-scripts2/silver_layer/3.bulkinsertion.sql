/*
creating a stored procedure: Load Data Bronze layer --> Silver Layer
-------------------------------------------------------------------------
- Using the warehouse functions this will make my work easier
- The functions simplified rough work into a single function
- For the bronze schema we created a procedure named bulkinsertion
- In this case we can call it bulk_insertion_silver
*/
USE BaraaDataWarehouse ; 
GO 
-- Listing all the procedure schemas in this database
SELECT name , SCHEMA_NAME(schema_id) AS schema_name FROM sys.procedures WHERE name='bulk_insertion' ;  
GO
-- Checking all the functions defined in the data exploration sections
SELECT name, SCHEMA_NAME(schema_id) AS schema_name FROM sys.objects WHERE type = 'IF'; 
GO 
-- Creating the procedure for loading data into the silver layer
CREATE OR ALTER PROCEDURE warehouse_procedures.bulk_insertion_silver
AS
BEGIN TRY 
    BEGIN
	      ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ; 
	      -- Listing all the tables in this database
		  DECLARE @table_name SYSNAME ; 
		  DECLARE tables_list CURSOR FOR SELECT name FROM sys.tables WHERE SCHEMA_NAME(schema_id) = 'silver' ;
		  OPEN tables_list ; 
		  FETCH NEXT FROM tables_list INTO @table_name  ; 
		  WHILE @@FETCH_STATUS = 0
		  BEGIN
		      PRINT 'Processing Table: ' + @table_name ; 
			  FETCH NEXT FROM tables_list INTO @table_name ; 
		  END
		  CLOSE tables_list ; 
		  DEALLOCATE tables_list ; 
          ----------------------------------------------Customer Information Table ----------------------------------------------------
		  DECLARE @count INT , @starting_time DATETIME = GETDATE(), @total_time INT; 
		  SET @count = 0 ; 
		  TRUNCATE TABLE silver.crm_cust_info ; 
          INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date)
          SELECT * FROM warehouse_functions.customer_info_cleanup() ;
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.crm_cust_info ; 
		  IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.crm_cust_info table: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.crm_cust_info table', 10, 1) WITH NOWAIT ; 
          ----------------------------------------------Product Information Table -------------------------------------------------------
          SET @count = 0 ; 
          INSERT INTO silver.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, cat_id) 
          SELECT * FROM warehouse_functions.prd_info_cleanup() ; 
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.crm_prd_info ; 
          IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.crm_prd_info table: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.crm_prd_info table', 10, 1) WITH NOWAIT ; 
          --------------------------------------------------Sales Details Table ----------------------------------------------------------
		  SET @count = 0 ; 
		  TRUNCATE TABLE silver.crm_sales_details ;
		  INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_price, sls_quantity, sls_sales)
          SELECT * FROM warehouse_functions.sales_details_cleanup () ;
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.crm_sales_details ; 
		  IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.crm_sales_details tables: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.crm_sales_details table', 10, 1) WITH NOWAIT ;
          -------------------------------------------------- erp_cust_cust_az12 Table----------------------------------------------------------
		  SET @count = 0 ;
          TRUNCATE TABLE silver.erp_cust_az12 ; 
          INSERT INTO  silver.erp_cust_az12 (cid, ckey, bdate, gen)
          SELECT * FROM warehouse_functions.cust_az12_cleanup () ; 
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.erp_cust_az12 ; 
		  IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.erp_cust_az12 tables: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.erp_cust_az12 table', 10, 1) WITH NOWAIT ;
    
          -------------------------------------------------- erp locations Table ----------------------------------------------------------
		  SET @count = 0 ;
		  TRUNCATE TABLE silver.erp_loc_a101 ; 
		  INSERT INTO silver.erp_loc_a101 (cid, cntry)
		  SELECT * FROM warehouse_functions.location_data_cleanup() ; 
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.erp_loc_a101 ; 
		  IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.erp_loc_a101  tables: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.erp_loc_a101  table', 10, 1) WITH NOWAIT ;
          -------------------------------------------------- erp description Table ----------------------------------------------------------
		  SET @count = 0 ;
          TRUNCATE TABLE silver.erp_px_cat_g1v2 ; 
          INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
          SELECT * FROM bronze.erp_px_cat_g1v2 ; 
		  SET @total_time = DATEDIFF(SECOND, @starting_time, GETDATE()) ; 
		  SET @starting_time = GETDATE() ; 
		  SELECT @count = COUNT(*) FROM silver.erp_px_cat_g1v2 ; 
		  IF @count > 0
		     RAISERROR('>>> %d Rows inserted into silver.erp_px_cat_g1v2  tables: Time = %d seconds ', 10, 1, @count, @total_time) WITH NOWAIT;
		  ELSE 
		     RAISERROR('>>> Error inserting values into silver.erp_px_cat_g1v2  table', 10, 1) WITH NOWAIT ;
	      ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ;  	 
	END
END TRY 
BEGIN CATCH 
      PRINT '1. Error Number' + CAST(ERROR_NUMBER() AS VARCHAR) ;
      PRINT '2. Error Line' + CAST(ERROR_LINE() AS VARCHAR) ;
      PRINT '3. Error Procedure' + COALESCE(ERROR_PROCEDURE(), 'n/a') ;
      PRINT '4. Error Message' + ERROR_MESSAGE() ;

END CATCH 


-- Executing the procedure and testing
EXEC warehouse_procedures.bulk_insertion_silver ; 
