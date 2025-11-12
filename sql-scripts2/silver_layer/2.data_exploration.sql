/*
Goal: Data Transformation of data before loading into the silver schema(layer)
------------------------------------------------------------------------------
1. customer information
------------------------------------------------------------------------------
1. Drop Null Values from the primary key column 
2. Pick the latest value for the customer id in duplicated cases
3. Remove whitespaces for the last name and first name 
4. Replace null values with n/a and rename the short values in gender and marital status

2. prd info table
-------------------------------------------------------------------------------
1. Checked for start dates if null - non was null
2. Checkied if pk (prd_id) if null
3. Derived new information and column - prd_key and cat_id
4. Replacing abbreviations
5. Made sure end date > start data:
    - if we replace the end date with the next start date chronological
3. Sales Details Table
---------------------------------------------------------------------------------
1. Convert the dates to Date objects 
2. Fix the logical errors in sales quantity and price

4. erp cust az12 Table
---------------------------------------------------------------------------------
1. Extracting the customer key from cid to link to the cst_key column in crm_cust_info
2. Removing Invalide bdates
3. Editing  the gender column 
*/
USE BaraaDataWarehouse ; 
GO 
--========================================Customer Infomation Table============================================

-- Creating a schema for functions
IF EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'warehouse_functions')
BEGIN
   PRINT 'functions schema already exists dropping and recreating: reinitializing ...' ; 
   EXEC('DROP SCHEMA warehouse_functions') ;
END
ELSE
BEGIN
   PRINT 'functions schema intializing ... ' ; 
END
GO 
CREATE SCHEMA warehouse_functions ; 
GO 
-- Customer info table transformation function
CREATE OR ALTER FUNCTION  warehouse_functions.customer_info_cleanup()
RETURNS TABLE
AS
RETURN (
    SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date
	FROM (
	   SELECT 
          cst_id, cst_key, 
		  ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date ASC) AS rank,
		  TRIM(cst_firstname) AS cst_firstname, TRIM(cst_lastname) AS cst_lastname,
	      CASE 
		     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		     WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		     ELSE 'n/a'
		  END AS cst_gndr,
		  CASE 
		     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		     ELSE 'n/a'
		  END AS cst_marital_status,
	  	  cst_create_date
       FROM bronze.crm_cust_info) AS sub1
	   WHERE rank = 1 AND cst_id IS NOT NULL 
) ; 
GO 
-- Inserting cleaned data into the silver customer table
TRUNCATE TABLE silver.crm_cust_info ; 
INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date)
SELECT * FROM warehouse_functions.customer_info_cleanup() ; 
-- Confirming insertion - primary key uniqueness check
SELECT 
   cst_id,
   COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1; 
GO 
--========================================Product Infomation Table============================================
-- Getting table overview
SELECT TOP 1000 * FROM bronze.crm_prd_info ; 
GO 
-- 1. Checking for nulls and duplicates and null values in information table
SELECT 
   prd_id,
   COUNT(*) AS count
FROM bronze.crm_prd_info 
GROUP BY prd_id              --> Primary Key is clean
HAVING COUNT(*) > 1 OR prd_id IS NULL; 
GO 
-- 2. Extracting the necessary information from the product key
-- Category id - first 5 values of the product key are the category id and replace the dash with underscore - foreign key 
-- First 5 values are the category id
SELECT DISTINCT * FROM bronze.erp_px_cat_g1v2 ; 
GO 
SELECT 
    REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id
FROM bronze.crm_prd_info  ; 
GO 
-- Confirming the match 
SELECT TOP 10 *
FROM bronze.crm_prd_info AS prd_info
INNER JOIN bronze.erp_px_cat_g1v2 AS cat_info
   ON REPLACE(LEFT(prd_info.prd_key, 5), '-', '_') = cat_info.id ; 
GO 

-- product key - the rest of the values are the prd_key
SELECT TOP 10 sls_prd_key FROM bronze.crm_sales_details ; 
GO 
SELECT TOP 10 
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info ;
GO    
-- Confirming the match 
SELECT TOP 10 
     sales_details.sls_prd_key
FROM bronze.crm_prd_info AS prd_info
INNER JOIN bronze.crm_sales_details AS sales_details
  ON SUBSTRING(prd_info.prd_key, 7, LEN(prd_info.prd_key)) = sales_details.sls_prd_key ; 
GO 
-- 3. Checking for NULLs or Negative Numbers
-- Expectation met for all object columns
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm ; 
GO 
-- Checking for NULLs or Negative Numbers - prd cost
-- Results = Nulls present - replace with zero
SELECT *
FROM(
 SELECT 
    prd_cost,
    CASE
	    WHEN prd_cost IS NULL OR prd_cost < 0 THEN 1
		ELSE 0
	END AS flag_negnull
 FROM bronze.crm_prd_info) AS sub
WHERE sub.flag_negnull = 1 ; 
GO 

-- 4. Prodcut line
-- Getting the distinct values 
-- Replacing abbreviations (M, R, S, T) - Mountain, Road, other Sales, n/a ,Touring 
SELECT DISTINCT 
      prd_line,
      COUNT(*) AS count
FROM bronze.crm_prd_info 
GROUP BY prd_line ; 
GO 
-- 5. Checking for invalid date orders
-- End date must be later than start date - invalid dates present
-- Grouping the invalid dates by category id and investigating using cost - first 5 values of the prd_key
-- Investigation - result - no overlapping
/*
- Each record must have a start and an end
- There should be no overlapping of records
*/
SELECT TOP 6 
   LEFT(prd_key, 5) AS cat_id, 
   prd_cost,
   prd_start_dt,
   prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
ORDER BY LEFT(prd_key, 5) ASC; 
GO 
-- Checking the values where the start date is null
SELECT * FROM bronze.crm_prd_info WHERE prd_start_dt IS NULL ; 
GO 
-- Solution = Ignore the end date and set the end date to the be the start of the next column - 1 day
-- End date = start date of the next start - 1
SELECT TOP 6 
  *,
  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY cat_id ORDER BY prd_start_dt ASC)) AS prd_end_dt_test
FROM (
  SELECT
    LEFT(prd_key, 5) AS cat_id, 
    prd_cost,
	prd_start_dt, 
	prd_end_dt
  FROM bronze.crm_prd_info 
  WHERE prd_start_dt > prd_end_dt) AS sub 
ORDER BY cat_id ;  
GO 
/*
Modifification of the customer information table for the silver layer
----------------------------------------------------------------------
1. Add cat_id column 
2. Modified the start date and end date to be DATE instead of DATETIME
*/
-- Creating a function to prepare the data for insertion 
CREATE OR ALTER FUNCTION warehouse_functions.prd_info_cleanup () 
RETURNS TABLE
AS 
RETURN (
    -- Extracting the cat_id and cleaning up the prd_key
	WITH cte_easy_cleanup AS
	(
	   SELECT 
	      prd_id,
		  -- Getting the product key
		  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		  -- Getting the category id
		  REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
		  prd_nm,
		  -- Replacing nulls with zero 
		  ISNULL(prd_cost, 0) AS prd_cost,
		  -- Full representation
		  CASE UPPER(TRIM(prd_line))
		     WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Sales'
			 WHEN 'S' THEN 'other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		  END AS prd_line,
		  prd_start_dt, prd_end_dt
	   FROM bronze.crm_prd_info 
	), cte_invalid_dates AS
	(
	   SELECT 
	       prd_id, prd_key, prd_nm, 
		   prd_cost, prd_line, 
		   CAST(prd_start_dt AS DATE) AS prd_start_dt, 
	       LEAD(prd_start_dt) OVER(PARTITION BY cat_id ORDER BY prd_start_dt ASC) AS prd_end_dt,
		   cat_id
		   FROM cte_easy_cleanup
	)
	SELECT * FROM cte_invalid_dates
)
GO 
-- Executing the function and confirming transformations
SELECT COUNT(*) AS error_count 
FROM warehouse_functions.prd_info_cleanup () 
WHERE prd_start_dt > prd_end_dt OR prd_id IS NULL OR prd_start_dt IS NULL ; 
GO 
SELECT TOP 3 * FROM warehouse_functions.prd_info_cleanup() ; 
GO
-- Altering the prd info table and inserting data into the silver schema - Using a procedure
TRUNCATE TABLE silver.crm_prd_info ;
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ; 
    TRUNCATE TABLE silver.crm_prd_info ;
    -- Adding columns and altering thhe datatypes
    ALTER TABLE silver.crm_prd_info 
	ADD cat_id VARCHAR(20) ;
	ALTER TABLE silver.crm_prd_info
	ALTER COLUMN prd_start_dt DATE
	ALTER TABLE silver.crm_prd_info
	ALTER COLUMN prd_end_dt DATE ; 
	ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ; 
END
GO 
-- Checking the column structure
SELECT TOP 3 * FROM silver.crm_prd_info ; 
GO 
-- Inserting the values cleaned data from the bronze schema into the silver layer
INSERT INTO silver.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, cat_id) 
SELECT * FROM warehouse_functions.prd_info_cleanup() ; 
GO 
-- Confirming the input
SELECT COUNT(*) FROM warehouse_functions.prd_info_cleanup() ;
GO 
SELECT *, COUNT(*) OVER() AS total_count FROM silver.crm_prd_info ; 
GO 
--========================================Sales Details Table============================================
USE BaraaDataWarehouse ; 
GO 
-- 1. Verifying the forgein keys
-- product key in sales details and product info table - clean
SELECT COUNT(*)
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) ;
GO
-- customer id in sales details and customer info table - clean
SELECT COUNT(*)
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info) ; 
GO 
-- 2. Verifying the dates - need to change the format
/*
- The dates are given in integers - YEAR|MONTH|DAY - length=8 charachters
- The date values cannot be zero if zero = NULL
- Order date should be less than shipping date (logic) - whoever is reading this
Transformations
-------------------------------------------------------------------------
1. Filter out data with zero dates and outside boundaries - Null
2. Convert the date integers to date objects
*/
-- Getting the dates with zero and if length != 8  + Boundary of the date(date as of now) or 1900
SELECT NULLIF(sls_order_dt, 0)
FROM bronze.crm_sales_details 
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8  OR sls_order_dt > 20251108 OR sls_order_dt < 19000101 ;
GO 
-- 3. Data consistency for sales quantity and price
/*
Business Rules - Talk to expert system
----------------------------------------------------------------------------------------
sales = Quantity * Price
All of the values cannot be negatives zeros or nulls - logic
- For the unaccepted values explicit conversion for all values satisfying the conditions
Solutions
----------------------------------------------------------------------------------------
1. Data issues will be fixed directly in the source system
2. Data issues has to be fixed in the data warehouse - approach 
Heuristic Rules
----------------------------------------------------------------------------------------
If sale is negative, zero, null, derive it using quantity and price
If price is zero or null calculate it using sales and quantity
If price is negative convert it to positive value
*/
-- Business Rules - checking errors
SELECT 
     CASE 
	    WHEN sls_price <= 0 THEN NULL
		ELSE sls_price
	 END AS sls_price,
	 CASE 
	    WHEN sls_quantity <= 0 THEN NULL
		ELSE sls_quantity
	 END AS sls_quantity,
	 CASE 
	    WHEN sls_sales <= 0 THEN NULL
		ELSE sls_sales
	 END AS sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price ; 
GO 
-- Fixes using heuristic rules
SELECT 
   ABS(CASE 
        WHEN (sls_price IS NULL OR sls_price <= 0) AND sls_quantity IS NOT NULL AND sls_sales IS NOT NULL
		    THEN sls_sales / sls_quantity
        ELSE sls_price
   END) AS sls_price,
   sls_quantity,
   sls_sales
FROM (
   SELECT 
   sls_price,
   sls_quantity,
   ABS(CASE
       WHEN (sls_sales != sls_quantity * sls_price OR sls_sales <= 0 OR sls_sales IS NULL) AND sls_price IS NOT NULL AND sls_quantity IS NOT NULL
	       THEN sls_price * sls_quantity
       ELSE sls_sales
   END) AS sls_sales
   FROM bronze.crm_sales_details
   WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0  
) AS sub1 ;
GO 
SELECT top 2 * FROM bronze.crm_sales_details ; 
-- Combining all this infomation
CREATE OR ALTER FUNCTION warehouse_functions.sales_details_cleanup ()
RETURNS TABLE
AS 
RETURN
(
SELECT 
   sls_ord_num, sls_prd_key, sls_cust_id,
   CASE 
     WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
   END AS sls_order_dt,
   CASE 
     WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
   END AS sls_ship_dt,
   CASE 
     WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
   END AS sls_due_dt,
   sls_price,sls_quantity,sls_sales
FROM bronze.crm_sales_details
WHERE NOT(sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0)    
UNION ALL
SELECT 
   sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
   sls_ship_dt, sls_due_dt,
   ABS(CASE 
        WHEN (sls_price IS NULL OR sls_price <= 0) AND sls_quantity IS NOT NULL AND sls_sales IS NOT NULL
		    THEN sls_sales / sls_quantity
        ELSE sls_price
   END) AS sls_price,
   sls_quantity,
   sls_sales
FROM (
   SELECT
   sls_ord_num, sls_prd_key, sls_cust_id,
   CASE 
     WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
   END AS sls_order_dt,
   CASE 
     WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
   END AS sls_ship_dt,
   CASE 
     WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
   END AS sls_due_dt,
   sls_price,
   sls_quantity,
   ABS(CASE
       WHEN (sls_sales != sls_quantity * sls_price OR sls_sales <= 0 OR sls_sales IS NULL) AND sls_price IS NOT NULL AND sls_quantity IS NOT NULL
	       THEN sls_price * sls_quantity
       ELSE sls_sales
   END) AS sls_sales
   FROM bronze.crm_sales_details
   WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0  
) AS sub1 
)
GO 
-- Getting the result of the function
SELECT TOP 3 * FROM warehouse_functions.sales_details_cleanup() ; 
GO 
-- Checking the original table
SELECT TOP 3 * FROM silver.crm_sales_details ; 
GO 
-- Altering the structure of the silver table
TRUNCATE TABLE silver.crm_sales_details ; 
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
BEGIN 
    ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ; 
	ALTER TABLE silver.crm_sales_details
	ALTER COLUMN sls_order_dt DATE ; 
	ALTER TABLE silver.crm_sales_details
	ALTER COLUMN sls_ship_dt DATE ; 
	ALTER TABLE silver.crm_sales_details
	ALTER COLUMN sls_due_dt DATE ; 
	ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ;
END
GO 
-- Inserting data into the table
INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_price, sls_quantity, sls_sales)
SELECT * FROM warehouse_functions.sales_details_cleanup () ; 
GO 
-- Checking insertion 
SELECT TOP 3 * FROM silver.crm_sales_details ; 
GO 


--========================================erp cust az12	Table============================================
USE BaraaDataWarehouse ;
GO 
-- Get the table overview
SELECT TOP 3 * FROM bronze.erp_cust_az12 ; 
GO 

-- 1. Extracting the key from the cid to join with customer id in customer information - CID = 'NAS' + AW00011000 --> AW00011000 = CST_KEY
-- Testing the join 
SELECT TOP 3 * FROM silver.crm_cust_info ; 
GO 
-- Getting the join 
SELECT TOP 3 
	l.cid, 
	SUBSTRING(l.cid, 4, LEN(l.cid)) AS sub_cid_link,
	r.cst_key
FROM bronze.erp_cust_az12 AS l
INNER JOIN silver.crm_cust_info AS r
  ON SUBSTRING(l.cid, 4, LEN(l.cid))  = r.cst_key ; 
GO 
-- 2. Checkng the oldest and newest date and ensuring their valid - Invalid future dates present
SELECT
   MIN(bdate) AS min_date,
   MAX(bdate) AS max_date
FROM bronze.erp_cust_az12 ; 
-- Replacing this dates that are too high or low with null
GO 
SELECT *
FROM(
  SELECT 
     bdate, 
     CASE
        WHEN	bdate >  CAST(GETDATE() AS DATE) THEN 'over'
	    WHEN bdate < CAST('1900-01-01' AS DATE) THEN 'under'
	    ELSE 'norm'
     END AS invalid_dt_flag
  FROM bronze.erp_cust_az12 ) AS sub1
WHERE invalid_dt_flag != 'norm' ; 
GO 

-- 3. Checking the gender information
-- Checking the unique values
SELECT gen, COUNT(*) AS total_count
FROM bronze.erp_cust_az12 
GROUP BY gen ; 
GO 
-- Making changes to the gender column
SELECT 
   gen,
   COUNT(*) AS total_count
FROM (
  SELECT
       NULLIF(CASE 
	      WHEN TRIM(gen) = '' THEN 'unknown'
		  WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		  WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
		  ELSE gen
	   END, 'unknown') AS gen
  FROM bronze.erp_cust_az12 ) AS sub1 
GROUP BY gen ; 
GO 
-- Preparing data for insertion 
CREATE OR ALTER FUNCTION warehouse_functions.cust_az12_cleanup ()
RETURNS TABLE
AS 
RETURN 
(
SELECT 
    cid,
	CASE
	    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE NULL
	END AS ckey,
	CASE  
       WHEN TRY_CAST(bdate AS DATE) IS NULL THEN NULL  
       WHEN TRY_CAST(bdate AS DATE) > CAST(GETDATE() AS DATE) THEN NULL  
       WHEN TRY_CAST(bdate AS DATE) < CAST('1900-01-01' AS DATE) THEN NULL  
       ELSE TRY_CAST(bdate AS DATE)  
    END AS bdate,
    NULLIF(CASE 
	   WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
	   WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
	   WHEN TRIM(gen) = '' THEN 'n/a'
	   ELSE gen
	END, 'n/a') AS gen
FROM bronze.erp_cust_az12 
)
GO 
-- Confirming the data for insertion 
SELECT TOP 10 * FROM warehouse_functions.cust_az12_cleanup() ; 
GO 
-- Altering the table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
BEGIN 
    ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ;
	ALTER TABLE silver.erp_cust_az12
	ALTER COLUMN bdate DATE ; 
	ALTER TABLE silver.erp_cust_az12
	ADD ckey VARCHAR(50) ;
	ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ; 
END 
GO 
-- Performing the insertion from the bronze layer to the silver layer
TRUNCATE TABLE silver.erp_cust_az12 ; 
INSERT INTO  silver.erp_cust_az12 (cid, ckey, bdate, gen)
SELECT * FROM warehouse_functions.cust_az12_cleanup () ; 
GO 
-- Connfirming the insertion of the date
SELECT TOP 4 * FROM silver.erp_cust_az12 ; 
GO 

--========================================erp location Table============================================
USE BaraaDataWarehouse ; 
GO 
SELECT TOP 2 * FROM bronze.erp_loc_a101 ; 
GO 
--1. Replacing the dash with empty string to allow for cid to connect to the cst_key in customer info table
SELECT TOP 2 cst_key FROM silver.crm_cust_info ; 
GO 
-- Replacement 
SELECT TOP 2 
   REPLACE(cid, '-', '') AS formatted_cid
FROM bronze.erp_loc_a101 ; 
GO 
-- Getting the unique values in the country column
SELECT 
  cntry, 
  COUNT(*) AS total_count
FROM (
  SELECT
    -- Editing the conventions
    NULLIF(CASE 
       WHEN UPPER(TRIM(cntry)) IN  ('US','USA') THEN 'United States'
	   WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	   WHEN TRIM(cntry) = '' THEN 'n/a'
	   ELSE TRIM(cntry)
    END, 'n/a') AS cntry
  FROM bronze.erp_loc_a101) AS sub1 
GROUP BY cntry ; 
GO 
-- Creating the function to prepare data for insertion 
CREATE OR ALTER FUNCTION warehouse_functions.location_data_cleanup ()
RETURNS TABLE
AS
RETURN (
  SELECT
    REPLACE(cid, '-', '') AS cid,
    NULLIF(CASE 
       WHEN UPPER(TRIM(cntry)) IN  ('US','USA') THEN 'United States'
	   WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	   WHEN TRIM(cntry) = '' THEN 'n/a'
	   ELSE TRIM(cntry)
    END, 'n/a') AS cntry
  FROM bronze.erp_loc_a101
)
GO 
-- Fetching the data 
SELECT TOP 4 * FROM warehouse_functions.location_data_cleanup() ; 
GO 
-- Inserting data into the silver layer
TRUNCATE TABLE silver.erp_loc_a101 ; 
GO 
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT * FROM warehouse_functions.location_data_cleanup() ; 
GO 
-- Confirming the insertion 
SELECT COUNT(*) AS total_row_count FROM silver.erp_loc_a101 ; 
GO 

--========================================erp product description Table============================================
USE BaraaDataWarehouse ; 
GO 
SELECT TOP 1 * FROM bronze.erp_px_cat_g1v2
GO 
-- 1. Check for unwanted spaces for all object columns- clean
SELECT *
FROM bronze.erp_px_cat_g1v2 
WHERE LEN(TRIM(cat)) != LEN(cat) OR LEN(TRIM(maintenance)) != LEN(maintenance)
OR LEN(TRIM(subcat)) != LEN(subcat); 
GO 

-- Getting the cardinality - clean
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2 ;  
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2 ;  
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2 ;  
GO 
-- This data is clean loading the data into the subcat
TRUNCATE TABLE silver.erp_px_cat_g1v2 ; 
GO 
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT * FROM bronze.erp_px_cat_g1v2 ; 
GO 
-- Confirming data insertion 
SELECT * FROM silver.erp_px_cat_g1v2 ; 

GO 
