/*
Goal: Creation of tables for the bronze schema(layer) from CRM and ERP sources
-------------------------------------------------------------------------------
- There are two source files from CRM and ERP systems.
- The tables created here will be used to ingest raw data into the bronze layer.
- This script will check if the tables already exist and drop them if they do before recreating them 
- There are a total of six tables to be created in the bronze schema
*/

-- Admin priviledges
USE BaraaDataWarehouse ;
GO 
ALTER DATABASE BaraaDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE ; 
GO 
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()
    
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()

);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATETIME,
    sls_ship_dt  DATETIME,
    sls_due_dt   DATETIME,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    -- Adding the create data column to track when the record was created - additional metadata
    dwh_create_date     DATETIME DEFAULT GETDATE()
);
GO
-- Turing off admin priviledges
ALTER DATABASE BaraaDataWarehouse SET MULTI_USER ;
GO 

