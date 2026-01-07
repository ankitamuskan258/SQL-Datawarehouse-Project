DDL Script: Create Bronze Tables
=================================================================================================

Script Purpose:
       This Script Creates tables in the 'bronze Schema', dropping existing tables
       if they already exist
       Run this script to re-define the DDL structure of 'bronze' Tables
==================================================================================================
*/

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME , @batch_end_time DATETIME;
    BEGIN TRY
    
    SET NOCOUNT ON;
    SET @batch_start_time= GETDATE();
    PRINT '========================================';
    PRINT 'Loading Bronze Layer';
    PRINT '========================================';

    /* ================= CRM CUSTOMER ================= */
    DROP TABLE IF EXISTS bronze.crm_cust_info;
    CREATE TABLE bronze.crm_cust_info (
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_material_status NVARCHAR(50),
        cst_gnder NVARCHAR(50),
        cst_create_date NVARCHAR(50)
    );

    SET @start_time=GETDATE();
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Dataset\temp\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';
    /* ================= CRM PRODUCT ================= */
    DROP TABLE IF EXISTS bronze.crm_prd_info;
    CREATE TABLE bronze.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt NVARCHAR(50),
        prd_end_dt NVARCHAR(50)
    );

    SET @start_time=GETDATE();
   BULK INSERT bronze.crm_prd_info
    FROM 'C:\Dataset\temp\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    /* ================= CRM SALES ================= */
    DROP TABLE IF EXISTS bronze.crm_sales_details;
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    );

    SET @start_time=GETDATE();
   BULK INSERT bronze.crm_sales_details
    FROM 'C:\Dataset\temp\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    /* ================= ERP LOCATION ================= */
    DROP TABLE IF EXISTS bronze.erp_loc_a101;
    CREATE TABLE bronze.erp_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50)
    );


    SET @start_time=GETDATE();
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Dataset\temp\source_erp\loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    
    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    /* ================= ERP CUSTOMER ================= */
    DROP TABLE IF EXISTS bronze.erp_cust_az12;
    CREATE TABLE bronze.erp_cust_az12 (
        cid NVARCHAR(50),
        bdate NVARCHAR(50),
        gen NVARCHAR(50)
    );

    SET @start_time=GETDATE();
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Dataset\temp\source_erp\cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    
    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    /* ================= ERP PRODUCT CATEGORY ================= */
    DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50)
    );

    SET @start_time=GETDATE();
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Dataset\temp\source_erp\px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time=GETDATE();
    print '>> Load Duration;' +CAST( DATEDIFF(second,@start_time ,@end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    SET @batch_end_time =GETDATE();
    Print '>>--------';
    Print 'Completed';
    print '>> Load Duration;' +CAST( DATEDIFF(second,@batch_start_time ,@batch_end_time)AS NVARCHAR)+ 'seconds';
    Print '>>--------';

    END TRY
    BEGIN CATCH
       PRINT'================================'
       PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
       PRINT'Error Message' + ERROR_MESSAGE();
       PRINT'Error Message' + CAST (ERROR_MESSAGE() AS NVARCHAR);
       PRINT'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
       PRINT'================================'
    END CATCH

    PRINT '========================================';
    PRINT 'Bronze Load Completed'
    PRINT '========================================';
END;
