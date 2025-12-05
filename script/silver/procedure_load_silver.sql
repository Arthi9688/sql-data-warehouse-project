/*
============================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
============================================================================================

Script purpose: 
       This stored procedure load data into the 'silver' schema from 'bronze' schema.
       It performs the following actions
        - Truncates the silver table before loading data
        - Uses the 'INSERT' command to load data from bronze tables to silver tables.
Parameters:
      None
    This stores procedure does not accept any parameters or return any values.

Usage Example:
       EXEC silver.load_silver;
==============================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '=======================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT'Loading CRM tables';
		PRINT '-------------------------------------------------------';
		PRINT '>> Truncating Data into: silver.[crm_cust_info]'
		TRUNCATE TABLE silver.[crm_cust_info]
		PRINT '>> Inserting Data into: silver.[crm_cust_info]'

		SET @start_time = GETDATE()
		INSERT INTO silver.[crm_cust_info] (
		[cst_id]
			,[cst_key]
			,[cst_firstname]
			,[cst_lastname]
			,[cst_marital_status]
			,[cst_gndr]
			,[cst_create_date])

		SELECT 
			[cst_id]
			,[cst_key]
			,TRIM([cst_firstname]) AS [cst_firstname]
			,TRIM([cst_lastname]) AS [cst_lastname]
			,CASE WHEN TRIM(UPPER([cst_marital_status]))= 'M' THEN 'Married'
				WHEN TRIM(UPPER([cst_marital_status])) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS [cst_marital_status]  ---Normalize to readable format
			,CASE WHEN TRIM(UPPER([cst_gndr])) = 'M' THEN 'Male'
				WHEN TRIM(UPPER([cst_gndr])) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS [cst_gndr]
			,[cst_create_date]
		FROM
		(SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY  cst_id ORDER BY cst_create_date desc) as rn
		FROM [bronze].[crm_cust_info]
		WHERE cst_id is not null
		)tbl
		WHERE rn = 1   --- most recent record
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'

		------------------------------------------------------------------------------------
		PRINT '>> Truncating Data into: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data into: silver.crm_prd_info'
		SET @start_time = GETDATE()
		INSERT INTO  silver.crm_prd_info(
			[prd_id]
			,[prd_key]
			,[prd_cat]
			,[prd_sls]
			,[prd_nm]
			,[prd_cost]
			,[prd_line]
			,[prd_start_dt]
			,[prd_end_dt])

		SELECT [prd_id]
			,[prd_key]
			,REPLACE(SUBSTRING([prd_key],1,5), '-', '_') AS prd_cat
			,SUBSTRING([prd_key], 7, LEN([prd_key])) AS prd_sls
			,[prd_nm]
			,ISNULL([prd_cost], 0) AS [prd_cost]
			,CASE UPPER(TRIM([prd_line]))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Training'
				WHEN 'R' THEN 'Road'
				ELSE 'n/a'
			END AS [prd_line]
			,CAST([prd_start_dt] AS DATE) AS [prd_start_dt]
			,CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM [bronze].[crm_prd_info]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'

		--------------------------------------------------------------------------------
		PRINT '>> Truncating Data into: silver.[crm_sales_details]'
		TRUNCATE TABLE silver.[crm_sales_details]
		PRINT '>> Inserting Data into: silver.[crm_sales_details]'
		SET @start_time = GETDATE()
		INSERT INTO [silver].[crm_sales_details] (
			[sls_ord_num]
			,[sls_prd_key]
			,[sls_cust_id]
			,[sls_order_dt]
			,[sls_ship_dt]
			,[sls_due_dt]
			,[sls_sales]
			,[sls_quantity]
			,[sls_price])

		SELECT [sls_ord_num],
			[sls_prd_key],
			[sls_cust_id],
			CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) != 8 THEN NULL
				ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE) 
			END AS [sls_order_dt],
			CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) != 8 THEN NULL
				ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE) 
			END AS [sls_ship_dt],
			CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) != 8 THEN NULL
				ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE) 
			END AS [sls_due_dt],
			CASE WHEN [sls_sales] <=0 OR [sls_sales] IS NULL OR [sls_sales] != [sls_quantity] * ABS([sls_price])
				THEN [sls_quantity] * ABS([sls_price])
				ELSE [sls_sales]
			END AS [sls_sales],
			[sls_quantity],
			CASE WHEN [sls_price] <= 0 OR [sls_price] IS NULL 
				THEN ABS([sls_sales])/NULLIF([sls_quantity],0)
				ELSE [sls_price]
			END AS [sls_price]
		FROM [DataWarehouse].[bronze].[crm_sales_details]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'

		PRINT '-------------------------------------------------------';
		PRINT'Loading ERP tables';
		PRINT '-------------------------------------------------------';

		PRINT '>> Truncating Data into: silver.[erp_cust_az12]'
		TRUNCATE TABLE silver.[erp_cust_az12]
		PRINT '>> Inserting Data into: silver.[erp_cust_az12]'
		SET @start_time = GETDATE()
		INSERT INTO [silver].[erp_cust_az12](
		cid    ,
		bdate  ,
		gen   )

		SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING([cid], 4, LEN([cid]))
				ELSE cid
			END AS cid,
			CASE WHEN [bdate] >= GETDATE() THEN NULL
				ELSE [bdate]
			END AS [bdate],
			CASE WHEN TRIM([gen]) = 'F' THEN 'Female'
				WHEN TRIM([gen]) = 'M' THEN 'Male'
				WHEN [gen] IS NULL THEN 'n/a'
				WHEN [gen] = '' THEN 'n/a'
				ELSE [gen]
			END AS [gen]
		FROM [DataWarehouse].[bronze].[erp_cust_az12]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'

		----------------------------------------------------------------------

		PRINT '>> Truncating Data into: [silver].[erp_loc_a101]'
		TRUNCATE TABLE silver.[erp_loc_a101]
		PRINT '>> Inserting Data into: [silver].[erp_loc_a101]'
		SET @start_time = GETDATE()
		INSERT INTO [silver].[erp_loc_a101](
				cid,
				cntry
				)
		SELECT REPLACE([cid], '-', '') as cid,
			CASE WHEN [cntry] = 'DE' THEN 'Germany'
				WHEN [cntry] IN ('US', 'USA') THEN 'United States'
				WHEN [cntry] IS NULL OR [cntry] = '' THEN 'n/a'
				ELSE [cntry]
			END AS [cntry]
		FROM [DataWarehouse].[bronze].[erp_loc_a101]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'

		---------------------------------------------------------------------
		PRINT '>> Truncating Data into: [silver].[erp_px_cat_g1v2]'
		TRUNCATE TABLE silver.[erp_px_cat_g1v2]
		PRINT '>> Inserting Data into: [silver].[erp_px_cat_g1v2]'
		SET @start_time = GETDATE()
		INSERT INTO [silver].[erp_px_cat_g1v2](
			[id]
			,[cat]
			,[subcat]
			,[maintenance])

		SELECT [id]
			,[cat]
			,[subcat]
			,[maintenance]
		FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------'
	END TRY
	BEGIN CATCH
			PRINT '----------------------------------------------'
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '----------------------------------------------'
		END CATCH
END

