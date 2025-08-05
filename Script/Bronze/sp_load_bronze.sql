/* 
-- Script Purpose:
	- This script contains a stored procedure that load data into the bronze schema from external csv files.

-- Actions Performed:
	- It truncates the bronze schema tables before inserting data into the table.
	- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  -This stored procedure does not accept any parameters or return any values.
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time	DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		set @batch_start_time = GETDATE()
		PRINT'===============================================';
		PRINT'Loading Bronze Layer';
		print'===============================================';

		print'-----------------------------------------------';
		print'Loading CRM Tables'
		print'-----------------------------------------------';
		SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'\/\/ Inserting Data Into: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Data warehousing1\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		-- The tablock locks the table during the insert i.e prevent other transactions from reading or writing to the table to optimize the performance.
		set @end_time = GETDATE();
		print'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'**************************';
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'\/\/ Inserting Data Into: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Data warehousing1\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		print'**************************';
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'\/\/ Inserting Data Into: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Data warehousing1\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		print'**************************';
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		print'-----------------------------------------------';
		print'Loading ERP Tables'
		print'-----------------------------------------------';
			SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'\/\/ Inserting Data Into: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Data warehousing1\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		print'**************************';
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'\/\/ Inserting Data Into: bronze.erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Data warehousing1\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		print'**************************';
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		SET @start_time = GETDATE();
		PRINT'\/\/ Truncating Table:    bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'\/\/ Inserting Data Into: bronze.erp_px_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Data warehousing1\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT'Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		print'**************************';
		SET @batch_end_time = GETDATE()
	-- ===================================================================================================================================
	-- ===================================================================================================================================
		PRINT'Loading Bronze Layer Is Complete'
		PRINT'Bronze Layer Load Time:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT'****************************'
		PRINT'Error Message:' + ERROR_MESSAGE();
		PRINT'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'****************************'
	END CATCH
END;

