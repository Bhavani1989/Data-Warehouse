-------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.crm_cust_info------
-------------------------------------------------------
SELECT * FROM bronze.crm_cust_info;

---- CHECK FOR NULL OR DUPLICATES IN PRIMARY KEY 
SELECT cst_id, count(*)
FROM bronze.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id IS NULL;

------ ELINMINATING THE DUPLICATES
SELECT * FROM
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info)t
where flag_last = 1;

------ STRING MANIPULATION
------- CHECK FOR UNWANTED SPACES
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_marital_status FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

------- REMOVING UNWANTED SPACES IN STRING COLUMNS
SELECT 
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info;

------- CHECK DATA CONSISTENCY (DISTINCT VALUES)

SELECT 
	DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT 
	DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

------- REPLACING WITH STANDARD NAMES

SELECT 
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a' 
	END AS cst_gndr
FROM bronze.crm_cust_info;

SELECT 
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a' 
	END AS cst_gndr
FROM bronze.crm_cust_info;


-------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.crm_cust_info------
-------------------------------------------------------

SELECT * FROM bronze.crm_prd_info;

---- CHECK FOR NULL OR DUPLICATES IN PRIMARY KEY 
SELECT prd_id, count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;

------ SPLIT INTO 2 NEW COLUMNS SUCH AS ' cat_id ' & ''
SELECT 
	SUBSTRING(prd_key, 1, 5) AS cat_id
FROM bronze.crm_prd_info;

SELECT 
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info;


------ RELACING '-' WITH  '_'
SELECT 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
FROM bronze.crm_prd_info;

------  CHECK FOR UNWANTED SPACES
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-------- CHECK FOR NULLS OR NEGATIVE NUMBERS

SELECT 
	*
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-------- REPLACE NULL WITH 0 IN NUMERIC COLUMNS
SELECT 
	ISNULL(prd_cost, 0)
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

----------- DATA NORMALISATION
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

SELECT 
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END
FROM bronze.crm_prd_info;

------------- CHECK FOR INVALID DATE ORDERS
SELECT 
	*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--------- CORRECTING THE INVALID DATE ISSUES
SELECT *,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_new
FROM bronze.crm_prd_info;

---------- CHANGING TYPE DATETIME TO DATE

SELECT CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_new
FROM bronze.crm_prd_info;


-------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.crm_cust_info------
-------------------------------------------------------

SELECT * FROM bronze.crm_sales_details;

---- CHECK FOR UNWANTED SPACES


SELECT * FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

---- CHECK FOR UNMATCHING DETAILS PRD_KEY COLUMN WITH PRD_KEY COLUMN IN PRD_INFO TABLE 

SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key from silver.crm_prd_info);

---- CHECK FOR UNMATCHING DETAILS CST_ID COLUMN WITH CST_ID COLUMN IN CUST_INFO TABLE 

SELECT * FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id from silver.crm_cust_info);

-------- CHECKING DATE COLUMNS FOR NULLS AND INVALID NUMBERS
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;
-------
SELECT * FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt)!= 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;
--------
SELECT * FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt)!= 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

--------- REPLACING NULL FOR THE DATE COLUMNS WITH INVALID DATES(0)
SELECT NULLIF(sls_order_dt,0) FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

SELECT NULLIF(sls_ship_dt,0) FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt)!= 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

SELECT NULLIF(sls_due_dt,0) FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt)!= 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

------------- CHANGING DATE COLUMN VALUES TO DATE TYPE FROM INTEGER TYPE

SELECT 
	CASE
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt
	FROM bronze.crm_sales_details;

SELECT 
	CASE
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt
	FROM bronze.crm_sales_details;

SELECT 
	CASE
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt
	FROM bronze.crm_sales_details;

------------------ CHECK FOR INVALID ORDERS

SELECT * FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt OR sls_order_dt > sls_due_dt;

------------- CHECKING INVALID DATA IN SALES COLUMNS

SELECT sls_sales, sls_quantity, sls_price FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


------------- CORRECTING THE SALES AMOUNT(REVENUE) VALUE

SELECT sls_sales AS OLD_SALES,sls_price AS OLD_PRICE,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != ABS(sls_sales) / NULLIF(sls_quantity,0) THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;

-------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.erp_cust_az12------
-------------------------------------------------------
SELECT * FROM bronze.erp_cust_az12;

------- CORRECTING THE CUSTOMER ID WHICH CONTAINS EXTRA CHARACTERS
SELECT 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
	END AS cid	
FROM bronze.erp_cust_az12;

--------------- DATA STANDARDISATION IN BIRTH DATE COLUMN
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR  bdate > GETDATE();

SELECT 
	CASE
	     WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR  bdate > GETDATE();

----------------- DATA STANDARDISATION IN GENDER COLUMN 
SELECT DISTINCT gen FROM bronze.erp_cust_az12;

SELECT 
	CASE
		WHEN UPPER(TRIM(gen)) = 'M' OR  UPPER(TRIM(gen))  = 'MALE' THEN 'Male'
		WHEN UPPER(TRIM(gen)) = 'F' OR  UPPER(TRIM(gen))  = 'FEMALE' THEN 'Female'
		ELSE 'N/A'
	END AS gen
FROM bronze.erp_cust_az12

-------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.erp_LOC_A101------
-------------------------------------------------------
SELECT * FROM bronze.erp_loc_a101;


-------------- CHANGING CID WHICH CONTAINS UNNECESSARY CHARACTERS

SELECT REPLACE(cid, '-', '') AS cid FROM bronze.erp_loc_a101;

----------------- CHECKING FOR DATA STANDARADISATION IN COUNTRY COLUMN
SELECT DISTINCT cntry  FROM bronze.erp_loc_a101;

SELECT
	CASE 
		WHEN TRIM(cntry) IN ('US', 'United States') THEN 'USA'
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

---------------------------------------------------------
---- QUALITY CHECK FOR TABLE bronze.erp_px_cat_g1v2------
---------------------------------------------------------

SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT cat  FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat  FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance  FROM bronze.erp_px_cat_g1v2;