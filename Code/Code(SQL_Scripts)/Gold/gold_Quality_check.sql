-------------- CHECKING FOR DUPLICATES

SELECT cst_id, COUNT(*)
FROM
	(SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid)t
GROUP BY cst_id
HAVING COUNT(*) > 1


-------------- MAKING 2 GENDER COLUMNS INTO 1 WHILE INTEGRATION

SELECT DISTINCT
		ci.cst_gndr,
		ca.gen, 
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
		END AS new_gen
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
	ORDER BY 1, 2;


------------ QUERYING THE DIM.CUSTOMERS VIEW
SELECT * FROM gold.dim_customers;

-------------- CHECKING FOR DUPLICATES

SELECT prd_key, COUNT(*) FROM
(SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,     
    pn.prd_start_dt 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL)t
GROUP BY prd_key
HAVING COUNT(*) > 1; 

------------ QUERYING THE DIM.PRODUCTS VIEW
SELECT * FROM gold.dim_products;

-------------- CHECKING QUALITY OF FACT (SALES) TABLE

SELECT
    sd.sls_ord_num ,
	pr.product_key,
	cu.customer_key,
    sd.sls_prd_key  ,
    sd.sls_cust_id ,
    sd.sls_order_dt ,
    sd.sls_ship_dt  ,
    sd.sls_due_dt   ,
    sd.sls_sales    ,
    sd.sls_quantity ,
    sd.sls_price    
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

------------ QUERYING THE FACT.SALES VIEW
SELECT * FROM gold.fact_sales;

--------- CHECKING THE INTEGRATION

SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE p.product_key IS NULL;

