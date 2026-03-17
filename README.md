# 🏗️ End-to-End Data Warehouse Project

## 📌 Overview

This project demonstrates the design and implementation of a **modern data warehouse** using SQL-based ETL pipelines and a layered architecture.

It covers the complete data lifecycle:
➡️ Data Ingestion → Data Transformation → Data Modeling → Analytics

---

## 🧱 Architecture

### 🔹 Data Layers

* **Bronze Layer (Raw)**

  * Ingests raw data from source systems
  * Uses BULK INSERT for high-performance loading

* **Silver Layer (Cleaned)**

  * Cleans and standardizes data
  * Applies business rules and transformations
  * Removes duplicates and handles missing values

* **Gold Layer (Business)**

  * Implements star schema
  * Provides analytics-ready datasets

---

## 🔄 Data Flow

1. Extract data from CRM and ERP systems
2. Load into Bronze layer (raw tables)
3. Transform into Silver layer (clean data)
4. Model into Gold layer (fact & dimension views)
5. Use for reporting and analytics

---

## ⚙️ Data Ingestion (Bronze Layer)

* Implemented using stored procedure: `bronze.load_bronze`
* Loads CSV files into SQL Server tables using `BULK INSERT`
* Handles:

  * CRM data (customers, products, sales)
  * ERP data (locations, customers, categories)
* Includes:

  * Load time tracking
  * Error handling (TRY-CATCH)

---

## 🔄 Data Transformation (Silver Layer)

* Implemented using: `silver.load_silver`

### Key Features:

* Data cleaning (TRIM, NULL handling)
* Standardization (gender, marital status)
* Deduplication using `ROW_NUMBER()`
* Business rule application
* Data validation and correction

### Techniques:

* Window functions (`ROW_NUMBER`, `LEAD`)
* Conditional logic (`CASE`)
* Derived columns and recalculations

---

## 🏆 Data Modeling (Gold Layer)

Implements a **Star Schema** using SQL views.

### 👤 Dimension: Customers

* Combines CRM + ERP data
* Enriches demographics
* Applies source prioritization

### 📦 Dimension: Products

* Product categorization
* Filters active products
* Joins category metadata

### 💰 Fact: Sales

* Transactional sales data
* Linked to dimensions via surrogate keys

---

## 🧠 Key Concepts Demonstrated

* ETL pipeline design
* Data warehouse architecture
* Data cleansing and validation
* Star schema modeling
* Surrogate key generation
* Multi-source data integration

---

## 📂 Project Structure

sql-data-warehouse-project/

* datasets/ → Source CSV files
* docs/ → Architecture & data catalog
* sql/bronze/ → Data ingestion
* sql/silver/ → Transformations
* sql/gold/ → Data modeling

---

## 🛠️ Technologies Used

* SQL Server
* T-SQL (Stored Procedures, Views)
* CSV Data Sources

---

## 🚀 How to Run

1. Create database and schemas: `bronze`, `silver`, `gold`
2. Run Bronze procedure:

   ```sql
   EXEC bronze.load_bronze;
   ```
3. Run Silver procedure:

   ```sql
   EXEC silver.load_silver;
   ```
4. Create Gold views:

   ```sql
   -- run create_gold_views.sql
   ```

---

## 📊 Output

The final output is a **fully functional data warehouse** ready for:

* Business Intelligence tools (Power BI, Tableau)
* Analytical queries
* Reporting

---

## ⭐ Conclusion

This project demonstrates an end-to-end data engineering workflow, from raw data ingestion to business-ready analytics models.
