# Pharmaceutical Data Pipeline & Sales Analysis Project

This project is a complete data pipeline and analysis solution for a pharmaceutical sales system. It covers **XML data ingestion**, **relational schema creation**, **data partitioning**, **ETL to a star schema**, and **visual analysis** in R using ggplot2.

<img width="1500" height="680" alt="image" src="https://github.com/user-attachments/assets/f8e945bc-5655-414e-92cd-687c2559acad" />


---

## Project Structure

- `LoadXML2DB.LiN.R`: Loads XML transaction files into a normalized SQLite database, partitions sales data by year, and prints previews.
- `CreateStarSchema.LiN.R`: Creates a MySQL-based star schema and loads data from the SQLite source.
- `Sales_Analysis_Report.Rmd`: Analytical report using `ggplot2`, `kableExtra`, and SQL queries against the MySQL star schema.

---

## Technologies Used

- **R**: Data manipulation, database connection, XML parsing, reporting
- **RSQLite**: Local storage for raw and partitioned sales data
- **RMySQL**: Star schema and BI-ready data warehouse
- **XML & xml2**: Parsing sales and rep data
- **DBI**, **dplyr**: SQL queries and data manipulation
- **ggplot2**, **kableExtra**: Visualization and reporting
- **RMarkdown**: Interactive analytical report

---

## Database Overview

### 1. **SQLite (OLTP - Normalized Schema)**

- `reps`, `customers`, `products`, `sales`
- Partitioned: `sales_2020`, `sales_2021`, `sales_2022`, `sales_2023`

### 2. **MySQL (OLAP - Star Schema)**

- **Dimensions**: `rep_dimension`, `customer_dimension`, `product_dimension`
- **Fact Tables**: `sales_facts`, `rep_facts`

---

## Setup & Execution

### Prerequisites

- R (>= 4.0)
- R packages: `DBI`, `RMySQL`, `RSQLite`, `XML`, `xml2`, `dplyr`, `ggplot2`, `knitr`, `kableExtra`, `stringr`
- Valid MySQL connection credentials (e.g., via FreeMySQLHosting)

### Step 1: Normalize XML to SQLite

source("LoadXML2DB.LiN.R")
This script:
- Creates normalized schema in SQLite
- Loads reps, customers, products, and transactions from XML
- Partitions `sales` table by year

### Step 2: Build Star Schema in MySQL
source("CreateStarSchema.LiN.R")
This script:
- Creates star schema in MySQL
- Loads dimensions and facts from SQLite

### Step 3: Generate Analytical Report

Open `Sales_Analysis_Report.Rmd` in RStudio and click **Knit** to HTML.

## Analysis Visualization
<img width="488" height="730" alt="Screenshot 2025-07-17 at 09 17 33" src="https://github.com/user-attachments/assets/dcd66603-5e77-40f3-9248-4c3567cfd6a5" />

<img width="774" height="756" alt="Screenshot 2025-07-17 at 09 17 47" src="https://github.com/user-attachments/assets/843939e2-9b52-4d4c-8e74-76aeb9218a96" />


