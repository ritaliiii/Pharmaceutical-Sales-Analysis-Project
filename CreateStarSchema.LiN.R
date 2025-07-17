# File: CreateStarSchema.LiN.R
# Author: Ning Li
# Date: Summer Full 2024


create_star_schema <- function(mydb){
  
  # drop tables if exist
  dbExecute(mydb, "DROP TABLE IF EXISTS sales_facts;")
  dbExecute(mydb, "DROP TABLE IF EXISTS rep_facts;")
  dbExecute(mydb, "DROP TABLE IF EXISTS customer_dimension;")
  dbExecute(mydb, "DROP TABLE IF EXISTS product_dimension;")
  dbExecute(mydb, "DROP TABLE IF EXISTS rep_dimension;")
  
  # create the customer_dimension table
  dbExecute(mydb, "
            CREATE TABLE customer_dimension (
              customerID INTEGER,
              customerName VARCHAR(50) NOT NULL,
              country VARCHAR(50) NOT NULL ,
              PRIMARY KEY (customerID)
            );
            ")
  
  # create the product_dimension table
  dbExecute(mydb, "
            CREATE TABLE product_dimension (
              productID INTEGER,
              productName VARCHAR(50) NOT NULL,
              PRIMARY KEY (productID)
            );
            ")
  
  # create the rep_dimension table
  dbExecute(mydb, "
            CREATE TABLE rep_dimension (
              repID INTEGER,
              repName VARCHAR(100) NOT NULL UNIQUE,
              phone TEXT,
              hireDate TEXT,
              commission INTEGER,
              territory TEXT,
              certified BOOLEAN,
              PRIMARY KEY (repID)
            );
            ")
  
  # create the sales_facts table
  dbExecute(mydb, "
            CREATE TABLE sales_facts (
              sfID INTEGER AUTO_INCREMENT,
              txnID INTEGER NOT NULL,
              customerID INTEGER NOT NULL,
              productID INTEGER NOT NULL,
              repID INTEGER NOT NULL,
              productName VARCHAR(100) NOT NULL,
              month INTEGER NOT NULL,
              quarter INTEGER NOT NULL,
              year INTEGER NOT NULL,
              country VARCHAR(50) NOT NULL,
              total_amount_sold DOUBLE NOT NULL,
              total_units_sold INTEGER NOT NULL,
              PRIMARY KEY (sfID),
              FOREIGN KEY (customerID) REFERENCES customer_dimension(customerID),
              FOREIGN KEY (productID) REFERENCES product_dimension(productID),
              FOREIGN KEY (repID) REFERENCES rep_dimension(repID)
            );
            ")
  
  # Create the rep_facts table
  dbExecute(mydb, "
            CREATE TABLE rep_facts (
              rfID INTEGER AUTO_INCREMENT,
              txnID INTEGER NOT NULL,
              repName VARCHAR(100) NOT NULL,
              repID INTEGER NOT NULL,
              month INTEGER NOT NULL,
              quarter INTEGER NOT NULL,
              year INTEGER NOT NULL,
              total_amount_sold DOUBLE NOT NULL,
              average_amount_sold DOUBLE NOT NULL,
              PRIMARY KEY (rfID),
              FOREIGN KEY (repID) REFERENCES rep_dimension(repID)
            );
            ")
}

# function to populate data for customer dimension
load_data_customer_dimension <- function(mydb_sqlite, mydb){
  # get distinct customers from customer table
  customers <- dbGetQuery(mydb_sqlite, "SELECT customerID, customerName, country FROM customers")
  dbWriteTable(mydb, "customer_dimension", customers, append = TRUE, row.names = FALSE)
}

# function to populate data for product dimension
load_data_product_dimension <- function(mydb_sqlite, mydb){
  # get distinct product from product table
  products <- dbGetQuery(mydb_sqlite, "SELECT productID, productName FROM products")
  dbWriteTable(mydb, "product_dimension", products, append = TRUE, row.names = FALSE)
}

# function to populate data for rep dimension
load_data_rep_dimension <- function(mydb_sqlite, mydb){
  # get distinct rep from rep table
  reps <- dbGetQuery(mydb_sqlite, "SELECT repID, (firstName || ' ' || lastName) AS repName, phone, hireDate, commission, territory, certified
    FROM reps")
  dbWriteTable(mydb, "rep_dimension", reps, append = TRUE, row.names = FALSE)
}

# function to populate data for sales facts table
load_sales_facts_data <- function(mydb_sqlite, mydb){
  sales_facts <- data.frame()
  
  # get all tables from sqlite database
  all_tables <- dbListTables(mydb_sqlite)
  # get all sale_partitionn table using the pattern
  sales_partition_tables <- grep("^sales_\\d{4}$", all_tables, value = TRUE)
  
  for (partition_table in sales_partition_tables) {
    
    # get the data group by year for better querying for analytics
    sales_data <- dbGetQuery(mydb_sqlite, sprintf("
                       SELECT 
                          s.saleDate AS date,
                          s.txnID,
                          s.customerID,
                          s.productID,
                          s.repID,
                          p.productName,
                          c.country,
                          SUM(s.unitCost * s.qty) AS total_amount_sold,
                          SUM(s.qty) AS total_units_sold,
                          substr(s.saleDate, 1, 2) AS month,
                          (CAST(substr(s.saleDate, 1, 2) AS INTEGER) - 1) / 3 + 1 AS quarter,
                          substr(s.saleDate, 7, 4) AS year
                        FROM '%s' s
                        JOIN products p ON s.productID = p.productID
                        JOIN customers c ON s.customerID = c.customerID
                        GROUP BY s.txnID, s.repID
                        ORDER BY year, quarter, month
                       ", partition_table))
    
    
    # combine all the data frames in one data frame 
    sales_facts <- bind_rows(sales_facts, sales_data)
    
  }
  
  # select the columns for batch insertion
  sales_facts <- sales_facts %>% select(txnID, customerID, productID, repID, productName, month, quarter, year, country, total_amount_sold, total_units_sold)
  # batch insertion
  dbWriteTable(mydb, "sales_facts", sales_facts, append = TRUE, row.names = FALSE)
}

# function to populate data for rep facts table
load_rep_facts_data <- function(mydb_sqlite, mydb){
  rep_facts <- data.frame()
  
  # get all tables from sqlite database
  all_tables <- dbListTables(mydb_sqlite)
  # get all sale_partitionn table using the pattern
  sales_partition_tables <- grep("^sales_\\d{4}$", all_tables, value = TRUE)
  
  for (partition_table in sales_partition_tables) {
    # get the data group by year for better querying for analytics
    reps_data <- dbGetQuery(mydb_sqlite, sprintf("
                      SELECT 
                          s.repID,
                          s.txnID,
                          (r.firstName || ' ' || r.lastName) AS repName,
                          s.saleDate AS date,
                          SUM(s.unitCost * s.qty) AS total_amount_sold,
                          AVG(s.unitCost * s.qty) AS average_amount_sold,
                          substr(s.saleDate, 1, 2) AS month,
                          (CAST(substr(s.saleDate, 1, 2) AS INTEGER) - 1) / 3 + 1 AS quarter,
                          substr(s.saleDate, 7, 4) AS year
                        FROM '%s' s
                        JOIN reps r ON s.repID = r.repID
                        GROUP BY s.txnID, s.repID
                        ORDER BY year, quarter, month
                      ", partition_table)) 
    
    
    # combine all the data frames in one data frame 
    rep_facts <- bind_rows(rep_facts, reps_data)
  }
  # select the columns for batch insertion
  rep_facts <- rep_facts %>% select(repName, repID, txnID, month, quarter, year, total_amount_sold, average_amount_sold)
  # batch insertion
  dbWriteTable(mydb, "rep_facts", rep_facts, append = TRUE, row.names = FALSE)
}

main <- function(){
  
  # necessary libraries
  library(RMySQL)
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(stringr)
  
  db_name_fh <- "sql12723156"
  db_user_fh <- "sql12723156"
  db_host_fh <- "sql12.freemysqlhosting.net"
  db_pwd_fh <- "aNHWI8BQML"
  db_port_fh <- 3306
  
  # connection setup
  mydb <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,dbname = 
                       db_name_fh, host = db_host_fh, port = db_port_fh)
  
  sqlite_db <- "SalesDB.sqlitedb.db" 
  mydb_sqlite <- dbConnect(RSQLite::SQLite(), sqlite_db)
  
  
  # create the star schema
  create_star_schema(mydb)
  # populate data for dimension tables
  load_data_customer_dimension(mydb_sqlite, mydb)
  print(dbGetQuery(mydb, "SELECT * FROM customer_dimension LIMIT 5"))
  load_data_product_dimension(mydb_sqlite, mydb)
  print(dbGetQuery(mydb, "SELECT * FROM product_dimension LIMIT 5"))
  load_data_rep_dimension(mydb_sqlite, mydb)
  print(dbGetQuery(mydb, "SELECT * FROM rep_dimension LIMIT 5"))
  # populate data for facts tables and test
  load_sales_facts_data(mydb_sqlite, mydb)
  print(dbGetQuery(mydb, "SELECT * FROM sales_facts LIMIT 5"))
  load_rep_facts_data(mydb_sqlite, mydb)
  print(dbGetQuery(mydb, "SELECT * FROM rep_facts LIMIT 5"))
  
  # disconnect from the two databases
  dbDisconnect(mydb)
  dbDisconnect(mydb_sqlite)
  
}

main()
