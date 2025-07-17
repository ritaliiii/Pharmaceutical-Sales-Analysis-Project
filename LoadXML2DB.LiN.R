# File: LoadXML2DB.LiN.R
# Author: Ning Li
# Date: Summer Full 2024

create_schema <- function(dbcon){
  # drop tables if exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS reps;")
  dbExecute(dbcon, "DROP TABLE IF EXISTS products;")
  dbExecute(dbcon, "DROP TABLE IF EXISTS customers;")
  dbExecute(dbcon, "DROP TABLE IF EXISTS sales;")
  
  dbExecute(dbcon, "
            CREATE TABLE reps (
            repID INTEGER NOT NULL,
            lastName TEXT NOT NULL,
            firstName TEXT NOT NULL,
            phone TEXT NOT NULL,
            hireDate DATE NOT NULL,
            commission REAL NOT NULL,
            territory TEXT NOT NULL,
            certified BOOLEAN NOT NULL,
            PRIMARY KEY (repID)
            );
            ")
  
  dbExecute(dbcon, "
            CREATE TABLE products (
            productID INTEGER PRIMARY KEY AUTOINCREMENT,
            productName TEXT NOT NULL
            );
            ")
  
  dbExecute(dbcon, "
            CREATE TABLE customers (
            customerID INTEGER PRIMARY KEY AUTOINCREMENT,
            customerName TEXT NOT NULL,
            country TEXT NOT NULL
            )
            ")
  
  dbExecute(dbcon, "
            CREATE TABLE sales (
            txnID INTEGER PRIMARY KEY AUTOINCREMENT,
            repID INTEGER NOT NULL,
            customerID INTEGER NOT NULL,
            productID INTEGER NOT NULL,
            saleDate DATE NOT NULL,
            unitCost REAL NOT NULL,
            currency TEXT NOT NULL,
            qty INTEGER NOT NULL,
            FOREIGN KEY(repID) REFERENCES reps(repID),
            FOREIGN KEY(customerID) REFERENCES customers(customerID),
            FOREIGN KEY(productID) REFERENCES products(productID)
            );
            ")
}

load_reps_data <- function(dbcon){
  # list files with pattern "pharmaReps*.xml" in the directory "txn-xml"
  repsFiles <- list.files("txn-xml", pattern = "pharmaReps.*\\.xml", full.names = TRUE)
  
  # loop through every file to load data
  for (file in repsFiles) {
    # load XML file
    xml_dom <- xmlParse(file)
    
    # extract data
    # remove prefix r in repID
    repID <- xpathSApply(xml_dom, "//rep/@rID")
    repID <- as.integer(sub("r", "", repID))
    # extract other values from xml files
    lastName <- xpathSApply(xml_dom, "//rep/demo/sur", xmlValue)
    firstName <- xpathSApply(xml_dom, "//rep/demo/first", xmlValue)
    phone <- xpathSApply(xml_dom, "//rep/demo/phone", xmlValue)
    hireDate <- xpathSApply(xml_dom, "//rep/demo/hiredate", xmlValue)
    # remove the percentage sign, convert the result to numeric
    commission <- as.numeric(gsub("%", "", xpathSApply(xml_dom, "//rep/commission", xmlValue)))
    territory <- xpathSApply(xml_dom, "//rep/territory", xmlValue)
    certified_element <- xpathSApply(xml_dom, "//rep/certified", xmlValue)
    
    # convert hireDate to a standard date format
    hireDate <- as.Date(hireDate, format = "%b %d %Y")
    hireDate <- format(hireDate, "%m-%d-%Y")
    
    # determine if each certified element is present or absent, return true and false
    certified <- sapply(certified_element, function(x) {
      if (is.null(x) || x == "") {
        return(FALSE)
      } else {
        return(TRUE)
      }
    })
    
    # create a data frame
    reps_df <- data.frame(
      repID, lastName, firstName, phone, hireDate, commission, territory, certified
    )
    
    # insert data into the table
    dbWriteTable(dbcon, "reps", reps_df, append = TRUE, row.names = FALSE)
  }
}

load_sales_data_products <- function(dbcon){
  uniqueProductNames <- c()  # Vector to store unique productName
  
  # list files with pattern "pharmaSalesTxn*.xml"
  txnFiles <- list.files("txn-xml", pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
  
  # loop through every file to load data
  for (file in txnFiles) {
    # load XML file
    xml_dom <- xmlParse(file)
    # extract data 
    productName <- xpathSApply(xml_dom, "//txn/sale/product", xmlValue)
    # filter out invalid (NULL, NA, missing) productName
    productName <- productName[!is.na(productName) & productName != ""]
    # get distinct productNames
    uniqueProductNames <- unique(c(uniqueProductNames, productName))
    products_df <- data.frame(
      productName = uniqueProductNames
    )
    # Insert data into the products table
    dbWriteTable(dbcon, "products", products_df, append = TRUE, row.names = FALSE)
  }
}

# helper function to get the product ID using the productName
get_productID <- function(dbcon, productName){
  query <- sprintf("SELECT productID FROM Products WHERE productName = '%s'", productName)
  productID <- dbGetQuery(dbcon, query)
  
  # if nothing founded, return NA
  if (nrow(productID) == 0) {
    return(NA)
  }
  return(productID$productID[1])
}

load_sales_data_customers <- function(dbcon){
  uniqueCustomers <- data.frame(customerName = character(),
                                country = character())
  # list files with pattern "pharmaSalesTxn*.xml"
  txnFiles <- list.files("txn-xml", pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
  
  # loop through every file to load data
  for (file in txnFiles) {
    # load XML file
    xml_dom <- xmlParse(file)
    # extract data 
    customerName <- xpathSApply(xml_dom, "//txn/customer", xmlValue)
    country <- xpathSApply(xml_dom, "//txn/country", xmlValue)
    # combine into a data frame
    customers_df <- data.frame(customerName = customerName,
                               country = country)
    # filter out invalid (NULL, NA, missing) customerName and country
    customers_df <- customers_df[!is.na(customers_df$customerName) & customers_df$customerName != "" &
                                   !is.na(customers_df$country) & customers_df$country != "", ]
    # get distinct customerName and country combination
    uniqueCustomers <- unique(rbind(uniqueCustomers, customers_df))
    
    # insert distinct customers into the customers table
    dbWriteTable(dbcon, "customers", uniqueCustomers, append = TRUE, row.names = FALSE)
  }
}

# helper function to get the customer ID using the customerName and country
get_CustomerID <- function(dbcon, customerName, country) {
  query <- sprintf("SELECT customerID FROM customers WHERE customerName = '%s' AND country = '%s'", 
                   customerName, country)
  customerID <- dbGetQuery(dbcon, query)
  
  # if nothing founded, return NA
  if (nrow(customerID) == 0) {
    return(NA)
  }
  return(customerID$customerID[1])
}


load_sales_data_sales <- function(dbcon){
  
  # list files with pattern "pharmaSalesTxn*.xml"
  txnFiles <- list.files("txn-xml", pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
  
  # loop through every file to load data
  for (file in txnFiles) {
    cat("Processing file:", file, "\n")
    
    # load XML file
    xml_dom <- xmlParse(file)
    
    # find all transaction nodes
    txn_nodes <- getNodeSet(xml_dom, "//txn")
    
    for (txn_node in txn_nodes) {
      
      # extract data for all the columns
      repID <- as.integer(as.character(xmlGetAttr(txn_node, "repID")))
      customerName <- xmlValue(getNodeSet(txn_node, ".//customer")[[1]])
      country <- xmlValue(getNodeSet(txn_node, ".//country")[[1]])
      customerID <- get_CustomerID(dbcon, customerName, country)
      saleDate <- xmlValue(getNodeSet(txn_node, ".//sale/date")[[1]])
      saleDate <- as.Date(saleDate, format = "%m/%d/%Y")
      saleDate <- format(saleDate, "%m-%d-%Y")
      productName <- xmlValue(getNodeSet(txn_node, ".//sale/product")[[1]])
      productID <- as.integer(get_productID(dbcon, productName))
      unitCost <- as.numeric(xmlValue(getNodeSet(txn_node, ".//sale/unitcost")[[1]]))
      currency <- xmlGetAttr(getNodeSet(txn_node, ".//sale/unitcost")[[1]], "currency")
      qty <- as.integer(as.character(xmlValue(getNodeSet(txn_node, ".//sale/qty")[[1]])))
      
      # Create a data frame for this transaction
      sales_df <- data.frame(
        repID, customerID, productID, saleDate, unitCost, currency, qty,
        stringsAsFactors = FALSE
      )
      
      # Insert data into the Sales table
      dbWriteTable(dbcon, "sales", sales_df, append = TRUE, row.names = FALSE)
    }
  }
}

partition_sales_table <- function(dbcon){
  # get distinct years from the saleDate column
  sales_years <- dbGetQuery(dbcon, "SELECT DISTINCT substr(saleDate, 7, 4) AS year FROM sales")
  # make sales_years accessible after dropping sales table
  assign("sales_years", sales_years, envir = .GlobalEnv)
  
  for (year in sales_years$year) {
    
    # create a tableName for each year's partition table
    tableName <- paste0("sales_", year)
    
    # drop tables if exist
    dbExecute(dbcon, sprintf("DROP TABLE IF EXISTS %s;", tableName))
    
    # create partition table query
    create_partition_table_query <- sprintf("
      CREATE TABLE %s (
        txnID INTEGER PRIMARY KEY AUTOINCREMENT,
        repID INTEGER NOT NULL,
        customerID INTEGER NOT NULL,
        productID INTEGER NOT NULL,
        saleDate TEXT,
        unitCost REAL,
        currency TEXT,
        qty INTEGER,
        FOREIGN KEY(repID) REFERENCES reps(repID),
        FOREIGN KEY(customerID) REFERENCES customers(customerID),
        FOREIGN KEY(productID) REFERENCES products(productID)
      );
    ", tableName)
    
    # execute the query
    dbExecute(dbcon, create_partition_table_query)
    
    # insert data into the new partition table
    insert_data_query <- sprintf("
      INSERT INTO %s (repID, customerID, productID, saleDate, unitCost, currency, qty)
      SELECT repID, customerID, productID, saleDate, unitCost, currency, qty
      FROM sales
      WHERE substr(saleDate, 7, 4) = '%s';
    ", tableName, year)
    dbExecute(dbcon, insert_data_query)
  }
  
  # drop the original big sales table
  dbExecute(dbcon, "DROP TABLE IF EXISTS sales;")
}


print_tables_for_testing <- function(dbcon){
  
  # print part of the reps table
  reps_query <- "SELECT * FROM reps LIMIT 5"
  reps_table <- dbGetQuery(dbcon, reps_query)
  print(reps_table)
  
  # print part of the products table
  products_query <- "SELECT * FROM products LIMIT 5"
  products_table <- dbGetQuery(dbcon, products_query)
  print(products_table)
  
  # print part of the customers table
  customers_query <- "SELECT * FROM customers LIMIT 5"
  customers_table <- dbGetQuery(dbcon, customers_query)
  print(customers_table)
  
  # print part of the sales table
  sales_query <- "SELECT * FROM sales LIMIT 5"
  sales_table <- dbGetQuery(dbcon, sales_query)
  print(sales_table)
}

print_partition_tables_for_testing <- function(dbcon){
  # print the partitin tables
  for (year in sales_years$year) {
    # assign the names using the pattern
    tableName <- paste0("sales_", year)
    print_partition_table_query <- sprintf("SELECT * FROM %s LIMIT 5", tableName)
    partition_table <- dbGetQuery(dbcon, print_partition_table_query)
    cat(sprintf("Data from %s:\n", tableName))
    print(partition_table)
  }
}

main <- function(){
  # necessary library
  library(DBI)
  library(XML)
  library(RSQLite)
  library(xml2)
  library(dplyr)
  
  dbName = "SalesDB.sqlitedb.db"
  
  # Connect to the database 
  dbcon <- dbConnect(RSQLite::SQLite(), dbName)
  
  # create the schema
  create_schema(dbcon)
  
  # load data to four tables
  load_reps_data(dbcon)
  load_sales_data_products(dbcon)
  load_sales_data_customers(dbcon)
  load_sales_data_sales(dbcon)
  
  # display part of these four tables for testing
  print_tables_for_testing(dbcon)
  
  # partition sales table to several smaller tables
  partition_sales_table(dbcon)
  
  # display partition tables for testing
  print_partition_tables_for_testing(dbcon)
  
  # disconnect from the database
  dbDisconnect(dbcon)
  
}

main()
