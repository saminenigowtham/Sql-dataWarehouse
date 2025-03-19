# Sql-dataWarehouse

## Overview
This project focuses on the need for a Data Warehouse and demonstrates the step-by-step process of building one using SQL. It covers various aspects of ETL (Extraction, Transformation, and Loading), data modeling techniques, and the implementation of the Medallion Architecture. Additionally, it emphasizes the principle of Separation of Concerns to ensure scalability and maintainability.

## Topics Covered
1. **Need for a Data Warehouse**
2. **Building a Data Warehouse using SQL**
3. **Types of Extraction, Transformation, and Loading (ETL) Processes**
4. **Types of Data Modeling**
5. **Implementation of Medallion Architecture**
6. **Separation of Concerns in Data Architecture**
7. **Data Pipeline Implementation**
8. **Fact and Dimension Tables with Surrogate Keys**
9. **Data Cataloging for the Data Warehouse**

## Architecture Design
The project architecture has been designed using **draw.io** and follows the Medallion Architecture principles:

![data_architecture](https://github.com/user-attachments/assets/67e09ff4-6546-4a03-bf69-43c610c9ef33)

### **Bronze Layer**
- Raw data ingestion from multiple sources.
- Data is stored in a structured format.
- All scripts related to data loading are stored in **Stored Procedures**.

### **Silver Layer**
- Data cleansing and validation.
- Data transformation and enrichment.
- Data quality checks and standardization.
- Exploratory data analysis.
- Processed data is stored for further usage.

### **Gold Layer**
- Implementation of Fact and Dimension tables.
- Use of **Surrogate Keys** for efficient table joins.
- Data cataloging for better metadata management.
- Data is now analytics-ready and can be used for reporting and business intelligence.

## Steps Involved in Data Processing
1. **Data Extraction:** Collecting data from various sources.
2. **Data Transformation:** Cleaning, enriching, and structuring the data.
3. **Data Loading:** Storing processed data in different layers.
4. **Data Analysis & Exploration:** Understanding patterns and trends.
5. **Data Validation & Quality Checks:** Ensuring accuracy and consistency.
6. **Data Enrichment:** Enhancing datasets with additional attributes.
7. **Fact & Dimension Tables:** Designing and implementing relationships.
8. **Data Cataloging:** Organizing metadata for efficient access.

## Technologies Used
- **SQL** for Data Warehousing
- **Stored Procedures** for ETL automation
- **draw.io** for architectural design
- **Medallion Architecture** for structured data processing
- **Surrogate Keys** for table relationships

## Conclusion
This project provides a structured approach to building a Data Warehouse using SQL. By implementing the Medallion Architecture and enforcing Separation of Concerns, we ensure data integrity, scalability, and analytical efficiency. The final warehouse is ready for business intelligence and reporting purposes.

