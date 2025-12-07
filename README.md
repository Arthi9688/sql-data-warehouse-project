# 📦 SQL Data Warehouse Project

A complete end-to-end data engineering and analytics project built using SQL Server.
This repository demonstrates raw data ingestion, data modeling, ETL pipelines, and SQL-based analytics following modern industry standards.

## 🏗️ Data Architecture (Medallion)

This project follows the Medallion Architecture:

#### 🔹 Bronze Layer – Raw Data
	- Stores raw data ingested from CSV files (ERP + CRM).
	- Loaded directly into SQL Server without transformations.

####  🔸 Silver Layer – Cleaned Data
	- Data cleaning, validation, and standardization.
	- Resolves duplicates, missing values, and type issues.

####  🟡 Gold Layer – Business Data
	- Final star-schema model with Fact & Dimension tables.
	- Used for reporting, dashboards, and analytics queries.

<img width="1023" height="592" alt="image" src="https://github.com/user-attachments/assets/4018d852-2a7d-49c9-8c80-e7002192adae" />


📖 Project Overview

## This project includes:
	### Data Architecture: Designing a modern SQL-based warehouse
	- ETL Pipelines: Extract → Transform → Load using SQL
	- Data Modeling: Fact/Dimension schema for analytics


## 🎯 Skills Demonstrated
	- SQL Development
	- Data Engineering
	- ETL Pipeline Design
	- Data Modeling (Star Schema)


## 🚀 Project Requirements

Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.
### Specifications
	- Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
	- Data Quality: Cleanse and resolve data quality issues prior to analysis.
	- Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
	- Scope: Focus on the latest dataset only; historization of data is not required.
	- Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.
