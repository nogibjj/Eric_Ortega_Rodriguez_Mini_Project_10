[![CI](https://github.com/nogibjj/Eric_Ortega_Rodriguez_Mini_Project_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Eric_Ortega_Rodriguez_Mini_Project_10/actions/workflows/cicd.yml)

# **PySpark Data Processing Project**

<div align="center">
    <img src="image.png" alt="Project Image" width="400">
</div>

## **Mini Project 10**
### **Eric Ortega Rodriguez**

This repository contains a PySpark-based project for data processing on a large dataset, developed as part of an assignment. The project utilizes Spark SQL for querying and data transformation.

---

## **Requirements**

- Use PySpark to perform data processing on a large dataset.
- Include at least one Spark SQL query and one data transformation.

---

## **Deliverables**

- **PySpark Script:** The main script used for data processing.
- **Output Data or Summary Report** (PDF or Markdown): A report or summary of the results obtained from the data processing.

---

## **Workflow**

The `main.py` script processes the dataset using PySpark with the following steps:

### **Step 1: Extract Data Path**
The `extract` function fetches the path to the dataset (e.g., `processed_data.csv`). If the file is missing or the path is invalid, the program exits gracefully with an error message.

### **Step 2: Start Spark Session**
The script initializes a Spark session using `start_spark`, naming it `PhoneDataProcessing`. This session is required for PySpark operations such as loading, querying, and transforming the data.

### **Step 3: Load Dataset**
Using the `load_data` function, the script reads the dataset into a Spark DataFrame. It ensures the dataset is loaded successfully before proceeding.

### **Step 4: Describe Dataset**
The `describe` function provides an overview of the dataset, including the schema and a preview of the first few rows.

### **Step 5: SQL Query - Calculate Average Price**
The script runs a Spark SQL query to calculate the **average price (in USD)** for each phone brand. The results are sorted in descending order of average price and displayed in the terminal.

### **Step 6: Transform Data - Filter High-Priced Phones**
The script applies a transformation to filter phones categorized as `high price`. It then selects the following columns for high-priced phones:
- `phone_brand`
- `phone_model`
- `price_USD`
- `store`

The transformation results are processed and displayed using the `example_transform` function.

### **Step 7: End Spark Session**
Finally, the script ends the Spark session with `end_spark`, releasing all resources and ensuring the session is properly terminated.


### **Output Report**

The generated output report summarizing the results of the data processing can be found [here](https://github.com/nogibjj/Eric_Ortega_Rodriguez_Mini_Project_10/blob/main/output_report.pdf).
