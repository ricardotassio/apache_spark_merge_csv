Here's the updated README with the correct file name:

---

# Data Difference Calculation with Apache Spark

This project aims to calculate the differences between two CSV datasets using Apache Spark. The script compares specific column values between two CSV files (`pre.csv` and `post.csv`), generates difference columns, and saves the result in a single CSV file.

## Project Structure

- `pre.csv` - CSV file containing the "pre" dataset.
- `post.csv` - CSV file containing the "post" dataset.
- `diff_calculation_spark.py` - Main Python script that performs the comparison and generates the output file.
- `output_single_file.csv` - Output CSV file containing the calculated differences.

## Requirements

- [Apache Spark](https://spark.apache.org/) - A distributed data processing tool.
- Python 3.x
- Python Libraries:
  - `pyspark` - For data manipulation using Spark.
  - `shutil` and `os` - For file and directory manipulation operations.

## Installation

1. Ensure that Apache Spark is installed and configured on your system.
2. Clone this repository or copy the files to your local environment.
3. Install the necessary dependencies:

```bash
pip install pyspark
```

## Usage

1. **Data Preparation**:

   - Place the `pre.csv` and `post.csv` files in the project's root directory.
   - Ensure that both CSV files have the same structure, meaning the same columns, to allow for a valid comparison.

2. **Running the Script**:
   - Run the `diff_calculation_spark.py` script:

```bash
python diff_calculation_spark.py
```

3. **Result**:
   - The result will be saved in a file named `output_single_file.csv` in the project's root directory. This file will contain the differences calculated between the two datasets.

## Explanation of the Script

The script performs the following steps:

1. **Setup and Configuration**:

   - Initializes a Spark session and configures it for the task.

2. **Data Loading**:

   - Loads the `pre.csv` and `post.csv` files into Spark DataFrames.

3. **Data Processing**:

   - Joins the DataFrames based on key columns (`InvoiceNo`, `StockCode`, `CustomerID`, `InvoiceDate`).
   - Compares non-key columns and generates difference columns.

4. **Output**:
   - Reduces the number of partitions to 1 to ensure that the result is saved as a single CSV file.
   - Moves the generated CSV file to the specified output location and cleans up any temporary files.
