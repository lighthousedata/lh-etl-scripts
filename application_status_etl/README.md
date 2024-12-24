### README: Setting Up Python Virtual Environment and Environment Variables for the ETL Script

---

## Overview

This repository contains an ETL pipeline script written in Python. It extracts data from an Excel file, transforms it into a desired format, and loads it into a MySQL database. The script utilizes a `.env` file to manage sensitive database credentials.

---

## Requirements

- Python 3.8+
- MySQL database
- Excel file (`Application status.xlsx`)

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository_url>
cd <repository_directory>
```

### 2. Set Up a Virtual Environment

Create a virtual environment to isolate dependencies.

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# For Windows:
venv\Scripts\activate
# For macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies

Install the required packages from `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 4. Configure the `.env` File

Create a `.env` file in the root directory to store database credentials. Add the following:

```env
DB_HOST=your_database_host
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
```

Replace `your_database_host`, `your_database_user`, `your_database_password`, and `your_database_name` with your actual database details.

### 5. Add the Excel File

Place the Excel file (`Application status.xlsx`) in the same directory as the script.

### 6. Run the ETL Script

To run the ETL pipeline, execute the following command:

```bash
python etl_script.py
```

---

## Script Functionality

1. **Extract**: Reads data from the specified Excel file using `pandas`.
2. **Transform**: Renames columns to match database schema and formats data.
3. **Load**: Updates records in the MySQL database using an `UPDATE` query.

---

## Notes

- Ensure the MySQL database and table (`etl_demo`) are properly configured before running the script.
- Modify the column mappings in the `transform_data` function if the database schema changes.
- The script uses `dotenv` to load environment variables securely.

---

## Troubleshooting

- **Database Connection Error**: Ensure your `.env` file contains valid credentials and the database server is accessible.
- **Missing Dependencies**: Run `pip install -r requirements.txt` to ensure all required packages are installed.
- **Excel File Errors**: Verify the structure and column names of the Excel file match the script’s expectations.

---

## Dependencies

- `pandas`: Data manipulation
- `mysql-connector-python`: MySQL database connection
- `python-dotenv`: Manage environment variables

To install any additional dependencies, use:

```bash
pip install <package_name>
```

---

### License

This project is licensed under the MIT License. See the `LICENSE` file for details.