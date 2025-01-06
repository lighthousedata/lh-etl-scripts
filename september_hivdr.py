import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
from datetime import date, datetime

# Load environment variables
load_dotenv()

table_name = os.getenv("DB_TABLE")

# Database connection function
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        print("Database connection successful.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
        return None

# Step 1: Extract
def extract_data(excel_file_path):
    print("Extracting data from Excel...")
    df = pd.read_excel(excel_file_path, parse_dates=True)
    print("Data extracted successfully.")
    return df

# Step 2: Transform
def transform_data(excel_df):
    print("Transforming data...")
    column_mapping = {
        "PID": "ApplicationID",
        "Approved": "Approved",
        "Approved Date": "ApprovedDate",
        "sample sent": "SampleSent",
        "sample sent date": "SampleSentDate",
        "results received": "ResultReceived",
        "results received date": "ResultDate",
        "Lead expert": "LeadExpert",
        "Expert 2": "SecondExpert",
        "Expert 3": "ThirdExpert",
    }

    # Rename columns using the mapping and filter only the required columns
    excel_df = excel_df.rename(columns=column_mapping)
    matching_columns = list(column_mapping.values())
    transformed_df = excel_df[matching_columns]

    # Convert all date columns to MySQL format
    date_columns = ["ApprovedDate", "SampleSentDate", "ResultDate"]
    for col in date_columns:
        if col in transformed_df.columns:
            transformed_df[col] = pd.to_datetime(transformed_df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    print("Data transformation completed.")
    return transformed_df

# Escape strings for SQL
def escape_string(value):
    if pd.isna(value) or value is None:
        return "NULL"  # Handle NULL values

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return f"'{value.strftime('%Y-%m-%d')}'"  # Format dates properly

    if isinstance(value, str):
            value = value.strip().lower()
            if value == 'yes':
                return "'1'"
            elif value == 'no':                  
                return "'2'"
            elif value == 'unknown':
                return "'3'"
            return f"'{value.replace("'", "''").title()}'"  # Escape quotes and capitalize

    return str(value)  # Handle other types

# Step 3: Load
def load_data(transformed_df):
    print("Loading data into the database...")
    connection = connect_to_database()
    if connection is None:
        print("Unable to connect to the database. Load step aborted.")
        return

    try:
        cursor = connection.cursor(dictionary=True)

        for _, row in transformed_df.iterrows():
            application_id = row['ApplicationID']
            print(f"Processing ApplicationID: {str(application_id).upper()}")

            # Fetch existing row
            cursor.execute(f"SELECT * FROM {table_name} WHERE ApplicationID = {escape_string(application_id)}")
            existing_row = cursor.fetchone()

            if not existing_row:
                print(f"ApplicationID {application_id} not found in database. Skipping.")
                continue

            update_fields = []

            # Handle dates
            for date_col in ["ApprovedDate", "SampleSentDate", "ResultDate"]:
                excel_date = row.get(date_col)
                db_date = existing_row.get(date_col)

                # Convert `0000-00-00` to NULL
                if db_date == "0000-00-00":
                    db_date = None

                # Overwrite database date if both Excel and DB dates are valid
                if pd.notna(excel_date) and db_date is not None:
                    update_fields.append(f"{date_col} = {escape_string(excel_date)}")
                elif db_date is None and pd.notna(excel_date):  # Update NULL with Excel date
                    update_fields.append(f"{date_col} = {escape_string(excel_date)}")

            # Update other fields
            for col in transformed_df.columns:
                if col == "ApplicationID" or col in ["ApprovedDate", "SampleSentDate", "ResultDate"]:
                    continue

                excel_value = row[col]
                db_value = existing_row[col]

                # Overwrite DB value with Excel value if Excel value is not null
                if pd.notna(excel_value):
                    update_fields.append(f"{col} = {escape_string(excel_value)}")

            if update_fields:
                update_query = f"""
                UPDATE {table_name}
                SET {', '.join(update_fields)}
                WHERE ApplicationID = {escape_string(application_id)};
                """
                print(f"Executing query: {update_query}")  # Debug log
                try:
                    cursor.execute(update_query)
                    print(f"Updated ApplicationID {application_id}")
                except mysql.connector.Error as err:
                    print(f"Error updating ApplicationID {application_id}: {err}")
                    continue
            else:
                print(f"No changes detected for ApplicationID {application_id}. Skipping update.")

        connection.commit()
        print("Data loaded successfully.")
    except mysql.connector.Error as err:
        print(f"Error during data load: {err}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Main ETL Pipeline
def etl_pipeline(excel_file_path):
    # Extract
    excel_df = extract_data(excel_file_path)
    # Transform
    transformed_df = transform_data(excel_df)
    # Load
    load_data(transformed_df)

if __name__ == "__main__":
    excel_file_path = r"C:\Users\SamsonMhango\Documents\LH\november.xlsx"
    etl_pipeline(excel_file_path)
