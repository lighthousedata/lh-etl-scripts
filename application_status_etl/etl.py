import pandas as pd
import mysql.connector
from dotenv import load_dotenv
load_dotenv()
import os
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
    # Mapping Excel columns to database columns
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

    transformed_df = excel_df.rename(columns=column_mapping)
    matching_columns = list(column_mapping.values())
    transformed_df = transformed_df[matching_columns]
    print("Data transformation completed.")
    return transformed_df

# Step 3: Load
def escape_string(value):
    """Escape single quotes in a string for MySQL, and handle datetime objects, booleans, and null values."""
    if pd.isna(value): 
        return 'NULL'
    
    # Handle "yes" and "no" mappings
    if isinstance(value, str):
        value = value.strip().lower() 
        if value == 'yes':
            return '1'
        elif value == 'no':
            return '2' 
        elif value == "unknown":
            return '3'

    if isinstance(value, pd.Timestamp):
        return f"'{value.strftime('%Y-%m-%d')}'" 

    if isinstance(value, str):
        return f"'{value.replace("'", "''")}'" 

    return str(value)



def load_data(transformed_df):
    print("Loading data into the database...")
    connection = connect_to_database()
    if connection is None:
        print("Unable to connect to the database. Load step aborted.")
        return
    
    try:
        cursor = connection.cursor()

        for _, row in transformed_df.iterrows():
            application_id = row['ApplicationID']
            print(f"Processing ApplicationID: {str(application_id).upper()}")

            # Preprocessing values to prevent SQL syntax errors
            approved = escape_string(row['Approved'])
            approved_date = escape_string(row['ApprovedDate'])
            sample_sent = escape_string(row['SampleSent'])
            sample_sent_date = escape_string(row['SampleSentDate'])
            result_received = escape_string(row['ResultReceived'])
            result_date = escape_string(row['ResultDate'])
            lead_expert = escape_string(row['LeadExpert'])
            second_expert = escape_string(row['SecondExpert'])
            third_expert = escape_string(row['ThirdExpert'])

            # Preparing the UPDATE SQL statement
            update_query = f"""
            UPDATE etl_demo
            SET 
                Approved = {approved},
                ApprovedDate = {approved_date},
                SampleSent = {sample_sent},
                SampleSentDate = {sample_sent_date},
                ResultReceived = {result_received},
                ResultDate = {result_date},
                LeadExpert = {lead_expert.title()},
                SecondExpert = {second_expert.title()},
                ThirdExpert = {third_expert.title()}
            WHERE ApplicationID = '{str(application_id).upper()}';
            """
            try:
                # Execute the query
                cursor.execute(update_query)
            except mysql.connector.Error as err:
                print(f"Error processing ApplicationID {application_id.upper()}: {err}")
                continue 
        
        # Commit the changes
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
    excel_file_path = "Application status.xlsx"  
    etl_pipeline(excel_file_path)
