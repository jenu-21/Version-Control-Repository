import os
import logging
import pandas as pd

# Constants
LOCAL_DATA_PATH = './'
LOG_FILE = os.path.join(LOCAL_DATA_PATH, 'pipeline.log')
RAW_DATA_FILE = os.path.join(LOCAL_DATA_PATH, '2022-01-cheshire-street.csv')
OUTCOMES_DATA_FILE = os.path.join(LOCAL_DATA_PATH, '2022-01-cheshire-outcomes.csv')
STAGED_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_cheshire_street.csv')
PROCESSING_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'processed_cheshire_street.csv')
REPORTING_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'reporting_cheshire_street.csv')


# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

def ingest_data(file_path):
    """
    Ingest raw data from a CSV file.
    """
    logging.info(f"Starting data ingestion from {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data ingestion from {file_path} completed successfully")
        return df
    except ValueError as e:
        logging.error(f"Error reading the CSV file {file_path}: {e}")
        return None

def merge_data(df, df_outcomes):
    """
    Merge the main data with outcomes data on 'Crime ID'.
    """
    return pd.merge(df, df_outcomes[['Crime ID', 'Outcome type']], how='left', on='Crime ID')

def finaloutcome(df):
    """
    Create 'Final Outcome' column based on 'Outcome type' and 'Last outcome category'.
    """
    df['Final Outcome'] = df.apply(
        lambda row: row['Outcome type'] if pd.notnull(row['Outcome type']) else row['Last outcome category'],
        axis=1
    )
    return df

def categorize_outcome(outcome):
    if outcome in ['Unable to prosecute suspect', 
                   'Investigation complete; no suspect identified', 
                   'Status update unavailable']:
        return 'No Further Action'
    elif outcome in ['Local resolution', 
                     'Offender given a caution', 
                     'Action to be taken by another organisation', 
                     'Awaiting court outcome']:
        return 'Non-criminal Outcome'
    elif outcome in ['Further investigation is not in the public interest', 
                     'Further action is not in the public interest', 
                     'Formal action is not in the public interest']:
        return 'Public Interest Consideration'
    else:
        return 'Unknown'  # Or any other category for unknown outcomes

def apply_categorization(df):
    """
    Apply categorization to 'Final Outcome' column.
    """
    df['Broad Outcome Category'] = df['Final Outcome'].apply(categorize_outcome)
    return df

def del_values_street(df):
    """
    Delete unnecessary columns from the DataFrame.
    """
    cols_to_delete = ['Reported by', 'Context', 'Location']
    df.drop(columns=cols_to_delete, inplace=True)
    return df

def stage_data(df, df_outcomes, output_file):
    """
    Store the data to a CSV file for staging.
    """
    logging.info("Starting data staging")
    try:
        # Apply transformations
        df = merge_data(df, df_outcomes)
        df = del_values_street(df)

        # Save to CSV
        df.to_csv(output_file, index=False)
        logging.info("Data staging completed successfully")
    except Exception as e:
        logging.error(f"Error during data staging: {e}")

def processing_data(df, output_file):
    """
    Processing Layer: Store the transformed data to a CSV file.
    """
    logging.info("Starting Data Processing")
    try:
        # Apply primary transformations
        df = finaloutcome(df)
        df = apply_categorization(df)

        # Save to CSV
        df.to_csv(output_file, index=False)
        logging.info("Data Processing completed successfully")
    except Exception as e:
        logging.error(f"Error during data processing: {e}")

def reporting_data(df, output_file):
    """
    Reporting Layer: Store the aggregated reporting data to a CSV file.
    """
    logging.info("Starting reporting data aggregation")
    try:
        # Apply aggregation directly within the function
        agg_df = df.groupby(['Crime type', 'Broad Outcome Category']).size().reset_index(name='Count')

        # Save to CSV
        agg_df.to_csv(output_file, index=False)
        logging.info("Reporting data aggregation completed successfully")
    except Exception as e:
        logging.error(f"Error during reporting data aggregation: {e}")

def main():
    logging.info("Pipeline execution started")
    try:
        df = ingest_data(RAW_DATA_FILE)
        df_outcomes = ingest_data(OUTCOMES_DATA_FILE)
        
        if df is not None and df_outcomes is not None:
            stage_data(df, df_outcomes, STAGED_DATA_FILE)
            df_staged = ingest_data(STAGED_DATA_FILE)  # Read the staged data for further processing
            processing_data(df_staged, PROCESSING_DATA_FILE)
            df_primary = ingest_data(PROCESSING_DATA_FILE)  # Read the primary data for reporting
            reporting_data(df_primary, REPORTING_DATA_FILE)
        logging.info("Pipeline execution completed successfully")
    except Exception as e:
        logging.critical(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
    
