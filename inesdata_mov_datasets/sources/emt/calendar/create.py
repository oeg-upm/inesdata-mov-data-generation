import sys
from minio import Minio
from datetime import datetime
import os
import json
import pandas as pd
import traceback
from dotenv import load_dotenv
load_dotenv()  # load env variables


def load_historical_dates_df(calendar_processed_data_filename: str) -> pd.DataFrame:
    if os.path.isfile(calendar_processed_data_filename):
        df = pd.read_csv(calendar_processed_data_filename, parse_dates=['date'])
    else:
        df = pd.DataFrame([], columns=['date', 'dayType'])
    return df


def generate_historical_dates_df(date: str):
    # Load historical dataset and check if it is created and not empty
    calendar_processed_data_filename = os.path.abspath(os.getenv('DATA_PATH')) + '/processed/emt/calendar_processed_dataset.csv'
    historical_dates_df = load_historical_dates_df(calendar_processed_data_filename)
    
    # Download minio files
    calendar_objects = minio_client.list_objects(os.getenv('BUCKET_NAME'), prefix=f"raw/emt/{date}/calendar/")
    for calendar_object in calendar_objects:
        calenadar_filename = os.path.abspath(os.getenv('DATA_PATH')) + '/' + calendar_object.object_name
        if not os.path.isfile(calenadar_filename):
            print(calendar_object.object_name)
            # Download raw data file
            minio_client.fget_object(os.getenv('BUCKET_NAME'), calendar_object.object_name, calenadar_filename)
            # Collect raw data for day type
            if os.path.isfile(calenadar_filename):
                with open(calenadar_filename, 'r') as f:
                    calendar_content = json.load(f)
                if len(calendar_content['data']) != 0:
                    historical_dates_df = pd.DataFrame(calendar_content['data'])
                    historical_dates_df['date'] = pd.to_datetime(historical_dates_df['date'], dayfirst=True)
                    historical_dates_df = historical_dates_df[['date', 'dayType']]
                # After getting the dat type, remove raw data file from folder
                os.remove(calenadar_filename)
    
    print(historical_dates_df)
    # Store processed dataset
    historical_dates_df.to_csv(calendar_processed_data_filename, index=None)


if __name__ == "__main__":
    start = datetime.now()
    
    # Connect to minio bucket
    minio_client = Minio(
        os.getenv('BUCKET_URL'),
        access_key=os.getenv('MINIO_ACCESS_KEY_ID'),
        secret_key=os.getenv('MINIO_SECRET_ACCESS_KEY'),
        region='us-east-1',
        secure=False
    )
    
    # Download day's raw data from EMT
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime('%Y/%m/%d')
    # date = datetime.strptime('2024/02/09', '%Y/%m/%d').strftime('%Y/%m/%d')
    print(f'Getting day type of {date}')

    try:
        generate_historical_dates_df(date)
    except Exception as e: 
        print(e)
        traceback.print_exc()
    
    end = datetime.now()
    print(end - start)
