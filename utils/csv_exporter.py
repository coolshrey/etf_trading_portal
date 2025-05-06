# utils/csv_exporter.py
import os
import csv
import pandas as pd
from datetime import datetime


def export_brokers_to_csv(brokers, csv_path='data/accounts.csv'):
    """
    Export broker connections from the database to accounts.csv

    Args:
        brokers: List of Broker objects from the database
        csv_path: Path to the accounts.csv file
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Create CSV data
    rows = []
    for broker in brokers:
        rows.append({
            'USER_ID': broker.user_id_broker,
            'PASSWORD': broker.password or '',
            'TOTP_SECRET': broker.totp_secret or '',
            'BROKER': broker.broker_name,
            'API_KEY': broker.api_key or '',
            'API_SECRET': broker.api_secret or '',
            'VENDOR_CODE': broker.vendor_code or '',
            'IMEI': broker.imei or '',
            'ACCESS_TOKEN': broker.access_token or '',
            'IS_MASTER': str(broker.is_master).upper(),
            'COPY_MULTIPLIER': broker.copy_multiplier,
            'COPY': str(broker.copy).upper(),
            'SUBSCRIPTION_EXPIRY': '',  # This will be filled from subscription module
            'SUBSCRIPTION_STATUS': 'Active'  # Default until subscription module is implemented
        })

    # Create the DataFrame and save as CSV
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    print(f"Exported {len(rows)} broker connections to {csv_path} at {datetime.now()}")


# utils/csv_exporter.py
def export_brokers_to_csv(brokers, csv_path='data/accounts.csv'):
    """
    Export broker connections from the database to accounts.csv
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Create CSV data
    rows = []
    for broker in brokers:
        # Get subscription status directly from broker
        subscription_expiry = ''
        if broker.subscription_expiry:
            subscription_expiry = broker.subscription_expiry.strftime('%d-%m-%Y')

        # Update status based on current date if expiry exists
        if broker.subscription_expiry and broker.subscription_expiry < datetime.datetime.now():
            # Update broker record if expired
            broker.subscription_status = 'Inactive'
            db.session.commit()

        rows.append({
            'USER_ID': broker.user_id_broker,
            'PASSWORD': broker.password or '',
            'TOTP_SECRET': broker.totp_secret or '',
            'VENDOR_CODE': broker.vendor_code or '',
            'API_SECRET': broker.api_secret or '',
            'IMEI': broker.imei or '',
            'IS_MASTER': str(broker.is_master).upper(),
            'COPY_MULTIPLIER': broker.copy_multiplier,
            'COPY': str(broker.copy).upper(),
            'SUBSCRIPTION_EXPIRY': subscription_expiry,
            'SUBSCRIPTION_STATUS': broker.subscription_status,
            'BROKER': broker.broker_name
        })

    # Create the DataFrame and save as CSV
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    print(f"Exported {len(rows)} broker connections to {csv_path} at {datetime.datetime.now()}")



