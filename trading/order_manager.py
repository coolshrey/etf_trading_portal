# order_manager.py
from datetime import datetime
import pandas as pd
from account import Account


def check_subscription_status(accounts_file):
    """
    Check subscription expiry dates and update subscription status accordingly.

    Args:
        accounts_file (str): Path to the accounts.csv file

    Returns:
        pd.DataFrame: Updated accounts DataFrame with current subscription status
    """
    print("Checking subscription status...")

    # Read the accounts CSV file
    df = pd.read_csv(accounts_file)

    # Ensure required columns exist
    required_columns = ['SUBSCRIPTION_EXPIRY', 'SUBSCRIPTION_STATUS']
    for col in required_columns:
        if col not in df.columns:
            df[col] = None  # Add missing columns
            if col == 'SUBSCRIPTION_STATUS':
                df[col] = 'Inactive'  # Default status

    # Get current date
    current_date = datetime.now().strftime('%d-%m-%Y')
    current_date = datetime.strptime(current_date, '%d-%m-%Y')

    # Check each account's subscription status
    for index, row in df.iterrows():
        try:
            # Skip if no expiry date
            if pd.isna(row['SUBSCRIPTION_EXPIRY']):
                df.at[index, 'SUBSCRIPTION_STATUS'] = 'Inactive'
                continue

            # Parse expiry date
            expiry_date = datetime.strptime(row['SUBSCRIPTION_EXPIRY'], '%d-%m-%Y')

            # Compare with current date
            if current_date > expiry_date:
                df.at[index, 'SUBSCRIPTION_STATUS'] = 'Inactive'
                print(f"Account {row['USER_ID']} subscription has expired. Status set to Inactive.")
            else:
                df.at[index, 'SUBSCRIPTION_STATUS'] = 'Active'
                print(f"Account {row['USER_ID']} subscription is Active.")
        except ValueError as e:
            print(f"Error processing date for account {row['USER_ID']}: {e}")
            # Set to inactive if date format is incorrect
            df.at[index, 'SUBSCRIPTION_STATUS'] = 'Inactive'

    # Save updated status back to CSV
    df.to_csv(accounts_file, index=False)

    return df


class OrderManager:
    def __init__(self, accounts_file):
        self.accounts = []
        self.master_account = None
        self.accounts_file = accounts_file
        self.load_accounts(accounts_file)

    def load_accounts(self, accounts_file):
        """Load accounts from a CSV file."""
        print("Loading accounts...")

        # Check subscription status first
        df = check_subscription_status(accounts_file)

        # Load accounts with valid subscriptions only
        for _, row in df.iterrows():
            try:
                # Determine if row has the broker column
                broker = row.get('BROKER', 'FINVASIA') if 'BROKER' in df.columns else 'FINVASIA'

                # Handle missing or NaN values
                def safe_get(row, col):
                    return None if col not in df.columns or pd.isna(row[col]) else row[col]

                # Create account with proper broker information
                account = Account(
                    user_id=row['USER_ID'],
                    password=row['PASSWORD'],
                    totp_secret=safe_get(row, 'TOTP_SECRET'),
                    broker=broker,
                    api_key=safe_get(row, 'API_KEY'),
                    api_secret=safe_get(row, 'API_SECRET'),
                    vendor_code=safe_get(row, 'VENDOR_CODE'),
                    imei=safe_get(row, 'IMEI'),
                    access_token=safe_get(row, 'ACCESS_TOKEN'),
                    is_master=row['IS_MASTER'],
                    multiplier=row['COPY_MULTIPLIER'],
                    copy=row['COPY'],
                    subscription_status=row['SUBSCRIPTION_STATUS'],
                    subscription_expiry=safe_get(row, 'SUBSCRIPTION_EXPIRY')
                )

                if account.is_master:
                    self.master_account = account
                elif account.subscription_status == 'Active':
                    self.accounts.append(account)
            except Exception as e:
                print(f"Error creating account for {row['USER_ID']}: {str(e)}")

        print("Accounts loaded successfully.")

    def login_all(self):
        """Log in to all broker accounts."""
        print("Logging in to all accounts...")

        # Login to master account
        if self.master_account:
            if not self.master_account.login():
                print(f"Failed to login master account: {self.master_account.user_id}")
                return False

        # Login to copy accounts
        for account in self.accounts:
            if not account.login():
                print(f"Failed to login account: {account.user_id}")

        return True

    def place_orders(self, filtered_etfs):
        """
        Place orders for ETFs across all active accounts using the broker abstraction.

        Args:
            filtered_etfs: Either a dictionary mapping symbols to quantities,
                          or a DataFrame with symbols and quantities

        Returns:
            list: Details of orders placed through master account
        """
        print("Placing orders...")

        # Determine if filtered_etfs is a DataFrame or dictionary
        if hasattr(filtered_etfs, 'iterrows'):
            # It's a DataFrame, process accordingly
            etf_data = {row['SYMBOL']: row['QTY'] for _, row in filtered_etfs.iterrows()}
        else:
            # It's already a dictionary
            etf_data = filtered_etfs

        # Place orders for master account
        master_orders = []
        if self.master_account and self.master_account.is_logged_in:
            for symbol, quantity in etf_data.items():
                try:
                    # Convert quantity to integer to ensure proper comparison
                    qty = int(quantity) if quantity else 0

                    # Ensure minimum quantity
                    if qty < 1:
                        qty = 1
                        print(f"Adjusting master order quantity to minimum 1 for {symbol}")

                    # Format symbol for exchange if needed
                    tradingsymbol = symbol if "-EQ" in symbol else f"{symbol}-EQ"

                    # Use the Account class's place_order method which will correctly use the broker handler
                    response = self.master_account.place_order(
                        symbol=tradingsymbol,
                        quantity=qty,
                        price=0.0,
                        order_type="MARKET",
                        transaction_type="BUY"
                    )

                    master_orders.append({
                        "symbol": symbol,
                        "quantity": qty,
                        "response": response
                    })
                    print(f"Master account order placed - {tradingsymbol}: {qty}")
                except Exception as e:
                    print(f"Error placing master order for {symbol}: {str(e)}")

        # Place orders for copy accounts
        for account in self.accounts:
            if account.is_logged_in and account.copy and account.subscription_status == 'Active':
                for symbol, quantity in etf_data.items():
                    try:
                        # Convert quantity to integer
                        qty = int(quantity) if quantity else 0

                        # Apply multiplier for copy account
                        copy_quantity = int(qty * account.multiplier)

                        if copy_quantity > 0:
                            # Format symbol for exchange if needed
                            tradingsymbol = symbol if "-EQ" in symbol else f"{symbol}-EQ"

                            # Use the Account class's place_order method which will correctly use the broker handler
                            response = account.place_order(
                                symbol=tradingsymbol,
                                quantity=copy_quantity,
                                price=0.0,
                                order_type="MARKET",
                                transaction_type="BUY"
                            )
                            print(f"Copy account {account.user_id} order placed - {tradingsymbol}: {copy_quantity}")
                    except Exception as e:
                        print(f"Error placing copy order for account {account.user_id}, symbol {symbol}: {str(e)}")

        return master_orders
