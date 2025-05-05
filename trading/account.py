# account.py
from broker_handlers import BrokerFactory


class Account:
    def __init__(self, user_id, password=None, totp_secret=None, broker="FINVASIA",
                 api_key=None, api_secret=None, vendor_code=None, imei=None,
                 access_token=None, is_master=False, multiplier=1, copy=False,
                 subscription_status='Inactive', subscription_expiry=None):
        # Common attributes
        self.user_id = user_id
        self.is_master = is_master
        self.multiplier = multiplier
        self.copy = copy
        self.subscription_status = subscription_status
        self.subscription_expiry = subscription_expiry
        self.is_logged_in = False

        # Broker information
        self.broker = broker

        # Authentication parameters
        self.auth_params = {
            'user_id': user_id,
            'password': password,
            'totp_secret': totp_secret,
            'api_key': api_key,
            'api_secret': api_secret,
            'vendor_code': vendor_code,
            'imei': imei,
            'access_token': access_token
        }

        # Broker handler
        self.broker_handler = None

    def generate_totp(self):
        """Generate TOTP for 2FA authentication (for backward compatibility)"""
        if self.auth_params.get('totp_secret'):
            import pyotp
            return pyotp.TOTP(self.auth_params['totp_secret']).now()
        return None

    def login(self):
        """Login to the appropriate broker platform."""
        try:
            self.broker_handler = BrokerFactory.get_broker_handler(self.broker)
            success = self.broker_handler.login(self.auth_params)

            if success:
                self.is_logged_in = True
                print(f"Successfully logged in to {self.broker} for account {self.user_id}")
                return True
            else:
                print(f"Failed to login to {self.broker} for account {self.user_id}")
                return False

        except Exception as e:
            print(f"Error logging in to {self.broker} for account {self.user_id}: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place an order using the appropriate broker."""
        if not self.is_logged_in:
            print(f"Account {self.user_id} is not logged in. Cannot place order.")
            return False

        try:
            response = self.broker_handler.place_order(
                symbol=symbol,
                quantity=quantity,
                price=price,
                order_type=order_type,
                transaction_type=transaction_type
            )
            return response
        except Exception as e:
            print(f"Error placing order for account {self.user_id}: {str(e)}")
            return False
