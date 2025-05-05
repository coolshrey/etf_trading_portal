# broker_handlers.py
from abc import ABC, abstractmethod


class BaseBrokerHandler(ABC):
    """Abstract base class for broker handlers."""

    def __init__(self, session=None):
        self.session = session

    @abstractmethod
    def login(self, auth_params):
        """Login to the broker platform."""
        pass

    @abstractmethod
    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place an order with standardized parameters."""
        pass

    @abstractmethod
    def get_positions(self):
        """Get current positions."""
        pass

    @abstractmethod
    def get_order_status(self, order_id):
        """Get status of an order."""
        pass


class FinvasiaBrokerHandler(BaseBrokerHandler):
    """Handler for Finvasia/Shoonya broker using existing login code."""

    def login(self, auth_params):
        """Login to Finvasia/Shoonya using the existing approach."""
        try:
            # Extract authentication parameters
            user_id = auth_params.get('user_id')
            password = auth_params.get('password')
            totp_secret = auth_params.get('totp_secret')
            vendor_code = auth_params.get('vendor_code')
            api_secret = auth_params.get('api_secret')
            imei = auth_params.get('imei')

            # Generate TOTP (your existing code)
            import pyotp
            # def generate_totp(self):
            raw_secret = auth_params.get('totp_secret', '')
            totp_secret = raw_secret.strip().replace(' ', '').replace('"', '').replace("'", '')
            totp = pyotp.TOTP(totp_secret).now()
            # print(totp)

            # Initialize API (your existing code)
            from NorenRestApiPy.NorenApi import NorenApi
            self.session = NorenApi("https://api.shoonya.com/NorenWClientTP/",
                                    "wss://api.shoonya.com/NorenWSTP/")

            # Login (your existing code)
            response = self.session.login(
                userid=user_id,
                password=password,
                twoFA=totp,
                vendor_code=vendor_code,
                api_secret=api_secret,
                imei=imei
            )

            # Check if login was successful
            if response and 'stat' in response and response['stat'] == 'Ok':
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                print(f"Finvasia login failed: {error_msg}")
                return False

        except Exception as e:
            print(f"Error in Finvasia login: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place an order with Finvasia/Shoonya."""
        try:
            if not self.session:
                print("Not logged in to Finvasia")
                return None

            # Map standardized parameters to Finvasia specific ones
            buy_or_sell = "B" if transaction_type == "BUY" else "S"
            price_type = "MKT" if order_type == "MARKET" else "LMT"

            # Place order (using existing Finvasia order parameters)
            response = self.session.place_order(
                buy_or_sell=buy_or_sell,
                product_type="C",
                exchange="NSE",
                tradingsymbol=symbol,
                quantity=quantity,
                discloseqty=0,  # Added this required parameter
                price_type=price_type,
                price=price if price else 0,
                retention="DAY",  # This seems to be required as well
                amo=None  # Including this for completeness
            )

            return response
        except Exception as e:
            print(f"Error placing Finvasia order: {str(e)}")
            return None

    def get_positions(self):
        """Get current positions from Finvasia."""
        try:
            if not self.session:
                print("Not logged in to Finvasia")
                return []

            return self.session.get_positions()
        except Exception as e:
            print(f"Error getting Finvasia positions: {str(e)}")
            return []

    def get_order_status(self, order_id):
        """Get order status from Finvasia."""
        try:
            if not self.session:
                print("Not logged in to Finvasia")
                return None

            return self.session.single_order_history(order_id)
        except Exception as e:
            print(f"Error getting Finvasia order status: {str(e)}")
            return None


class ZerodhaBrokerHandler(BaseBrokerHandler):
    """Handler for Zerodha broker."""

    def login(self, auth_params):
        """Login to Zerodha."""
        try:
            # Extract authentication parameters
            api_key = auth_params.get('api_key')
            api_secret = auth_params.get('api_secret')
            access_token = auth_params.get('access_token')

            # Initialize Kite
            from kiteconnect import KiteConnect

            self.session = KiteConnect(api_key=api_key)

            # Set access token if provided
            if access_token:
                self.session.set_access_token(access_token)
                return True
            else:
                print("Access token required for Zerodha")
                return False
        except Exception as e:
            print(f"Error in Zerodha login: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place an order with Zerodha."""
        try:
            # Map standardized parameters to Zerodha specific ones
            zerodha_order_type = "MARKET" if order_type == "MARKET" else "LIMIT"

            # Place order
            response = self.session.place_order(
                variety="regular",
                exchange="NSE",
                tradingsymbol=symbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product="CNC",
                order_type=zerodha_order_type,
                price=price if price and order_type != "MARKET" else None
            )

            return response
        except Exception as e:
            print(f"Error placing Zerodha order: {str(e)}")
            return None

    def get_positions(self):
        """Get current positions from Zerodha."""
        try:
            return self.session.positions()
        except Exception as e:
            print(f"Error getting positions: {str(e)}")
            return []

    def get_order_status(self, order_id):
        """Get order status from Zerodha."""
        try:
            return self.session.order_history(order_id)
        except Exception as e:
            print(f"Error getting order status: {str(e)}")
            return None


class UpstoxBrokerHandler(BaseBrokerHandler):
    """Handler for Upstox broker."""

    def login(self, auth_params):
        """Login to Upstox."""
        try:
            api_key = auth_params.get('api_key')
            api_secret = auth_params.get('api_secret')
            access_token = auth_params.get('access_token')

            from upstox_client.rest import ApiException
            from upstox_client.api.login_api import LoginApi
            from upstox_client.api.order_api import OrderApi
            import upstox_client

            # Configure OAuth2 access token for authorization
            configuration = upstox_client.Configuration()

            if access_token:
                configuration.access_token = access_token
                self.session = OrderApi(upstox_client.ApiClient(configuration))
                return True
            else:
                print("Access token required for Upstox API")
                return False
        except Exception as e:
            print(f"Error in Upstox login: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place order using Upstox API."""
        try:
            from upstox_client.models import PlaceOrderRequest

            order_request = PlaceOrderRequest(
                quantity=quantity,
                product="D",  # Delivery
                validity="DAY",
                price=price if price else 0,
                tag="ETF-AUTO",
                instrument_token=symbol,  # Would need mapping for actual instrument token
                order_type="MARKET" if order_type == "MARKET" else "LIMIT",
                transaction_type=transaction_type,
                disclosed_quantity=0
            )

            response = self.session.place_order(order_request)
            return response
        except Exception as e:
            print(f"Error placing Upstox order: {str(e)}")
            return None

    def get_positions(self):
        """Get positions from Upstox."""
        try:
            return self.session.get_positions_data()
        except Exception as e:
            print(f"Error getting positions: {str(e)}")
            return []

    def get_order_status(self, order_id):
        """Get order status from Upstox."""
        try:
            return self.session.get_order_details(order_id)
        except Exception as e:
            print(f"Error getting order status: {str(e)}")
            return None


class DhanBrokerHandler(BaseBrokerHandler):
    """Handler for Dhan broker based on v2 API documentation."""

    def login(self, auth_params):
        """Login to Dhan."""
        try:
            client_id = auth_params.get('user_id')  # Using USER_ID as client_id
            access_token = auth_params.get('access_token')

            # Dhan API requires both client_id and access_token
            if not client_id or not access_token:
                print("Both client_id and access_token are required for Dhan API")
                return False

            # Import Dhan client library
            import requests

            # Initialize session
            self.session = requests.Session()
            self.session.headers.update({
                'client_id': client_id,
                'access-token': access_token,
                'Content-Type': 'application/json'
            })

            # Test connection by getting user details
            response = self.session.get('https://api.dhan.co/userdetails')

            if response.status_code == 200:
                user_data = response.json()
                print(f"Successfully connected to Dhan for client: {client_id}")
                self.client_id = client_id
                return True
            else:
                print(f"Failed to connect to Dhan API: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"Error in Dhan login: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place order using Dhan API."""
        try:
            # According to Dhan API v2 documentation
            order_data = {
                "securityId": symbol,  # Dhan uses a security ID format
                "exchange": "NSE",  # Can be NSE, BSE, NFO, etc.
                "transactionType": transaction_type,  # BUY or SELL
                "quantity": quantity,  # Integer
                "validity": "DAY",  # DAY, IOC, FOK
                "productType": "DELIVERY",  # DELIVERY, INTRADAY, MARGIN
                "orderType": "MARKET" if order_type == "MARKET" else "LIMIT"
            }

            # Add price for limit orders
            if order_type == "LIMIT" and price:
                order_data["price"] = price

            # Place order
            response = self.session.post(
                'https://api.dhan.co/orders',
                json=order_data
            )

            if response.status_code in (200, 201):
                return response.json()
            else:
                print(f"Order placement failed: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"Error placing Dhan order: {str(e)}")
            return None

    def get_positions(self):
        """Get positions from Dhan."""
        try:
            response = self.session.get('https://api.dhan.co/positions')

            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch positions: {response.status_code} - {response.text}")
                return []

        except Exception as e:
            print(f"Error getting positions: {str(e)}")
            return []

    def get_order_status(self, order_id):
        """Get order status from Dhan."""
        try:
            response = self.session.get(f'https://api.dhan.co/orders/{order_id}')

            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch order status: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"Error getting order status: {str(e)}")
            return None


class MstockBrokerHandler(BaseBrokerHandler):
    """Handler for Mstock broker."""

    def login(self, auth_params):
        """Login to Mstock."""
        try:
            # Implementation based on Mstock's API documentation
            # This is a placeholder - would need to be updated with actual Mstock API
            self.session = "Mstock session placeholder"
            return True
        except Exception as e:
            print(f"Error in Mstock login: {str(e)}")
            return False

    def place_order(self, symbol, quantity, price=None, order_type="MARKET", transaction_type="BUY"):
        """Place order using Mstock API."""
        try:
            # Placeholder implementation
            print(f"Mstock order placed: {symbol}, {quantity}, {transaction_type}")
            return {"status": "success"}
        except Exception as e:
            print(f"Error placing Mstock order: {str(e)}")
            return None

    def get_positions(self):
        """Get positions from Mstock."""
        try:
            # Placeholder implementation
            return []
        except Exception as e:
            print(f"Error getting positions: {str(e)}")
            return []

    def get_order_status(self, order_id):
        """Get order status from Mstock."""
        try:
            # Placeholder implementation
            return {"status": "success"}
        except Exception as e:
            print(f"Error getting order status: {str(e)}")
            return None


class BrokerFactory:
    """Factory class for getting appropriate broker handler."""

    @staticmethod
    def get_broker_handler(broker_name):
        """
        Get the appropriate broker handler based on name.

        Args:
            broker_name (str): Name of the broker

        Returns:
            BaseBrokerHandler: Appropriate broker handler instance
        """
        broker_handlers = {
            "FINVASIA": FinvasiaBrokerHandler,
            "SHOONYA": FinvasiaBrokerHandler,  # Alias
            "ZERODHA": ZerodhaBrokerHandler,
            "UPSTOX": UpstoxBrokerHandler,
            "DHAN": DhanBrokerHandler,
            "MSTOCK": MstockBrokerHandler
        }

        handler_class = broker_handlers.get(broker_name.upper())
        if not handler_class:
            raise ValueError(f"Unsupported broker: {broker_name}")

        return handler_class()
