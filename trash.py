# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

password = quote_plus("P@ssword123")  # This encodes the @ symbol properly
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://postgres:{password}@localhost/etf_portal"

# Add this near the top of your app.py file, after the load_dotenv() line
print(f"Using database URL: {os.getenv('DATABASE_URL', 'Not found, using default')}")