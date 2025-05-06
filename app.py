# app.py
from flask import Flask, jsonify, request, render_template, redirect, url_for, session, g, flash
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, get_jwt
from werkzeug.security import generate_password_hash, check_password_hash
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
import datetime
from utils.csv_exporter import export_brokers_to_csv
from datetime import timedelta
from functools import wraps
from flask import flash, redirect, url_for, session, g, request, render_template
from functools import wraps
import os
import pandas as pd
import uuid
from sqlalchemy.sql import text



# Load environment variables
load_dotenv()

# Encode the password properly
password = quote_plus("P@ssword123")
db_url = f"postgresql://postgres:{password}@localhost/etf_portal"

print(f"Using database URL: {db_url}")

# Initialize Flask app
app = Flask(__name__)

# Configure database
app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Configure JWT
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'dev-secret-key')  # Change this in production!

# Configure sessions
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev_secret_key')

# Initialize extensions
db = SQLAlchemy(app)
jwt = JWTManager(app)


# User model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    last_login = db.Column(db.DateTime)
    # Add this new line below:
    customer_id = db.Column(db.String(50), unique=True, nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


# Add this after the User model
class Broker(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    broker_name = db.Column(db.String(50), nullable=False)
    user_id_broker = db.Column(db.String(100), nullable=False)  # User ID at the broker
    password = db.Column(db.String(255), nullable=True)  # Optional for some brokers
    totp_secret = db.Column(db.String(255), nullable=True)  # Optional for some brokers
    api_key = db.Column(db.String(255), nullable=True)  # Optional for some brokers
    api_secret = db.Column(db.String(255), nullable=True)  # Optional for some brokers
    vendor_code = db.Column(db.String(100), nullable=True)  # For Finvasia
    imei = db.Column(db.String(100), nullable=True)  # For Finvasia
    access_token = db.Column(db.Text, nullable=True)  # For token-based auth
    is_master = db.Column(db.Boolean, default=False)
    copy_multiplier = db.Column(db.Integer, default=1)
    copy = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    last_updated = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # Add these new fields
    subscription_expiry = db.Column(db.DateTime, nullable=True)
    subscription_status = db.Column(db.String(20), default='Inactive')

    # Relationship to user
    user = db.relationship('User', backref=db.backref('brokers', lazy=True))


# Add this after the Broker model
class Subscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    plan_name = db.Column(db.String(50), nullable=False)
    start_date = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    expiry_date = db.Column(db.DateTime, nullable=False)
    payment_status = db.Column(db.String(20), nullable=False, default='Paid')  # Paid, Pending, Failed
    payment_method = db.Column(db.String(50))
    payment_id = db.Column(db.String(100))
    amount = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    # Relationship to user
    user = db.relationship('User', backref=db.backref('subscriptions', lazy=True))

    def __repr__(self):
        return f'<Subscription {self.plan_name} for {self.user_id}>'


# Add this after the Subscription model
class SubscriptionPlan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    description = db.Column(db.Text)
    duration_days = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float, nullable=False)
    features = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f'<SubscriptionPlan {self.name}>'


# Before request handler to load user
@app.before_request
def load_logged_in_user():
    user_id = session.get('user_id')

    if user_id is None:
        g.user = None
    else:
        g.user = User.query.get(user_id)


# Template context processor to make current_user available in templates
@app.context_processor
def inject_user():
    return {'current_user': g.user}


# Custom template filter for dates
@app.template_filter('datetime')
def format_datetime(value, format='%Y-%m-%d %H:%M:%S'):
    if value:
        return value.strftime(format)
    return ""


# Custom Jinja function for current year
@app.context_processor
def inject_now():
    def now():
        return datetime.datetime.now().strftime('%Y')
    return {'now': now}


@app.context_processor
def inject_current_year():
    return {'current_year': datetime.datetime.now().year}



# Home route
@app.route('/')
def home():
    return render_template('index.html')


# Simple API route
@app.route('/api/status')
def api_status():
    return jsonify({"status": "online", "message": "ETF Trading Portal API is running"})


# Authentication API routes
@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json()

    # Check if user already exists
    if User.query.filter_by(username=data['username']).first():
        return jsonify({"error": "Username already exists"}), 400

    if User.query.filter_by(email=data['email']).first():
        return jsonify({"error": "Email already exists"}), 400

    # Create new user
    user = User(
        username=data['username'],
        email=data['email'],
        is_admin=False  # Regular users cannot register as admins
    )
    user.set_password(data['password'])

    db.session.add(user)
    db.session.commit()

    return jsonify({"message": "User registered successfully"}), 201


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()

    # Find user by username
    user = User.query.filter_by(username=data['username']).first()

    # Check if user exists and password is correct
    if not user or not user.check_password(data['password']):
        return jsonify({"error": "Invalid username or password"}), 401

    # Update last login time
    user.last_login = datetime.datetime.utcnow()
    db.session.commit()

    # Generate JWT token
    access_token = create_access_token(
        identity=user.id,
        additional_claims={"is_admin": user.is_admin}
    )

    return jsonify({
        "access_token": access_token,
        "user_id": user.id,
        "username": user.username,
        "is_admin": user.is_admin
    }), 200


# Protected API routes
@app.route('/api/profile', methods=['GET'])
@jwt_required()
def profile():
    current_user_id = get_jwt_identity()
    user = User.query.get(current_user_id)

    if not user:
        return jsonify({"error": "User not found"}), 404

    return jsonify({
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "is_admin": user.is_admin,
        "created_at": user.created_at,
        "last_login": user.last_login
    }), 200


# Admin-only API route
@app.route('/api/users', methods=['GET'])
@jwt_required()
def get_users():
    claims = get_jwt()
    if not claims.get("is_admin", False):
        return jsonify({"error": "Admin privileges required"}), 403

    users = User.query.all()
    users_list = []

    for user in users:
        users_list.append({
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_admin": user.is_admin,
            "created_at": str(user.created_at),
            "last_login": str(user.last_login) if user.last_login else None
        })

    return jsonify(users_list), 200


# Web routes for templates
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    error = None

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        user = User.query.filter_by(username=username).first()

        if not user or not user.check_password(password):
            error = "Invalid username or password"
        else:
            # Update last login time
            user.last_login = datetime.datetime.utcnow()
            db.session.commit()

            # Store user in session
            session['user_id'] = user.id

            return redirect(url_for('dashboard'))

    return render_template('login.html', error=error)


@app.route('/register', methods=['GET', 'POST'])
def register_page():
    error = None

    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')

        # Validate input
        if User.query.filter_by(username=username).first():
            error = "Username already exists"
        elif User.query.filter_by(email=email).first():
            error = "Email already exists"
        elif password != confirm_password:
            error = "Passwords do not match"
        else:
            # Create new user
            # Get the next customer ID (ADD THIS CODE)
            result = db.session.execute(db.text("SELECT nextval('user_customer_id_seq')"))
            next_id = result.scalar()
            customer_id = f"smarteft_user_{next_id}"

            user = User(
                username=username,
                email=email,
                customer_id=customer_id  # ADD THIS LINE HERE
            )
            user.set_password(password)

            db.session.add(user)
            db.session.commit()

            return redirect(url_for('login_page'))

    return render_template('register.html', error=error)


@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('home'))


@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user = User.query.get(session['user_id'])

    if not user:
        session.pop('user_id', None)
        return redirect(url_for('login_page'))

    # Get current subscription
    current_subscription = Subscription.query.filter_by(user_id=user.id).order_by(
        Subscription.expiry_date.desc()).first()

    # Get broker connections for the current user
    broker_connections = Broker.query.filter_by(user_id=user.id).all()

    # Admin-specific statistics
    user_count = 0
    active_subscriptions = 0
    broker_count = 0

    if user.is_admin:
        user_count = User.query.count()
        active_subscriptions = Subscription.query.filter(Subscription.expiry_date > datetime.datetime.now()).count()
        broker_count = Broker.query.count()

    return render_template(
        'dashboard.html',
        user=user,
        current_subscription=current_subscription,
        broker_connections=broker_connections,
        now=datetime.datetime.now(),
        user_count=user_count,
        active_subscriptions=active_subscriptions,
        broker_count=broker_count
    )


# Broker management routes
@app.route('/brokers')
def brokers():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user = User.query.get(session['user_id'])
    if not user:
        session.pop('user_id', None)
        return redirect(url_for('login_page'))

    # Get broker connections for the current user
    brokers = Broker.query.filter_by(user_id=user.id).all()

    return render_template('brokers.html', brokers=brokers)


@app.route('/brokers/add', methods=['POST'])
def add_broker():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    error = None
    user_id = session['user_id']

    # Extract broker details from form
    broker_name = request.form.get('broker_name')
    user_id_broker = request.form.get('user_id_broker')
    is_master = 'is_master' in request.form
    copy = 'copy' in request.form
    copy_multiplier = int(request.form.get('copy_multiplier', 1))

    # Check if another master account exists when trying to add a master account
    if is_master:
        existing_master = Broker.query.filter_by(user_id=user_id, is_master=True).first()
        if existing_master:
            error = "You already have a master account. Only one master account is allowed."
            brokers = Broker.query.filter_by(user_id=user_id).all()
            return render_template('brokers.html', brokers=brokers, error=error)

    # Create new broker connection
    broker = Broker(
        user_id=user_id,
        broker_name=broker_name,
        user_id_broker=user_id_broker,
        is_master=is_master,
        copy=copy,
        copy_multiplier=copy_multiplier
    )

    # Handle broker-specific fields
    if broker_name == 'FINVASIA':
        broker.password = request.form.get('password')
        broker.totp_secret = request.form.get('totp_secret')
        broker.vendor_code = request.form.get('vendor_code')
        broker.api_secret = request.form.get('api_secret')
        broker.imei = request.form.get('imei')
    elif broker_name in ['ZERODHA', 'UPSTOX', 'MSTOCK']:
        broker.api_key = request.form.get('api_key')
        broker.api_secret = request.form.get('api_secret')
        if broker_name in ['ZERODHA', 'UPSTOX']:
            broker.access_token = request.form.get('access_token')
    elif broker_name == 'DHAN':
        broker.access_token = request.form.get('access_token')

    db.session.add(broker)
    db.session.commit()

    return redirect(url_for('brokers'))


@app.route('/brokers/edit/<int:broker_id>', methods=['GET', 'POST'])
def edit_broker(broker_id):
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user_id = session['user_id']

    # Simple defensive query
    broker = Broker.query.get(broker_id)

    if not broker:
        flash("Broker not found.")
        return redirect(url_for('brokers'))

    if broker.user_id != user_id:
        flash("You don't have permission to edit this broker.")
        return redirect(url_for('brokers'))

    if request.method == 'POST':
        error = None

        # Check if another master account exists when trying to make this account a master
        is_master = 'is_master' in request.form
        if is_master and not broker.is_master:
            existing_master = Broker.query.filter_by(user_id=user_id, is_master=True).first()
            if existing_master and existing_master.id != broker.id:
                error = "You already have a master account. Only one master account is allowed."
                return render_template('edit_broker.html', broker=broker, error=error)

        # Update broker details
        broker.user_id_broker = request.form.get('user_id_broker')
        broker.is_master = is_master
        broker.copy = 'copy' in request.form
        broker.copy_multiplier = int(request.form.get('copy_multiplier', 1))

        # Handle broker-specific fields
        if broker.broker_name == 'FINVASIA':
            password = request.form.get('password')
            if password:  # Only update password if provided
                broker.password = password
            broker.totp_secret = request.form.get('totp_secret')
            broker.vendor_code = request.form.get('vendor_code')
            broker.api_secret = request.form.get('api_secret')
            broker.imei = request.form.get('imei')
        elif broker.broker_name in ['ZERODHA', 'UPSTOX', 'MSTOCK']:
            broker.api_key = request.form.get('api_key')
            broker.api_secret = request.form.get('api_secret')
            if broker.broker_name in ['ZERODHA', 'UPSTOX']:
                broker.access_token = request.form.get('access_token')
        elif broker.broker_name == 'DHAN':
            broker.access_token = request.form.get('access_token')

        db.session.commit()

        # Export brokers to CSV
        all_brokers = Broker.query.all()
        export_brokers_to_csv(all_brokers)

        return redirect(url_for('brokers'))

    return render_template('edit_broker.html', broker=broker)


@app.route('/brokers/delete/<int:broker_id>')
def delete_broker(broker_id):
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user_id = session['user_id']
    broker = Broker.query.filter_by(id=broker_id, user_id=user_id).first_or_404()

    db.session.delete(broker)
    db.session.commit()

    return redirect(url_for('brokers'))


# ADD THE NEW ROUTE RIGHT HERE, after the delete_broker function
@app.route('/brokers/export', methods=['POST'])
def export_brokers():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    all_brokers = Broker.query.all()
    export_brokers_to_csv(all_brokers)

    return redirect(url_for('brokers'))


# Add this to your imports
from datetime import timedelta


# Subscription routes
@app.route('/subscription')
def subscription():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user_id = session['user_id']

    # Get current subscription
    current_subscription = Subscription.query.filter_by(user_id=user_id).order_by(
        Subscription.expiry_date.desc()).first()

    # Get subscription history
    subscription_history = Subscription.query.filter_by(user_id=user_id).order_by(Subscription.created_at.desc()).all()

    # Get available plans
    plans = SubscriptionPlan.query.filter_by(is_active=True).all()

    return render_template(
        'subscription.html',
        current_subscription=current_subscription,
        subscription_history=subscription_history,
        plans=plans,
        now=datetime.datetime.now()
    )



@app.route('/subscription/purchase', methods=['POST'])
def purchase_subscription():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user_id = session['user_id']
    plan_id = request.form.get('plan_id')

    plan = SubscriptionPlan.query.get_or_404(plan_id)

    # Get current subscription if exists
    current_subscription = Subscription.query.filter_by(user_id=user_id).order_by(
        Subscription.expiry_date.desc()).first()

    return render_template('purchase_subscription.html', plan=plan, current_subscription=current_subscription)


@app.route('/subscription/confirm', methods=['POST'])
def confirm_subscription():
    if 'user_id' not in session:
        return redirect(url_for('login_page'))

    user_id = session['user_id']
    plan_id = request.form.get('plan_id')
    payment_method = request.form.get('payment_method')

    plan = SubscriptionPlan.query.get_or_404(plan_id)

    # Calculate expiry date
    start_date = datetime.datetime.now()
    expiry_date = start_date + timedelta(days=plan.duration_days)

    # Create subscription record (keep this for history)
    subscription = Subscription(
        user_id=user_id,
        plan_name=plan.name,
        start_date=start_date,
        expiry_date=expiry_date,
        payment_status='Paid',
        payment_method=payment_method,
        payment_id=f"SIM-{int(datetime.datetime.now().timestamp())}",
        amount=plan.price
    )
    db.session.add(subscription)

    # Update all broker records for this user
    user_brokers = Broker.query.filter_by(user_id=user_id).all()
    for broker in user_brokers:
        broker.subscription_expiry = expiry_date
        broker.subscription_status = 'Active'

    db.session.commit()

    # Export updated broker data to CSV
    all_brokers = Broker.query.all()
    export_brokers_to_csv(all_brokers)

    flash(f"You have successfully purchased the {plan.name} plan!", "success")
    return redirect(url_for('subscription'))


# Add this after all your routes
@app.errorhandler(Exception)
def handle_error(e):
    if app.debug:
        # In debug mode, let Flask's debugger handle the error
        raise e
    # In production, show a user-friendly error page
    error_message = str(e)
    return render_template('error.html', error=error_message), 500


def create_default_plans():
    """Create default subscription plans if none exist."""
    if SubscriptionPlan.query.count() == 0:
        plans = [
            SubscriptionPlan(
                name="Basic Monthly",
                description="Basic ETF trading features for a single broker",
                duration_days=30,
                price=499.00,
                features="Single broker support\nDaily ETF recommendations\nBasic reports",
                is_active=True
            ),
            SubscriptionPlan(
                name="Standard Quarterly",
                description="Standard features with multiple brokers for 3 months",
                duration_days=90,
                price=1299.00,
                features="Multi-broker support\nDaily ETF recommendations\nDetailed reports\nPriority support",
                is_active=True
            ),
            SubscriptionPlan(
                name="Premium Annual",
                description="Premium features with all benefits for 12 months",
                duration_days=365,
                price=4999.00,
                features="Full broker support\nPremium ETF recommendations\nComprehensive reports\n24/7 Priority support\nStrategy customization",
                is_active=True
            )
        ]

        for plan in plans:
            db.session.add(plan)

        db.session.commit()
        print(f"Created {len(plans)} default subscription plans")


# Admin middleware to check admin permissions
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))

        user = User.query.get(session['user_id'])
        if not user or not user.is_admin:
            flash("Admin privileges required", "error")
            return redirect(url_for('dashboard'))

        return f(*args, **kwargs)

    return decorated_function


# Admin Users
@app.route('/admin/users')
@admin_required
def admin_users():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search = request.args.get('search', '')

    query = User.query

    if search:
        query = query.filter(
            (User.username.ilike(f'%{search}%')) |
            (User.email.ilike(f'%{search}%'))
        )

    pagination = query.order_by(User.id).paginate(page=page, per_page=per_page, error_out=False)
    users = pagination.items

    return render_template(
        'admin/users.html',
        users=users,
        page=page,
        total_pages=pagination.pages or 1,
        search=search,
        active_page='users'
    )


@app.route('/admin/users/edit/<int:user_id>', methods=['GET', 'POST'])
@admin_required
def admin_edit_user(user_id):
    user = User.query.get_or_404(user_id)

    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        customer_id = request.form.get('customer_id')
        is_admin = 'is_admin' in request.form

        # Check if username already exists (for another user)
        existing_user = User.query.filter(User.username == username, User.id != user_id).first()
        if existing_user:
            flash("Username already taken", "error")
            return render_template('admin/edit_user.html', user=user, active_page='users')

        # Check if email already exists (for another user)
        existing_email = User.query.filter(User.email == email, User.id != user_id).first()
        if existing_email:
            flash("Email already taken", "error")
            return render_template('admin/edit_user.html', user=user, active_page='users')

        # Update user
        user.username = username
        user.email = email
        user.customer_id = customer_id
        user.is_admin = is_admin

        # Update password if provided
        password = request.form.get('password')
        if password and len(password) >= 8:
            user.set_password(password)

        db.session.commit()
        flash("User updated successfully", "success")
        return redirect(url_for('admin_users'))

    return render_template('admin/edit_user.html', user=user, active_page='users')


# Admin Plans
@app.route('/admin/plans')
@admin_required
def admin_plans():
    plans = SubscriptionPlan.query.order_by(SubscriptionPlan.id).all()

    return render_template(
        'admin/plans.html',
        plans=plans,
        active_page='plans'
    )


# Admin Subscriptions
@app.route('/admin/subscriptions')
@admin_required
def admin_subscriptions():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search = request.args.get('search', '')
    status = request.args.get('status', '')

    # Query broker table for subscription data, joined with user table
    query = Broker.query.join(User, User.id == Broker.user_id)

    if search:
        query = query.filter(User.username.ilike(f'%{search}%'))

    if status == 'active':
        query = query.filter(Broker.subscription_status == 'Active')
    elif status == 'inactive':
        query = query.filter(Broker.subscription_status == 'Inactive')

    pagination = query.order_by(Broker.user_id).paginate(page=page, per_page=per_page, error_out=False)
    subscriptions = pagination.items

    return render_template(
        'admin/subscriptions.html',
        subscriptions=subscriptions,
        page=page,
        total_pages=pagination.pages or 1,
        search=search,
        status=status,
        now=datetime.datetime.now(),
        active_page='subscriptions'
    )


# Admin Broker Connections
@app.route('/admin/trading_accounts')
@admin_required
def admin_trading_accounts():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search = request.args.get('search', '')
    broker_filter = request.args.get('broker', '')

    query = Broker.query.join(User, User.id == Broker.user_id)

    if search:
        query = query.filter(
            (User.username.ilike(f'%{search}%')) |
            (Broker.user_id_broker.ilike(f'%{search}%'))
        )

    if broker_filter:
        query = query.filter(Broker.broker_name == broker_filter)

    pagination = query.order_by(Broker.id).paginate(page=page, per_page=per_page, error_out=False)
    brokers = pagination.items

    return render_template(
        'admin/brokers.html',
        brokers=brokers,
        page=page,
        total_pages=pagination.pages or 1,
        search=search,
        broker_filter=broker_filter,
        active_page='trading_accounts',

    )


# Admin Exports
@app.route('/admin/exports')
@admin_required
def admin_exports():
    return render_template(
        'admin/exports.html',
        active_page='exports'
    )


# Export Brokers
@app.route('/admin/exports/brokers', methods=['POST'])
@admin_required
def admin_export_brokers():
    export_path = request.form.get('export_path', '')

    all_brokers = Broker.query.all()

    try:
        if export_path:
            export_brokers_to_csv(all_brokers, export_path)
        else:
            export_brokers_to_csv(all_brokers)

        flash("Broker connections exported successfully!", "success")
    except Exception as e:
        flash(f"Failed to export broker connections: {str(e)}", "error")

    return redirect(url_for('admin_exports'))


# Export Users
@app.route('/admin/exports/users', methods=['POST'])
@admin_required
def admin_export_users():
    try:
        users = User.query.all()

        # Create a DataFrame
        data = []
        for user in users:
            data.append({
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'is_admin': user.is_admin,
                'created_at': user.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'last_login': user.last_login.strftime('%Y-%m-%d %H:%M:%S') if user.last_login else ''
            })

        df = pd.DataFrame(data)

        # Ensure the directory exists
        os.makedirs('data', exist_ok=True)

        # Save to CSV
        export_file = f'data/users_export_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(export_file, index=False)

        flash(f"Users exported successfully to {export_file}!", "success")
    except Exception as e:
        flash(f"Failed to export users: {str(e)}", "error")

    return redirect(url_for('admin_exports'))


# Export Subscriptions
@app.route('/admin/exports/subscriptions', methods=['POST'])
@admin_required
def admin_export_subscriptions():
    try:
        subscriptions = Subscription.query.all()

        # Create a DataFrame
        data = []
        for sub in subscriptions:
            user = User.query.get(sub.user_id)
            data.append({
                'id': sub.id,
                'user_id': sub.user_id,
                'username': user.username if user else '',
                'plan_name': sub.plan_name,
                'start_date': sub.start_date.strftime('%Y-%m-%d'),
                'expiry_date': sub.expiry_date.strftime('%Y-%m-%d'),
                'payment_status': sub.payment_status,
                'payment_method': sub.payment_method,
                'amount': sub.amount,
                'created_at': sub.created_at.strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(data)

        # Ensure the directory exists
        os.makedirs('data', exist_ok=True)

        # Save to CSV
        export_file = f'data/subscriptions_export_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(export_file, index=False)

        flash(f"Subscriptions exported successfully to {export_file}!", "success")
    except Exception as e:
        flash(f"Failed to export subscriptions: {str(e)}", "error")

    return redirect(url_for('admin_exports'))


# Basic database backup
@app.route('/admin/exports/backup', methods=['POST'])
@admin_required
def admin_backup_database():
    try:
        # Simple backup by dumping tables to CSV
        os.makedirs('backups', exist_ok=True)
        backup_dir = f'backups/backup_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}'
        os.makedirs(backup_dir, exist_ok=True)

        # Back up users
        users = User.query.all()
        users_data = []
        for user in users:
            users_data.append({
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'password_hash': user.password_hash,
                'is_admin': user.is_admin,
                'created_at': user.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'last_login': user.last_login.strftime('%Y-%m-%d %H:%M:%S') if user.last_login else ''
            })
        pd.DataFrame(users_data).to_csv(f'{backup_dir}/users.csv', index=False)

        # Back up brokers
        brokers = Broker.query.all()
        brokers_data = []
        for broker in brokers:
            brokers_data.append({
                'id': broker.id,
                'user_id': broker.user_id,
                'broker_name': broker.broker_name,
                'user_id_broker': broker.user_id_broker,
                'password': broker.password,
                'totp_secret': broker.totp_secret,
                'api_key': broker.api_key,
                'api_secret': broker.api_secret,
                'vendor_code': broker.vendor_code,
                'imei': broker.imei,
                'access_token': broker.access_token,
                'is_master': broker.is_master,
                'copy_multiplier': broker.copy_multiplier,
                'copy': broker.copy,
                'created_at': broker.created_at.strftime('%Y-%m-%d %H:%M:%S') if hasattr(broker, 'created_at') else '',
                'last_updated': broker.last_updated.strftime('%Y-%m-%d %H:%M:%S') if hasattr(broker,
                                                                                             'last_updated') else ''
            })
        pd.DataFrame(brokers_data).to_csv(f'{backup_dir}/brokers.csv', index=False)

        # Back up subscriptions
        subscriptions = Subscription.query.all()
        subs_data = []
        for sub in subscriptions:
            subs_data.append({
                'id': sub.id,
                'user_id': sub.user_id,
                'plan_name': sub.plan_name,
                'start_date': sub.start_date.strftime('%Y-%m-%d'),
                'expiry_date': sub.expiry_date.strftime('%Y-%m-%d'),
                'payment_status': sub.payment_status,
                'payment_method': sub.payment_method,
                'payment_id': sub.payment_id,
                'amount': sub.amount,
                'created_at': sub.created_at.strftime('%Y-%m-%d %H:%M:%S')
            })
        pd.DataFrame(subs_data).to_csv(f'{backup_dir}/subscriptions.csv', index=False)

        # Back up plans
        plans = SubscriptionPlan.query.all()
        plans_data = []
        for plan in plans:
            plans_data.append({
                'id': plan.id,
                'name': plan.name,
                'description': plan.description,
                'duration_days': plan.duration_days,
                'price': plan.price,
                'features': plan.features,
                'is_active': plan.is_active,
                'created_at': plan.created_at.strftime('%Y-%m-%d %H:%M:%S')
            })
        pd.DataFrame(plans_data).to_csv(f'{backup_dir}/plans.csv', index=False)

        flash(f"Database backed up successfully to {backup_dir}!", "success")
    except Exception as e:
        flash(f"Failed to backup database: {str(e)}", "error")

    return redirect(url_for('admin_exports'))


@app.route('/admin/subscriptions/edit/<int:broker_id>', methods=['GET', 'POST'])
@admin_required
def edit_subscription(broker_id):
    broker = Broker.query.get_or_404(broker_id)
    user = User.query.get(broker.user_id)

    if request.method == 'POST':
        # Update subscription data
        expiry_date_str = request.form.get('expiry_date')
        status = request.form.get('status')

        try:
            expiry_date = datetime.datetime.strptime(expiry_date_str, '%Y-%m-%d')
            broker.subscription_expiry = expiry_date
            broker.subscription_status = status
            db.session.commit()

            # Export to CSV to update trading system
            all_brokers = Broker.query.all()
            export_brokers_to_csv(all_brokers)

            flash('Subscription updated successfully', 'success')
            return redirect(url_for('admin_subscriptions'))
        except Exception as e:
            flash(f'Error updating subscription: {str(e)}', 'error')

    return render_template(
        'admin/edit_subscription.html',
        broker=broker,
        user=user,
        active_page='subscriptions'
    )


# Create admin command
@app.cli.command("create-admin")
def create_admin():
    """Create an admin user."""
    username = input("Enter admin username: ")
    email = input("Enter admin email: ")
    password = input("Enter admin password: ")

    # Check if user already exists
    if User.query.filter_by(username=username).first():
        print("Username already exists!")
        return

    if User.query.filter_by(email=email).first():
        print("Email already exists!")
        return

    user = User(
        username=username,
        email=email,
        is_admin=True
    )
    user.set_password(password)

    db.session.add(user)
    db.session.commit()

    print(f"Admin user {username} created successfully!")


@app.cli.command("check-subscriptions")
def check_subscriptions():
    """Check and update expired subscriptions."""
    with app.app_context():
        now = datetime.datetime.now()
        expired_brokers = Broker.query.filter(
            Broker.subscription_expiry < now,
            Broker.subscription_status == 'Active'
        ).all()

        if expired_brokers:
            for broker in expired_brokers:
                broker.subscription_status = 'Inactive'
                print(f"Marking broker {broker.id} ({broker.user_id_broker}) as inactive")

            db.session.commit()
            print(f"Updated {len(expired_brokers)} expired subscriptions")

            # Export updated CSV
            all_brokers = Broker.query.all()
            export_brokers_to_csv(all_brokers)
        else:
            print("No expired subscriptions found")


# Run the app
# Run the app
if __name__ == '__main__':
    try:
        with app.app_context():
            # Get existing tables from the database
            inspector = db.inspect(db.engine)
            existing_tables = inspector.get_table_names()

            # Create specific missing tables without dropping existing ones
            print("Checking for missing tables...")

            # Create new models as required
            if 'subscription_plan' not in existing_tables:
                print("Creating subscription_plan table...")
                db.metadata.tables['subscription_plan'].create(db.engine)
                print("subscription_plan table created.")

            if 'subscription' not in existing_tables:
                print("Creating subscription table...")
                db.metadata.tables['subscription'].create(db.engine)
                print("subscription table created.")

            # Check if customer_id column exists in User table
            print("Checking for customer_id column in User table...")
            user_columns = [col['name'] for col in inspector.get_columns('user')]
            if 'customer_id' not in user_columns:
                print("Adding customer_id column to User table...")
                # Use raw SQL to add the column with a default value since SQLAlchemy doesn't handle this well
                db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN customer_id VARCHAR(36) UNIQUE'))
                db.session.commit()

                # Now generate and set customer_id for all existing users
                print("Setting customer_id for existing users...")
                users = User.query.all()
                for user in users:
                    user.customer_id = str(uuid.uuid4())
                db.session.commit()
                print("customer_id column added and populated for all users.")

            # Check for admin user
            admin = User.query.filter_by(is_admin=True).first()
            if not admin:
                print("No admin user found. Creating default admin...")
                admin_user = User(
                    username="admin",
                    email="admin@example.com",
                    is_admin=True,
                    customer_id=str(uuid.uuid4())
                )
                admin_user.set_password("adminpassword")
                db.session.add(admin_user)
                db.session.commit()
                print("Default admin user created.")

            # Create default subscription plans
            try:
                create_default_plans()
            except Exception as plan_error:
                print(f"Error creating default plans: {plan_error}")

        app.run(debug=True)
    except Exception as e:
        print(f"Error during startup: {e}")
