"""
Authentication module for School Scraper API
Handles user authentication, JWT token generation, and password hashing
"""

import jwt
import bcrypt
import os
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify

# JWT Secret Key (from environment variable, REQUIRED in production)
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise ValueError("JWT_SECRET environment variable must be set. This is required for security.")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

def _load_users() -> dict:
    """Load user credentials from AUTH_USERS env var.
    Format: 'Username:password,Username2:password2'
    Passwords are hashed with bcrypt on load.
    """
    raw = os.getenv("AUTH_USERS", "")
    if not raw:
        from school_run_log import log_warn
        log_warn("AUTH_USERS not set - no users can log in")
        return {}
    users = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        username, password = entry.split(":", 1)
        username = username.strip()
        password = password.strip()
        if username and password:
            users[username] = {
                "password_hash": bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()),
                "username": username,
            }
    if users:
        from school_run_log import log_warn
        log_warn(f"Auth: {len(users)} user(s) loaded")
    else:
        from school_run_log import log_warn
        log_warn("AUTH_USERS set but no valid entries parsed")
    return users


USERS = _load_users()


def verify_password(username: str, password: str) -> bool:
    """Verify user password against stored hash"""
    if username not in USERS:
        return False

    stored_hash = USERS[username]["password_hash"]
    # Handle both bytes and string formats
    if isinstance(stored_hash, str):
        stored_hash = stored_hash.encode('utf-8')

    return bcrypt.checkpw(password.encode('utf-8'), stored_hash)


def generate_token(username: str) -> str:
    """Generate JWT token for authenticated user"""
    payload = {
        "username": username,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(token: str) -> dict:
    """Verify JWT token and return payload if valid"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def require_auth(f):
    """Decorator to require authentication for API endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Skip auth for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return f(*args, **kwargs)

        # Get token from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return jsonify({
                "status": "error",
                "error": "Authorization header missing"
            }), 401

        # Extract token from "Bearer <token>" format
        try:
            token = auth_header.split(" ")[1] if " " in auth_header else auth_header
        except IndexError:
            return jsonify({
                "status": "error",
                "error": "Invalid authorization header format"
            }), 401

        # Verify token
        payload = verify_token(token)
        if not payload:
            return jsonify({
                "status": "error",
                "error": "Invalid or expired token"
            }), 401

        # Add user info to request context
        request.current_user = payload.get("username")

        return f(*args, **kwargs)

    return decorated_function
