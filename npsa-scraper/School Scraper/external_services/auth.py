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

# JWT Secret Key (from environment variable, fallback for development)
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# User credentials (passwords are hashed with bcrypt)
# In production, these should be stored in a database
# Pre-hash passwords on module load
USERS = {
    "Koen": {
        "password_hash": bcrypt.hashpw("admin".encode('utf-8'), bcrypt.gensalt()),
        "username": "Koen"
    },
    "Brad": {
        "password_hash": bcrypt.hashpw("user1".encode('utf-8'), bcrypt.gensalt()),
        "username": "Brad"
    },
    "Stuart": {
        "password_hash": bcrypt.hashpw("user2".encode('utf-8'), bcrypt.gensalt()),
        "username": "Stuart"
    }
}


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
