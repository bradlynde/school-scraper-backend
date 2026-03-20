"""
Auth Service - Centralized authentication for NPSA tools.
Handles login, JWT issuance. Backends validate tokens with shared JWT_SECRET.
"""

import os
from datetime import datetime, timedelta

import bcrypt
import jwt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import get_user_by_username, init_db

# Config
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise ValueError("JWT_SECRET environment variable must be set")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

app = FastAPI(title="NPSA Auth Service")

# CORS - allow frontend. Set CORS_ORIGINS in Railway (comma-separated, e.g. https://yourapp.vercel.app)
_cors = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    status: str
    token: str
    username: str


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "auth"}


@app.post("/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    user = get_user_by_username(req.username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    stored_hash = user["password_hash"]
    if isinstance(stored_hash, str):
        stored_hash = stored_hash.encode("utf-8")
    elif not isinstance(stored_hash, bytes):
        # PostgreSQL BYTEA returns memoryview; bcrypt needs bytes
        stored_hash = bytes(stored_hash)
    if not bcrypt.checkpw(req.password.encode("utf-8"), stored_hash):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    payload = {
        "username": req.username,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
        "iat": datetime.utcnow(),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return LoginResponse(status="success", token=token, username=req.username)
