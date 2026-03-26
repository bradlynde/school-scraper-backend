// @ts-nocheck
"use client";

import React, { useState } from "react";
import Image from "next/image";
import { useAuth } from "../contexts/AuthContext";

export default function LoginForm() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);

    try {
      const success = await login(username, password);
      if (!success) {
        setError("Invalid username or password");
        setLoading(false);
      }
    } catch (err: any) {
      console.error("Login form error:", err);
      let errorMessage = err?.message || "An error occurred during login.";

      if (errorMessage.startsWith("CONNECTION_FAILED:")) {
        const attemptedUrl = errorMessage.replace("CONNECTION_FAILED:", "");
        const origin = typeof window !== "undefined" ? window.location.origin : "unknown";
        errorMessage = `Cannot connect to the auth server.\n\nAttempted: ${attemptedUrl}\nYour origin: ${origin}\n\nFix: 1) Set NEXT_PUBLIC_AUTH_API_URL in Vercel. 2) Add "${origin}" to CORS_ORIGINS on the auth service (Railway).`;
      } else if (errorMessage.includes("timed out")) {
        errorMessage = "Connection timed out. The server may be slow or unreachable.";
      }

      setError(errorMessage);
      setLoading(false);
    }
  };

  const inputStyle: React.CSSProperties = {
    width: "100%",
    padding: "12px 16px",
    background: "#f3f4f6",
    border: "1px solid #e5e7eb",
    borderRadius: 10,
    fontSize: 14,
    color: "#111827",
    outline: "none",
    boxSizing: "border-box",
    transition: "border-color 0.2s, box-shadow 0.2s",
  };

  const labelStyle: React.CSSProperties = {
    display: "block",
    fontSize: 12,
    fontWeight: 600,
    color: "#6b7280",
    textTransform: "uppercase",
    letterSpacing: "0.06em",
    marginBottom: 8,
  };

  return (
    <div style={{
      minHeight: "100dvh",
      display: "flex",
      flexDirection: "column",
      background: "#f7f8fa",
      fontFamily: "Inter, system-ui, sans-serif",
    }}>
      {/* Logo - top left */}
      <div style={{ padding: "28px 32px" }}>
        <Image
          src="/npsa-logo.png"
          alt="NPSA"
          width={160}
          height={48}
          style={{ height: "auto", objectFit: "contain" }}
          priority
        />
      </div>

      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", marginTop: -60 }}>
        <div style={{ width: "100%", maxWidth: 400, padding: "0 16px" }}>
          <div style={{
            background: "#ffffff",
            borderRadius: 14,
            border: "1px solid #e5e7eb",
            boxShadow: "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)",
            padding: "36px 32px",
          }}>
          <div style={{ marginBottom: 28, textAlign: "center" }}>
            <h2 style={{
              margin: "0 0 4px",
              fontSize: 22,
              fontWeight: 700,
              color: "#111827",
              letterSpacing: "-0.02em",
            }}>
              Welcome back
            </h2>
            <p style={{ margin: 0, fontSize: 13, color: "#9ca3af" }}>
              Sign in to continue
            </p>
          </div>

          <form onSubmit={handleSubmit}>
            <div style={{ marginBottom: 20 }}>
              <label style={labelStyle}>Username</label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                placeholder="Enter your username"
                autoComplete="username"
                style={inputStyle}
                onFocus={(e) => { e.target.style.borderColor = "#1e3a5f"; e.target.style.boxShadow = "0 0 0 3px rgba(30,58,95,0.1)"; }}
                onBlur={(e) => { e.target.style.borderColor = "#e5e7eb"; e.target.style.boxShadow = "none"; }}
              />
            </div>

            <div style={{ marginBottom: 20 }}>
              <label style={labelStyle}>Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                placeholder="Enter your password"
                autoComplete="current-password"
                style={inputStyle}
                onFocus={(e) => { e.target.style.borderColor = "#1e3a5f"; e.target.style.boxShadow = "0 0 0 3px rgba(30,58,95,0.1)"; }}
                onBlur={(e) => { e.target.style.borderColor = "#e5e7eb"; e.target.style.boxShadow = "none"; }}
              />
            </div>

            {error && (
              <div style={{
                padding: 12,
                background: "#fef2f2",
                border: "1px solid #fecaca",
                borderRadius: 10,
                marginBottom: 18,
              }}>
                <p style={{ color: "#dc2626", fontSize: 13, fontWeight: 500, margin: 0, whiteSpace: "pre-line" }}>{error}</p>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              style={{
                width: "100%",
                padding: "14px 24px",
                borderRadius: 10,
                fontSize: 14,
                fontWeight: 600,
                color: "#fff",
                background: loading ? "#9ca3af" : "#1e3a5f",
                border: "none",
                cursor: loading ? "not-allowed" : "pointer",
                boxShadow: loading ? "none" : "0 2px 8px rgba(30,58,95,0.25)",
                transition: "all 0.2s",
                letterSpacing: "0.01em",
              }}
            >
              {loading ? "Signing in..." : "Sign In"}
            </button>
          </form>
          </div>
        </div>
      </div>
    </div>
  );
}
