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
    background: "rgba(255,255,255,0.06)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: 10,
    fontSize: 14,
    color: "#e8eaf0",
    outline: "none",
    boxSizing: "border-box",
    transition: "border-color 0.15s",
  };

  const labelStyle: React.CSSProperties = {
    display: "block",
    fontSize: 12,
    fontWeight: 500,
    color: "rgba(255,255,255,0.5)",
    textTransform: "uppercase",
    letterSpacing: "0.06em",
    marginBottom: 8,
  };

  return (
    <div style={{
      minHeight: "100vh",
      display: "flex",
      flexDirection: "column",
      background: "linear-gradient(135deg, #0a1628 0%, #0f1f38 50%, #162d4d 100%)",
      fontFamily: "Inter, system-ui, sans-serif",
    }}>
      {/* Logo - top left */}
      <div style={{ position: "absolute", top: 0, left: 0, padding: "24px 28px", minHeight: 72, display: "flex", alignItems: "center" }}>
        <Image
          src="/npsa-logo.png"
          alt="NPSA"
          width={140}
          height={42}
          style={{ height: "auto", objectFit: "contain", filter: "brightness(0) invert(1)", opacity: 0.8 }}
          priority
        />
      </div>

      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ width: "100%", maxWidth: 380, padding: "0 16px" }}>
          <div style={{
            background: "rgba(255,255,255,0.04)",
            borderRadius: 16,
            border: "1px solid rgba(255,255,255,0.08)",
            backdropFilter: "blur(20px)",
            padding: "36px 32px",
          }}>
            <h2 style={{
              margin: "0 0 4px",
              fontSize: 20,
              fontWeight: 700,
              color: "#fff",
              letterSpacing: "-0.02em",
            }}>
              Welcome back
            </h2>
            <p style={{ margin: "0 0 28px", fontSize: 13, color: "rgba(255,255,255,0.4)" }}>
              Sign in to continue
            </p>

            <form onSubmit={handleSubmit}>
              <div style={{ marginBottom: 18 }}>
                <label style={labelStyle}>Username</label>
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                  placeholder="Enter your username"
                  autoComplete="username"
                  style={inputStyle}
                />
              </div>

              <div style={{ marginBottom: 18 }}>
                <label style={labelStyle}>Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  placeholder="Enter your password"
                  autoComplete="current-password"
                  style={inputStyle}
                />
              </div>

              {error && (
                <div style={{
                  padding: 12,
                  background: "rgba(220, 38, 38, 0.15)",
                  border: "1px solid rgba(220, 38, 38, 0.3)",
                  borderRadius: 10,
                  marginBottom: 18,
                }}>
                  <p style={{ color: "#fca5a5", fontSize: 13, fontWeight: 500, margin: 0, whiteSpace: "pre-line" }}>{error}</p>
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
                  background: loading ? "rgba(255,255,255,0.1)" : "#1e3a5f",
                  border: "none",
                  cursor: loading ? "not-allowed" : "pointer",
                  boxShadow: loading ? "none" : "0 4px 12px rgba(30,58,95,0.4)",
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
