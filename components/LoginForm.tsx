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
  const { login, authApiUrl } = useAuth();

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
    padding: "14px 18px",
    background: "#fff",
    border: "1px solid #d1d5db",
    borderRadius: 12,
    fontSize: 15,
    color: "#111827",
    outline: "none",
    boxSizing: "border-box",
  };

  const labelStyle: React.CSSProperties = {
    display: "block",
    fontSize: 13,
    fontWeight: 600,
    color: "#374151",
    textTransform: "uppercase" as const,
    letterSpacing: "0.05em",
    marginBottom: 10,
  };

  return (
    <div style={{
      minHeight: "100vh",
      display: "flex",
      flexDirection: "column",
      background: "#fff",
      fontFamily: "Inter, system-ui, sans-serif",
    }}>
      {/* Logo - top left */}
      <div style={{ position: "absolute", top: 0, left: 0, padding: 16, minHeight: 72, display: "flex", alignItems: "center" }}>
        <Image
          src="/npsa-logo.png"
          alt="NPSA"
          width={160}
          height={48}
          style={{ height: "auto", objectFit: "contain" }}
          priority
        />
      </div>

      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ width: "100%", maxWidth: 420, padding: "0 16px" }}>
          <div style={{
            background: "#fff",
            borderRadius: 16,
            border: "1px solid #e5e7eb",
            boxShadow: "0 4px 24px rgba(0,0,0,0.08)",
            padding: "40px 36px",
          }}>
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
                />
              </div>

              {error && (
                <div style={{
                  padding: 14,
                  background: "#fef2f2",
                  border: "1px solid #fecaca",
                  borderRadius: 12,
                  marginBottom: 20,
                }}>
                  <p style={{ color: "#b91c1c", fontSize: 14, fontWeight: 500, margin: 0, whiteSpace: "pre-line" }}>{error}</p>
                </div>
              )}

              <button
                type="submit"
                disabled={loading}
                style={{
                  width: "100%",
                  padding: "16px 24px",
                  borderRadius: 12,
                  fontSize: 16,
                  fontWeight: 600,
                  color: "#fff",
                  background: loading ? "#9ca3af" : "#1e3a5f",
                  border: "none",
                  cursor: loading ? "not-allowed" : "pointer",
                  boxShadow: loading ? "none" : "0 4px 12px rgba(30,58,95,0.25)",
                  transition: "all 0.2s",
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
