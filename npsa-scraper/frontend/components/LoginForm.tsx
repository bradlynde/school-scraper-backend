"use client";

import React, { useState } from "react";
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
      // If successful, login function will update auth state and component will unmount
    } catch (err: any) {
      console.error("Login form error:", err);
      let errorMessage = err?.message || "An error occurred during login. Please check your connection and try again.";
      
      // Connection failed - include URL and actionable steps
      if (errorMessage.startsWith("CONNECTION_FAILED:")) {
        const attemptedUrl = errorMessage.replace("CONNECTION_FAILED:", "");
        const origin = typeof window !== "undefined" ? window.location.origin : "unknown";
        errorMessage = `Cannot connect to the auth server.\n\nAttempted: ${attemptedUrl}\nYour origin: ${origin}\n\nFix: 1) Set NEXT_PUBLIC_AUTH_API_URL in Vercel. 2) Add "${origin}" to CORS_ORIGINS on the auth service (Railway).`;
      } else if (errorMessage.includes("Failed to connect to API")) {
        const urlMatch = errorMessage.match(/at (https?:\/\/[^\s]+)/);
        const attemptedUrl = urlMatch ? urlMatch[1] : authApiUrl;
        const origin = typeof window !== "undefined" ? window.location.origin : "unknown";
        errorMessage = `Cannot connect to the auth server.\n\nAttempted: ${attemptedUrl}\nYour origin: ${origin}\n\nFix: 1) Set NEXT_PUBLIC_AUTH_API_URL in Vercel. 2) Add "${origin}" to CORS_ORIGINS on the auth service (Railway).`;
      } else if (errorMessage.includes("timed out")) {
        errorMessage = "Connection timed out. The server may be slow or unreachable.";
      }
      
      setError(errorMessage);
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center" style={{ backgroundColor: '#ffffff' }}>
      <div className="w-full max-w-md">
        <div className="bg-white rounded-2xl border border-gray-200 shadow-lg p-8 md:p-12">
          <form onSubmit={handleSubmit} className="space-y-6">
            <div>
              <label htmlFor="username" className="block text-sm font-semibold text-gray-700 uppercase tracking-wide mb-3">
                Username
              </label>
              <input
                id="username"
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                className="w-full px-5 py-3.5 bg-white border border-gray-300 rounded-xl text-base text-gray-900 focus:outline-none focus:ring-2 focus:ring-[#1e3a5f] focus:border-[#1e3a5f] transition-all shadow-sm"
                placeholder="Enter your username"
                autoComplete="username"
              />
            </div>

            <div>
              <label htmlFor="password" className="block text-sm font-semibold text-gray-700 uppercase tracking-wide mb-3">
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="w-full px-5 py-3.5 bg-white border border-gray-300 rounded-xl text-base text-gray-900 focus:outline-none focus:ring-2 focus:ring-[#1e3a5f] focus:border-[#1e3a5f] transition-all shadow-sm"
                placeholder="Enter your password"
                autoComplete="current-password"
              />
            </div>

            {error && (
              <div className="p-4 bg-red-50 border border-red-200 rounded-xl space-y-2">
                <p className="text-red-700 text-sm font-medium">Connection error</p>
                <p className="text-red-600 text-sm whitespace-pre-line">{error}</p>
                <details className="text-xs text-gray-600 mt-2">
                  <summary className="cursor-pointer hover:text-gray-800">Debug info</summary>
                  <div className="mt-2 space-y-1 font-mono">
                    <p>Auth API: {authApiUrl}</p>
                    <p>Origin: {typeof window !== "undefined" ? window.location.origin : "—"}</p>
                    <p>AUTH_API_URL set: {process.env.NEXT_PUBLIC_AUTH_API_URL ? "yes" : "no"}</p>
                  </div>
                </details>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              className={`w-full px-8 py-5 rounded-xl text-lg font-semibold text-white transition-all duration-200 shadow-lg flex items-center justify-center gap-3 ${
                loading
                  ? "bg-gray-400 cursor-not-allowed opacity-60"
                  : "bg-[#1e3a5f] hover:bg-[#2c5282] hover:shadow-xl transform hover:-translate-y-1"
              }`}
            >
              {loading ? "Signing in..." : "Sign In"}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
