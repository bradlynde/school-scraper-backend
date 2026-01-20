"use client";

import React, { createContext, useContext, useState, useEffect, ReactNode } from "react";

interface AuthContextType {
  isAuthenticated: boolean;
  username: string | null;
  token: string | null;
  login: (username: string, password: string) => Promise<boolean>;
  logout: () => void;
  loading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [username, setUsername] = useState<string | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Update last activity timestamp
  const updateLastActivity = () => {
    if (typeof window !== "undefined") {
      localStorage.setItem("auth_last_activity", Date.now().toString());
    }
  };

  const logout = React.useCallback(() => {
    if (typeof window !== "undefined") {
      localStorage.removeItem("auth_token");
      localStorage.removeItem("auth_username");
      localStorage.removeItem("auth_last_activity");
    }
    setToken(null);
    setUsername(null);
    setIsAuthenticated(false);
  }, []);

  // Check for existing token on mount
  useEffect(() => {
    if (typeof window !== "undefined") {
      const storedToken = localStorage.getItem("auth_token");
      const storedUsername = localStorage.getItem("auth_username");
      const lastActivityStr = localStorage.getItem("auth_last_activity");
      
      if (storedToken && storedUsername) {
        // Check if 7 days have passed since last activity
        if (lastActivityStr) {
          const lastActivity = parseInt(lastActivityStr, 10);
          const now = Date.now();
          const daysSinceActivity = (now - lastActivity) / (24 * 60 * 60 * 1000);
          
          if (daysSinceActivity >= 7) {
            // Auto-logout - 7 days of inactivity
            localStorage.removeItem("auth_token");
            localStorage.removeItem("auth_username");
            localStorage.removeItem("auth_last_activity");
            setLoading(false);
            return;
          }
        }
        
        // Verify token is still valid (basic check - backend will verify)
        setToken(storedToken);
        setUsername(storedUsername);
        setIsAuthenticated(true);
        
        // Update last activity on successful mount
        updateLastActivity();
      }
    }
    setLoading(false);
  }, []);

  // Check for auto-logout periodically (every hour)
  useEffect(() => {
    if (!isAuthenticated) return;
    
    const checkAutoLogout = () => {
      if (typeof window !== "undefined") {
        const lastActivityStr = localStorage.getItem("auth_last_activity");
        if (lastActivityStr) {
          const lastActivity = parseInt(lastActivityStr, 10);
          const now = Date.now();
          const daysSinceActivity = (now - lastActivity) / (24 * 60 * 60 * 1000);
          
          if (daysSinceActivity >= 7) {
            // Auto-logout after 7 days of inactivity
            logout();
          }
        }
      }
    };
    
    const interval = setInterval(checkAutoLogout, 60 * 60 * 1000); // Check every hour
    
    return () => clearInterval(interval);
  }, [isAuthenticated, logout]);

  // Update last activity on user interactions (mouse clicks, keyboard, etc.)
  useEffect(() => {
    if (!isAuthenticated) return;
    
    const events = ['mousedown', 'keydown', 'scroll', 'touchstart'];
    const handleActivity = () => {
      updateLastActivity();
    };
    
    events.forEach(event => {
      window.addEventListener(event, handleActivity, { passive: true });
    });
    
    return () => {
      events.forEach(event => {
        window.removeEventListener(event, handleActivity);
      });
    };
  }, [isAuthenticated]);

  const login = async (username: string, password: string): Promise<boolean> => {
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-200036585956.us-central1.run.app").replace(/\/+$/, '');
      console.log("Attempting login to:", `${apiUrl}/login`);
      
      // Add timeout to prevent hanging
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
      
      try {
        const response = await fetch(`${apiUrl}/login`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ username, password }),
          signal: controller.signal,
        });

        clearTimeout(timeoutId);
        console.log("Login response status:", response.status);

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({ error: "Unknown error" }));
          console.error("Login failed:", errorData);
          return false;
        }

        const data = await response.json();
        console.log("Login response data:", data);
        
        if (data.status === "success" && data.token) {
          // Store token and username
          if (typeof window !== "undefined") {
            localStorage.setItem("auth_token", data.token);
            localStorage.setItem("auth_username", data.username);
            // Set last activity timestamp on login
            updateLastActivity();
          }
          setToken(data.token);
          setUsername(data.username);
          setIsAuthenticated(true);
          console.log("Login successful, authenticated:", true);
          return true;
        }
        
        console.error("Login response missing token or success status");
        return false;
      } catch (fetchError: any) {
        clearTimeout(timeoutId);
        if (fetchError.name === 'AbortError') {
          console.error("Login request timed out");
          throw new Error("Request timed out. Please check your connection and try again.");
        }
        console.error("Fetch error details:", fetchError);
        // Provide more helpful error message
        if (fetchError.message && fetchError.message.includes('fetch')) {
          throw new Error(`Failed to connect to API at ${apiUrl}. Please check that the backend is running and NEXT_PUBLIC_API_URL is set correctly.`);
        }
        throw fetchError;
      }
    } catch (error: any) {
      console.error("Login error:", error);
      throw error; // Re-throw to let LoginForm handle it
    }
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, username, token, login, logout, loading }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
