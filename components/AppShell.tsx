// @ts-nocheck
"use client";

import { AuthProvider, useAuth } from "../contexts/AuthContext";
import LoginForm from "./LoginForm";
import Sidebar from "./Sidebar";

function AuthGate({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return (
      <div style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontFamily: "Inter, system-ui, sans-serif",
        color: "#6b7280",
      }}>
        Loading...
      </div>
    );
  }

  if (!isAuthenticated) {
    return <LoginForm />;
  }

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      <Sidebar />
      <main style={{ flex: 1, overflowY: "auto", background: "#f4f5f7", display: "flex", alignItems: "center", justifyContent: "center", minHeight: 0 }}>
        <div style={{ width: "100%" }}>
          {children}
        </div>
      </main>
    </div>
  );
}

export default function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <AuthProvider>
      <AuthGate>{children}</AuthGate>
    </AuthProvider>
  );
}
