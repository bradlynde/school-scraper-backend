// @ts-nocheck
"use client";

import { useState } from "react";
import { AuthProvider, useAuth } from "../contexts/AuthContext";
import LoginForm from "./LoginForm";
import Sidebar, { MobileMenuButton } from "./Sidebar";

function AuthGate({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, loading } = useAuth();
  const [mobileOpen, setMobileOpen] = useState(false);

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
    <div style={{ display: "flex", minHeight: "100dvh" }}>
      <Sidebar mobileOpen={mobileOpen} onMobileClose={() => setMobileOpen(false)} />
      <MobileMenuButton onClick={() => setMobileOpen(true)} />
      <main style={{ flex: 1, overflowY: "auto", background: "#f7f8fa" }}>
        {children}
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
