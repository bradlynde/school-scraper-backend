// @ts-nocheck
"use client";

import Image from "next/image";
import Link from "next/link";
import { useState } from "react";
import { usePathname } from "next/navigation";
import { useAuth } from "../contexts/AuthContext";

/* ── SVG Icons (from original sidebar) ────────────────────────── */

const HomeIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1h-2z" />
  </svg>
);

const ChurchIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
  </svg>
);

const BookIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
  </svg>
);

const DocIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
  </svg>
);

const UserIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
  </svg>
);

const LogoutIcon = () => (
  <svg style={{ width: 20, height: 20, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
  </svg>
);

const ChevronIcon = ({ open }: { open: boolean }) => (
  <svg style={{ width: 16, height: 16, flexShrink: 0, transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "rotate(0deg)" }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
  </svg>
);

/* ── Nav items ────────────────────────────────────────────────── */

const NAV_ITEMS = [
  { href: "/", label: "Home", Icon: HomeIcon },
  { href: "/church", label: "Church Scraper", Icon: ChurchIcon },
  { href: "/school", label: "School Scraper", Icon: BookIcon },
  { href: "/loe", label: "LOE Generator", Icon: DocIcon },
];

/* ── Component ────────────────────────────────────────────────── */

export default function Sidebar() {
  const pathname = usePathname();
  const { username, logout } = useAuth();
  const [menuOpen, setMenuOpen] = useState(false);

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  return (
    <aside style={{
      width: 260,
      background: "#ffffff",
      borderRight: "1px solid #e5e7eb",
      display: "flex",
      flexDirection: "column",
      flexShrink: 0,
      height: "100vh",
      overflow: "hidden",
    }}>
      {/* Logo */}
      <Link href="/" style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "20px 16px",
        borderBottom: "1px solid #e5e7eb",
        minHeight: 72,
        textDecoration: "none",
      }}>
        <Image
          src="/npsa-logo.png"
          alt="Nonprofit Security Advisors"
          width={180}
          height={54}
          style={{ height: "auto", objectFit: "contain" }}
          priority
        />
      </Link>

      {/* Navigation */}
      <nav style={{ flex: 1, padding: "16px 12px", display: "flex", flexDirection: "column", gap: 2 }}>
        {NAV_ITEMS.map(({ href, label, Icon }) => {
          const active = isActive(href);
          return (
            <Link
              key={href}
              href={href}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 14,
                padding: "11px 16px",
                borderRadius: 10,
                textDecoration: "none",
                fontSize: 14,
                fontWeight: active ? 600 : 500,
                color: active ? "#1e3a5f" : "#4b5563",
                background: active ? "rgba(30, 58, 95, 0.08)" : "transparent",
                borderLeft: active ? "3px solid #1e3a5f" : "3px solid transparent",
                transition: "all 0.15s ease",
              }}
            >
              <span style={{ color: active ? "#1e3a5f" : "#6b7280" }}>
                <Icon />
              </span>
              <span>{label}</span>
            </Link>
          );
        })}
      </nav>

      {/* User profile section at bottom */}
      <div style={{
        padding: "12px",
        borderTop: "1px solid #e5e7eb",
        position: "relative",
      }}>
        {/* Sign out pop-up menu */}
        {menuOpen && (
          <div style={{
            position: "absolute",
            bottom: "100%",
            left: 8,
            right: 8,
            marginBottom: 8,
            background: "#fff",
            border: "1px solid #e5e7eb",
            borderRadius: 10,
            boxShadow: "0 4px 16px rgba(0,0,0,0.10)",
            overflow: "hidden",
            zIndex: 50,
          }}>
            <button
              onClick={() => { logout(); setMenuOpen(false); }}
              style={{
                width: "100%",
                display: "flex",
                alignItems: "center",
                gap: 12,
                padding: "12px 16px",
                border: "none",
                background: "transparent",
                cursor: "pointer",
                fontSize: 14,
                color: "#374151",
                transition: "background 0.15s",
              }}
              onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
              onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
            >
              <LogoutIcon />
              <span>Sign Out</span>
            </button>
          </div>
        )}

        {/* User button */}
        <button
          onClick={() => setMenuOpen(!menuOpen)}
          style={{
            width: "100%",
            display: "flex",
            alignItems: "center",
            gap: 14,
            padding: "11px 16px",
            borderRadius: 10,
            border: "none",
            background: "transparent",
            cursor: "pointer",
            fontSize: 14,
            fontWeight: 500,
            color: "#4b5563",
            transition: "background 0.15s",
          }}
          onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
          onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
        >
          <span style={{ color: "#6b7280" }}><UserIcon /></span>
          <span style={{ flex: 1, textAlign: "left", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {username || "User"}
          </span>
          <ChevronIcon open={menuOpen} />
        </button>
      </div>
    </aside>
  );
}
