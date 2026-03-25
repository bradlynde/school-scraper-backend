"use client";
// @ts-nocheck

import { useState, useEffect } from "react";
import Link from "next/link";
import { COLORS } from "../lib/constants";
import { fetchRuns } from "../lib/api";
import StatCard from "../components/StatCard";
import StatusBadge from "../components/StatusBadge";
import type { RunMetadata } from "../lib/types";

function formatState(state: string): string {
  return state.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());
}

function formatDate(dateStr?: string): string {
  if (!dateStr) return "-";
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  } catch {
    return dateStr;
  }
}

export default function HomePage() {
  const [churchRuns, setChurchRuns] = useState<RunMetadata[]>([]);
  const [schoolRuns, setSchoolRuns] = useState<RunMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const [cr, sr] = await Promise.allSettled([
          fetchRuns("church"),
          fetchRuns("school"),
        ]);
        if (cr.status === "fulfilled") setChurchRuns(cr.value);
        if (sr.status === "fulfilled") setSchoolRuns(sr.value);
      } catch (e) {
        setError("Failed to load dashboard data");
      }
      setLoading(false);
    }
    load();
  }, []);

  const allRuns = [...churchRuns, ...schoolRuns];
  const totalRuns = allRuns.length;
  const totalContacts = allRuns.reduce(
    (s, r) => s + (r.total_contacts || 0),
    0
  );
  const activeRuns = allRuns.filter(
    (r) => r.status === "running" || r.status === "finalizing"
  ).length;
  const statesCompleted = new Set(
    allRuns
      .filter((r) => r.status === "done" || r.status === "completed")
      .map((r) => r.state)
  ).size;

  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  const cardStyle: React.CSSProperties = {
    background: COLORS.cardBg,
    borderRadius: 12,
    padding: "20px 24px",
    boxShadow: COLORS.cardShadow,
    border: `1px solid ${COLORS.cardBorder}`,
  };

  const miniTableRow = (run: RunMetadata, type: string) => (
    <Link
      key={run.run_id}
      href={`/${type}/${run.run_id}`}
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "8px 0",
        borderBottom: `1px solid ${COLORS.cardBorder}`,
        textDecoration: "none",
        color: COLORS.textPrimary,
        fontSize: 13,
      }}
    >
      <span style={{ fontWeight: 500 }}>
        {run.display_name || formatState(run.state)}
      </span>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <span style={{ fontSize: 12, color: COLORS.textMuted }}>
          {(run.total_contacts || 0).toLocaleString()} contacts
        </span>
        <StatusBadge status={run.status} />
      </div>
    </Link>
  );

  if (loading) {
    return (
      <div style={{ padding: "40px 48px" }}>
        <div
          style={{ fontSize: 13, color: COLORS.textMuted, marginTop: 40 }}
        >
          Loading dashboard...
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: "32px 48px", maxWidth: 1200, margin: "0 auto" }}>
      {/* Header */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-end",
          marginBottom: 28,
        }}
      >
        <div>
          <h1
            style={{
              fontSize: 26,
              fontWeight: 700,
              color: COLORS.textPrimary,
              margin: 0,
            }}
          >
            Dashboard
          </h1>
          <div
            style={{ fontSize: 13, color: COLORS.textMuted, marginTop: 4 }}
          >
            {today}
          </div>
        </div>
      </div>

      {error && (
        <div
          style={{
            ...cardStyle,
            background: COLORS.errorBg,
            color: COLORS.error,
            marginBottom: 20,
            fontSize: 13,
          }}
        >
          {error}
        </div>
      )}

      {/* Stat Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 20,
          marginBottom: 28,
        }}
      >
        <StatCard label="Total Runs" value={totalRuns} />
        <StatCard label="Total Contacts" value={totalContacts} />
        <StatCard label="Active Runs" value={activeRuns} />
        <StatCard label="States Completed" value={statesCompleted} />
      </div>

      {/* Two-column: Church + School */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 20,
          marginBottom: 28,
        }}
      >
        {/* Church Scraper */}
        <div style={cardStyle}>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 14,
            }}
          >
            <h3
              style={{
                margin: 0,
                fontSize: 16,
                fontWeight: 700,
                color: COLORS.textPrimary,
              }}
            >
              Church Scraper
            </h3>
            <Link
              href="/church"
              style={{
                fontSize: 12,
                color: COLORS.accent,
                textDecoration: "none",
                fontWeight: 500,
              }}
            >
              View All &rarr;
            </Link>
          </div>
          {churchRuns.length === 0 ? (
            <div
              style={{
                fontSize: 13,
                color: COLORS.textMuted,
                padding: "16px 0",
              }}
            >
              No runs yet
            </div>
          ) : (
            churchRuns.slice(0, 3).map((r) => miniTableRow(r, "church"))
          )}
        </div>

        {/* School Scraper */}
        <div style={cardStyle}>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 14,
            }}
          >
            <h3
              style={{
                margin: 0,
                fontSize: 16,
                fontWeight: 700,
                color: COLORS.textPrimary,
              }}
            >
              School Scraper
            </h3>
            <Link
              href="/school"
              style={{
                fontSize: 12,
                color: COLORS.accent,
                textDecoration: "none",
                fontWeight: 500,
              }}
            >
              View All &rarr;
            </Link>
          </div>
          {schoolRuns.length === 0 ? (
            <div
              style={{
                fontSize: 13,
                color: COLORS.textMuted,
                padding: "16px 0",
              }}
            >
              No runs yet
            </div>
          ) : (
            schoolRuns.slice(0, 3).map((r) => miniTableRow(r, "school"))
          )}
        </div>
      </div>

      {/* Quick Actions */}
      <div style={cardStyle}>
        <h3
          style={{
            margin: "0 0 14px",
            fontSize: 16,
            fontWeight: 700,
            color: COLORS.textPrimary,
          }}
        >
          Quick Actions
        </h3>
        <div style={{ display: "flex", gap: 12 }}>
          <Link
            href="/church/new"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "10px 20px",
              background: COLORS.accent,
              color: "#fff",
              borderRadius: 8,
              textDecoration: "none",
              fontSize: 13,
              fontWeight: 600,
            }}
          >
            + New Church Run
          </Link>
          <Link
            href="/school/new"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "10px 20px",
              background: COLORS.accent,
              color: "#fff",
              borderRadius: 8,
              textDecoration: "none",
              fontSize: 13,
              fontWeight: 600,
            }}
          >
            + New School Run
          </Link>
          <Link
            href="/loe"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "10px 20px",
              background: "#fff",
              color: COLORS.accent,
              border: `1px solid ${COLORS.cardBorder}`,
              borderRadius: 8,
              textDecoration: "none",
              fontSize: 13,
              fontWeight: 600,
            }}
          >
            LOE Generator
          </Link>
        </div>
      </div>
    </div>
  );
}
