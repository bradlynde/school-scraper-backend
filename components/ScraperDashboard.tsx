"use client";
// @ts-nocheck

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { COLORS, SCRAPER_LABELS } from "../lib/constants";
import { fetchRuns, fetchPipelineStatus, stopRun } from "../lib/api";
import StatCard from "./StatCard";
import ProgressBar from "./ProgressBar";
import RunsTable from "./RunsTable";
import StatusBadge from "./StatusBadge";
import type { RunMetadata, ScraperType, PipelineStatus } from "../lib/types";

export default function ScraperDashboard({ scraperType }: { scraperType: ScraperType }) {
  const labels = SCRAPER_LABELS[scraperType];
  const [runs, setRuns] = useState<RunMetadata[]>([]);
  const [showArchived, setShowArchived] = useState(false);
  const [loading, setLoading] = useState(true);
  const [activeStatus, setActiveStatus] = useState<PipelineStatus | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadRuns = async () => {
    try {
      const data = await fetchRuns(scraperType, showArchived);
      setRuns(data);
    } catch {
      // silently handle
    }
    setLoading(false);
  };

  useEffect(() => {
    loadRuns();
  }, [scraperType, showArchived]);

  // Find active run and poll for updates
  const activeRun = runs.find(r => r.status === "running" || r.status === "finalizing");

  useEffect(() => {
    if (pollRef.current) clearInterval(pollRef.current);
    if (!activeRun) {
      setActiveStatus(null);
      return;
    }

    const poll = async () => {
      try {
        const status = await fetchPipelineStatus(scraperType, activeRun.run_id);
        setActiveStatus(status);
      } catch {
        // ignore
      }
    };
    poll();
    pollRef.current = setInterval(poll, 5000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [activeRun?.run_id, scraperType]);

  const handleStop = async () => {
    if (!activeRun) return;
    if (!confirm("Stop this run? This cannot be undone.")) return;
    try {
      await stopRun(scraperType, activeRun.run_id);
      loadRuns();
    } catch (e) {
      alert(e instanceof Error ? e.message : "Failed to stop run");
    }
  };

  const completedRuns = runs.filter(r => r.status === "done" || r.status === "completed");
  const totalContacts = completedRuns.reduce((s, r) => s + (r.total_contacts || 0), 0);
  const totalWithEmail = completedRuns.reduce((s, r) => s + (r.total_contacts_with_emails || 0), 0);
  const statesScraped = new Set(completedRuns.map(r => r.state)).size;

  const nonArchivedRuns = runs.filter(r => !r.archived && r.status !== "running" && r.status !== "finalizing");

  // Active run stats from polling
  const as = activeStatus;
  const activeCountiesTotal = as?.totalCounties ?? as?.total_counties ?? 0;
  const activeCountiesDone = as?.countiesProcessed ?? as?.counties_processed ?? 0;
  const activeContacts = as?.totalContacts ?? as?.total_contacts ?? 0;
  const activeWithEmail = as?.totalContactsWithEmails ?? as?.total_contacts_with_emails ?? 0;

  if (loading) {
    return (
      <div style={{ padding: "40px 48px" }}>
        <div style={{ fontSize: 13, color: COLORS.textMuted }}>Loading...</div>
      </div>
    );
  }

  return (
    <div style={{ padding: "32px 48px", maxWidth: 1200, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 28 }}>
        <h1 style={{ fontSize: 26, fontWeight: 700, color: COLORS.textPrimary, margin: 0 }}>
          {labels.title}
        </h1>
        <Link
          href={`/${scraperType}/new`}
          style={{
            display: "inline-flex", alignItems: "center", gap: 8,
            padding: "10px 20px", background: COLORS.accent, color: "#fff",
            borderRadius: 8, textDecoration: "none", fontSize: 13, fontWeight: 600,
          }}
        >
          + New Run
        </Link>
      </div>

      {/* Active Run */}
      {activeRun && (
        <div style={{
          background: COLORS.cardBg, borderRadius: 12, padding: "20px 24px",
          boxShadow: COLORS.cardShadow, border: `1px solid ${COLORS.cardBorder}`,
          marginBottom: 20,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: COLORS.textPrimary }}>
                Current Run
              </h3>
              <StatusBadge status={activeRun.status} />
              <span style={{ fontSize: 13, color: COLORS.textSecondary }}>
                {activeRun.display_name || activeRun.state?.replace(/_/g, " ").replace(/\b\w/g, l => l.toUpperCase())}
              </span>
            </div>
            <button
              onClick={handleStop}
              style={{
                background: "none", border: `1px solid ${COLORS.error}`, borderRadius: 6,
                padding: "6px 14px", fontSize: 12, color: COLORS.error, cursor: "pointer", fontWeight: 500,
              }}
            >
              Stop Run
            </button>
          </div>

          <ProgressBar completed={activeCountiesDone} total={activeCountiesTotal} />

          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginTop: 16 }}>
            <StatCard label="Counties Done" value={`${activeCountiesDone} / ${activeCountiesTotal}`} />
            <StatCard label="Contacts Found" value={activeContacts} />
            <StatCard label="With Email" value={activeWithEmail} />
            <StatCard label="Current County" value={as?.currentCounty || "-"} />
          </div>

          <div style={{ marginTop: 12, textAlign: "right" }}>
            <Link
              href={`/${scraperType}/${activeRun.run_id}`}
              style={{ fontSize: 12, color: COLORS.accent, textDecoration: "none", fontWeight: 500 }}
            >
              View Details &rarr;
            </Link>
          </div>
        </div>
      )}

      {/* Aggregated Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 20, marginBottom: 28 }}>
        <StatCard label="Total Contacts" value={totalContacts} />
        <StatCard label="With Email" value={totalWithEmail} />
        <StatCard label="States Scraped" value={statesScraped} />
        <StatCard label="Completed Runs" value={completedRuns.length} />
      </div>

      {/* Previous Runs */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: COLORS.textPrimary }}>
            Previous Runs
          </h3>
        </div>
        <RunsTable runs={nonArchivedRuns} scraperType={scraperType} onArchive={loadRuns} />
      </div>

      {/* Archive Link */}
      <div style={{ textAlign: "center", padding: "8px 0" }}>
        <button
          onClick={() => setShowArchived(!showArchived)}
          style={{
            background: "none", border: "none", fontSize: 12,
            color: COLORS.textMuted, cursor: "pointer", textDecoration: "underline",
          }}
        >
          {showArchived ? "Hide archived runs" : "View archived runs"}
        </button>
      </div>
    </div>
  );
}
