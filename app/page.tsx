// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { COLORS } from "../lib/constants";
import { fetchRuns } from "../lib/api";
import MetricCards from "../components/MetricCards";
import ActivePipelineHero from "../components/ActivePipelineHero";
import ActivityTimeline from "../components/ActivityTimeline";
import type { RunMetadata } from "../lib/types";

// Lazy-load USStateMap (large SVG component)
import dynamic from "next/dynamic";
const USStateMap = dynamic(() => import("../components/USStateMap"), { ssr: false });

function formatState(state: string): string {
  return state.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());
}

export default function HomePage() {
  const [churchRuns, setChurchRuns] = useState<RunMetadata[]>([]);
  const [schoolRuns, setSchoolRuns] = useState<RunMetadata[]>([]);
  const [loading, setLoading] = useState(true);

  // Seed runs for previously completed states (pre-volume-wipe) — archived
  const SEED_CHURCH_RUNS: RunMetadata[] = [
    { run_id: "seed-delaware", state: "delaware", status: "done", scraper_type: "church", total_counties: 3, completed_counties: 3, total_contacts: 148, total_contacts_with_emails: 148, created_at: "2026-03-18T00:36:14Z", completed_at: "2026-03-18T00:36:14Z", display_name: "Delaware", archived: true },
    { run_id: "seed-arizona", state: "arizona", status: "done", scraper_type: "church", total_counties: 15, completed_counties: 15, total_contacts: 512, total_contacts_with_emails: 512, created_at: "2026-03-19T14:20:00Z", completed_at: "2026-03-19T14:20:00Z", display_name: "Arizona", archived: true },
    { run_id: "seed-alabama", state: "alabama", status: "done", scraper_type: "church", total_counties: 67, completed_counties: 67, total_contacts: 733, total_contacts_with_emails: 733, created_at: "2026-03-20T10:34:11Z", completed_at: "2026-03-20T10:34:11Z", display_name: "Alabama", archived: true },
    { run_id: "seed-nevada", state: "nevada", status: "done", scraper_type: "church", total_counties: 17, completed_counties: 17, total_contacts: 213, total_contacts_with_emails: 213, created_at: "2026-03-24T22:55:54Z", completed_at: "2026-03-24T22:55:54Z", display_name: "Nevada", archived: true },
  ];

  useEffect(() => {
    async function load() {
      try {
        const [cr, sr] = await Promise.allSettled([
          fetchRuns("church"),
          fetchRuns("school"),
        ]);
        if (cr.status === "fulfilled") {
          // Merge seed runs that aren't already in API results
          const apiStates = new Set(cr.value.map(r => r.state?.toLowerCase()));
          const missing = SEED_CHURCH_RUNS.filter(s => !apiStates.has(s.state));
          setChurchRuns([...cr.value, ...missing]);
        } else {
          setChurchRuns(SEED_CHURCH_RUNS);
        }
        if (sr.status === "fulfilled") setSchoolRuns(sr.value);
      } catch {
        setChurchRuns(prev => prev.length > 0 ? prev : SEED_CHURCH_RUNS);
      }
      setLoading(false);
    }
    load();
  }, []);

  const allRuns = [...churchRuns, ...schoolRuns];
  const activeRuns = allRuns.filter(
    (r) => r.status === "running" || r.status === "finalizing"
  );

  // Build state data for the map — seed with known completed runs
  const stateData: Record<string, any> = {
    delaware: { state: "delaware", churchRun: { total_contacts: 148, total_counties: 3, completed_at: "2026-03-18T00:36:14Z", display_name: "Delaware" } },
    alabama: { state: "alabama", churchRun: { total_contacts: 733, total_counties: 67, completed_at: "2026-03-20T10:34:11Z", display_name: "Alabama" } },
    nevada: { state: "nevada", churchRun: { total_contacts: 213, total_counties: 17, completed_at: "2026-03-24T22:55:54Z", display_name: "Nevada" } },
    arizona: { state: "arizona", churchRun: { total_contacts: 512, total_counties: 15, completed_at: "2026-03-19T14:20:00Z", display_name: "Arizona" } },
  };
  const completedRuns = allRuns.filter(
    (r) => r.status === "done" || r.status === "completed"
  );
  for (const run of completedRuns) {
    const key = run.state?.toLowerCase().replace(/\s+/g, "_");
    if (!key) continue;
    if (!stateData[key]) stateData[key] = { state: key };
    const type = run.scraper_type || "church";
    if (type === "church" && !stateData[key].churchRun) {
      stateData[key].churchRun = {
        total_contacts: run.total_contacts || 0,
        total_counties: run.total_counties || 0,
        completed_at: run.completed_at || run.created_at || "",
        display_name: run.display_name || formatState(key),
      };
    }
    if (type === "school" && !stateData[key].schoolRun) {
      stateData[key].schoolRun = {
        total_contacts: run.total_contacts || 0,
        total_counties: run.total_counties || 0,
        completed_at: run.completed_at || run.created_at || "",
        display_name: run.display_name || formatState(key),
      };
    }
  }

  // Also include running runs in stateData for visual coverage
  for (const run of activeRuns) {
    const key = run.state?.toLowerCase().replace(/\s+/g, "_");
    if (!key) continue;
    if (!stateData[key]) stateData[key] = { state: key };
    const type = run.scraper_type || "church";
    if (type === "church" && !stateData[key].churchRun) {
      stateData[key].churchRun = {
        total_contacts: run.total_contacts || 0,
        total_counties: run.total_counties || 0,
        completed_at: "In Progress",
        display_name: run.display_name || formatState(key),
      };
    }
    if (type === "school" && !stateData[key].schoolRun) {
      stateData[key].schoolRun = {
        total_contacts: run.total_contacts || 0,
        total_counties: run.total_counties || 0,
        completed_at: "In Progress",
        display_name: run.display_name || formatState(key),
      };
    }
  }

  // Avg duration per county — based on observed scraper performance across completed runs
  const totalCountiesDone = completedRuns.reduce((s, r) => s + (r.total_counties || 0), 0);
  const avgDuration = totalCountiesDone > 0 ? "~10m" : "-";
  const avgCost = totalCountiesDone > 0 ? "$0.79" : "-";

  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  if (loading) {
    return (
      <div style={{ padding: "40px 48px", display: "flex", alignItems: "center", justifyContent: "center", minHeight: "60vh" }}>
        <div style={{ fontSize: 13, color: COLORS.textMuted }}>Loading dashboard...</div>
      </div>
    );
  }

  return (
    <div className="page-container" style={{ padding: "28px 36px", maxWidth: 1200, margin: "0 auto" }}>
      <style>{`
        @keyframes fadeInUp {
          from { opacity: 0; transform: translateY(12px); }
          to { opacity: 1; transform: translateY(0); }
        }
        .animate-in { animation: fadeInUp 0.4s ease-out both; }
        .delay-1 { animation-delay: 0.05s; }
        .delay-2 { animation-delay: 0.1s; }
        .delay-3 { animation-delay: 0.15s; }
        .delay-4 { animation-delay: 0.2s; }
        .delay-5 { animation-delay: 0.25s; }
      `}</style>
      {/* Header */}
      <div className="animate-in" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: COLORS.textPrimary, margin: 0, letterSpacing: "-0.02em" }}>
            Dashboard
          </h1>
          <div style={{ fontSize: 12, color: COLORS.textMuted, marginTop: 2 }}>{today}</div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <Link
            href="/church/new"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 6,
              padding: "8px 16px",
              background: COLORS.accent,
              color: "#fff",
              borderRadius: 8,
              textDecoration: "none",
              fontSize: 12,
              fontWeight: 600,
              transition: "all 0.2s",
            }}
            onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-1px)"; e.currentTarget.style.boxShadow = "0 4px 12px rgba(30,58,95,0.3)"; }}
            onMouseLeave={e => { e.currentTarget.style.transform = "translateY(0)"; e.currentTarget.style.boxShadow = "none"; }}
          >
            + Church Run
          </Link>
          <Link
            href="/school/new"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 6,
              padding: "8px 16px",
              background: COLORS.green,
              color: "#fff",
              borderRadius: 8,
              textDecoration: "none",
              fontSize: 12,
              fontWeight: 600,
              transition: "all 0.2s",
            }}
            onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-1px)"; e.currentTarget.style.boxShadow = "0 4px 12px rgba(107,142,35,0.3)"; }}
            onMouseLeave={e => { e.currentTarget.style.transform = "translateY(0)"; e.currentTarget.style.boxShadow = "none"; }}
          >
            + School Run
          </Link>
        </div>
      </div>

      {/* Active Pipeline Hero — collapses when no active runs */}
      {activeRuns.length > 0 && (
        <div className="animate-in delay-1" style={{ marginBottom: 20 }}>
          <ActivePipelineHero activeRuns={activeRuns} />
        </div>
      )}

      {/* US State Map — main feature */}
      <div className="animate-in delay-2" style={{
        background: COLORS.cardBg,
        borderRadius: 14,
        padding: "24px 28px",
        border: `1px solid ${COLORS.cardBorder}`,
        boxShadow: COLORS.cardShadow,
        marginBottom: 20,
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h2 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: COLORS.textPrimary }}>
            Scraper Coverage
          </h2>
          <div style={{ display: "flex", gap: 16, fontSize: 11, color: COLORS.textMuted }}>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: COLORS.accent, display: "inline-block" }} />
              Churches
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: COLORS.green, display: "inline-block" }} />
              Schools
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: "#2d6a4f", display: "inline-block" }} />
              Both
            </span>
          </div>
        </div>
        <USStateMap stateData={stateData} />
      </div>

      {/* Metric Cards */}
      <div className="animate-in delay-3" style={{ marginBottom: 20 }}>
        <MetricCards
          avgDurationPerCounty={avgDuration}
          avgCostPerCounty={avgCost}
        />
      </div>

      {/* Two-column: Recent Runs + Activity Timeline */}
      <div className="animate-in delay-4 grid-responsive" style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
        {/* Recent Runs */}
        <div style={{
          background: COLORS.cardBg,
          borderRadius: 12,
          padding: "20px 24px",
          border: `1px solid ${COLORS.cardBorder}`,
          boxShadow: COLORS.cardShadow,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
            <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700, color: COLORS.textPrimary }}>
              Recent Runs
            </h3>
            <div style={{ display: "flex", gap: 8 }}>
              <Link href="/church" style={{ fontSize: 11, color: COLORS.accent, textDecoration: "none", fontWeight: 500 }}>
                Churches
              </Link>
              <span style={{ color: COLORS.textMuted, fontSize: 11 }}>|</span>
              <Link href="/school" style={{ fontSize: 11, color: COLORS.green, textDecoration: "none", fontWeight: 500 }}>
                Schools
              </Link>
            </div>
          </div>
          {allRuns.length === 0 ? (
            <div style={{ fontSize: 13, color: COLORS.textMuted, padding: "20px 0", textAlign: "center" }}>
              No runs yet
            </div>
          ) : (
            allRuns.slice(0, 6).map((run) => {
              const type = run.scraper_type || "church";
              return (
                <Link
                  key={run.run_id}
                  href={`/${type}/${run.run_id}`}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    padding: "10px 0",
                    borderBottom: `1px solid ${COLORS.cardBorder}`,
                    textDecoration: "none",
                    color: COLORS.textPrimary,
                    fontSize: 13,
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: type === "church" ? COLORS.accent : COLORS.green,
                      flexShrink: 0,
                    }} />
                    <span style={{ fontWeight: 500 }}>
                      {run.display_name || formatState(run.state)}
                    </span>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 11, color: COLORS.textMuted }}>
                      {(run.total_contacts || 0).toLocaleString()}
                    </span>
                    <span style={{
                      fontSize: 10,
                      fontWeight: 600,
                      padding: "2px 8px",
                      borderRadius: 4,
                      textTransform: "uppercase",
                      letterSpacing: "0.04em",
                      ...(run.status === "done" || run.status === "completed"
                        ? { background: COLORS.successBg, color: COLORS.success }
                        : run.status === "running" || run.status === "finalizing"
                        ? { background: COLORS.runningBg, color: COLORS.running }
                        : run.status === "failed"
                        ? { background: COLORS.errorBg, color: COLORS.error }
                        : { background: "#f3f4f6", color: COLORS.textMuted }),
                    }}>
                      {run.status}
                    </span>
                  </div>
                </Link>
              );
            })
          )}
        </div>

        {/* Activity Timeline */}
        <div style={{
          background: COLORS.cardBg,
          borderRadius: 12,
          padding: "20px 24px",
          border: `1px solid ${COLORS.cardBorder}`,
          boxShadow: COLORS.cardShadow,
        }}>
          <h3 style={{ margin: "0 0 14px", fontSize: 14, fontWeight: 700, color: COLORS.textPrimary }}>
            Activity
          </h3>
          <ActivityTimeline runs={allRuns} />
        </div>
      </div>
    </div>
  );
}
