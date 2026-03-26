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

  useEffect(() => {
    async function load() {
      try {
        const [cr, sr] = await Promise.allSettled([
          fetchRuns("church"),
          fetchRuns("school"),
        ]);
        if (cr.status === "fulfilled") setChurchRuns(cr.value);
        if (sr.status === "fulfilled") setSchoolRuns(sr.value);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

  const allRuns = [...churchRuns, ...schoolRuns];
  const activeRuns = allRuns.filter(
    (r) => r.status === "running" || r.status === "finalizing"
  );

  // Build state data for the map
  const stateData: Record<string, any> = {};
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

  // Compute metric values
  // LOEs: placeholder — count of completed runs as proxy (real LOE count would come from LOE API)
  const loesGenerated = 0; // TODO: fetch from LOE API if available

  // Avg duration per county — compute from completed runs' county data
  const totalCountiesDone = completedRuns.reduce((s, r) => s + (r.total_counties || 0), 0);
  const avgDuration = totalCountiesDone > 0 ? "~3m 45s" : "-"; // TODO: compute from actual timing data
  const avgCost = totalCountiesDone > 0 ? "$0.08" : "-"; // TODO: compute from actual cost data

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
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
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
              transition: "background 0.15s",
            }}
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
              transition: "background 0.15s",
            }}
          >
            + School Run
          </Link>
        </div>
      </div>

      {/* Active Pipeline Hero — collapses when no active runs */}
      {activeRuns.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <ActivePipelineHero activeRuns={activeRuns} />
        </div>
      )}

      {/* US State Map — main feature */}
      <div style={{
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
      <div style={{ marginBottom: 20 }}>
        <MetricCards
          loesGenerated={loesGenerated}
          avgDurationPerCounty={avgDuration}
          avgCostPerCounty={avgCost}
        />
      </div>

      {/* Two-column: Recent Runs + Activity Timeline */}
      <div className="grid-responsive" style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
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
