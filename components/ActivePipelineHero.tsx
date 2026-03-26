// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { COLORS } from "../lib/constants";
import StatusBadge from "./StatusBadge";
import type { RunMetadata } from "../lib/types";

interface ActivePipelineHeroProps {
  activeRuns: RunMetadata[];
}

function formatState(state: string): string {
  return state.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());
}

export default function ActivePipelineHero({ activeRuns }: ActivePipelineHeroProps) {
  const [elapsed, setElapsed] = useState("");

  const run = activeRuns[0]; // Show the primary active run

  useEffect(() => {
    if (!run?.created_at) return;
    const update = () => {
      const start = new Date(run.created_at).getTime();
      const now = Date.now();
      const diff = Math.floor((now - start) / 1000);
      const hrs = Math.floor(diff / 3600);
      const mins = Math.floor((diff % 3600) / 60);
      const secs = diff % 60;
      if (hrs > 0) {
        setElapsed(`${hrs}h ${mins}m ${secs}s`);
      } else if (mins > 0) {
        setElapsed(`${mins}m ${secs}s`);
      } else {
        setElapsed(`${secs}s`);
      }
    };
    update();
    const interval = setInterval(update, 1000);
    return () => clearInterval(interval);
  }, [run?.created_at]);

  // Collapse when no active runs
  if (!activeRuns || activeRuns.length === 0) return null;

  // completed_counties may be a number OR an array of county names from the API
  const rawCounties = run.completed_counties;
  const countiesDone = Array.isArray(rawCounties) ? rawCounties.length : (typeof rawCounties === "number" ? rawCounties : 0);
  const countyNames = Array.isArray(rawCounties) ? rawCounties.join(", ") : "";
  const progress = (run.total_counties && countiesDone)
    ? Math.round((countiesDone / run.total_counties) * 100)
    : 0;

  const scraperType = run.scraper_type || "church";
  const href = `/${scraperType}/${run.run_id}`;

  return (
    <Link href={href} style={{ textDecoration: "none", display: "block" }}>
      <div style={{
        background: `linear-gradient(135deg, ${COLORS.accent} 0%, #0f2847 100%)`,
        borderRadius: 14,
        padding: "24px 28px",
        color: "#fff",
        position: "relative",
        overflow: "hidden",
        cursor: "pointer",
        transition: "transform 0.15s ease, box-shadow 0.15s ease",
      }}
        onMouseEnter={e => {
          e.currentTarget.style.transform = "translateY(-1px)";
          e.currentTarget.style.boxShadow = "0 8px 24px rgba(30,58,95,0.3)";
        }}
        onMouseLeave={e => {
          e.currentTarget.style.transform = "translateY(0)";
          e.currentTarget.style.boxShadow = "none";
        }}
      >
        {/* Animated background pulse */}
        <div style={{
          position: "absolute",
          top: -40,
          right: -40,
          width: 160,
          height: 160,
          borderRadius: "50%",
          background: "rgba(255,255,255,0.03)",
        }} />
        <div style={{
          position: "absolute",
          bottom: -60,
          right: 80,
          width: 200,
          height: 200,
          borderRadius: "50%",
          background: "rgba(255,255,255,0.02)",
        }} />

        <div style={{ position: "relative", zIndex: 1 }}>
          {/* Top row */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              {/* Pulsing dot */}
              <div style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: "#4ade80",
                boxShadow: "0 0 8px rgba(74,222,128,0.6)",
                animation: "pulse 2s ease-in-out infinite",
              }} />
              <span style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em", opacity: 0.7 }}>
                Active Pipeline
              </span>
            </div>
            <span style={{ fontSize: 12, opacity: 0.6 }}>
              {elapsed}
            </span>
          </div>

          {/* Run info */}
          <div style={{ marginBottom: 16 }}>
            <h3 style={{ margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: "-0.01em" }}>
              {run.display_name || formatState(run.state)}
            </h3>
            <span style={{ fontSize: 12, opacity: 0.6, textTransform: "capitalize" }}>
              {scraperType} Scraper
            </span>
          </div>

          {/* Progress bar */}
          <div style={{ marginBottom: 8 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
              <span style={{ fontSize: 12, opacity: 0.7 }}>
                {countiesDone} / {run.total_counties || 0} counties
              </span>
              <span style={{ fontSize: 13, fontWeight: 600 }}>{isNaN(progress) ? 0 : progress}%</span>
            </div>
            <div style={{
              height: 6,
              borderRadius: 3,
              background: "rgba(255,255,255,0.15)",
              overflow: "hidden",
            }}>
              <div style={{
                height: "100%",
                borderRadius: 3,
                background: "linear-gradient(90deg, #4ade80, #22d3ee)",
                width: `${progress}%`,
                transition: "width 0.5s ease",
              }} />
            </div>
          </div>

          {/* Bottom stats */}
          <div style={{ display: "flex", gap: 24, fontSize: 12, opacity: 0.6 }}>
            <span>{(run.total_contacts || 0).toLocaleString()} contacts found</span>
            {activeRuns.length > 1 && (
              <span>+{activeRuns.length - 1} more active</span>
            )}
          </div>
        </div>

        <style>{`
          @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.4; }
          }
        `}</style>
      </div>
    </Link>
  );
}
