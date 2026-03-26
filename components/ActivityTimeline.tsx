// @ts-nocheck
"use client";

import React from "react";
import type { RunMetadata } from "../lib/types";

interface ActivityTimelineProps {
  runs: RunMetadata[];
}

function formatRelativeTime(dateStr: string): string {
  const now = new Date();
  const date = new Date(dateStr);
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHr = Math.floor(diffMin / 60);
  const diffDays = Math.floor(diffHr / 24);

  if (diffSec < 60) return "Just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHr < 24) return `${diffHr}h ago`;
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;

  const month = date.toLocaleString("en-US", { month: "short" });
  const day = date.getDate();
  return `${month} ${day}`;
}

function getEventColor(status: string): string {
  switch (status) {
    case "done":
      return "#2d8a4e";
    case "running":
    case "finalizing":
      return "#1e3a5f";
    case "failed":
    case "cancelled":
      return "#d32f2f";
    default:
      return "#e6a117";
  }
}

function getStatusIcon(status: string): string {
  switch (status) {
    case "done":
      return "\u2713";
    case "running":
    case "finalizing":
      return "\u25B6";
    case "failed":
    case "cancelled":
      return "\u2717";
    default:
      return "\u25CF";
  }
}

function getStatusLabel(status: string): string {
  switch (status) {
    case "done":
      return "Completed";
    case "running":
      return "Running";
    case "finalizing":
      return "Finalizing";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    default:
      return "Queued";
  }
}

function formatScraperType(type?: string): string {
  if (!type) return "Scraper";
  return type.charAt(0).toUpperCase() + type.slice(1) + " Scraper";
}

function getDescription(run: RunMetadata): string {
  if (run.status === "done" && run.total_contacts != null) {
    const emailNote =
      run.total_contacts_with_emails != null
        ? ` (${run.total_contacts_with_emails} with emails)`
        : "";
    return `Completed with ${run.total_contacts.toLocaleString()} contacts${emailNote}`;
  }
  if (run.status === "running" || run.status === "finalizing") {
    if (run.completed_counties != null && run.total_counties != null) {
      const done = Array.isArray(run.completed_counties) ? run.completed_counties.length : run.completed_counties;
      return `${getStatusLabel(run.status)} — ${done}/${run.total_counties} counties`;
    }
    return getStatusLabel(run.status);
  }
  if (run.status === "failed") return "Run failed";
  if (run.status === "cancelled") return "Run was cancelled";
  return "Queued for processing";
}

export default function ActivityTimeline({ runs }: ActivityTimelineProps) {
  const sortedRuns = [...runs]
    .sort((a, b) => {
      const dateA = a.created_at ? new Date(a.created_at).getTime() : 0;
      const dateB = b.created_at ? new Date(b.created_at).getTime() : 0;
      return dateB - dateA;
    })
    .slice(0, 10);

  return (
    <div
      style={{
        background: "#ffffff",
        borderRadius: 12,
        border: "1px solid #e8eaed",
        boxShadow: "0 2px 12px rgba(0,0,0,0.06)",
        padding: "20px 24px",
        fontFamily: "inherit",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 20,
        }}
      >
        <svg
          width="18"
          height="18"
          viewBox="0 0 18 18"
          fill="none"
          style={{ flexShrink: 0 }}
        >
          <rect x="8" y="1" width="2" height="16" rx="1" fill="#6b8e23" />
          <circle cx="9" cy="4" r="2.5" fill="#1e3a5f" />
          <circle cx="9" cy="9" r="2.5" fill="#6b8e23" />
          <circle cx="9" cy="14" r="2.5" fill="#1e3a5f" />
        </svg>
        <span
          style={{
            fontSize: 15,
            fontWeight: 700,
            color: "#1a1a1a",
            letterSpacing: "-0.01em",
          }}
        >
          Recent Activity
        </span>
      </div>

      {/* Timeline */}
      {sortedRuns.length === 0 ? (
        <div
          style={{
            textAlign: "center",
            color: "#8c8c8c",
            fontSize: 13,
            padding: "24px 0",
          }}
        >
          No recent activity
        </div>
      ) : (
        <div
          style={{
            maxHeight: 420,
            overflowY: "auto",
            paddingRight: 4,
          }}
        >
          {sortedRuns.map((run, index) => {
            const color = getEventColor(run.status);
            const isLast = index === sortedRuns.length - 1;
            const title = `${run.display_name || run.state} — ${formatScraperType(run.scraper_type)}`;
            const description = getDescription(run);
            const time = run.created_at
              ? formatRelativeTime(run.created_at)
              : "";
            const icon = getStatusIcon(run.status);
            const isActive =
              run.status === "running" || run.status === "finalizing";

            return (
              <div
                key={run.run_id}
                style={{
                  display: "flex",
                  gap: 14,
                  position: "relative",
                  paddingBottom: isLast ? 0 : 20,
                }}
              >
                {/* Timeline column */}
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    width: 24,
                    flexShrink: 0,
                  }}
                >
                  {/* Dot */}
                  <div
                    style={{
                      width: 24,
                      height: 24,
                      borderRadius: "50%",
                      background: color,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      color: "#ffffff",
                      fontSize: 11,
                      fontWeight: 700,
                      flexShrink: 0,
                      boxShadow: isActive
                        ? `0 0 0 4px ${color}22`
                        : "none",
                      animation: isActive ? "pulse 2s infinite" : "none",
                    }}
                  >
                    {icon}
                  </div>
                  {/* Connecting line */}
                  {!isLast && (
                    <div
                      style={{
                        width: 2,
                        flex: 1,
                        background: "#e8eaed",
                        marginTop: 4,
                      }}
                    />
                  )}
                </div>

                {/* Content */}
                <div style={{ flex: 1, paddingTop: 1, minWidth: 0 }}>
                  <div
                    style={{
                      display: "flex",
                      alignItems: "flex-start",
                      justifyContent: "space-between",
                      gap: 8,
                    }}
                  >
                    <span
                      style={{
                        fontSize: 14,
                        fontWeight: 700,
                        color: "#1a1a1a",
                        lineHeight: 1.3,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        flex: 1,
                      }}
                    >
                      {title}
                    </span>
                    {time && (
                      <span
                        style={{
                          fontSize: 12,
                          color: "#8c8c8c",
                          whiteSpace: "nowrap",
                          flexShrink: 0,
                          lineHeight: 1.3,
                          paddingTop: 1,
                        }}
                      >
                        {time}
                      </span>
                    )}
                  </div>
                  <div
                    style={{
                      fontSize: 13,
                      color: "#5f6368",
                      marginTop: 2,
                      lineHeight: 1.4,
                    }}
                  >
                    {description}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Pulse animation for active runs */}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.7; }
        }
      `}</style>
    </div>
  );
}
