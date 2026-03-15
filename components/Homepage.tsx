"use client";

import React, { useState, useEffect } from "react";
import { useAuth } from "../contexts/AuthContext";

type RunMetadata = {
  run_id: string;
  state: string;
  status: string;
  total_counties?: number;
  completed_counties?: string[];
  total_contacts?: number;
  created_at?: string;
  completed_at?: string;
};

const getSchoolApiUrl = () => {
  let url = process.env.NEXT_PUBLIC_SCHOOL_API_URL || "https://school-scraper-backend-production.up.railway.app";
  url = url.replace(/\/+$/, "");
  if (!url.match(/^https?:\/\//)) url = `https://${url}`;
  return url;
};

const getChurchApiUrl = () => {
  let url = process.env.NEXT_PUBLIC_CHURCH_API_URL || "https://church-scraper-backend-production.up.railway.app";
  url = url.replace(/\/+$/, "");
  if (!url.match(/^https?:\/\//)) url = `https://${url}`;
  return url;
};

export default function Homepage({
  onNavigate,
  onViewRun,
}: {
  onNavigate?: (tab: string) => void;
  onViewRun?: (runId: string, source: "school" | "church", status: string) => void;
}) {
  const { token, logout } = useAuth();
  const [schoolRuns, setSchoolRuns] = useState<RunMetadata[]>([]);
  const [churchRuns, setChurchRuns] = useState<RunMetadata[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchRuns() {
      if (!token) return;
      try {
        const [schoolRes, churchRes] = await Promise.all([
          fetch(`${getSchoolApiUrl()}/runs`, {
            headers: { Authorization: `Bearer ${token}` },
          }),
          fetch(`${getChurchApiUrl()}/runs`, {
            headers: { Authorization: `Bearer ${token}` },
          }),
        ]);
        if (schoolRes.ok) {
          const data = await schoolRes.json();
          setSchoolRuns(data.runs || []);
        } else if (schoolRes.status === 401) logout();
        if (churchRes.ok) {
          const data = await churchRes.json();
          setChurchRuns(data.runs || []);
        } else if (churchRes.status === 401) logout();
      } catch (err) {
        console.error("Error fetching runs for homepage:", err);
      } finally {
        setLoading(false);
      }
    }
    fetchRuns();
  }, [token, logout]);

  const totalContacts =
    [...schoolRuns, ...churchRuns].reduce((sum, r) => sum + (r.total_contacts || 0), 0);
  const completedRuns = [...schoolRuns, ...churchRuns].filter(
    (r) => r.status === "completed" || r.status === "error"
  ).length;
  const activeRuns = [...schoolRuns, ...churchRuns].filter((r) => r.status === "running").length;

  const recentActivity = [...schoolRuns, ...churchRuns]
    .map((r) => ({
      ...r,
      source: schoolRuns.some((s) => s.run_id === r.run_id) ? "school" as const : "church" as const,
    }))
    .sort((a, b) => {
      const aTime = new Date(a.created_at || 0).getTime();
      const bTime = new Date(b.created_at || 0).getTime();
      return bTime - aTime;
    })
    .slice(0, 8);

  const formatDate = (dateString?: string) => {
    if (!dateString) return "Unknown";
    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return dateString;
      return date.toLocaleDateString("en-US", {
        timeZone: "America/Chicago",
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: true,
      });
    } catch {
      return dateString;
    }
  };

  const getStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
      completed: "bg-green-100 text-green-800",
      running: "bg-blue-100 text-blue-800",
      error: "bg-red-100 text-red-800",
    };
    const label = status === "completed" ? "Finished" : status === "running" ? "Running" : status;
    return (
      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status] || "bg-gray-100 text-gray-800"}`}>
        {label}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center p-12">
        <div className="text-gray-500">Loading activity...</div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto p-8 md:p-12">
      <div className="max-w-4xl mx-auto space-y-10">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Recent Activity</h1>
          <p className="text-gray-600 mt-1">Overview of your scraping and generation activity</p>
        </div>

        {/* Stats cards */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
          <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6">
            <p className="text-sm font-medium text-gray-500 uppercase tracking-wide">Total Contacts</p>
            <p className="text-3xl font-bold text-[#1e3a5f] mt-1">
              {totalContacts.toLocaleString()}
            </p>
            <p className="text-xs text-gray-500 mt-2">Across all completed runs</p>
          </div>
          <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6">
            <p className="text-sm font-medium text-gray-500 uppercase tracking-wide">Completed Runs</p>
            <p className="text-3xl font-bold text-[#1e3a5f] mt-1">{completedRuns}</p>
            <p className="text-xs text-gray-500 mt-2">School + Church scrapes</p>
          </div>
          <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6">
            <p className="text-sm font-medium text-gray-500 uppercase tracking-wide">Active Runs</p>
            <p className="text-3xl font-bold text-[#1e3a5f] mt-1">{activeRuns}</p>
            <p className="text-xs text-gray-500 mt-2">Currently in progress</p>
          </div>
        </div>

        {/* Recent activity list */}
        <div className="bg-white rounded-2xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Latest Runs</h2>
          </div>
          <div className="divide-y divide-gray-100">
            {recentActivity.length === 0 ? (
              <div className="px-6 py-12 text-center text-gray-500">
                No runs yet. Start a School or Church scrape to see activity here.
              </div>
            ) : (
              recentActivity.map((run) => (
                <div
                  key={run.run_id}
                  className="px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
                >
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-gray-900 truncate">
                      {run.state?.replace("_", " ").replace(/\b\w/g, (l) => l.toUpperCase()) || "Unknown"}
                    </p>
                    <p className="text-sm text-gray-500 mt-0.5">
                      {formatDate(run.created_at)} · {run.source === "school" ? "School" : "Church"} Scraper
                    </p>
                  </div>
                  <div className="flex items-center gap-4 ml-4 flex-shrink-0">
                    {run.total_contacts !== undefined && (
                      <span className="text-sm text-gray-600">{run.total_contacts} contacts</span>
                    )}
                    {getStatusBadge(run.status)}
                    {onViewRun && (run.status === "running" || run.status === "completed" || run.status === "error") && (
                      <button
                        onClick={() => onViewRun(run.run_id, run.source, run.status)}
                        className="text-sm font-medium text-[#1e3a5f] hover:underline"
                      >
                        View
                      </button>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Quick actions */}
        <div className="flex flex-wrap gap-4">
          {onNavigate && (
            <>
              <button
                onClick={() => onNavigate("school")}
                className="px-6 py-3 bg-[#1e3a5f] text-white rounded-xl font-medium hover:bg-[#2a4f7a] transition-colors"
              >
                Start School Scrape
              </button>
              <button
                onClick={() => onNavigate("church")}
                className="px-6 py-3 bg-[#1e3a5f] text-white rounded-xl font-medium hover:bg-[#2a4f7a] transition-colors"
              >
                Start Church Scrape
              </button>
              <button
                onClick={() => onNavigate("loe")}
                className="px-6 py-3 bg-white border border-gray-200 text-gray-700 rounded-xl font-medium hover:bg-gray-50 transition-colors"
              >
                Generate LOE
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
