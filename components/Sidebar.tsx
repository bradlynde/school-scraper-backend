"use client";

import Image from "next/image";
import { useState, useEffect } from "react";
import { useAuth } from "../contexts/AuthContext";

type RunMetadata = {
  run_id: string;
  state: string;
  status: string;
  total_counties?: number;
  completed_counties?: string[];
  total_contacts?: number;
  total_contacts_with_emails?: number;
  created_at?: string;
  completed_at?: string;
  csv_filename?: string;
  archived?: boolean;
  scraper_type?: "school" | "church";
  churchesFound?: number;
  churchesProcessed?: number;
  countyChurches?: number[];
  schoolsFound?: number;
  schoolsProcessed?: number;
  countySchools?: number[];
};

export type TabType = 'home' | 'loe' | 'loe-archive' | 'loe-finished' | 'school' | 'church' | 'running' | 'finished' | 'archive';

type SidebarProps = {
  activeTab: TabType;
  onTabChange: (tab: TabType) => void;
  onRunSelect?: (runId: string) => void;
  onCollapsedChange?: (collapsed: boolean) => void;
  scraperContext?: 'school' | 'church';
};

// Koen = full access. Stuart = LOE + School + Church + Running/Finished/Archive. Brad = School + Running/Finished/Archive only.
const BRAD_TABS = new Set(["school", "running", "finished", "archive"] as const);
const STUART_TABS = new Set(["loe", "loe-archive", "loe-finished", "school", "church", "running", "finished", "archive"] as const);
const canAccessTab = (username: string | null, tab: string) => {
  if (username === "Koen") return true;
  if (username === "Stuart") return STUART_TABS.has(tab as any);
  if (username === "Brad") return BRAD_TABS.has(tab as any);
  return false;
};

/** Returns true if run belongs to the given scraper context (handles scraper_type and legacy runs) */
function runMatchesScraperContext(run: RunMetadata, context: "school" | "church"): boolean {
  // Only exclude runs that clearly belong to the other context
  if (run.scraper_type === "school" && context === "church") return false;
  if (run.scraper_type === "church" && context === "school") return false;
  // Infer from payload when scraper_type is missing (legacy or shared-storage runs)
  const isChurch = run.churchesFound !== undefined || run.churchesProcessed !== undefined || (run.countyChurches && run.countyChurches.length > 0);
  const isSchool = run.schoolsFound !== undefined || run.schoolsProcessed !== undefined || (run.countySchools && run.countySchools.length > 0);
  if (context === "church") return !isSchool; // show unless clearly a school run
  return !isChurch; // school context: show unless clearly a church run
}

const getApiUrl = (scraperContext: 'school' | 'church') => {
  const isChurch = scraperContext === 'church';
  let url = isChurch
    ? (process.env.NEXT_PUBLIC_CHURCH_API_URL || "https://church-scraper-backend-production.up.railway.app")
    : (process.env.NEXT_PUBLIC_SCHOOL_API_URL || "https://school-scraper-backend-production.up.railway.app");
  url = url.replace(/\/+$/, '');
  if (!url.match(/^https?:\/\//)) url = `https://${url}`;
  return url;
};

const Sidebar = ({ activeTab, onTabChange, onRunSelect, onCollapsedChange, scraperContext = 'school' }: SidebarProps) => {
  const { token, username, logout } = useAuth();
  const isDev = username === "Koen";
  const [runs, setRuns] = useState<RunMetadata[]>([]);
  const [collapsed, setCollapsed] = useState(true);
  const [loading, setLoading] = useState(true);
  const [schoolExpanded, setSchoolExpanded] = useState(false);
  const [churchExpanded, setChurchExpanded] = useState(false);
  const [loeExpanded, setLoeExpanded] = useState(false);

  // Auto-expand parent when it becomes active; collapse others
  useEffect(() => {
    if (activeTab === 'school' || activeTab === 'running' || activeTab === 'finished' || activeTab === 'archive') {
      if (scraperContext === 'school') {
        setSchoolExpanded(true);
        setChurchExpanded(false);
        setLoeExpanded(false);
      }
    } else if (activeTab === 'church') {
      setChurchExpanded(true);
      setSchoolExpanded(false);
      setLoeExpanded(false);
    } else if (activeTab === 'loe' || activeTab === 'loe-archive' || activeTab === 'loe-finished') {
      setLoeExpanded(true);
      setSchoolExpanded(false);
      setChurchExpanded(false);
    }
  }, [activeTab, scraperContext]);

  useEffect(() => {
    onCollapsedChange?.(collapsed);
  }, [collapsed, onCollapsedChange]);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [deleteConfirmRunId, setDeleteConfirmRunId] = useState<string | null>(null);

  useEffect(() => {
    fetchRuns();
    // Refresh runs every 30 seconds
    const interval = setInterval(fetchRuns, 30000);
    return () => clearInterval(interval);
  }, [scraperContext]);

  const fetchRuns = async () => {
    if (!token) return;
    
    try {
      const apiUrl = getApiUrl(scraperContext);
      const response = await fetch(`${apiUrl}/runs`, {
        headers: {
          "Authorization": `Bearer ${token}`,
        },
      });
      if (response.ok) {
        const data = await response.json();
        setRuns(data.runs || []);
      } else if (response.status === 401) {
        logout();
      }
    } catch (error) {
      console.error("Error fetching runs:", error);
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return "Unknown";
    try {
      // Parse the date string - handle both ISO strings and other formats
      const date = new Date(dateString);
      
      // Check if date is valid
      if (isNaN(date.getTime())) {
        return dateString;
      }
      
      // Format date and time in US Central Time
      const dateStr = date.toLocaleDateString('en-US', { 
        timeZone: 'America/Chicago',
        month: 'numeric',
        day: 'numeric',
        year: 'numeric'
      });
      
      const timeStr = date.toLocaleTimeString('en-US', { 
        timeZone: 'America/Chicago',
        hour: '2-digit', 
        minute: '2-digit',
        hour12: true
      });
      
      return `${dateStr} ${timeStr}`;
    } catch {
      return dateString;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "completed":
        return "text-green-600 bg-green-50";
      case "running":
        return "text-blue-600 bg-blue-50";
      case "error":
        return "text-red-600 bg-red-50";
      default:
        return "text-gray-600 bg-gray-50";
    }
  };

  const downloadCSV = async (runId: string, filename?: string) => {
    if (!token) return;
    
    try {
      const apiUrl = getApiUrl(scraperContext);
      const response = await fetch(`${apiUrl}/runs/${runId}/download`, {
        headers: {
          "Authorization": `Bearer ${token}`,
        },
      });
      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename || `run_${runId}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      } else if (response.status === 401) {
        logout();
      }
    } catch (error) {
      console.error("Error downloading CSV:", error);
      alert("Failed to download CSV");
    }
  };

  const ChevronDown = ({ expanded }: { expanded: boolean }) => (
    <svg className={`w-4 h-4 flex-shrink-0 transition-transform ${expanded ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
    </svg>
  );

  const NavBtn = ({ tab, icon, label, restricted }: { tab: TabType; icon: React.ReactNode; label: string; restricted?: boolean }) => {
    const allowed = !restricted || canAccessTab(username, tab);
    const active = activeTab === tab;
    return (
      <button
        onClick={() => allowed && onTabChange(tab)}
        disabled={!allowed}
        title={label}
        className={`w-full flex items-center gap-4 py-3 rounded-lg font-medium transition-all duration-200 ${
          collapsed ? "justify-center px-2" : "px-4"
        } ${
          !allowed
            ? "text-gray-400 bg-gray-50 cursor-not-allowed opacity-75"
            : active
              ? "bg-[#1e3a5f] text-white shadow-md"
              : "text-gray-700 hover:bg-gray-100"
        }`}
      >
        {icon}
        {!collapsed && (
          <>
            <span className="flex-1 text-left truncate whitespace-nowrap overflow-hidden">{label}</span>
            {!allowed && <span className="text-[10px] font-semibold text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded shrink-0">In Development</span>}
          </>
        )}
      </button>
    );
  };

  const SubNavBtn = ({ tab, label, parent }: { tab: TabType; label: string; parent: 'school' | 'church' | 'loe' }) => {
    const active = parent === 'loe'
      ? activeTab === tab
      : activeTab === tab && scraperContext === parent;
    return (
      <button
        onClick={() => onTabChange(tab)}
        title={label}
        className={`w-full flex items-center gap-3 py-2.5 rounded-lg font-medium transition-all duration-200 pl-4 pr-4 ${
          active ? "bg-[#1e3a5f]/10 text-[#1e3a5f]" : "text-gray-600 hover:bg-gray-100"
        }`}
      >
        <span className="flex-1 text-left truncate text-sm">{label}</span>
      </button>
    );
  };

  const DocIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
    </svg>
  );
  const ArchiveIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4" />
    </svg>
  );
  const BookIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
    </svg>
  );
  const ChurchIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
    </svg>
  );
  const PlayIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  );
  const CheckIcon = () => (
    <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  );

  return (
    <aside
      className="h-full bg-white border-r border-gray-200 shadow-sm flex flex-col transition-all duration-300 ease-out overflow-hidden"
      style={{ width: collapsed ? 72 : 320 }}
      onMouseEnter={() => { setCollapsed(false); onCollapsedChange?.(false); }}
      onMouseLeave={() => { setCollapsed(true); onCollapsedChange?.(true); }}
    >
        {/* Logo Section — collapses to icon-sized; clickable to go home */}
        <button
          onClick={() => onTabChange("home")}
          className="w-full p-4 border-b border-gray-200 flex items-center justify-center min-h-[72px] hover:bg-gray-50 transition-colors"
        >
          <div className="transition-all duration-300 ease-out overflow-hidden flex items-center justify-center" style={{ width: collapsed ? 40 : 160 }}>
            <Image
              src="/npsa-logo.png"
              alt="NPSA"
              width={collapsed ? 40 : 160}
              height={collapsed ? 12 : 48}
              className="h-auto object-contain"
              priority
            />
          </div>
        </button>

        {/* Navigation Items — 3 parent tabs with expandable sub-tabs */}
      <nav className="p-3 space-y-1 flex-shrink-0">
          {/* School Scraper */}
          <div>
            <button
              onClick={() => onTabChange("school")}
              title="School Scraper"
              className={`w-full flex items-center gap-4 py-3 rounded-lg font-medium transition-all duration-200 ${
                collapsed ? "justify-center px-2" : "px-4"
              } ${
                activeTab === "school" || ((activeTab === "running" || activeTab === "finished" || activeTab === "archive") && scraperContext === "school")
                  ? "bg-[#1e3a5f] text-white shadow-md"
                  : "text-gray-700 hover:bg-gray-100"
              }`}
            >
              <BookIcon />
              {!collapsed && <span className="flex-1 text-left truncate">School Scraper</span>}
              {!collapsed && (
                <span
                  role="button"
                  tabIndex={0}
                  onClick={(e) => { e.stopPropagation(); setSchoolExpanded((v) => !v); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); setSchoolExpanded((v) => !v); } }}
                  className="flex-shrink-0 p-1 -m-1 rounded hover:opacity-80 transition-opacity"
                  title={schoolExpanded ? "Collapse" : "Expand"}
                >
                  <ChevronDown expanded={schoolExpanded} />
                </span>
              )}
            </button>
            {!collapsed && schoolExpanded && (
              <div className="ml-2 mt-1 space-y-0.5 border-l-2 border-gray-200 pl-2">
                <SubNavBtn tab="running" label="Running" parent="school" />
                <SubNavBtn tab="finished" label="Finished" parent="school" />
                <SubNavBtn tab="archive" label="Archive" parent="school" />
              </div>
            )}
          </div>

          {/* Church Scraper */}
          <div>
            <button
              onClick={() => canAccessTab(username, "church") && onTabChange("church")}
              disabled={!canAccessTab(username, "church")}
              title="Church Scraper"
              className={`w-full flex items-center gap-4 py-3 rounded-lg font-medium transition-all duration-200 ${
                collapsed ? "justify-center px-2" : "px-4"
              } ${
                !canAccessTab(username, "church")
                  ? "text-gray-400 bg-gray-50 cursor-not-allowed opacity-75"
                  : activeTab === "church" || ((activeTab === "running" || activeTab === "finished" || activeTab === "archive") && scraperContext === "church")
                    ? "bg-[#1e3a5f] text-white shadow-md"
                    : "text-gray-700 hover:bg-gray-100"
              }`}
            >
              <ChurchIcon />
              {!collapsed && <span className="flex-1 text-left truncate">Church Scraper</span>}
              {!collapsed && canAccessTab(username, "church") && (
                <span
                  role="button"
                  tabIndex={0}
                  onClick={(e) => { e.stopPropagation(); setChurchExpanded((v) => !v); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); setChurchExpanded((v) => !v); } }}
                  className="flex-shrink-0 p-1 -m-1 rounded hover:opacity-80 transition-opacity"
                  title={churchExpanded ? "Collapse" : "Expand"}
                >
                  <ChevronDown expanded={churchExpanded} />
                </span>
              )}
            </button>
            {!collapsed && churchExpanded && (
              <div className="ml-2 mt-1 space-y-0.5 border-l-2 border-gray-200 pl-2">
                <SubNavBtn tab="running" label="Running" parent="church" />
                <SubNavBtn tab="finished" label="Finished" parent="church" />
                <SubNavBtn tab="archive" label="Archive" parent="church" />
              </div>
            )}
          </div>

          {/* LOE Generator */}
          <div>
            <button
              onClick={() => canAccessTab(username, "loe") && onTabChange("loe")}
              disabled={!canAccessTab(username, "loe")}
              title="LOE Generator"
              className={`w-full flex items-center gap-4 py-3 rounded-lg font-medium transition-all duration-200 ${
                collapsed ? "justify-center px-2" : "px-4"
              } ${
                !canAccessTab(username, "loe")
                  ? "text-gray-400 bg-gray-50 cursor-not-allowed opacity-75"
                  : activeTab === "loe" || activeTab === "loe-archive" || activeTab === "loe-finished"
                    ? "bg-[#1e3a5f] text-white shadow-md"
                    : "text-gray-700 hover:bg-gray-100"
              }`}
            >
              <DocIcon />
              {!collapsed && <span className="flex-1 text-left truncate">LOE Generator</span>}
              {!collapsed && canAccessTab(username, "loe") && (
                <span
                  role="button"
                  tabIndex={0}
                  onClick={(e) => { e.stopPropagation(); setLoeExpanded((v) => !v); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); setLoeExpanded((v) => !v); } }}
                  className="flex-shrink-0 p-1 -m-1 rounded hover:opacity-80 transition-opacity"
                  title={loeExpanded ? "Collapse" : "Expand"}
                >
                  <ChevronDown expanded={loeExpanded} />
                </span>
              )}
            </button>
            {!collapsed && loeExpanded && (
              <div className="ml-2 mt-1 space-y-0.5 border-l-2 border-gray-200 pl-2">
                <SubNavBtn tab="loe-finished" label="Finished" parent="loe" />
                <SubNavBtn tab="loe-archive" label="Archive" parent="loe" />
              </div>
            )}
          </div>
        </nav>

      {/* Run List Section — hidden when collapsed */}
      {(activeTab === 'running' || activeTab === 'finished' || activeTab === 'archive') && !collapsed && (
        <div className="flex-1 overflow-y-auto min-h-0">
          <div className="p-4">
            <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-3">
              {activeTab === 'running' ? 'Active Runs' : activeTab === 'archive' ? 'Archived Runs' : 'Completed Runs'}
            </h3>
            {loading ? (
              <div className="text-center text-gray-500 py-4">Loading...</div>
            ) : (
              (() => {
                const filteredRuns = runs
                  .filter(run => runMatchesScraperContext(run, scraperContext))
                  .filter(run => {
                    if (activeTab === 'running') {
                      return run.status === 'running' && !run.archived;
                    } else if (activeTab === 'archive') {
                      return run.archived === true;
                    } else {
                      return (run.status === 'completed' || run.status === 'error') && !run.archived;
                    }
                  });
                
                if (filteredRuns.length === 0) {
                  return (
                    <div className="text-center text-gray-500 py-4 text-sm">
                      {activeTab === 'running' ? 'No active runs' : activeTab === 'archive' ? 'No archived runs' : 'No completed runs'}
                    </div>
                  );
                }
                
                return (
                  <div className="space-y-3">
                    {filteredRuns.map((run) => {
                      const completedCount = run.completed_counties?.length || 0;
                      const totalCount = run.total_counties || 0;
                      const progressPercent = totalCount > 0 ? Math.round((completedCount / totalCount) * 100) : 0;
                      
                      return (
                      <div
                        key={run.run_id}
                        className="p-4 bg-white rounded-2xl border border-gray-200 shadow-sm hover:shadow-lg transition-all relative group"
                        style={{ borderRadius: '16px' }}
                      >
                        {deleteConfirmRunId === run.run_id ? (
                          /* Delete Confirmation - replaces card content */
                          <div className="flex flex-col items-center justify-center py-6">
                            <p className="text-sm font-semibold text-gray-900 mb-4 text-center">Delete Permanently?</p>
                            <div className="flex items-center justify-center gap-3">
                              {/* Deny button */}
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setDeleteConfirmRunId(null);
                                }}
                                className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg transition-colors"
                              >
                                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                                </svg>
                                <span className="text-sm font-medium">Cancel</span>
                              </button>
                              {/* Confirm button */}
                              <button
                                onClick={async (e) => {
                                  e.stopPropagation();
                                  setDeleteConfirmRunId(null);
                                  try {
                                    const apiUrl = getApiUrl(scraperContext);
                                    const response = await fetch(`${apiUrl}/runs/${run.run_id}/delete`, {
                                      method: 'DELETE',
                                      headers: {
                                        "Authorization": `Bearer ${token}`,
                                      },
                                    });
                                    if (response.ok) {
                                      // Remove locally so it never reappears in the list this session
                                      setRuns(prev => prev.filter(r => r.run_id !== run.run_id));
                                    } else if (response.status === 401) {
                                      logout();
                                    } else {
                                      // Try to surface backend error for easier debugging
                                      try {
                                        const errorData = await response.json();
                                        alert(errorData.error || 'Failed to delete run');
                                      } catch {
                                        alert('Failed to delete run');
                                      }
                                    }
                                  } catch (error) {
                                    console.error('Error deleting run:', error);
                                    alert('Failed to delete run');
                                  }
                                }}
                                className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors"
                              >
                                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                                </svg>
                                <span className="text-sm font-medium">Delete</span>
                              </button>
                            </div>
                          </div>
                        ) : (
                          <>
                            <div 
                              className="cursor-pointer"
                              onClick={() => onRunSelect?.(run.run_id)}
                            >
                              <div className="flex items-start justify-between mb-3">
                                <div className="flex-1 min-w-0">
                                  <div className="font-semibold text-sm text-gray-900 truncate">
                                    {run.state?.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()) || 'Unknown State'}
                                  </div>
                                  <div className="text-xs text-gray-500 mt-1">
                                    {formatDate(run.created_at)}
                                  </div>
                                </div>
                                {/* Live badge/pill with pulsing dot for running (dashboard-20) */}
                                <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                                  {run.status === "running" && (
                                    <div className="relative">
                                      <div className="absolute inset-0 w-2 h-2 rounded-full bg-blue-500 opacity-30 animate-ping"></div>
                                      <div className="relative w-2 h-2 rounded-full bg-blue-500"></div>
                                    </div>
                                  )}
                                  <span className={`px-2.5 py-1 rounded-full text-xs font-semibold ${getStatusColor(run.status)}`}>
                                    {run.status === "running" ? "Running" : run.status === "completed" ? "Finished" : run.status === "error" ? "Error" : run.status === "finalizing" ? "Finalizing" : run.status}
                                  </span>
                                </div>
                              </div>
                              
                              {/* Tiny progress indicator (dashboard-21) */}
                              {run.status === "running" && totalCount > 0 && (
                                <div className="mb-2">
                                  <div className="flex items-center justify-between text-xs text-gray-600 mb-1">
                                    <span>{completedCount}/{totalCount}</span>
                                    <span>{progressPercent}%</span>
                                  </div>
                                  <div className="w-full bg-gray-200 rounded-full h-1.5">
                                    <div 
                                      className="bg-[#1e3a5f] h-1.5 rounded-full transition-all duration-300"
                                      style={{ width: `${progressPercent}%` }}
                                    ></div>
                                  </div>
                                </div>
                              )}
                              
                              {run.total_contacts !== undefined && (
                                <div className="text-xs text-gray-600 mt-2">
                                  {run.total_contacts} contacts
                                </div>
                              )}
                            </div>
                            
                            {/* Quick action icon buttons (dashboard-22) */}
                            <div className="flex items-center gap-2 mt-3 pt-3 border-t border-gray-100">
                              {/* Delete button - show confirmation */}
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setDeleteConfirmRunId(run.run_id);
                                }}
                                className="p-1.5 text-red-600 hover:text-red-700 hover:bg-red-50 rounded-lg transition-colors"
                                title="Delete run"
                              >
                                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                                </svg>
                              </button>
                              
                              {/* Archive button - only in finished tab */}
                              {activeTab === 'finished' && (
                            <button
                              onClick={async (e) => {
                                e.stopPropagation();
                                try {
                                  const apiUrl = getApiUrl(scraperContext);
                                  const response = await fetch(`${apiUrl}/runs/${run.run_id}/archive`, {
                                    method: 'POST',
                                    headers: {
                                      "Authorization": `Bearer ${token}`,
                                    },
                                  });
                                  if (response.ok) {
                                    fetchRuns();
                                  } else if (response.status === 401) {
                                    logout();
                                  } else {
                                    alert('Failed to archive run');
                                  }
                                } catch (error) {
                                  console.error('Error archiving run:', error);
                                  alert('Failed to archive run');
                                }
                              }}
                              className="p-1.5 text-gray-600 hover:text-gray-700 hover:bg-gray-100 rounded transition-colors"
                              title="Archive run"
                            >
                              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4" />
                              </svg>
                            </button>
                          )}
                          
                          {/* Unarchive button - only in archive tab */}
                          {activeTab === 'archive' && (
                            <button
                              onClick={async (e) => {
                                e.stopPropagation();
                                try {
                                  const apiUrl = getApiUrl(scraperContext);
                                  const response = await fetch(`${apiUrl}/runs/${run.run_id}/unarchive`, {
                                    method: 'POST',
                                    headers: {
                                      "Authorization": `Bearer ${token}`,
                                    },
                                  });
                                  if (response.ok) {
                                    fetchRuns();
                                  } else if (response.status === 401) {
                                    logout();
                                  } else {
                                    alert('Failed to unarchive run');
                                  }
                                } catch (error) {
                                  console.error('Error unarchiving run:', error);
                                  alert('Failed to unarchive run');
                                }
                              }}
                              className="p-1.5 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded transition-colors"
                              title="Unarchive run"
                            >
                              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                              </svg>
                            </button>
                          )}
                          
                              {/* Download CSV button */}
                              {run.status === "completed" && run.csv_filename && (
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    downloadCSV(run.run_id, run.csv_filename);
                                  }}
                                  className="ml-auto text-xs px-2 py-1 bg-[#1e3a5f] text-white rounded hover:bg-[#2a4f7a] transition-colors"
                                >
                                  Download CSV
                                </button>
                              )}
                            </div>
                          </>
                        )}
                      </div>
                      );
                    })}
                  </div>
                );
              })()
            )}
          </div>
      </div>
      )}

        {/* User Section at Bottom - Always stays at bottom */}
        <div className="mt-auto p-3 border-t border-gray-200 flex-shrink-0">
          <div className="relative">
            <button
              onClick={() => setUserMenuOpen(!userMenuOpen)}
              title={username || "User"}
              className={`w-full flex items-center gap-4 py-3 rounded-lg font-medium transition-all duration-200 text-gray-700 hover:bg-gray-100 ${collapsed ? "justify-center px-2" : "px-4"}`}
            >
              <svg
                className="w-5 h-5 flex-shrink-0"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"
                />
              </svg>
              {!collapsed && (
                <>
                  <span className="flex-1 text-left truncate">{username || "User"}</span>
                  <svg
                    className={`w-4 h-4 transition-transform ${userMenuOpen ? 'rotate-180' : ''}`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </>
              )}
            </button>
            
            {userMenuOpen && (
              <div className="absolute bottom-full left-0 right-0 mb-2 bg-white border border-gray-200 rounded-lg shadow-lg overflow-hidden z-50">
                <button
                  onClick={() => {
                    logout();
                    setUserMenuOpen(false);
                  }}
                  className="w-full flex items-center gap-3 px-4 py-3 text-left text-gray-700 hover:bg-gray-100 transition-colors"
                >
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
                  </svg>
                  <span>Sign Out</span>
                </button>
              </div>
            )}
          </div>
      </div>
    </aside>
  );
};

export default Sidebar;

