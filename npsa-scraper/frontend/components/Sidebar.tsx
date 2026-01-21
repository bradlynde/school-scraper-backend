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
};

type SidebarProps = {
  activeTab: 'school' | 'church' | 'running' | 'finished' | 'archive';
  onTabChange: (tab: 'school' | 'church' | 'running' | 'finished' | 'archive') => void;
  onRunSelect?: (runId: string) => void;
};

const Sidebar = ({ activeTab, onTabChange, onRunSelect }: SidebarProps) => {
  const { token, username, logout } = useAuth();
  const [runs, setRuns] = useState<RunMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  useEffect(() => {
    fetchRuns();
    // Refresh runs every 30 seconds
    const interval = setInterval(fetchRuns, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchRuns = async () => {
    if (!token) return;
    
    try {
      // Ensure API URL includes protocol
      let apiUrl = process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app";
      apiUrl = apiUrl.replace(/\/+$/, ''); // Remove trailing slashes
      if (!apiUrl.match(/^https?:\/\//)) {
        // If no protocol, assume https
        apiUrl = `https://${apiUrl}`;
      }
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
      // Ensure API URL includes protocol
      let apiUrl = process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app";
      apiUrl = apiUrl.replace(/\/+$/, ''); // Remove trailing slashes
      if (!apiUrl.match(/^https?:\/\//)) {
        // If no protocol, assume https
        apiUrl = `https://${apiUrl}`;
      }
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

  return (
    <aside className="h-full w-80 bg-white border-r border-gray-200 shadow-sm flex flex-col">
        {/* Logo Section */}
        <div className="p-6 border-b border-gray-200 flex items-center justify-center">
          <Image
            src="/npsa-logo.png"
            alt="Nonprofit Security Advisors"
            width={160}
            height={48}
            className="h-auto"
            priority
          />
        </div>

        {/* Navigation Items */}
      <nav className="p-4 space-y-2 flex-shrink-0">
          <button
            onClick={() => onTabChange('school')}
            className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
              activeTab === 'school'
                ? 'bg-[#1e3a5f] text-white shadow-md'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
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
                d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"
              />
            </svg>
            <span>School Scraper</span>
          </button>

          <button
            onClick={() => onTabChange('church')}
            className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
              activeTab === 'church'
                ? 'bg-[#1e3a5f] text-white shadow-md'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
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
                d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"
              />
            </svg>
            <span>Church Scraper</span>
          </button>

        <button
          onClick={() => onTabChange('running')}
          className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
            activeTab === 'running'
              ? 'bg-[#1e3a5f] text-white shadow-md'
              : 'text-gray-700 hover:bg-gray-100'
          }`}
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
              d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"
            />
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span>Running</span>
        </button>

        <button
          onClick={() => onTabChange('finished')}
          className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
            activeTab === 'finished'
              ? 'bg-[#1e3a5f] text-white shadow-md'
              : 'text-gray-700 hover:bg-gray-100'
          }`}
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
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span>Finished</span>
          </button>

          <button
            onClick={() => onTabChange('archive')}
            className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
              activeTab === 'archive'
                ? 'bg-[#1e3a5f] text-white shadow-md'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
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
                d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4"
              />
            </svg>
            <span>Archive</span>
          </button>
        </nav>

      {/* Run List Section */}
      {(activeTab === 'running' || activeTab === 'finished' || activeTab === 'archive') && (
        <div className="flex-1 overflow-y-auto min-h-0">
          <div className="p-4">
            <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-3">
              {activeTab === 'running' ? 'Active Runs' : activeTab === 'archive' ? 'Archived Runs' : 'Completed Runs'}
            </h3>
            {loading ? (
              <div className="text-center text-gray-500 py-4">Loading...</div>
            ) : (
              (() => {
                const filteredRuns = runs.filter(run => {
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
                          {/* Delete button - remove from UI only (no backend delete) */}
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              if (confirm(`Hide this run from the list? It will not be deleted from the backend.`)) {
                                // Frontend-only removal: filter the local runs list
                                setRuns(prev => prev.filter(r => r.run_id !== run.run_id));
                              }
                            }}
                            className="p-1.5 text-red-600 hover:text-red-700 hover:bg-red-50 rounded-lg transition-colors ml-auto"
                            title="Remove run from list"
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
                                  // Ensure API URL includes protocol
      let apiUrl = process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app";
      apiUrl = apiUrl.replace(/\/+$/, ''); // Remove trailing slashes
      if (!apiUrl.match(/^https?:\/\//)) {
        // If no protocol, assume https
        apiUrl = `https://${apiUrl}`;
      }
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
                                  // Ensure API URL includes protocol
      let apiUrl = process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app";
      apiUrl = apiUrl.replace(/\/+$/, ''); // Remove trailing slashes
      if (!apiUrl.match(/^https?:\/\//)) {
        // If no protocol, assume https
        apiUrl = `https://${apiUrl}`;
      }
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
        <div className="mt-auto p-4 border-t border-gray-200 flex-shrink-0">
          <div className="relative">
            <button
              onClick={() => setUserMenuOpen(!userMenuOpen)}
              className="w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 text-gray-700 hover:bg-gray-100"
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
              <span className="flex-1 text-left truncate">{username || "User"}</span>
              <svg
                className={`w-4 h-4 transition-transform ${userMenuOpen ? 'rotate-180' : ''}`}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
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

