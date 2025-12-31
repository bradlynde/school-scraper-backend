"use client";

import Image from "next/image";
import { useState, useEffect } from "react";

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
};

type SidebarProps = {
  activeTab: 'school' | 'church' | 'running' | 'finished';
  onTabChange: (tab: 'school' | 'church' | 'running' | 'finished') => void;
  onRunSelect?: (runId: string) => void;
};

const Sidebar = ({ activeTab, onTabChange, onRunSelect }: SidebarProps) => {
  const [runs, setRuns] = useState<RunMetadata[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchRuns();
    // Refresh runs every 30 seconds
    const interval = setInterval(fetchRuns, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchRuns = async () => {
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-200036585956.us-central1.run.app").replace(/\/+$/, '');
      const response = await fetch(`${apiUrl}/runs`);
      if (response.ok) {
        const data = await response.json();
        setRuns(data.runs || []);
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
      const date = new Date(dateString);
      return date.toLocaleDateString() + " " + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
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
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-200036585956.us-central1.run.app").replace(/\/+$/, '');
      const response = await fetch(`${apiUrl}/runs/${runId}/download`);
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
      }
    } catch (error) {
      console.error("Error downloading CSV:", error);
      alert("Failed to download CSV");
    }
  };

  return (
    <aside className="h-full w-64 bg-white border-r border-gray-200 shadow-sm flex flex-col">
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
      <nav className="p-4 space-y-2 border-b border-gray-200">
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
      </nav>

      {/* Run List Section */}
      {(activeTab === 'running' || activeTab === 'finished') && (
        <div className="flex-1 overflow-y-auto">
          <div className="p-4">
            <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-3">
              {activeTab === 'running' ? 'Active Runs' : 'Completed Runs'}
            </h3>
            {loading ? (
              <div className="text-center text-gray-500 py-4">Loading...</div>
            ) : (
              (() => {
                const filteredRuns = runs.filter(run => 
                  activeTab === 'running' 
                    ? run.status === 'running' 
                    : (run.status === 'completed' || run.status === 'error')
                );
                
                if (filteredRuns.length === 0) {
                  return (
                    <div className="text-center text-gray-500 py-4 text-sm">
                      {activeTab === 'running' ? 'No active runs' : 'No completed runs'}
                    </div>
                  );
                }
                
                return (
                  <div className="space-y-2">
                    {filteredRuns.map((run) => (
                      <div
                        key={run.run_id}
                        className="p-3 bg-gray-50 rounded-lg border border-gray-200 hover:border-gray-300 transition-colors cursor-pointer"
                        onClick={() => onRunSelect?.(run.run_id)}
                      >
                        <div className="flex items-start justify-between mb-2">
                          <div className="flex-1">
                            <div className="font-medium text-sm text-gray-900">
                              {run.state?.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase()) || 'Unknown State'}
                            </div>
                            <div className="text-xs text-gray-500 mt-1">
                              {formatDate(run.created_at)}
                            </div>
                          </div>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(run.status)}`}>
                            {run.status}
                          </span>
                        </div>
                        
                        {run.total_contacts !== undefined && (
                          <div className="text-xs text-gray-600 mt-2">
                            {run.total_contacts} contacts
                            {run.total_contacts_with_emails !== undefined && (
                              <span className="ml-2">({run.total_contacts_with_emails} with emails)</span>
                            )}
                          </div>
                        )}
                        
                        {run.status === "completed" && run.csv_filename && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              downloadCSV(run.run_id, run.csv_filename);
                            }}
                            className="mt-2 w-full text-xs px-2 py-1 bg-[#1e3a5f] text-white rounded hover:bg-[#2a4f7a] transition-colors"
                          >
                            Download CSV
                          </button>
                        )}
                      </div>
                    ))}
                  </div>
                );
              })()
            )}
          </div>
        </div>
      )}
    </aside>
  );
};

export default Sidebar;

