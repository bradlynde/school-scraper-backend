"use client";
// Force Vercel redeploy - 2025-12-13

import React, { useState, useEffect } from "react";
import Sidebar from "../components/Sidebar";
import LoginForm from "../components/LoginForm";
import { useAuth } from "../contexts/AuthContext";

type StepSummary = {
  name: string;
  schoolsFound?: number;
  pagesDiscovered?: number;
  emailsFound?: number;
  contactsWithEmails?: number;
  contactsWithoutEmails?: number;
  finalContactsWithEmails?: number;
  finalContactsWithoutEmails?: number;
  [key: string]: number | string | undefined;
};

type PipelineSummary = {
  status: string;
  steps: StepSummary[];
  totalContacts: number;
  schoolsFound: number;
  schoolsProcessed?: number;
  runId: string;
  csvData?: string;
  csvFilename?: string;
  estimatedTimeRemaining?: number;
  currentStep?: number;
  totalSteps?: number;
  countiesProcessed?: number;
  totalCounties?: number;
  currentCounty?: string;
  currentCountyIndex?: number;
  statusMessage?: string;
  countyContacts?: number[];
  countySchools?: number[];
};

type ViewState = "start" | "progress" | "summary" | "running" | "finished" | "archive";

// Single source of truth for run progress/state
type RunProgress = {
  totalCounties: number;
  completedCounties: number;
  currentCountyIndex: number; // 1-based index of currently processing county
  currentCountyName?: string;
  schoolsProcessed: number;
  contactsProcessed: number;
  errorCount: number;
  status: "idle" | "running" | "paused" | "finished" | "error";
  elapsedMs: number;
  etaRemainingMs?: number;
  lastUpdatedAt?: Date;
};

// Activity log entry type
type ActivityLogEntry = {
  level: "info" | "success" | "warning" | "error";
  message: string;
  timestamp: Date;
};

const US_STATES = [
  { value: "alabama", label: "Alabama" },
  { value: "alaska", label: "Alaska" },
  { value: "arizona", label: "Arizona" },
  { value: "arkansas", label: "Arkansas" },
  { value: "california", label: "California" },
  { value: "colorado", label: "Colorado" },
  { value: "connecticut", label: "Connecticut" },
  { value: "delaware", label: "Delaware" },
  { value: "florida", label: "Florida" },
  { value: "georgia", label: "Georgia" },
  { value: "hawaii", label: "Hawaii" },
  { value: "idaho", label: "Idaho" },
  { value: "illinois", label: "Illinois" },
  { value: "indiana", label: "Indiana" },
  { value: "iowa", label: "Iowa" },
  { value: "kansas", label: "Kansas" },
  { value: "kentucky", label: "Kentucky" },
  { value: "louisiana", label: "Louisiana" },
  { value: "maine", label: "Maine" },
  { value: "maryland", label: "Maryland" },
  { value: "massachusetts", label: "Massachusetts" },
  { value: "michigan", label: "Michigan" },
  { value: "minnesota", label: "Minnesota" },
  { value: "mississippi", label: "Mississippi" },
  { value: "missouri", label: "Missouri" },
  { value: "montana", label: "Montana" },
  { value: "nebraska", label: "Nebraska" },
  { value: "nevada", label: "Nevada" },
  { value: "new_hampshire", label: "New Hampshire" },
  { value: "new_jersey", label: "New Jersey" },
  { value: "new_mexico", label: "New Mexico" },
  { value: "new_york", label: "New York" },
  { value: "north_carolina", label: "North Carolina" },
  { value: "north_dakota", label: "North Dakota" },
  { value: "ohio", label: "Ohio" },
  { value: "oklahoma", label: "Oklahoma" },
  { value: "oregon", label: "Oregon" },
  { value: "pennsylvania", label: "Pennsylvania" },
  { value: "rhode_island", label: "Rhode Island" },
  { value: "south_carolina", label: "South Carolina" },
  { value: "south_dakota", label: "South Dakota" },
  { value: "tennessee", label: "Tennessee" },
  { value: "texas", label: "Texas" },
  { value: "utah", label: "Utah" },
  { value: "vermont", label: "Vermont" },
  { value: "virginia", label: "Virginia" },
  { value: "washington", label: "Washington" },
  { value: "west_virginia", label: "West Virginia" },
  { value: "wisconsin", label: "Wisconsin" },
  { value: "wyoming", label: "Wyoming" },
];

// County counts per state (from assets/data/state_counties/*.txt files)
const STATE_COUNTY_COUNTS: Record<string, number> = {
  "alabama": 67,
  "alaska": 29,
  "arizona": 15,
  "arkansas": 75,
  "california": 58,
  "colorado": 64,
  "connecticut": 8,
  "delaware": 3,
  "florida": 67,
  "georgia": 159,
  "hawaii": 5,
  "idaho": 44,
  "illinois": 101,
  "indiana": 92,
  "iowa": 99,
  "kansas": 105,
  "kentucky": 120,
  "louisiana": 64,
  "maine": 16,
  "maryland": 24,
  "massachusetts": 14,
  "michigan": 83,
  "minnesota": 87,
  "mississippi": 82,
  "missouri": 115,
  "montana": 56,
  "nebraska": 93,
  "nevada": 16,
  "new_hampshire": 10,
  "new_jersey": 21,
  "new_mexico": 33,
  "new_york": 62,
  "north_carolina": 100,
  "north_dakota": 53,
  "ohio": 88,
  "oklahoma": 77,
  "oregon": 36,
  "pennsylvania": 67,
  "rhode_island": 5,
  "south_carolina": 46,
  "south_dakota": 66,
  "tennessee": 95,
  "texas": 254,
  "utah": 29,
  "vermont": 14,
  "virginia": 95,
  "washington": 39,
  "west_virginia": 55,
  "wisconsin": 72,
  "wyoming": 23,
};

// Average time per county: 579.4 seconds (~9.7 minutes)
const SECONDS_PER_COUNTY = 579.4;

// Helper function to format estimated time
function formatEstimatedTime(seconds: number): string {
  if (seconds < 3600) {
    // Less than 1 hour, show as minutes
    return `~${Math.round(seconds / 60)}m`;
  } else if (seconds < 86400) {
    // Less than 1 day, show as hours
    const hours = seconds / 3600;
    return `~${hours.toFixed(1)}h`;
  } else {
    // 1 day or more, show as days
    const days = seconds / 86400;
    return `~${days.toFixed(1)}d`;
  }
}

// Formatting helpers for run progress
function formatCountiesCompleted(runProgress: RunProgress): string {
  return `${runProgress.completedCounties} / ${runProgress.totalCounties} Counties`;
}

function formatPercentComplete(runProgress: RunProgress): number {
  if (runProgress.totalCounties === 0) return 0;
  return Math.round((runProgress.completedCounties / runProgress.totalCounties) * 100);
}

function formatCountyPosition(runProgress: RunProgress): string {
  const base = `County ${runProgress.currentCountyIndex} of ${runProgress.totalCounties}`;
  if (runProgress.currentCountyName) {
    return `${base} — ${runProgress.currentCountyName}`;
  }
  return base;
}

function formatStatusLabel(runProgress: RunProgress): string {
  switch (runProgress.status) {
    case "running":
      return "Running";
    case "paused":
      return "Paused";
    case "finished":
      return "Complete";
    case "error":
      return "Attention needed";
    case "idle":
      return "Ready";
    default:
      return "Unknown";
  }
}

// Helper function to derive runProgress from summary and state
function deriveRunProgress(
  summary: PipelineSummary | null,
  elapsedTimeDisplay: number,
  estimatedTime: number | null,
  startTime: number | null
): RunProgress {
  const countiesProcessed = summary?.countiesProcessed || 0;
  const totalCounties = summary?.totalCounties || 1;
  const currentCountyIndex = countiesProcessed + 1;
  
  // Determine status from summary
  let status: RunProgress["status"] = "idle";
  if (summary?.status === "running") {
    status = "running";
  } else if (summary?.status === "paused") {
    status = "paused";
  } else if (summary?.status === "completed") {
    status = "finished";
  } else if (summary?.status === "error") {
    status = "error";
  }
  
  return {
    totalCounties,
    completedCounties: countiesProcessed,
    currentCountyIndex,
    currentCountyName: summary?.currentCounty,
    schoolsProcessed: summary?.schoolsProcessed || summary?.schoolsFound || 0,
    contactsProcessed: summary?.totalContacts || 0,
    errorCount: 0, // TODO: Add error tracking from backend
    status,
    elapsedMs: elapsedTimeDisplay * 1000,
    etaRemainingMs: estimatedTime ? estimatedTime * 1000 : undefined,
    lastUpdatedAt: startTime ? new Date(startTime) : undefined,
  };
}

// Helper function to create cumulative line graph
function createLineGraph(data: number[], width: number = 200, height: number = 80, color: string = "#6b8e23") {
  if (!data || data.length === 0) return null;
  
  // Calculate cumulative values
  const cumulative = [];
  let sum = 0;
  for (const value of data) {
    sum += value;
    cumulative.push(sum);
  }
  
  const max = Math.max(...cumulative, 1);
  const padding = 10;
  const graphWidth = width - padding * 2;
  const graphHeight = height - padding * 2;
  
  // Generate path points
  const points = cumulative.map((value, index) => {
    const x = padding + (index / (cumulative.length - 1 || 1)) * graphWidth;
    const y = padding + graphHeight - (value / max) * graphHeight;
    return `${x},${y}`;
  }).join(' ');
  
  // Create area path for fill
  const areaPath = `M ${padding},${height - padding} L ${points} L ${width - padding},${height - padding} Z`;
  
  return (
    <svg width={width} height={height} className="w-full h-full">
      <defs>
        <linearGradient id={`gradient-${color}`} x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0.05" />
        </linearGradient>
      </defs>
      <path d={areaPath} fill={`url(#gradient-${color})`} />
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export default function Home() {
  const { isAuthenticated, token, loading, logout } = useAuth();
  const [viewState, setViewState] = useState<ViewState>("start");
  const [selectedState, setSelectedState] = useState<string>("");
  const [selectedType, setSelectedType] = useState<"school" | "church" | "running" | "finished" | "archive">("school");
  const [status, setStatus] = useState("");
  const [summary, setSummary] = useState<PipelineSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [currentStep, setCurrentStep] = useState(0);
  const [progress, setProgress] = useState(0);
  const [estimatedTime, setEstimatedTime] = useState<number | null>(null);
  const [startTime, setStartTime] = useState<number | null>(null);
  const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null);
  const [elapsedTimeDisplay, setElapsedTimeDisplay] = useState<number>(0);
  const [completedCounties, setCompletedCounties] = useState<string[]>([]);
  const [completedCountiesSet, setCompletedCountiesSet] = useState<Set<string>>(new Set());
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [isFinalizing, setIsFinalizing] = useState(false);
  const [finalizingMessage, setFinalizingMessage] = useState<string | null>(null);
  const [activityLog, setActivityLog] = useState<ActivityLogEntry[]>([]);

  // Restore elapsed time from localStorage on page load
  useEffect(() => {
    if (typeof window !== 'undefined' && selectedRunId && !startTime) {
      const storedStartTime = localStorage.getItem(`run_startTime_${selectedRunId}`);
      if (storedStartTime) {
        const start = parseInt(storedStartTime, 10);
        setStartTime(start);
        const elapsed = (Date.now() - start) / 1000;
        setElapsedTimeDisplay(elapsed);
      }
    }
  }, [selectedRunId, startTime]);

  // Stop updating when run completes (viewState === "summary")
  useEffect(() => {
    if (startTime && viewState === "progress") {
      const interval = setInterval(() => {
        const elapsed = (Date.now() - startTime) / 1000;
        setElapsedTimeDisplay(elapsed);
      }, 1000);
      return () => clearInterval(interval);
    } else if (!startTime) {
      // Only reset to 0 when startTime is null (before a run starts)
      setElapsedTimeDisplay(0);
    }
  }, [startTime, viewState]);

  useEffect(() => {
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [pollingInterval]);

  // Show login form if not authenticated (after all hooks are called)
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center" style={{ backgroundColor: '#f5f5f5' }}>
        <div className="text-gray-600">Loading...</div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <LoginForm />;
  }

  function downloadCSV(csvContent: string, filename: string) {
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", filename);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  function formatTime(seconds: number): string {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  }

  // Helper: Get activity log icon based on level
  function getActivityIcon(level: ActivityLogEntry["level"]): string {
    switch (level) {
      case "success":
        return "✔";
      case "warning":
        return "⚠";
      case "error":
        return "✖";
      case "info":
      default:
        return "→";
    }
  }

  // Helper: Format relative time
  function formatRelativeTime(timestamp: number): string {
    const now = Date.now();
    const diff = Math.floor((now - timestamp) / 1000);
    
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  // Helper: Truncate with ellipsis
  function truncateText(text: string, maxLength: number): string {
    if (text.length <= maxLength) return text;
    return text.slice(0, maxLength) + "...";
  }

  async function checkPipelineStatus(runId: string) {
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app").replace(/\/+$/, '');
      const response = await fetch(`${apiUrl}/pipeline-status/${runId}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        if (response.status === 401) {
          logout();
          return;
        }
        if (response.status === 404) {
          const errorData = await response.json().catch(() => ({ error: "Run ID not found" }));
          if (pollingInterval) {
            clearInterval(pollingInterval);
            setPollingInterval(null);
          }
          setError(errorData.error || "Pipeline failed to start. The run ID was not found on the server.");
          setStatus("Pipeline failed");
          setIsRunning(false);
          setSummary(null);
          return;
        }
        if (response.status === 410) {
          // 410 Gone - run completed and status no longer available, stop polling
          if (pollingInterval) {
            clearInterval(pollingInterval);
            setPollingInterval(null);
          }
          // If we don't have summary yet, try to get it from the response
          try {
            const data = await response.json();
            if (data.status === "completed" && summary) {
              // Keep existing summary, just stop polling
              setIsRunning(false);
            } else {
              setIsRunning(false);
              setStatus("Pipeline completed");
            }
          } catch {
            setIsRunning(false);
            setStatus("Pipeline completed");
          }
          return;
        }
        throw new Error(`Status check failed: ${response.status}`);
      }

      const data = await response.json();
      
      if (data.status === "completed") {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
        
        // Calculate final elapsed time - elapsedTimeDisplay will continue updating via useEffect
        // but we ensure it's set correctly here as well
        if (startTime) {
          const finalElapsed = (Date.now() - startTime) / 1000;
          setElapsedTimeDisplay(finalElapsed);
        }
        
        setSummary(data);
        setStatus("Pipeline completed successfully!");
        setProgress(100);
        setEstimatedTime(0);
        setIsRunning(false);
        
        // If run was selected from Finished tab, show summary; otherwise default behavior
        if (selectedRunId === runId && selectedType === 'finished') {
        setViewState("summary");
        } else if (selectedRunId === runId) {
          setViewState("summary");
        } else {
          setViewState("summary");
        }
      } else if (data.status === "error") {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
        
        setError(data.error || data.statusMessage || "Pipeline failed");
        setStatus("Pipeline failed - see error below");
        setIsRunning(false);
        setSummary(data);
      } else if (data.status === "finalizing") {
        setSummary(data);
        setIsFinalizing(true);
        setFinalizingMessage(data.statusMessage || "Run is finalizing. Please wait 2 minutes for the container to restart.");
        setIsRunning(false);
      } else if (data.status === "running") {
        setSummary(data);
        setCurrentStep(data.currentStep || 0);
        
        const countiesProcessed = data.countiesProcessed || 0;
        const totalCounties = data.totalCounties || 1;
        const countyProgress = Math.round((countiesProcessed / totalCounties) * 100);
        setProgress(countyProgress);
        
        setEstimatedTime(data.estimatedTimeRemaining || null);
        
        // Track completed counties for activity log using Set to prevent duplicates
        // When countiesProcessed increases, the previous county is completed
        // We track this by comparing with previous summary state
        if (summary && summary.countiesProcessed !== undefined) {
          const prevCountiesProcessed = summary.countiesProcessed || 0;
          if (countiesProcessed > prevCountiesProcessed) {
            // A county just completed - use the previous currentCounty
            const completedCounty = summary.currentCounty;
            if (completedCounty && !completedCountiesSet.has(completedCounty)) {
              setCompletedCountiesSet(prev => {
                const newSet = new Set(prev);
                newSet.add(completedCounty);
                return newSet;
              });
              setCompletedCounties(prev => [...prev, completedCounty]);
              
              // Add activity log entry for county completion
              setActivityLog(prev => [...prev, {
                level: "success",
                message: `County ${prevCountiesProcessed + 1} completed`,
                timestamp: new Date(),
              }]);
            }
          }
          
          // Check if a new county started
          if (data.currentCounty && data.currentCounty !== summary.currentCounty) {
            setActivityLog(prev => [...prev, {
              level: "info",
              message: `County ${countiesProcessed + 1} started`,
              timestamp: new Date(),
            }]);
          }
        } else if (data.currentCounty && countiesProcessed === 0) {
          // First county starting
          setActivityLog(prev => [...prev, {
            level: "info",
            message: "County 1 started",
            timestamp: new Date(),
          }]);
        }
        
        // If run was selected from Running/Finished tab, switch to progress view
        if (selectedRunId === runId && (selectedType === 'running' || selectedType === 'finished')) {
          setViewState("progress");
          setIsRunning(true);
          // Start polling if not already polling
          if (!pollingInterval) {
            const interval = setInterval(() => {
              checkPipelineStatus(runId);
            }, 60000); // Poll every 1 minute
            setPollingInterval(interval);
          }
        }
      }
    } catch (err) {
      console.error("Status check error:", err);
    }
  }

  async function runPipeline() {
    if (!selectedState) {
      setError("Please select a state");
      return;
    }

    if (selectedType === "church") {
      setError("Church scraping is not yet available");
      return;
    }

    if (!token) {
      setError("Authentication token is missing. Please log in again.");
      logout();
      return;
    }

    setViewState("progress");
    setStatus("Starting pipeline...");
    setSummary(null);
    setError(null);
    setIsRunning(true);
    setCurrentStep(0);
    setProgress(0);
    setStartTime(Date.now());
    setEstimatedTime(null);

    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app").replace(/\/+$/, '');
      console.log("Starting pipeline with API URL:", apiUrl);
      console.log("Request payload:", { state: selectedState.toLowerCase().replace(' ', '_'), type: selectedType });

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

      const response = await fetch(`${apiUrl}/run-pipeline`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
        },
        body: JSON.stringify({ 
          state: selectedState.toLowerCase().replace(' ', '_'),
          type: selectedType,
        }),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        if (response.status === 401) {
          // Unauthorized - token expired or invalid
          logout();
          return;
        }
        // Try to parse error response as JSON
        let errorMessage = `Backend responded with ${response.status}: ${response.statusText}`;
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorData.message || errorMessage;
        } catch {
          // If JSON parse fails, use text response
        const errorText = await response.text();
          if (errorText) {
            errorMessage = errorText;
          }
        }
        
        // Handle specific error codes
        if (response.status === 409) {
          const errorData = await response.json().catch(() => ({}));
          if (errorData.isFinalizing) {
            setIsFinalizing(true);
            setFinalizingMessage(errorMessage || "A run is currently finalizing. Please wait 2 minutes for the container to restart before starting a new run.");
          } else {
            errorMessage = errorMessage || "Another run is already in progress. Please wait for it to complete or stop it first.";
          }
        }
        
        throw new Error(errorMessage);
      }

      const data = await response.json();
      console.log("Pipeline response:", data);
      
      if (data.runId) {
        const now = Date.now();
        setStartTime(now);
        // Store start time in localStorage for persistence across page reloads
        if (typeof window !== 'undefined') {
          localStorage.setItem(`run_startTime_${data.runId}`, now.toString());
        }
        const interval = setInterval(() => {
          checkPipelineStatus(data.runId);
        }, 60000); // Poll every 1 minute
        setPollingInterval(interval);
        checkPipelineStatus(data.runId);
      } else {
        setSummary(data);
        setStatus("Pipeline completed successfully!");
        setProgress(100);
        await new Promise(resolve => setTimeout(resolve, 1000));
        setViewState("summary");
        setIsRunning(false);
      }
    } catch (err) {
      console.error("Pipeline error:", err);
      let errorMessage = "Unknown error occurred";
      
      if (err instanceof Error) {
        errorMessage = err.message;
        
        // Check for timeout
        if (err.name === 'AbortError' || err.message.includes('timeout')) {
          errorMessage = "Request timed out. The backend may be slow to respond or unavailable. Please try again.";
        }
        // Check for network errors
        else if (err.message.includes("fetch") || err.message.includes("Failed to fetch") || err.message.includes("NetworkError") || err.message.includes("Network request failed")) {
          const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app").replace(/\/+$/, '');
          errorMessage = `Failed to connect to the backend API at ${apiUrl}. This could mean:\n\n1. The backend server is not running\n2. The API URL is incorrect\n3. There's a network connectivity issue\n\nPlease verify that NEXT_PUBLIC_API_URL is set correctly in your Vercel environment variables and that the Railway backend is running.`;
        }
      }
      
      setError(errorMessage);
      setStatus("Pipeline failed");
      setViewState("start");
      setIsRunning(false);
      
      if (pollingInterval) {
        clearInterval(pollingInterval);
        setPollingInterval(null);
      }
    }
  }

  function resetToStart() {
    setViewState("start");
    setStatus("");
    setSummary(null);
    setError(null);
    setCurrentStep(0);
    setProgress(0);
    setEstimatedTime(null);
    setStartTime(null);
    setElapsedTimeDisplay(0);
    setCompletedCounties([]);
    setCompletedCountiesSet(new Set());
    setActivityLog([]);
    setSelectedRunId(null);
    setIsRunning(false);
    
    // Reset to school tab if on running/finished tabs
    if (selectedType === 'running' || selectedType === 'finished') {
      setSelectedType('school');
    }
    
    if (pollingInterval) {
      clearInterval(pollingInterval);
      setPollingInterval(null);
    }
  }

  // Derive runProgress from summary and state
  const runProgress = deriveRunProgress(summary, elapsedTimeDisplay, estimatedTime, startTime);

  // Render views with fade-in transitions
  return (
    <div className="min-h-screen h-screen flex" style={{ backgroundColor: '#f5f5f5' }}>
      {/* Mobile Hamburger Menu */}
      <button
        onClick={() => setSidebarOpen(!sidebarOpen)}
        className="fixed top-4 left-4 z-50 md:hidden bg-white p-2 rounded-lg shadow-md border border-gray-200"
      >
        <svg className="w-6 h-6 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>

      {/* Sidebar Overlay for Mobile */}
      {sidebarOpen && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 z-40 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      <div className={`fixed left-0 top-0 h-screen w-80 z-50 transition-transform duration-300 ${
        sidebarOpen ? 'translate-x-0' : '-translate-x-full'
      } md:translate-x-0 md:relative md:z-auto md:h-full flex-shrink-0`}>
        <Sidebar 
          activeTab={selectedType} 
          onTabChange={(tab) => {
            setSelectedType(tab);
            setSidebarOpen(false); // Close mobile menu on tab change
            // Switch view based on tab
            if (tab === 'school' || tab === 'church') {
              setViewState("start");
              setSelectedRunId(null);
            } else if (tab === 'running' || tab === 'finished' || tab === 'archive') {
              setViewState(tab);
              setSelectedRunId(null);
            }
          }}
          onRunSelect={async (runId) => {
            setSelectedRunId(runId);
            setSidebarOpen(false); // Close mobile menu on run select
            // Fetch run status to determine if it's running or finished
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app").replace(/\/+$/, '');
            try {
              // First, try to get run from /runs endpoint to get metadata
              const runsResponse = await fetch(`${apiUrl}/runs`, {
                headers: {
                  "Authorization": `Bearer ${token}`,
                },
              });
              let runMetadata = null;
              if (runsResponse.ok) {
                const runsData = await runsResponse.json();
                runMetadata = runsData.runs?.find((r: any) => r.run_id === runId);
              }
              
              // Then try pipeline-status endpoint
              const response = await fetch(`${apiUrl}/pipeline-status/${runId}`, {
                headers: {
                  "Authorization": `Bearer ${token}`,
                },
              });
              if (response.ok) {
                const data = await response.json();
                if (data.status === "running") {
                  setViewState("progress");
                  setIsRunning(true);
                  // Start polling
                  if (pollingInterval) {
                    clearInterval(pollingInterval);
                  }
                  const interval = setInterval(() => {
                    checkPipelineStatus(runId);
                  }, 60000);
                  setPollingInterval(interval);
                  checkPipelineStatus(runId);
                } else if (data.status === "completed" || data.status === "error") {
                  // Use data from pipeline-status if available, otherwise use metadata
                  const summaryData: PipelineSummary = {
                    ...data,
                    steps: data.steps || [],
                    schoolsFound: data.schoolsFound || data.schoolsProcessed || runMetadata?.schools_processed || 0,
                    runId: data.runId || runId,
                    totalContacts: data.totalContacts || runMetadata?.total_contacts || 0,
                    schoolsProcessed: data.schoolsProcessed || data.schoolsFound || runMetadata?.schools_processed || 0,
                    countyContacts: data.countyContacts || [],
                    countySchools: data.countySchools || [],
                    csvData: data.csvData,
                    csvFilename: data.csvFilename || runMetadata?.csv_filename,
                  };
                  
                  // Calculate elapsed time
                  if (runMetadata?.created_at && runMetadata?.completed_at) {
                    const start = new Date(runMetadata.created_at).getTime();
                    const end = new Date(runMetadata.completed_at).getTime();
                    const elapsed = (end - start) / 1000;
                    setElapsedTimeDisplay(elapsed);
                    setStartTime(start);
                  } else if (data.elapsedTime) {
                    setElapsedTimeDisplay(data.elapsedTime);
                  }
                  
                  setSummary(summaryData);
                  setViewState("summary");
                  setIsRunning(false);
                }
              } else if (response.status === 401) {
                logout();
              } else if (response.status === 401) {
                logout();
              } else if (response.status === 410 || response.status === 404) {
                // Run is completed and status endpoint no longer available, use metadata
                if (runMetadata) {
                  const summaryData: PipelineSummary = {
                    status: runMetadata.status || "completed",
                    steps: [], // No step data available from metadata
                    schoolsFound: runMetadata.schools_processed || 0,
                    runId: runId,
                    totalContacts: runMetadata.total_contacts || 0,
                    schoolsProcessed: runMetadata.schools_processed || 0,
                    countyContacts: [],
                    countySchools: [],
                    csvData: undefined,
                    csvFilename: runMetadata.csv_filename,
                  };
                  
                  // Calculate elapsed time from metadata
                  if (runMetadata.created_at && runMetadata.completed_at) {
                    const start = new Date(runMetadata.created_at).getTime();
                    const end = new Date(runMetadata.completed_at).getTime();
                    const elapsed = (end - start) / 1000;
                    setElapsedTimeDisplay(elapsed);
                    setStartTime(start);
                  }
                  
                  setSummary(summaryData);
                  setViewState("summary");
                  setIsRunning(false);
                }
              }
            } catch (error) {
              console.error("Error fetching run status:", error);
            }
          }}
        />
      </div>
      <div className="flex-1 min-h-screen">
        {/* START VIEW - Only show when school/church tab is active */}
        {viewState === "start" && (selectedType === "school" || selectedType === "church") && (
          <div className="animate-fade-in">
            <div className="flex items-center justify-center p-12 min-h-screen">
            <div className="w-full max-w-2xl relative">
              {/* In Development Overlay for Church Scraper */}
              {selectedType === "church" && (
                <div className="absolute inset-0 bg-gray-900/50 backdrop-blur-sm z-10 rounded-xl flex items-center justify-center">
                  <div className="text-center">
                    <h2 className="text-4xl font-bold text-white mb-3">In Development</h2>
                    <p className="text-gray-300 text-lg">Church scraper functionality coming soon</p>
                  </div>
                </div>
              )}
              
              <div className="bg-white rounded-2xl border border-gray-200 shadow-lg p-8 md:p-12">
                <div className="flex flex-col space-y-8">
                  <div>
                    <h1 className="text-3xl font-bold text-gray-900 mb-3">Start New Search</h1>
                  </div>

                  {/* State Selection */}
                  <div>
                    <label htmlFor="state" className="block text-sm font-semibold text-gray-700 uppercase tracking-wide mb-3">
                      Select State
                    </label>
                    <select
                      id="state"
                      value={selectedState}
                      onChange={(e) => setSelectedState(e.target.value)}
                      className="w-full px-5 py-3.5 bg-white border border-gray-300 rounded-xl text-base text-gray-900 focus:outline-none focus:ring-2 focus:ring-[#1e3a5f] focus:border-[#1e3a5f] transition-all shadow-sm"
                    >
                      <option value="">-- Select a state --</option>
                      {US_STATES.map((state) => (
                        <option key={state.value} value={state.value}>
                          {state.label}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Preview row after state selection */}
                  {selectedState && (() => {
                    const countyCount = STATE_COUNTY_COUNTS[selectedState] || 0;
                    const estimatedSeconds = countyCount * SECONDS_PER_COUNTY;
                    const estimatedTime = formatEstimatedTime(estimatedSeconds);
                    
                    return (
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-8 pt-8 border-t border-gray-200">
                        <div>
                          <p className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-3">Counties</p>
                          <p className="text-3xl font-bold text-gray-900">{countyCount}</p>
                        </div>
                        <div>
                          <p className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-3">Est. Time</p>
                          <p className="text-3xl font-bold text-gray-900">{estimatedTime}</p>
                        </div>
                      </div>
                    );
                  })()}

                  {error && (
                    <div className="p-5 bg-red-50 border border-red-200 rounded-xl">
                      <p className="text-red-700 text-base">{error}</p>
                    </div>
                  )}

                  {isFinalizing && finalizingMessage && (
                    <div className="p-5 bg-yellow-50 border border-yellow-200 rounded-xl">
                      <p className="text-yellow-800 text-base font-medium">{finalizingMessage}</p>
                    </div>
                  )}
                  
                  {/* Enhanced primary CTA button (dashboard-18) */}
                  <button
                    onClick={runPipeline}
                    disabled={!selectedState || selectedType === "church" || isFinalizing}
                    className={`w-full px-8 py-5 rounded-xl text-lg font-semibold text-white transition-all duration-200 shadow-lg flex items-center justify-center gap-3 ${
                      !selectedState || selectedType === "church" || isFinalizing
                        ? "bg-gray-400 cursor-not-allowed opacity-60"
                        : "bg-[#1e3a5f] hover:bg-[#2c5282] hover:shadow-xl transform hover:-translate-y-1"
                    }`}
                  >
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                    </svg>
                    Start Scan
                  </button>

                </div>
              </div>
            </div>
          </div>
          </div>
        )}

        {/* PROGRESS VIEW - Show when progress state OR when run selected from Running tab */}
        {viewState === "progress" && summary && (
            <div className="animate-fade-in min-h-screen flex items-center justify-center" style={{ backgroundColor: '#f5f5f5' }}>
            <div className="w-full max-w-4xl px-4 sm:px-6 md:px-8 py-12">
              {/* Single Card with all progress information */}
              <div className="bg-white rounded-2xl border border-gray-200 shadow-lg p-10" style={{ borderRadius: '16px', boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)' }}>
                {/* X/Y Counties with percentage */}
                <div className="mb-8">
                  <div className="flex items-baseline gap-3 mb-6">
                    <span className="text-6xl font-bold text-[#1e3a5f]">{runProgress.completedCounties}</span>
                    <span className="text-3xl text-gray-500">/ {runProgress.totalCounties}</span>
                    <span className="text-2xl font-semibold text-gray-600">Counties</span>
                    <span className="text-2xl font-semibold text-gray-600 ml-auto">{formatPercentComplete(runProgress)}% complete</span>
                  </div>
                  
                  {/* Progress bar */}
                  <div className="w-full bg-gray-200 rounded-full h-3">
                    <div 
                      className="bg-[#1e3a5f] h-3 rounded-full transition-all duration-500"
                      style={{ width: `${formatPercentComplete(runProgress)}%` }}
                    ></div>
                  </div>
                </div>

                {/* Stats row: Elapsed, Estimated Remaining, Status */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-8 pt-8 border-t border-gray-200">
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-3">Elapsed</p>
                    <p className="text-3xl font-bold text-gray-900">{formatTime(elapsedTimeDisplay)}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-3">Estimated Remaining</p>
                    {runProgress.etaRemainingMs && runProgress.etaRemainingMs > 0 ? (
                      <p className="text-3xl font-bold text-gray-900">{formatTime(runProgress.etaRemainingMs / 1000)}</p>
                    ) : (
                      <p className="text-3xl font-bold text-gray-900">—</p>
                    )}
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide font-medium mb-3">Status</p>
                    <p className="text-3xl font-bold text-[#1e3a5f]">{formatStatusLabel(runProgress)}</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* SUMMARY VIEW - Show when summary state */}
        {viewState === "summary" && summary && (
          <div className="animate-fade-in">
              <div className="flex items-center justify-center min-h-screen py-12 px-4 sm:px-6 md:px-8">
              <div className="w-full max-w-7xl">
                {/* Header */}
                <div className="mb-10 text-center">
                  <h1 className="text-3xl sm:text-4xl font-bold text-gray-900 mb-2">Scraping Complete</h1>
                  <p className="text-lg text-gray-600">Pipeline finished successfully</p>
                </div>

                {/* 3 Card Grid */}
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8 mb-10">
                  
                  {/* Card 1: Total Contacts */}
                  <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-6 md:p-8">
                    <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Contacts Extracted</h3>
                    <div className="text-6xl font-bold text-[#1e3a5f] mb-6">
                      {summary.totalContacts || 0}
                    </div>
                    <div className="h-28 -mx-8 -mb-8 mt-4">
                      {createLineGraph(summary.countyContacts || [], 400, 112, "#6b8e23")}
                    </div>
                  </div>

                  {/* Card 2: Schools Processed */}
                  <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-6 md:p-8">
                    <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Schools Processed</h3>
                    <div className="text-6xl font-bold text-[#1e3a5f] mb-6">
                      {summary.schoolsProcessed || summary.schoolsFound || 0}
                    </div>
                    <div className="h-28 -mx-8 -mb-8 mt-4">
                      {createLineGraph(summary.countySchools || [], 400, 112, "#1e3a5f")}
                    </div>
                  </div>

                  {/* Card 3: Processing Time */}
                  <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-6 md:p-8">
                    <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Processing Time</h3>
                    <div className="text-6xl font-bold text-[#1e3a5f]">
                      {formatTime(elapsedTimeDisplay || 0)}
                    </div>
                  </div>
                </div>

                {/* Download Button */}
                <div className="flex flex-col space-y-4 max-w-2xl mx-auto">
                  {summary.totalContacts && summary.totalContacts > 0 ? (
                    <button
                      onClick={async () => {
                        if (selectedRunId) {
                          const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-backend-production.up.railway.app").replace(/\/+$/, '');
                          try {
                            const response = await fetch(`${apiUrl}/runs/${selectedRunId}/download`, {
                              headers: {
                                "Authorization": `Bearer ${token}`,
                              },
                            });
                            if (response.ok) {
                              const blob = await response.blob();
                              const url = window.URL.createObjectURL(blob);
                              const link = document.createElement("a");
                              link.href = url;
                              link.download = summary.csvFilename || `run_${selectedRunId}.csv`;
                              document.body.appendChild(link);
                              link.click();
                              document.body.removeChild(link);
                              window.URL.revokeObjectURL(url);
                            } else {
                              alert("Failed to download CSV");
                            }
                          } catch (error) {
                            console.error("Error downloading CSV:", error);
                            alert("Failed to download CSV");
                          }
                        } else if (summary.csvData && summary.csvFilename) {
                          downloadCSV(summary.csvData, summary.csvFilename);
                        }
                      }}
                      className="w-full px-8 py-5 bg-[#1e3a5f] hover:bg-[#2c5282] text-white rounded-xl text-lg font-semibold transition-all duration-200 shadow-md hover:shadow-lg transform hover:-translate-y-0.5"
                    >
                      Download Leads ({summary.totalContacts || 0} contacts)
                    </button>
                  ) : (
                    <div className="w-full px-8 py-5 bg-yellow-50 border border-yellow-200 rounded-xl text-center shadow-sm">
                      <p className="text-yellow-800 text-base font-medium">
                        No contacts were found. This may be normal if no schools were discovered or no contacts were extracted.
                      </p>
                    </div>
                  )}
                  
                  <button
                    onClick={resetToStart}
                    className="w-full px-8 py-5 bg-gray-200 hover:bg-gray-300 text-gray-800 rounded-xl text-lg font-semibold transition-all duration-200 shadow-sm hover:shadow-md"
                  >
                    Run Another Search
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* RUNNING TAB VIEW - Show empty state when no run selected */}
        {selectedType === "running" && !selectedRunId && (
          <div className="animate-fade-in">
            <div className="flex items-center justify-center min-h-screen py-12 px-4 sm:px-6 md:px-8">
              <div className="w-full max-w-7xl">
                <div className="mb-10">
                  <h1 className="text-3xl sm:text-4xl font-bold text-gray-900">In Progress</h1>
                  <p className="text-base sm:text-lg text-gray-600 mt-2">Select a run from the sidebar to view its progress</p>
                </div>
                <div className="bg-white rounded-xl border border-gray-200 shadow-md p-8 md:p-12 text-center">
                  <p className="text-gray-500 text-base sm:text-lg">No run selected. Choose a run from the sidebar to view its progress.</p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* FINISHED TAB VIEW - Show empty state when no run selected */}
        {selectedType === "finished" && !selectedRunId && (
          <div className="animate-fade-in">
            <div className="flex items-center justify-center min-h-screen py-12 px-4 sm:px-6 md:px-8">
              <div className="w-full max-w-7xl">
                <div className="mb-10">
                  <h1 className="text-3xl sm:text-4xl font-bold text-gray-900">Finished Runs</h1>
                  <p className="text-base sm:text-lg text-gray-600 mt-2">Select a run from the sidebar to view its results</p>
                </div>
                <div className="bg-white rounded-xl border border-gray-200 shadow-md p-8 md:p-12 text-center">
                  <p className="text-gray-500 text-base sm:text-lg">No run selected. Choose a run from the sidebar to view its results.</p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
