"use client";
// Force Vercel redeploy - 2025-12-13

import { useState, useEffect } from "react";
import Sidebar from "../components/Sidebar";

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

type ViewState = "start" | "progress" | "summary";

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

export default function Home() {
  const [viewState, setViewState] = useState<ViewState>("start");
  const [selectedState, setSelectedState] = useState<string>("");
  const [selectedType, setSelectedType] = useState<"school" | "church">("school");
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
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

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

  async function checkPipelineStatus(runId: string) {
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-200036585956.us-central1.run.app").replace(/\/+$/, '');
      const response = await fetch(`${apiUrl}/pipeline-status/${runId}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
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
        setViewState("summary");
      } else if (data.status === "error") {
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
        
        setError(data.error || data.statusMessage || "Pipeline failed");
        setStatus("Pipeline failed - see error below");
        setIsRunning(false);
        setSummary(data);
      } else if (data.status === "running") {
        setSummary(data);
        setCurrentStep(data.currentStep || 0);
        
        const countiesProcessed = data.countiesProcessed || 0;
        const totalCounties = data.totalCounties || 1;
        const countyProgress = Math.round((countiesProcessed / totalCounties) * 100);
        setProgress(countyProgress);
        
        setEstimatedTime(data.estimatedTimeRemaining || null);
        
        // Track completed counties for activity log
        // When countiesProcessed increases, the previous county is completed
        // We track this by comparing with previous summary state
        if (summary && summary.countiesProcessed !== undefined) {
          const prevCountiesProcessed = summary.countiesProcessed || 0;
          if (countiesProcessed > prevCountiesProcessed) {
            // A county just completed - use the previous currentCounty
            const completedCounty = summary.currentCounty;
            if (completedCounty && !completedCounties.includes(completedCounty)) {
              setCompletedCounties(prev => [...prev, completedCounty]);
            }
          }
        }
        
        let statusMsg = data.statusMessage || "Processing...";
        if (data.currentCounty) {
          statusMsg = `Processing ${data.currentCounty} County (${countiesProcessed + 1} of ${totalCounties})`;
        }
        setStatus(statusMsg);
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
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "https://school-scraper-200036585956.us-central1.run.app").replace(/\/+$/, '');

      const response = await fetch(`${apiUrl}/run-pipeline`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ 
          state: selectedState.toLowerCase().replace(' ', '_'),
          type: selectedType,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Backend responded with ${response.status}: ${errorText || response.statusText}`);
      }

      const data = await response.json();
      console.log("Pipeline response:", data);
      
      if (data.runId) {
        const interval = setInterval(() => {
          checkPipelineStatus(data.runId);
        }, 2000); // Poll every 2 seconds
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
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      setStatus("Pipeline failed");
      console.error("Pipeline error:", err);
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
    
    if (pollingInterval) {
      clearInterval(pollingInterval);
      setPollingInterval(null);
    }
  }

  // Update elapsed time display every second (real-time counter)
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

  // Calculate progress view variables
  const countiesProcessed = summary?.countiesProcessed || 0;
  const totalCounties = summary?.totalCounties || 0;
  const schoolsProcessed = summary?.schoolsProcessed || summary?.schoolsFound || 0;
  const currentCounty = summary?.currentCounty || "Initializing...";

  // Helper function to create cumulative line graph
  const createLineGraph = (data: number[], width: number = 200, height: number = 80, color: string = "#6b8e23") => {
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
  };

  // Render all views with fade transitions
  return (
    <div className="min-h-screen bg-gray-50">
      <Sidebar activeTab={selectedType} onTabChange={setSelectedType} isCollapsed={sidebarCollapsed} onCollapseChange={setSidebarCollapsed} />
      <div className={`min-h-screen transition-all duration-300 ${sidebarCollapsed ? 'ml-20' : 'ml-64'}`}>
        {/* START VIEW */}
        <div
          className={`transition-opacity duration-500 ${
            viewState === "start" ? "opacity-100 relative" : "opacity-0 absolute inset-0 pointer-events-none"
          }`}
        >
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
              
              <div className="bg-white rounded-xl border border-gray-200 shadow-lg p-10">
                <div className="flex flex-col space-y-8">
                  <div>
                    <h1 className="text-2xl font-semibold text-gray-900 mb-1">Start New Search</h1>
                  </div>

                  {/* State Selection */}
                  <div>
                    <label htmlFor="state" className="block text-base font-medium text-gray-700 mb-3">
                      Select State
                    </label>
                    <select
                      id="state"
                      value={selectedState}
                      onChange={(e) => setSelectedState(e.target.value)}
                      className="w-full px-5 py-3.5 bg-white border border-gray-300 rounded-lg text-base text-gray-900 focus:outline-none focus:ring-2 focus:ring-[#1e3a5f] focus:border-[#1e3a5f] transition-all shadow-sm"
                    >
                      <option value="">-- Select a state --</option>
                      {US_STATES.map((state) => (
                        <option key={state.value} value={state.value}>
                          {state.label}
                        </option>
                      ))}
                    </select>
                  </div>

                  {error && (
                    <div className="p-5 bg-red-50 border border-red-200 rounded-lg">
                      <p className="text-red-700 text-base">{error}</p>
                    </div>
                  )}

                  <button
                    onClick={runPipeline}
                    disabled={isRunning || !selectedState || selectedType === "church"}
                    className={`w-full px-8 py-4 rounded-lg text-base font-semibold text-white transition-all duration-200 shadow-md ${
                      isRunning || !selectedState || selectedType === "church"
                        ? "bg-gray-400 cursor-not-allowed opacity-60"
                        : "bg-[#1e3a5f] hover:bg-[#2c5282] hover:shadow-lg transform hover:-translate-y-0.5"
                    }`}
                  >
                    {isRunning ? "Starting..." : "Start Search"}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* PROGRESS VIEW */}
        <div
          className={`transition-opacity duration-500 ${
            viewState === "progress" ? "opacity-100 relative" : "opacity-0 absolute inset-0 pointer-events-none"
          }`}
        >
          <div className="flex items-center justify-center min-h-screen py-12 px-8">
            <div className="w-full max-w-7xl">
              {/* Header */}
              <div className="mb-10 text-center">
                <h1 className="text-4xl font-bold text-gray-900">Running Pipeline</h1>
              </div>

              {/* Two Column Layout */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                {/* Left Column - Progress Cards */}
                <div className="lg:col-span-2 space-y-6">
                  {/* 3 Progress Cards */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {/* Card 1: Completed Counties */}
                    <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                      <div className="flex items-center justify-between mb-5">
                        <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">Completed Counties</h3>
                        <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
                        </svg>
                      </div>
                      <div className="flex items-baseline gap-2 mb-2">
                        <span className="text-5xl font-bold text-[#1e3a5f]">{countiesProcessed}</span>
                        <span className="text-xl text-gray-500">/ {totalCounties}</span>
                      </div>
                      <p className="text-sm text-gray-500">
                        {totalCounties > 0 ? Math.round((countiesProcessed / totalCounties) * 100) : 0}% complete
                      </p>
                    </div>

                    {/* Card 2: Processed Schools */}
                    <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                      <div className="flex items-center justify-between mb-5">
                        <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">Processed Schools</h3>
                        <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                        </svg>
                      </div>
                      <div className="flex items-baseline mb-2">
                        <span className="text-5xl font-bold text-[#1e3a5f]">{schoolsProcessed}</span>
                      </div>
                      <p className="text-sm text-gray-500">Schools discovered and processed</p>
                    </div>

                    {/* Card 3: Current County with Pulse */}
                    <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                      <div className="flex items-center justify-between mb-5">
                        <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">Current County</h3>
                        <div className="relative">
                          <div className="absolute inset-0 rounded-full bg-[#1e3a5f] opacity-20 animate-pulse"></div>
                          <div className="relative w-4 h-4 rounded-full bg-[#1e3a5f]"></div>
                        </div>
                      </div>
                      <div className="flex items-baseline mb-2 mt-1">
                        <span className="text-3xl font-bold text-[#1e3a5f]">{currentCounty}</span>
                      </div>
                      <p className="text-sm text-gray-500 mt-1">Currently processing</p>
                    </div>
                  </div>

                  {/* Activity Log */}
                  <div className="bg-white rounded-xl border border-gray-200 shadow-md p-8">
                    <div className="flex items-center justify-between mb-6">
                      <h3 className="text-lg font-bold text-gray-900">Activity Log</h3>
                      <span className="text-sm text-gray-500 font-medium">{completedCounties.length} completed</span>
                    </div>
                    <div className="space-y-3 max-h-96 overflow-y-auto">
                      {summary?.currentCounty && (
                        <div className="flex items-center gap-4 text-sm py-2">
                          <div className="relative flex-shrink-0">
                            <div className="absolute inset-0 w-2.5 h-2.5 rounded-full bg-[#1e3a5f] opacity-30 animate-ping"></div>
                            <div className="relative w-2.5 h-2.5 rounded-full bg-[#1e3a5f]"></div>
                          </div>
                          <span className="text-[#1e3a5f] font-semibold">Processing {summary.currentCounty} County...</span>
                        </div>
                      )}
                      {completedCounties.length > 0 ? (
                        completedCounties.slice().reverse().map((county, index) => (
                          <div key={index} className="flex items-center gap-4 text-sm py-2">
                            <div className="w-2.5 h-2.5 rounded-full bg-green-500 flex-shrink-0"></div>
                            <span className="text-gray-700 font-medium">Completed {county} County</span>
                          </div>
                        ))
                      ) : (
                        <div className="flex items-center gap-4 text-sm text-gray-500 py-2">
                          <div className="w-2.5 h-2.5 rounded-full bg-gray-300 flex-shrink-0"></div>
                          <span>Waiting for county completion...</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                {/* Right Column - Stats */}
                <div className="space-y-6">
                  <div className="bg-white rounded-xl border border-gray-200 shadow-md p-8 flex flex-col h-full">
                    <h3 className="text-lg font-bold text-gray-900 mb-6">Run Statistics</h3>
                    <div className="flex-1 flex flex-col justify-center space-y-6">
                      <div>
                        <p className="text-xs text-gray-500 mb-2 uppercase tracking-wide font-medium">Elapsed Time</p>
                        <p className="text-2xl font-bold text-gray-900">{formatTime(elapsedTimeDisplay)}</p>
                      </div>
                      {estimatedTime !== null && estimatedTime > 0 && (
                        <div>
                          <p className="text-xs text-gray-500 mb-2 uppercase tracking-wide font-medium">Estimated Remaining</p>
                          <p className="text-2xl font-bold text-gray-900">{formatTime(estimatedTime)}</p>
                        </div>
                      )}
                      <div>
                        <p className="text-xs text-gray-500 mb-2 uppercase tracking-wide font-medium">Status</p>
                        <p className="text-base font-semibold text-[#1e3a5f]">{status}</p>
                      </div>
                    </div>
                  </div>

                  {error && (
                    <div className="bg-red-50 border border-red-200 rounded-xl shadow-md p-8">
                      <div className="flex items-start">
                        <div className="flex-shrink-0">
                          <svg className="h-6 w-6 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                          </svg>
                        </div>
                        <div className="ml-3 flex-1">
                          <h3 className="text-base font-semibold text-red-800 mb-2">Pipeline Error</h3>
                          <p className="text-red-700 text-sm whitespace-pre-wrap">{error}</p>
                        </div>
                      </div>
                      <div className="mt-4">
                        <button
                          onClick={resetToStart}
                          className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg text-sm font-medium transition-colors"
                        >
                          Return to Start
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
          </div>
        </div>

        {/* SUMMARY VIEW */}
        {summary && (
          <div
            className={`transition-opacity duration-500 ${
              viewState === "summary" ? "opacity-100 relative" : "opacity-0 absolute inset-0 pointer-events-none"
            }`}
          >
            {(() => {
              const totalProcessingTime = elapsedTimeDisplay || 0;

              return (
                <div className="flex items-center justify-center min-h-screen py-12 px-8">
            <div className="w-full max-w-7xl">
              {/* Header */}
              <div className="mb-10 text-center">
                <h1 className="text-4xl font-bold text-gray-900 mb-2">Scraping Complete</h1>
                <p className="text-lg text-gray-600">Pipeline finished successfully</p>
              </div>

              {/* 3 Card Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8 mb-10">
                
                {/* Card 1: Total Contacts */}
                <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                  <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Contacts Extracted</h3>
                  <div className="text-6xl font-bold text-[#1e3a5f] mb-6">
                    {summary.totalContacts || 0}
                  </div>
                  <div className="h-28 -mx-8 -mb-8 mt-4">
                    {createLineGraph(summary.countyContacts || [], 400, 112, "#6b8e23")}
                  </div>
                </div>

                {/* Card 2: Schools Processed */}
                <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                  <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Schools Processed</h3>
                  <div className="text-6xl font-bold text-[#1e3a5f] mb-6">
                    {summary.schoolsProcessed || summary.schoolsFound || 0}
                  </div>
                  <div className="h-28 -mx-8 -mb-8 mt-4">
                    {createLineGraph(summary.countySchools || [], 400, 112, "#1e3a5f")}
                  </div>
                </div>

                {/* Card 3: Processing Time */}
                <div className="bg-white rounded-xl border border-gray-200 shadow-md hover:shadow-lg transition-shadow p-8">
                  <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-5">Processing Time</h3>
                  <div className="text-6xl font-bold text-[#1e3a5f]">
                    {formatTime(totalProcessingTime)}
                  </div>
                </div>
              </div>

              {/* Download Button */}
              <div className="flex flex-col space-y-4 max-w-2xl mx-auto">
                {summary.csvData && summary.csvFilename ? (
                  <button
                    onClick={() => downloadCSV(summary.csvData!, summary.csvFilename!)}
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
              );
            })()}
          </div>
        )}
      </div>
    </div>
  );
}
