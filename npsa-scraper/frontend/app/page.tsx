"use client";

import { useState, useEffect } from "react";
import Navigation from "../components/Navigation";

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
    const secs = Math.round(seconds % 60);
    if (minutes < 60) return `${minutes}m ${secs}s`;
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
        
        setSummary(data);
        setStatus("Pipeline completed successfully!");
        setProgress(100);
        setEstimatedTime(0);
        setViewState("summary");
        setIsRunning(false);
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
    
    if (pollingInterval) {
      clearInterval(pollingInterval);
      setPollingInterval(null);
    }
  }

  // Update elapsed time display every second (real-time counter)
  useEffect(() => {
    if (startTime && isRunning) {
      const interval = setInterval(() => {
        const elapsed = (Date.now() - startTime) / 1000;
        setElapsedTimeDisplay(elapsed);
      }, 1000);
      return () => clearInterval(interval);
    } else {
      setElapsedTimeDisplay(0);
    }
  }, [startTime, isRunning]);

  useEffect(() => {
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [pollingInterval]);

  // START VIEW
  if (viewState === "start") {
    return (
      <div className="min-h-screen bg-white relative">
        <Navigation activeTab={selectedType} onTabChange={setSelectedType} />
        <div className="flex items-center justify-center p-8 min-h-[calc(100vh-80px)]">
          <div className="w-full max-w-2xl relative">
            {/* In Development Overlay for Church Scraper */}
            {selectedType === "church" && (
              <div className="absolute inset-0 bg-gray-900/50 backdrop-blur-sm z-10 rounded-lg flex items-center justify-center">
                <div className="text-center">
                  <h2 className="text-3xl font-bold text-white mb-2">In Development</h2>
                  <p className="text-gray-300">Church scraper functionality coming soon</p>
                </div>
              </div>
            )}
            
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-8">
              <div className="flex flex-col space-y-6">
                {/* State Selection */}
                <div>
                  <label htmlFor="state" className="block text-sm font-medium text-gray-700 mb-2">
                    Select State
                  </label>
                  <select
                    id="state"
                    value={selectedState}
                    onChange={(e) => setSelectedState(e.target.value)}
                    className="w-full px-4 py-3 bg-white border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-[#1e3a5f] focus:border-[#1e3a5f] transition-colors shadow-sm"
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
                  <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
                    <p className="text-red-700 text-sm">{error}</p>
                  </div>
                )}

                <button
                  onClick={runPipeline}
                  disabled={isRunning || !selectedState || selectedType === "church"}
                  className={`w-full px-8 py-4 rounded-lg font-semibold text-white transition-all duration-200 shadow-md ${
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
    );
  }

  // PROGRESS VIEW
  if (viewState === "progress") {
    const hasRemainingTime = estimatedTime !== null && estimatedTime > 0;
    const timeDisplay = hasRemainingTime
      ? `Elapsed: ${formatTime(elapsedTimeDisplay)} | Remaining: ${formatTime(estimatedTime)}`
      : `Elapsed: ${formatTime(elapsedTimeDisplay)}`;

    return (
      <div className="min-h-screen bg-white">
        <Navigation activeTab={selectedType} onTabChange={setSelectedType} />
        <div className="flex items-center justify-center p-8 min-h-[calc(100vh-80px)]">
          <div className="w-full max-w-4xl">
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-8">
              {/* Progress Bar */}
              <div className="mb-6">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-sm text-gray-600">Progress</span>
                  <span className="text-sm text-gray-600">{Math.round(progress)}%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-4 mb-2">
                  <div
                    className="bg-[#1e3a5f] h-4 rounded-full transition-all duration-500 shadow-sm"
                    style={{ width: `${progress}%` }}
                  ></div>
                </div>
                <div className="flex justify-between items-center text-sm text-gray-600">
                  <span>{timeDisplay}</span>
                  <span>
                    {summary?.countiesProcessed || 0} / {summary?.totalCounties || 0} counties
                  </span>
                </div>
              </div>

              {/* Current Status - Simplified Gray Bar */}
              <div className="mb-6 p-4 bg-gray-100 rounded-lg border border-gray-200">
                <p className="text-gray-800 font-medium">{status}</p>
              </div>

              {error && (
                <div className="mt-6 p-6 bg-red-50 border-2 border-red-300 rounded-lg">
                  <div className="flex items-start">
                    <div className="flex-shrink-0">
                      <svg className="h-6 w-6 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    </div>
                    <div className="ml-3 flex-1">
                      <h3 className="text-lg font-semibold text-red-800 mb-2">Pipeline Error</h3>
                      <p className="text-red-700 text-sm whitespace-pre-wrap">{error}</p>
                    </div>
                  </div>
                  <div className="mt-4">
                    <button
                      onClick={resetToStart}
                      className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg font-medium transition-colors"
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
    );
  }

  // SUMMARY VIEW
  if (viewState === "summary" && summary) {
    return (
      <div className="min-h-screen bg-white">
        <Navigation activeTab={selectedType} onTabChange={setSelectedType} />
        <div className="flex items-center justify-center p-8 min-h-[calc(100vh-80px)]">
          <div className="w-full max-w-2xl">
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-8">
              
              {/* Results Section */}
              <div className="bg-gray-50 rounded-lg p-6 mb-6 border border-gray-200">
                <h3 className="text-xl font-semibold mb-4 text-gray-900">Results</h3>
                <div className="space-y-3">
                  <p className="text-gray-700">
                    <span className="font-semibold text-[#1e3a5f]">{summary.schoolsFound || 0}</span> Schools found
                  </p>
                  <p className="text-gray-700">
                    <span className="font-semibold text-[#1e3a5f]">{summary.totalContacts || 0}</span> Contacts
                  </p>
                </div>
              </div>

              {/* Download Button */}
              <div className="flex flex-col space-y-4">
                {summary.csvData && summary.csvFilename ? (
                  <button
                    onClick={() => downloadCSV(summary.csvData!, summary.csvFilename!)}
                    className="w-full px-6 py-4 bg-[#1e3a5f] hover:bg-[#2c5282] text-white rounded-lg font-semibold transition-all duration-200 shadow-md hover:shadow-lg transform hover:-translate-y-0.5"
                  >
                    Download Leads ({summary.totalContacts || 0} contacts)
                  </button>
                ) : (
                  <div className="w-full px-6 py-4 bg-yellow-50 border border-yellow-200 rounded-lg text-center mb-4">
                    <p className="text-yellow-800 text-sm">
                      No contacts were found. This may be normal if no schools were discovered or no contacts were extracted.
                    </p>
                  </div>
                )}
                
                <button
                  onClick={resetToStart}
                  className="w-full px-6 py-4 bg-gray-200 hover:bg-gray-300 text-gray-800 rounded-lg font-medium transition-all duration-200"
                >
                  Run Another Search
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return null;
}
