"use client";

import { useState, useEffect } from "react";

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
  totalContactsNoEmails: number;
  schoolsFound: number;
  runId: string;
  csvData?: string;
  csvFilename?: string;
  csvNoEmailsData?: string;
  csvNoEmailsFilename?: string;
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
        // If 404, the run ID doesn't exist (likely an error occurred during initialization)
        if (response.status === 404) {
          const errorData = await response.json().catch(() => ({ error: "Run ID not found" }));
          // Stop polling
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
        // Stop polling
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
        // Stop polling
        if (pollingInterval) {
          clearInterval(pollingInterval);
          setPollingInterval(null);
        }
        
        setError(data.error || data.statusMessage || "Pipeline failed");
        setStatus("Pipeline failed - see error below");
        setIsRunning(false);
        setSummary(data);
        // Stay in progress view to show the error
      } else if (data.status === "running") {
        // Update progress
        setSummary(data);
        setCurrentStep(data.currentStep || 0);
        
        // Calculate progress based on counties processed
        const countiesProcessed = data.countiesProcessed || 0;
        const totalCounties = data.totalCounties || 1;
        const countyProgress = Math.round((countiesProcessed / totalCounties) * 100);
        setProgress(countyProgress);
        
        setEstimatedTime(data.estimatedTimeRemaining || null);
        
        // Build status message with county info
        let statusMsg = data.statusMessage || "Processing...";
        if (data.currentCounty) {
          statusMsg = `Processing ${data.currentCounty} County (${countiesProcessed + 1} of ${totalCounties})`;
        }
        setStatus(statusMsg);
      }
    } catch (err) {
      console.error("Status check error:", err);
      // Don't stop polling on transient errors
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
        // Start polling for status updates
        const interval = setInterval(() => {
          checkPipelineStatus(data.runId);
        }, 15000); // Poll every 15 seconds
        setPollingInterval(interval);
        
        // Initial status check
        checkPipelineStatus(data.runId);
      } else {
        // Legacy: immediate completion (for testing)
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
    
    if (pollingInterval) {
      clearInterval(pollingInterval);
      setPollingInterval(null);
    }
  }

  // Calculate elapsed time
  useEffect(() => {
    if (startTime && isRunning) {
      const interval = setInterval(() => {
        const elapsed = (Date.now() - startTime) / 1000;
        // Update estimated time if available, otherwise show elapsed
        if (!estimatedTime || estimatedTime > 0) {
          // Keep showing elapsed time
        }
      }, 1000);
      return () => clearInterval(interval);
    }
  }, [startTime, isRunning, estimatedTime]);

  // Cleanup polling on unmount
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
      <div className="min-h-screen bg-black text-white flex items-center justify-center p-8">
        <div className="w-full max-w-2xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h1 className="text-3xl font-bold text-center mb-8 text-white">
              School Contact Scraper
            </h1>
            
            <div className="flex flex-col space-y-6">
              {/* State Selection */}
              <div>
                <label htmlFor="state" className="block text-sm font-medium text-gray-300 mb-2">
                  Select State
                </label>
                <select
                  id="state"
                  value={selectedState}
                  onChange={(e) => setSelectedState(e.target.value)}
                  className="w-full px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">-- Select a state --</option>
                  {US_STATES.map((state) => (
                    <option key={state.value} value={state.value}>
                      {state.label}
                    </option>
                  ))}
                </select>
              </div>

              {/* Type Selection */}
              <div>
                <label htmlFor="type" className="block text-sm font-medium text-gray-300 mb-2">
                  Select Type
                </label>
                <div className="flex space-x-4">
                  <label className="flex items-center">
                    <input
                      type="radio"
                      name="type"
                      value="school"
                      checked={selectedType === "school"}
                      onChange={(e) => setSelectedType(e.target.value as "school" | "church")}
                      className="mr-2"
                    />
                    <span className="text-gray-300">School</span>
                  </label>
                  <label className="flex items-center">
                    <input
                      type="radio"
                      name="type"
                      value="church"
                      checked={selectedType === "church"}
                      onChange={(e) => setSelectedType(e.target.value as "school" | "church")}
                      disabled
                      className="mr-2 opacity-50 cursor-not-allowed"
                    />
                    <span className="text-gray-400 line-through">Church (Coming Soon)</span>
                  </label>
                </div>
              </div>

              {error && (
                <div className="p-4 bg-red-900/30 border border-red-800 rounded-lg">
                  <p className="text-red-300 text-sm">{error}</p>
                </div>
              )}

              <button
                onClick={runPipeline}
                disabled={isRunning || !selectedState}
                className={`w-full px-8 py-4 rounded-lg font-medium text-white transition-colors ${
                  isRunning || !selectedState
                    ? "bg-gray-600 cursor-not-allowed"
                    : "bg-blue-600 hover:bg-blue-700"
                }`}
              >
                {isRunning ? "Starting..." : "Start Search"}
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // PROGRESS VIEW
  if (viewState === "progress") {
    const elapsedTime = startTime ? (Date.now() - startTime) / 1000 : 0;
    const timeDisplay = estimatedTime !== null && estimatedTime > 0 
      ? `Est. ${formatTime(estimatedTime)} remaining`
      : `Elapsed: ${formatTime(elapsedTime)}`;

    const getStepMessage = (stepIndex: number) => {
      if (!summary?.steps) return "Processing...";
      
      const step = summary.steps[stepIndex];
      if (!step) return "Processing...";
      
      if (step.name.includes("Discovery")) {
        return step.schoolsFound 
          ? `Step 1: ${step.schoolsFound} Schools Discovered`
          : "Step 1: Discovering schools...";
      }
      if (step.name.includes("Page")) {
        return step.pagesDiscovered
          ? `Step 2: ${step.pagesDiscovered} Pages Discovered`
          : "Step 2: Discovering pages...";
      }
      if (step.name.includes("Content")) {
        return step.emailsFound
          ? `Step 3: ${step.emailsFound} Emails Found`
          : "Step 3: Collecting content...";
      }
      if (step.name.includes("Parsing") || step.name.includes("LLM")) {
        return step.contactsWithEmails !== undefined
          ? `Step 4: ${step.contactsWithEmails} Contacts with Emails, ${step.contactsWithoutEmails || 0} without`
          : "Step 4: Parsing with LLM...";
      }
      if (step.name.includes("Filter")) {
        return "Step 5: Filtering contacts...";
      }
      if (step.name.includes("Compilation") || step.name.includes("Final")) {
        return "Step 6: Final compilation...";
      }
      return step.name || "Processing...";
    };

    const stepMessages = summary?.steps 
      ? summary.steps.map((_, i) => getStepMessage(i))
      : ["Starting pipeline..."];

    return (
      <div className="min-h-screen bg-black text-white flex items-center justify-center p-8">
        <div className="w-full max-w-4xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h2 className="text-2xl font-bold text-center mb-2 text-white">
              SCRAPING IN PROGRESS
            </h2>
            <p className="text-center text-gray-400 mb-8">
              {selectedState ? US_STATES.find(s => s.value === selectedState)?.label : "Unknown"} - {selectedType === "school" ? "Schools" : "Churches"}
            </p>
            
            {/* Progress Bar */}
            <div className="mb-6">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm text-gray-400">Progress</span>
                <span className="text-sm text-gray-400">{Math.round(progress)}%</span>
              </div>
              <div className="w-full bg-gray-800 rounded-full h-4 mb-2">
                <div
                  className="bg-green-500 h-4 rounded-full transition-all duration-500"
                  style={{ width: `${progress}%` }}
                ></div>
              </div>
              <div className="flex justify-between items-center text-sm text-gray-400">
                <span>{timeDisplay}</span>
                <span>
                  {summary?.countiesProcessed || 0} / {summary?.totalCounties || 0} counties
                </span>
              </div>
            </div>

            {/* Current Status */}
            <div className="mb-6 p-4 bg-gray-800 rounded-lg border border-gray-700">
              <p className="text-white font-medium">{status}</p>
            </div>

            {/* Milestones */}
            <div className="space-y-2">
              <h3 className="text-lg font-semibold mb-3 text-white">Current Progress</h3>
              
              {/* County Progress */}
              {summary?.totalCounties && (
                <div className="p-3 rounded-lg bg-blue-900/30 border border-blue-800 mb-3">
                  <p className="text-sm text-blue-300">
                    <strong>Counties:</strong> {summary.countiesProcessed || 0} of {summary.totalCounties} processed
                    {summary.currentCounty && (
                      <span className="block mt-1">Currently processing: <strong>{summary.currentCounty}</strong></span>
                    )}
                  </p>
                </div>
              )}
              
              {/* Step Progress */}
              {summary?.steps && summary.steps.length > 0 && (
                <div className="space-y-2">
                  {summary.steps.map((step: StepSummary, index: number) => (
                    <div
                      key={index}
                      className="p-3 rounded-lg bg-green-900/30 border border-green-800"
                    >
                      <p className="text-sm text-green-300">
                        ✓ {step.name}
                        {step.schoolsFound !== undefined && (
                          <span className="block mt-1 text-xs text-green-400">
                            {step.schoolsFound} schools found
                          </span>
                        )}
                        {step.contactsWithEmails !== undefined && (
                          <span className="block mt-1 text-xs text-green-400">
                            {step.contactsWithEmails} contacts with emails, {step.contactsWithoutEmails || 0} without
                          </span>
                        )}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {error && (
              <div className="mt-6 p-6 bg-red-900/50 border-2 border-red-600 rounded-lg">
                <div className="flex items-start">
                  <div className="flex-shrink-0">
                    <svg className="h-6 w-6 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  </div>
                  <div className="ml-3 flex-1">
                    <h3 className="text-lg font-semibold text-red-300 mb-2">Pipeline Error</h3>
                    <p className="text-red-200 text-sm whitespace-pre-wrap">{error}</p>
                  </div>
                </div>
                <div className="mt-4">
                  <button
                    onClick={resetToStart}
                    className="px-4 py-2 bg-red-700 hover:bg-red-600 text-white rounded-lg font-medium transition-colors"
                  >
                    Return to Start
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  // SUMMARY VIEW
  if (viewState === "summary" && summary) {
    return (
      <div className="min-h-screen bg-black text-white flex items-center justify-center p-8">
        <div className="w-full max-w-2xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h2 className="text-2xl font-bold text-center mb-8 text-white">
              SCRAPING COMPLETE
            </h2>
            
            {/* Results Section */}
            <div className="bg-gray-800 rounded-lg p-6 mb-6 border border-gray-700">
              <h3 className="text-xl font-semibold mb-4 text-white">Results</h3>
              <div className="space-y-3">
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.schoolsFound || 0}</span> Schools found
                </p>
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.totalContacts || 0}</span> Contacts with emails
                </p>
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.totalContactsNoEmails || 0}</span> Contacts without emails
                </p>
              </div>
            </div>

            {/* Download Button */}
            <div className="flex flex-col space-y-4">
              {summary.csvData && summary.csvFilename ? (
                <button
                  onClick={() => downloadCSV(summary.csvData!, summary.csvFilename!)}
                  className="w-full px-6 py-4 bg-green-600 hover:bg-green-700 text-white rounded-lg font-medium transition-colors"
                >
                  Download Leads ({summary.totalContacts || 0} contacts)
                </button>
              ) : (
                <div className="w-full px-6 py-4 bg-yellow-900/30 border border-yellow-800 rounded-lg text-center mb-4">
                  <p className="text-yellow-300 text-sm">
                    No contacts were found. This may be normal if no schools were discovered or no contacts were extracted.
                  </p>
                </div>
              )}
              
              <button
                onClick={resetToStart}
                className="w-full px-6 py-4 bg-gray-700 hover:bg-gray-600 text-white rounded-lg font-medium transition-colors"
          >
                Run Another Search
              </button>
            </div>
          </div>
        </div>
    </div>
  );
  }

  return null;
}
