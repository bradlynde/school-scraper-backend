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
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "";
      const response = await fetch(`${apiUrl}/pipeline-status/${runId}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
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
        
        setError(data.error || "Pipeline failed");
        setStatus("Pipeline failed");
        setIsRunning(false);
        setViewState("start");
      } else if (data.status === "running") {
        // Update progress
        setSummary(data);
        setCurrentStep(data.currentStep || 0);
        setProgress(data.progress || 0);
        setEstimatedTime(data.estimatedTimeRemaining || null);
        setStatus(data.statusMessage || "Processing...");
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
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "";

      if (!apiUrl) {
        throw new Error("API URL not configured. Please set NEXT_PUBLIC_API_URL in Vercel environment variables.");
      }

      const response = await fetch(`${apiUrl}/run-pipeline`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ 
          state: selectedState,
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
        }, 2000); // Poll every 2 seconds
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
                <span>{summary?.currentStep || 0} / {summary?.totalSteps || 7} steps</span>
              </div>
            </div>

            {/* Current Status */}
            <div className="mb-6 p-4 bg-gray-800 rounded-lg border border-gray-700">
              <p className="text-white font-medium">{status}</p>
            </div>

            {/* Milestones */}
            <div className="space-y-2">
              <h3 className="text-lg font-semibold mb-3 text-white">Milestones</h3>
              {stepMessages.map((message, index) => (
                <div
                  key={index}
                  className={`p-3 rounded-lg ${
                    index < (summary?.steps?.length || 0)
                      ? "bg-green-900/30 border border-green-800"
                      : "bg-gray-800 border border-gray-700"
                  }`}
                >
                  <p className={`text-sm ${
                    index < (summary?.steps?.length || 0)
                      ? "text-green-300"
                      : "text-gray-400"
                  }`}>
                    {index < (summary?.steps?.length || 0) ? "✓" : "○"} {message}
                  </p>
                </div>
              ))}
            </div>

            {error && (
              <div className="mt-6 p-4 bg-red-900/30 border border-red-800 rounded-lg">
                <p className="text-red-300">
                  <strong>Error:</strong> {error}
                </p>
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

            {/* Download Buttons */}
            <div className="flex flex-col space-y-4">
              {summary.csvData && summary.csvFilename && (
                <button
                  onClick={() => downloadCSV(summary.csvData!, summary.csvFilename!)}
                  className="w-full px-6 py-4 bg-green-600 hover:bg-green-700 text-white rounded-lg font-medium transition-colors"
                >
                  Download Leads with Emails ({summary.totalContacts || 0} contacts)
                </button>
              )}
              
              {summary.csvNoEmailsData && summary.csvNoEmailsFilename && (
                <button
                  onClick={() => downloadCSV(summary.csvNoEmailsData!, summary.csvNoEmailsFilename!)}
                  className="w-full px-6 py-4 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors"
                >
                  Download Leads without Emails ({summary.totalContactsNoEmails || 0} contacts)
                </button>
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
