"use client";

import { useState } from "react";

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
};

type ViewState = "start" | "progress" | "summary";

export default function Home() {
  const [viewState, setViewState] = useState<ViewState>("start");
  const [status, setStatus] = useState("");
  const [summary, setSummary] = useState<PipelineSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [currentStep, setCurrentStep] = useState(0);
  const [progress, setProgress] = useState(0);

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

  async function runPipeline() {
    setViewState("progress");
    setStatus("Starting pipeline...");
    setSummary(null);
    setError(null);
    setIsRunning(true);
    setCurrentStep(0);
    setProgress(0);

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
        body: JSON.stringify({ maxSchools: 100 }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Backend responded with ${response.status}: ${errorText || response.statusText}`);
      }

      const data = await response.json();
      console.log("Pipeline response:", data);
      
      // Set summary immediately so progress view can show step results
      setSummary(data);
      
      // Simulate step-by-step progress updates based on actual steps
      if (data.steps && data.steps.length > 0) {
        for (let i = 0; i < data.steps.length; i++) {
          await new Promise(resolve => setTimeout(resolve, 800));
          setCurrentStep(i + 1);
          setProgress(((i + 1) / data.steps.length) * 100);
        }
      }
      
      setStatus("Pipeline completed successfully!");
      // Wait a moment before switching to summary
      await new Promise(resolve => setTimeout(resolve, 1000));
      setViewState("summary");
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      setStatus("Pipeline failed");
      console.error("Pipeline error:", err);
      setViewState("start");
    } finally {
      setIsRunning(false);
    }
  }

  function resetToStart() {
    setViewState("start");
    setStatus("");
    setSummary(null);
    setError(null);
    setCurrentStep(0);
    setProgress(0);
  }

  // START VIEW
  if (viewState === "start") {
    return (
      <div className="min-h-screen bg-black text-white flex items-center justify-center p-8">
        <div className="w-full max-w-6xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h1 className="text-3xl font-bold text-center mb-8 text-white">
              School Contact Scraper
            </h1>
            
            <div className="flex flex-col items-center justify-center space-y-6">
              <p className="text-gray-400 text-center">
                Click the button below to start scraping school contacts
              </p>
              
              <button
                onClick={runPipeline}
                disabled={isRunning}
                className={`px-8 py-4 rounded-lg font-medium text-white transition-colors ${
                  isRunning
                    ? "bg-gray-600 cursor-not-allowed"
                    : "bg-blue-600 hover:bg-blue-700"
                }`}
              >
                {isRunning ? "Starting..." : "Start Scraping"}
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // PROGRESS VIEW
  if (viewState === "progress") {
    const getStepMessage = (stepIndex: number) => {
      if (stepIndex === 0) return "Starting scraping...";
      if (stepIndex === 1) {
        const step = summary?.steps[0];
        return step ? `Step 1: ${step.schoolsFound || 0} Schools Discovered` : "Step 1: Discovering schools...";
      }
      if (stepIndex === 2) {
        const step = summary?.steps[1];
        return step ? `Step 2: ${step.pagesDiscovered || 0} Pages Discovered` : "Step 2: Discovering pages...";
      }
      if (stepIndex === 3) {
        const step = summary?.steps[2];
        return step ? `Step 3: ${step.emailsFound || 0} Emails Found` : "Step 3: Collecting content...";
      }
      if (stepIndex === 4) {
        const step = summary?.steps[3];
        return step ? `Step 4: ${step.contactsWithEmails || 0} Contacts with Emails, ${step.contactsWithoutEmails || 0} Contacts without Emails` : "Step 4: Parsing with LLM...";
      }
      if (stepIndex === 5) {
        const step = summary?.steps[4];
        return step ? `Step 5: Final compilation complete` : "Step 5: Compiling results...";
      }
      return "Processing...";
    };

    const stepMessages = Array.from({ length: currentStep + 1 }, (_, i) => getStepMessage(i));

    return (
      <div className="min-h-screen bg-black text-white flex items-center justify-center p-8">
        <div className="w-full max-w-6xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h2 className="text-2xl font-bold text-center mb-8 text-white">
              SCRAPING IN PROGRESS
            </h2>
            
            {/* Progress Bar */}
            <div className="mb-8">
              <div className="w-full bg-gray-800 rounded-full h-4 mb-4">
                <div
                  className="bg-green-500 h-4 rounded-full transition-all duration-500"
                  style={{ width: `${progress}%` }}
                ></div>
              </div>
            </div>

            {/* Status Messages */}
            <div className="space-y-2">
              {stepMessages.slice(0, currentStep + 1).map((message, index) => (
                <p key={index} className="text-gray-300">
                  {message}
                </p>
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
        <div className="w-full max-w-6xl">
          <div className="bg-gray-900 rounded-lg border border-gray-800 p-8 shadow-xl">
            <h2 className="text-2xl font-bold text-center mb-8 text-white">
              SUMMARY
            </h2>
            
            {/* Results Section */}
            <div className="bg-gray-800 rounded-lg p-6 mb-6 border border-gray-700">
              <h3 className="text-xl font-semibold mb-4 text-white">Results</h3>
              <div className="space-y-3">
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.schoolsFound || 0}</span> Schools found
                </p>
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.totalContacts || 0}</span> Contacts with emails found
                </p>
                <p className="text-gray-300">
                  <span className="font-semibold">{summary.totalContactsNoEmails || 0}</span> Contacts without emails found
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
                  Download Contacts with Emails
                </button>
              )}
              
              {summary.csvNoEmailsData && summary.csvNoEmailsFilename && (
                <button
                  onClick={() => downloadCSV(summary.csvNoEmailsData!, summary.csvNoEmailsFilename!)}
                  className="w-full px-6 py-4 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors"
                >
                  Download Contacts without Emails
                </button>
              )}
              
              <button
                onClick={resetToStart}
                className="w-full px-6 py-4 bg-gray-700 hover:bg-gray-600 text-white rounded-lg font-medium transition-colors"
              >
                Run Again
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return null;
}
