"use client";

import { useState } from "react";

type StepSummary = {
  name: string;
  [key: string]: number | string;
};

type PipelineSummary = {
  status: string;
  steps: StepSummary[];
  totalContacts: number;
  runId: string;
  csvData?: string;
  csvFilename?: string;
};

export default function Home() {
  const [status, setStatus] = useState("");
  const [summary, setSummary] = useState<PipelineSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);

  function downloadCSV(csvContent: string, filename: string) {
    // Create a blob with the CSV content
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    
    // Create a download link
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    
    link.setAttribute("href", url);
    link.setAttribute("download", filename);
    link.style.visibility = "hidden";
    
    // Trigger download
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Clean up
    URL.revokeObjectURL(url);
  }

  async function runPipeline() {
    setStatus("Starting pipeline...");
    setSummary(null);
    setError(null);
    setIsRunning(true);

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
        body: JSON.stringify({ maxSchools: 5 }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Backend responded with ${response.status}: ${errorText || response.statusText}`);
      }

      const data = await response.json();
      console.log("Pipeline response:", data);
      console.log("CSV data present:", !!data.csvData);
      setSummary(data);
      setStatus("Pipeline completed successfully!");
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      setStatus("Pipeline failed");
      console.error("Pipeline error:", err);
    } finally {
      setIsRunning(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">
      <main className="flex min-h-screen w-full max-w-4xl flex-col items-start py-16 px-8 bg-white dark:bg-black">
        <h1 className="text-4xl font-bold mb-8 text-black dark:text-zinc-50">
          School Scraper Dashboard
        </h1>

        <div className="w-full mb-8">
          <button
            onClick={runPipeline}
            disabled={isRunning}
            className={`px-6 py-3 rounded-lg font-medium text-white transition-colors ${
              isRunning
                ? "bg-gray-400 cursor-not-allowed"
                : "bg-blue-600 hover:bg-blue-700"
            }`}
          >
            {isRunning ? "Running Pipeline..." : "Run Full Pipeline"}
          </button>
        </div>

        {status && (
          <div className="w-full mb-4">
            <p className={`text-lg ${error ? "text-red-600" : "text-gray-700 dark:text-gray-300"}`}>
              <strong>Status:</strong> {status}
            </p>
          </div>
        )}

        {error && (
          <div className="w-full mb-4 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
            <p className="text-red-800 dark:text-red-200">
              <strong>Error:</strong> {error}
            </p>
            <p className="text-sm text-red-600 dark:text-red-400 mt-2">
              Make sure your backend endpoint accepts POST requests at /run-pipeline
            </p>
          </div>
        )}

        {summary && (
          <section className="w-full mt-6 p-6 bg-gray-50 dark:bg-gray-900 rounded-lg">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-2xl font-semibold text-black dark:text-zinc-50">
                Latest Run Summary
              </h2>
              {summary.csvData && summary.csvFilename && (
                <button
                  onClick={() => downloadCSV(summary.csvData!, summary.csvFilename!)}
                  className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg font-medium transition-colors"
                >
                  Download CSV
                </button>
              )}
            </div>
            <div className="space-y-2 mb-4">
              <p className="text-gray-700 dark:text-gray-300">
                <strong>Run ID:</strong> {summary.runId}
              </p>
              <p className="text-gray-700 dark:text-gray-300">
                <strong>Total Contacts:</strong> {summary.totalContacts}
              </p>
            </div>
            <div className="mt-4">
              <h3 className="text-xl font-semibold mb-2 text-black dark:text-zinc-50">Steps:</h3>
              <ul className="list-disc list-inside space-y-1">
                {summary.steps.map((step, index) => (
                  <li key={index} className="text-gray-700 dark:text-gray-300">
                    <strong>{step.name}:</strong>{" "}
                    {Object.entries(step)
                      .filter(([key]) => key !== "name")
                      .map(([key, value]) => `${key}: ${value}`)
                      .join(", ")}
                  </li>
                ))}
              </ul>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}
