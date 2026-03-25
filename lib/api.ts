import { API_URLS } from './constants';
import type { ScraperType, RunMetadata, PipelineStatus, QueueJob } from './types';

function getBaseUrl(type: ScraperType): string {
  return (API_URLS[type] || '').replace(/\/+$/, '');
}

function getToken(): string | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem('auth_token');
}

function authHeaders(): Record<string, string> {
  const token = getToken();
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

export async function fetchRuns(type: ScraperType, includeArchived = false): Promise<RunMetadata[]> {
  const url = `${getBaseUrl(type)}/runs${includeArchived ? '?include_archived=true' : ''}`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch runs: ${res.status}`);
  const data = await res.json();
  return data.runs || [];
}

export async function fetchPipelineStatus(type: ScraperType, runId: string): Promise<PipelineStatus> {
  const url = `${getBaseUrl(type)}/pipeline-status/${runId}`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch pipeline status: ${res.status}`);
  return await res.json();
}

export type StartResult = {
  status: 'started' | 'queued';
  runId?: string;
  jobId?: number;
  position?: number;
  message?: string;
};

export async function startPipeline(type: ScraperType, state: string): Promise<StartResult> {
  const label = state.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  const kind = type === 'church' ? 'Churches' : 'Schools';
  const url = `${getBaseUrl(type)}/run-pipeline`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ state, display_name: `${label} ${kind}` }),
  });

  const data = await res.json().catch(() => null);

  // 202 = queued
  if (res.status === 202 && data?.status === 'queued') {
    return {
      status: 'queued',
      jobId: data.jobId,
      position: data.position,
      message: data.message,
    };
  }

  if (!res.ok) {
    const errMsg = data?.error || `Failed to start pipeline: ${res.status}`;
    throw new Error(errMsg);
  }

  return {
    status: 'started',
    runId: data?.runId || data?.run_id,
  };
}

export async function fetchQueue(type: ScraperType): Promise<QueueJob[]> {
  const url = `${getBaseUrl(type)}/queue`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) return []; // queue may not be enabled
  const data = await res.json();
  return (data.jobs || []).filter((j: QueueJob) => j.status === 'queued');
}

export async function cancelQueueJob(type: ScraperType, jobId: number): Promise<void> {
  const url = `${getBaseUrl(type)}/queue/${jobId}`;
  const res = await fetch(url, { method: 'DELETE', headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to cancel queue job: ${res.status}`);
}

export async function downloadCsv(type: ScraperType, runId: string): Promise<void> {
  const url = `${getBaseUrl(type)}/runs/${runId}/download`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to download CSV: ${res.status}`);
  const blob = await res.blob();
  const disposition = res.headers.get('content-disposition');
  const filename = disposition?.match(/filename="?([^"]+)"?/)?.[1] || `${runId}.csv`;
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(a.href);
}

export async function archiveRun(type: ScraperType, runId: string): Promise<void> {
  const url = `${getBaseUrl(type)}/runs/${runId}/archive`;
  const res = await fetch(url, { method: 'POST', headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to archive run: ${res.status}`);
}

export async function stopRun(type: ScraperType, runId: string): Promise<void> {
  const url = `${getBaseUrl(type)}/runs/${runId}/stop`;
  const res = await fetch(url, { method: 'POST', headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to stop run: ${res.status}`);
}
