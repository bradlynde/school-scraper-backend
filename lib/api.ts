import { API_URLS } from './constants';
import type { ScraperType, RunMetadata, PipelineStatus } from './types';

function getBaseUrl(type: ScraperType): string {
  return (API_URLS[type] || '').replace(/\/+$/, '');
}

export async function fetchRuns(type: ScraperType, includeArchived = false): Promise<RunMetadata[]> {
  const url = `${getBaseUrl(type)}/runs${includeArchived ? '?include_archived=true' : ''}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch runs: ${res.status}`);
  const data = await res.json();
  return data.runs || [];
}

export async function fetchPipelineStatus(type: ScraperType, runId: string): Promise<PipelineStatus> {
  const url = `${getBaseUrl(type)}/pipeline-status/${runId}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch pipeline status: ${res.status}`);
  return await res.json();
}

export async function startPipeline(type: ScraperType, state: string): Promise<{ run_id: string }> {
  const label = state.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  const kind = type === 'church' ? 'Churches' : 'Schools';
  const url = `${getBaseUrl(type)}/run-pipeline`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ state, display_name: `${label} ${kind}` }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(err || `Failed to start pipeline: ${res.status}`);
  }
  return await res.json();
}

export async function downloadCsv(type: ScraperType, runId: string): Promise<void> {
  const url = `${getBaseUrl(type)}/runs/${runId}/download`;
  const res = await fetch(url);
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
  const res = await fetch(url, { method: 'POST' });
  if (!res.ok) throw new Error(`Failed to archive run: ${res.status}`);
}

export async function stopRun(type: ScraperType, runId: string): Promise<void> {
  const url = `${getBaseUrl(type)}/runs/${runId}/stop`;
  const res = await fetch(url, { method: 'POST' });
  if (!res.ok) throw new Error(`Failed to stop run: ${res.status}`);
}
