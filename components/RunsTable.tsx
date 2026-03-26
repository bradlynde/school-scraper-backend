"use client";

import Link from 'next/link';
import { COLORS } from '../lib/constants';
import { downloadCsv, archiveRun } from '../lib/api';
import StatusBadge from './StatusBadge';
import type { RunMetadata, ScraperType } from '../lib/types';

function formatDate(dateStr?: string): string {
  if (!dateStr) return '-';
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/Chicago' });
  } catch {
    return dateStr;
  }
}

function formatState(state: string): string {
  return state.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

export default function RunsTable({ runs, scraperType, onArchive }: {
  runs: RunMetadata[];
  scraperType: ScraperType;
  onArchive?: () => void;
}) {
  const handleDownload = async (runId: string) => {
    try {
      await downloadCsv(scraperType, runId);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Download failed';
      alert(msg);
    }
  };

  const handleArchive = async (runId: string) => {
    if (!confirm('Archive this run? It will be hidden from the main view.')) return;
    try {
      await archiveRun(scraperType, runId);
      onArchive?.();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Archive failed';
      alert(msg);
    }
  };

  if (runs.length === 0) {
    return (
      <div style={{
        background: COLORS.cardBg,
        borderRadius: 12,
        padding: '40px 24px',
        textAlign: 'center',
        color: COLORS.textMuted,
        fontSize: 14,
        boxShadow: COLORS.cardShadow,
        border: `1px solid ${COLORS.cardBorder}`,
      }}>
        No runs yet. Start a new run to get started.
      </div>
    );
  }

  const thStyle: React.CSSProperties = {
    padding: '10px 14px',
    textAlign: 'left',
    fontSize: 11,
    fontWeight: 600,
    color: COLORS.textMuted,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    borderBottom: `2px solid ${COLORS.cardBorder}`,
  };

  const tdStyle: React.CSSProperties = {
    padding: '12px 14px',
    fontSize: 13,
    color: COLORS.textPrimary,
    borderBottom: `1px solid ${COLORS.cardBorder}`,
  };

  return (
    <div className="table-responsive" style={{
      background: COLORS.cardBg,
      borderRadius: 12,
      overflow: 'hidden',
      boxShadow: COLORS.cardShadow,
      border: `1px solid ${COLORS.cardBorder}`,
    }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th style={thStyle}>Run</th>
            <th style={thStyle}>State</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Contacts</th>
            <th style={thStyle}>Date</th>
            <th style={{ ...thStyle, textAlign: 'right' }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run, i) => (
            <tr key={run.run_id} style={{ background: i % 2 === 1 ? '#fafbfc' : '#fff', transition: 'background 0.15s' }}
              onMouseEnter={e => { e.currentTarget.style.background = '#f3f4f6'; }}
              onMouseLeave={e => { e.currentTarget.style.background = i % 2 === 1 ? '#fafbfc' : '#fff'; }}
            >
              <td style={tdStyle}>
                <Link
                  href={`/${scraperType}/${run.run_id}`}
                  style={{ color: COLORS.accent, textDecoration: 'none', fontWeight: 600 }}
                >
                  {run.display_name || `${formatState(run.state)} Run`}
                </Link>
              </td>
              <td style={tdStyle}>{formatState(run.state)}</td>
              <td style={tdStyle}><StatusBadge status={run.status} /></td>
              <td style={tdStyle}>{(run.total_contacts || 0).toLocaleString()}</td>
              <td style={tdStyle}>{formatDate(run.created_at)}</td>
              <td style={{ ...tdStyle, textAlign: 'right' }}>
                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                  {(run.status === 'done' || run.status === 'completed') && (
                    <button
                      onClick={() => handleDownload(run.run_id)}
                      style={{
                        background: 'none',
                        border: `1px solid ${COLORS.cardBorder}`,
                        borderRadius: 6,
                        padding: '4px 10px',
                        fontSize: 11,
                        color: COLORS.accent,
                        cursor: 'pointer',
                        fontWeight: 500,
                      }}
                    >
                      CSV
                    </button>
                  )}
                  {!run.archived && (run.status === 'done' || run.status === 'completed') && (
                    <button
                      onClick={() => handleArchive(run.run_id)}
                      style={{
                        background: 'none',
                        border: 'none',
                        padding: '4px 6px',
                        fontSize: 11,
                        color: COLORS.textMuted,
                        cursor: 'pointer',
                      }}
                    >
                      Archive
                    </button>
                  )}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
