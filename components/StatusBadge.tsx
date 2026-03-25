import { COLORS } from '../lib/constants';

const STATUS_STYLES: Record<string, { bg: string; color: string }> = {
  running: { bg: COLORS.runningBg, color: COLORS.running },
  finalizing: { bg: COLORS.runningBg, color: COLORS.running },
  done: { bg: COLORS.successBg, color: COLORS.success },
  completed: { bg: COLORS.successBg, color: COLORS.success },
  failed: { bg: COLORS.errorBg, color: COLORS.error },
  cancelled: { bg: COLORS.errorBg, color: COLORS.error },
  queued: { bg: COLORS.warningBg, color: COLORS.warning },
  pending: { bg: '#f0f0f0', color: '#888' },
  processing: { bg: COLORS.runningBg, color: COLORS.running },
};

export default function StatusBadge({ status }: { status: string }) {
  const s = STATUS_STYLES[status] || { bg: '#f0f0f0', color: '#888' };
  return (
    <span style={{
      display: 'inline-block',
      padding: '3px 10px',
      borderRadius: 20,
      fontSize: 11,
      fontWeight: 600,
      textTransform: 'capitalize',
      background: s.bg,
      color: s.color,
      letterSpacing: 0.3,
    }}>
      {status}
    </span>
  );
}
