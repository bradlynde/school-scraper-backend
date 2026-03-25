import { COLORS } from '../lib/constants';

export default function ProgressBar({ completed, total, label }: {
  completed: number;
  total: number;
  label?: string;
}) {
  const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
  return (
    <div>
      {label && (
        <div style={{ fontSize: 12, color: COLORS.textSecondary, marginBottom: 6 }}>{label}</div>
      )}
      <div style={{
        background: '#e5e7eb',
        borderRadius: 6,
        height: 22,
        position: 'relative',
        overflow: 'hidden',
      }}>
        <div style={{
          background: COLORS.accent,
          height: '100%',
          width: `${pct}%`,
          borderRadius: 8,
          transition: 'width 0.5s ease',
        }} />
        <div style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 11,
          fontWeight: 600,
          color: pct > 50 ? '#fff' : COLORS.textPrimary,
        }}>
          {completed} / {total} counties ({pct}%)
        </div>
      </div>
    </div>
  );
}
