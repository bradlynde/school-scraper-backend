import { COLORS } from '../lib/constants';
import type { ReactNode } from 'react';

export default function StatCard({ label, value, subtitle, icon }: {
  label: string;
  value: string | number;
  subtitle?: string;
  icon?: ReactNode;
}) {
  return (
    <div style={{
      background: COLORS.cardBg,
      borderRadius: 12,
      padding: '20px 24px',
      boxShadow: COLORS.cardShadow,
      border: `1px solid ${COLORS.cardBorder}`,
      flex: 1,
      minWidth: 160,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <div style={{ fontSize: 13, color: COLORS.textSecondary, marginBottom: 6, fontWeight: 500 }}>
            {label}
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, color: COLORS.textPrimary, lineHeight: 1.1 }}>
            {typeof value === 'number' ? value.toLocaleString() : value}
          </div>
          {subtitle && (
            <div style={{ fontSize: 12, color: COLORS.textMuted, marginTop: 6 }}>
              {subtitle}
            </div>
          )}
        </div>
        {icon && (
          <div style={{ fontSize: 24, color: COLORS.accent, opacity: 0.6 }}>
            {icon}
          </div>
        )}
      </div>
    </div>
  );
}
