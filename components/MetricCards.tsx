// @ts-nocheck
"use client";

import { COLORS } from "../lib/constants";

interface MetricCardsProps {
  loesGenerated: number;
  avgDurationPerCounty: string; // e.g. "4m 32s"
  avgCostPerCounty: string; // e.g. "$0.12"
}

export default function MetricCards({ loesGenerated, avgDurationPerCounty, avgCostPerCounty }: MetricCardsProps) {
  const cards = [
    {
      label: "LOEs Generated",
      value: loesGenerated,
      icon: (
        <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke={COLORS.accent}>
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
      ),
    },
    {
      label: "Avg Duration / County",
      value: avgDurationPerCounty,
      icon: (
        <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke={COLORS.accent}>
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
    },
    {
      label: "Avg Cost / County",
      value: avgCostPerCounty,
      icon: (
        <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke={COLORS.accent}>
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
    },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
      {cards.map((card) => (
        <div
          key={card.label}
          style={{
            background: COLORS.cardBg,
            borderRadius: 12,
            padding: "20px 24px",
            border: `1px solid ${COLORS.cardBorder}`,
            boxShadow: COLORS.cardShadow,
            display: "flex",
            alignItems: "center",
            gap: 16,
          }}
        >
          <div style={{
            width: 40,
            height: 40,
            borderRadius: 10,
            background: COLORS.runningBg,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexShrink: 0,
          }}>
            {card.icon}
          </div>
          <div>
            <div style={{ fontSize: 12, color: COLORS.textMuted, fontWeight: 500, marginBottom: 2, textTransform: "uppercase", letterSpacing: "0.04em" }}>
              {card.label}
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: COLORS.textPrimary }}>
              {typeof card.value === "number" ? card.value.toLocaleString() : card.value}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
