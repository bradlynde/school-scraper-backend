// @ts-nocheck
"use client";

import { COLORS } from "../lib/constants";

interface MetricCardsProps {
  avgDurationPerCounty: string; // e.g. "4m 32s"
  avgCostPerContact: string; // e.g. "$0.03"
}

export default function MetricCards({ avgDurationPerCounty, avgCostPerContact }: MetricCardsProps) {
  const cards = [
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
      label: "Avg Cost / Contact",
      value: avgCostPerContact,
      icon: (
        <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke={COLORS.accent}>
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
    },
  ];

  return (
    <div className="grid-responsive" style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 16 }}>
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
            transition: "transform 0.2s ease, box-shadow 0.2s ease",
            cursor: "default",
          }}
          onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-2px)"; e.currentTarget.style.boxShadow = COLORS.cardShadowHover; }}
          onMouseLeave={e => { e.currentTarget.style.transform = "translateY(0)"; e.currentTarget.style.boxShadow = COLORS.cardShadow; }}
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
