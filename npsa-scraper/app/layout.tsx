import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "LOE Generator | NPSA",
  description: "Engagement Letter Generator for Nonprofit Security Advisors",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
