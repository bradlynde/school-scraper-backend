import type { Metadata } from "next";
import AppShell from "../components/AppShell";

export const metadata: Metadata = {
  title: "NPSA Tools",
  description: "Nonprofit Security Advisors — Tools Dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <link
          href="https://fonts.googleapis.com/css2?family=Ms+Madi&display=swap"
          rel="stylesheet"
        />
      </head>
      <body style={{ margin: 0, fontFamily: "Inter, system-ui, sans-serif" }}>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
