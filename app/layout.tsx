import type { Metadata } from "next";
import Sidebar from "../components/Sidebar";

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
        <div style={{ display: "flex", height: "100vh" }}>
          <Sidebar />
          <main style={{ flex: 1, overflowY: "auto", background: "#f4f5f7" }}>
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
