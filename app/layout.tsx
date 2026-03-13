import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "LOE Generator API | NPSA",
  description: "Backend API for Engagement Letter Generator",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: "system-ui" }}>{children}</body>
    </html>
  );
}
