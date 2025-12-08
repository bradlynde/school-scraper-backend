import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'School Contact Scraper',
  description: 'Scrape contact information from Christian schools',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}

