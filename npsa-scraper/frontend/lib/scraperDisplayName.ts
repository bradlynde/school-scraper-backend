/** Normalize API/legacy labels so the scraper suffix is title case, e.g. "Delaware Churches". */
export function normalizeScraperDisplayTitle(label: string): string {
  const t = label.trim();
  if (!t) return t;
  if (/churches$/i.test(t)) return t.replace(/churches$/i, "Churches");
  if (/schools$/i.test(t)) return t.replace(/schools$/i, "Schools");
  return t;
}
