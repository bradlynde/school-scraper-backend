/**
 * School vs church backends must use different base URLs.
 * If both resolve to the same string, the Running/Finished lists will duplicate
 * and scraper_type filtering cannot fix mixed data from one API.
 */

export function normalizeScraperApiBaseUrl(raw: string): string {
  let url = raw.trim().replace(/\/+$/, "");
  if (!url) return "";
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  return url;
}

export function resolveSchoolScraperApiUrl(): string {
  return normalizeScraperApiBaseUrl(
    process.env.NEXT_PUBLIC_SCHOOL_API_URL ||
      process.env.NEXT_PUBLIC_API_URL ||
      "https://school-scraper-backend-production.up.railway.app"
  );
}

export function resolveChurchScraperApiUrl(): string {
  return normalizeScraperApiBaseUrl(
    process.env.NEXT_PUBLIC_CHURCH_API_URL ||
      "https://church-scraper-backend-production.up.railway.app"
  );
}

export function getApiUrlForScraperContext(ctx: "school" | "church"): string {
  return ctx === "church"
    ? resolveChurchScraperApiUrl()
    : resolveSchoolScraperApiUrl();
}

/** True when misconfigured env points both tabs at the same host (normalized). */
export function scraperSchoolAndChurchUrlsCollide(): boolean {
  const a = resolveSchoolScraperApiUrl().toLowerCase();
  const b = resolveChurchScraperApiUrl().toLowerCase();
  return Boolean(a && b && a === b);
}
