export const COLORS = {
  sidebarBg: '#1a2540',
  sidebarText: '#e8eaf0',
  sidebarMuted: '#6c7a9c',
  sidebarActive: '#2a3d60',
  sidebarBorder: '#2a3550',
  accent: '#1a4a6e',
  accentLight: '#5b9ec9',
  pageBg: '#f4f5f7',
  cardBg: '#ffffff',
  cardBorder: '#e8eaed',
  cardShadow: '0 2px 12px rgba(0,0,0,0.06)',
  textPrimary: '#1a1a1a',
  textSecondary: '#666',
  textMuted: '#999',
  success: '#2d8a4e',
  successBg: '#e8f5e9',
  warning: '#e6a117',
  warningBg: '#fff8e1',
  error: '#d32f2f',
  errorBg: '#ffebee',
  running: '#1a4a6e',
  runningBg: '#e3f0fa',
};

function ensureProtocol(url: string): string {
  if (!url) return url;
  return url.match(/^https?:\/\//) ? url : `https://${url}`;
}

export const API_URLS: Record<string, string> = {
  church: ensureProtocol(process.env.NEXT_PUBLIC_CHURCH_API_URL || 'https://church-scraper-production.up.railway.app'),
  school: ensureProtocol(process.env.NEXT_PUBLIC_SCHOOL_API_URL || 'https://npsa-scraper.up.railway.app'),
};

export const LOE_URL = ensureProtocol(process.env.NEXT_PUBLIC_LOE_URL || 'https://loe-generator-production.up.railway.app');

export const US_STATES = [
  { value: 'alabama', label: 'Alabama' },
  { value: 'alaska', label: 'Alaska' },
  { value: 'arizona', label: 'Arizona' },
  { value: 'arkansas', label: 'Arkansas' },
  { value: 'california', label: 'California' },
  { value: 'colorado', label: 'Colorado' },
  { value: 'connecticut', label: 'Connecticut' },
  { value: 'delaware', label: 'Delaware' },
  { value: 'florida', label: 'Florida' },
  { value: 'georgia', label: 'Georgia' },
  { value: 'hawaii', label: 'Hawaii' },
  { value: 'idaho', label: 'Idaho' },
  { value: 'illinois', label: 'Illinois' },
  { value: 'indiana', label: 'Indiana' },
  { value: 'iowa', label: 'Iowa' },
  { value: 'kansas', label: 'Kansas' },
  { value: 'kentucky', label: 'Kentucky' },
  { value: 'louisiana', label: 'Louisiana' },
  { value: 'maine', label: 'Maine' },
  { value: 'maryland', label: 'Maryland' },
  { value: 'massachusetts', label: 'Massachusetts' },
  { value: 'michigan', label: 'Michigan' },
  { value: 'minnesota', label: 'Minnesota' },
  { value: 'mississippi', label: 'Mississippi' },
  { value: 'missouri', label: 'Missouri' },
  { value: 'montana', label: 'Montana' },
  { value: 'nebraska', label: 'Nebraska' },
  { value: 'nevada', label: 'Nevada' },
  { value: 'new_hampshire', label: 'New Hampshire' },
  { value: 'new_jersey', label: 'New Jersey' },
  { value: 'new_mexico', label: 'New Mexico' },
  { value: 'new_york', label: 'New York' },
  { value: 'north_carolina', label: 'North Carolina' },
  { value: 'north_dakota', label: 'North Dakota' },
  { value: 'ohio', label: 'Ohio' },
  { value: 'oklahoma', label: 'Oklahoma' },
  { value: 'oregon', label: 'Oregon' },
  { value: 'pennsylvania', label: 'Pennsylvania' },
  { value: 'rhode_island', label: 'Rhode Island' },
  { value: 'south_carolina', label: 'South Carolina' },
  { value: 'south_dakota', label: 'South Dakota' },
  { value: 'tennessee', label: 'Tennessee' },
  { value: 'texas', label: 'Texas' },
  { value: 'utah', label: 'Utah' },
  { value: 'vermont', label: 'Vermont' },
  { value: 'virginia', label: 'Virginia' },
  { value: 'washington', label: 'Washington' },
  { value: 'west_virginia', label: 'West Virginia' },
  { value: 'wisconsin', label: 'Wisconsin' },
  { value: 'wyoming', label: 'Wyoming' },
];

export const SCRAPER_LABELS = {
  church: { singular: 'Church', plural: 'Churches', title: 'Church Scraper' },
  school: { singular: 'School', plural: 'Schools', title: 'School Scraper' },
};
