export type ScraperType = 'church' | 'school';

export type RunStatus = 'running' | 'finalizing' | 'done' | 'failed' | 'cancelled';

export type RunMetadata = {
  run_id: string;
  state: string;
  status: RunStatus;
  scraper_type?: ScraperType;
  total_counties?: number;
  completed_counties?: number;
  total_contacts?: number;
  total_contacts_with_emails?: number;
  created_at?: string;
  completed_at?: string;
  csv_filename?: string;
  archived?: boolean;
  display_name?: string;
};

export type CountyTask = {
  id: number;
  run_id: string;
  county: string;
  status: 'pending' | 'processing' | 'done' | 'failed';
  claimed_by?: string;
  claimed_at?: string;
  completed_at?: string;
  result_json?: {
    success: boolean;
    churches?: number;
    schools?: number;
    contacts: number;
    contacts_with_emails: number;
    contacts_without_emails: number;
    places_api_calls?: number;
    openai_calls?: number;
  };
  error?: string;
};

export type PipelineStatus = {
  status: string;
  run_id: string;
  state?: string;
  totalContacts?: number;
  totalContactsWithEmails?: number;
  countiesProcessed?: number;
  totalCounties?: number;
  currentCounty?: string;
  countyTasks?: CountyTask[];
  csvData?: string;
  csvFilename?: string;
  total_contacts?: number;
  total_contacts_with_emails?: number;
  counties_processed?: number;
  total_counties?: number;
};

export type QueueJob = {
  id: number;
  state: string;
  display_name?: string;
  status: string;
  run_id?: string;
  created_at?: string;
};
