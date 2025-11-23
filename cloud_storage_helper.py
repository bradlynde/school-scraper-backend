"""
Cloud Storage Helper Functions
Handles reading/writing CSV files to/from Google Cloud Storage
"""

import os
from google.cloud import storage
from typing import Optional
import pandas as pd
import io

class CloudStorageHelper:
    """Helper class for Cloud Storage operations"""
    
    def __init__(self, bucket_name: str):
        """
        Initialize Cloud Storage client
        
        Args:
            bucket_name: Name of the GCS bucket
        """
        self.bucket_name = bucket_name
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
        except Exception as e:
            # If credentials not available, will fail when actually using
            self.client = None
            self.bucket = None
            print(f"Warning: Cloud Storage client initialization failed: {e}")
            print("Will use local file system as fallback")
    
    def upload_csv(self, local_path: str, gcs_path: str) -> bool:
        """
        Upload a CSV file to Cloud Storage
        
        Args:
            local_path: Local file path
            gcs_path: GCS path (e.g., 'runs/run_id/step1_schools.csv')
            
        Returns:
            True if successful, False otherwise
        """
        if not self.bucket:
            return False
        
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            return True
        except Exception as e:
            print(f"Error uploading to Cloud Storage: {e}")
            return False
    
    def download_csv(self, gcs_path: str, local_path: str) -> bool:
        """
        Download a CSV file from Cloud Storage
        
        Args:
            gcs_path: GCS path (e.g., 'runs/run_id/step1_schools.csv')
            local_path: Local file path to save to
            
        Returns:
            True if successful, False otherwise
        """
        if not self.bucket:
            return False
        
        try:
            blob = self.bucket.blob(gcs_path)
            if not blob.exists():
                return False
            blob.download_to_filename(local_path)
            return True
        except Exception as e:
            print(f"Error downloading from Cloud Storage: {e}")
            return False
    
    def read_csv_to_dataframe(self, gcs_path: str) -> Optional[pd.DataFrame]:
        """
        Read a CSV from Cloud Storage directly into a pandas DataFrame
        
        Args:
            gcs_path: GCS path
            
        Returns:
            DataFrame or None if error
        """
        if not self.bucket:
            return None
        
        try:
            blob = self.bucket.blob(gcs_path)
            if not blob.exists():
                return None
            content = blob.download_as_text()
            return pd.read_csv(io.StringIO(content))
        except Exception as e:
            print(f"Error reading CSV from Cloud Storage: {e}")
            return None
    
    def write_dataframe_to_csv(self, df: pd.DataFrame, gcs_path: str) -> bool:
        """
        Write a DataFrame directly to Cloud Storage as CSV
        
        Args:
            df: pandas DataFrame
            gcs_path: GCS path
            
        Returns:
            True if successful, False otherwise
        """
        if not self.bucket:
            return False
        
        try:
            blob = self.bucket.blob(gcs_path)
            csv_string = df.to_csv(index=False)
            blob.upload_from_string(csv_string, content_type='text/csv')
            return True
        except Exception as e:
            print(f"Error writing DataFrame to Cloud Storage: {e}")
            return False
    
    def file_exists(self, gcs_path: str) -> bool:
        """
        Check if a file exists in Cloud Storage
        
        Args:
            gcs_path: GCS path
            
        Returns:
            True if exists, False otherwise
        """
        if not self.bucket:
            return False
        
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            print(f"Error checking file existence: {e}")
            return False
    
    def get_file_content(self, gcs_path: str) -> Optional[str]:
        """
        Get file content as string
        
        Args:
            gcs_path: GCS path
            
        Returns:
            File content as string or None if error
        """
        if not self.bucket:
            return None
        
        try:
            blob = self.bucket.blob(gcs_path)
            if not blob.exists():
                return None
            return blob.download_as_text()
        except Exception as e:
            print(f"Error reading file from Cloud Storage: {e}")
            return None

