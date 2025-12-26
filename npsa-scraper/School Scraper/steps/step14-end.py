"""
STEP 14: CLEANUP AND RESOURCE MANAGEMENT
=========================================
Cleanup module to ensure proper resource management and prevent resource leaks.

This module provides cleanup utilities for:
- Selenium Chrome drivers
- Pipeline resources
- Temporary files and connections

Critical for long-running processes to prevent resource exhaustion.
"""

import logging
from typing import Optional


class ResourceCleanup:
    """Utility class for cleaning up resources"""
    
    @staticmethod
    def cleanup_selenium_driver(driver) -> bool:
        """
        Safely cleanup a Selenium Chrome driver.
        
        Args:
            driver: Selenium WebDriver instance (or None)
            
        Returns:
            bool: True if cleanup succeeded, False otherwise
        """
        if driver is None:
            return True
        
        try:
            # Try to quit the driver gracefully
            driver.quit()
            return True
        except Exception as e:
            # If quit() fails, try close()
            try:
                driver.close()
                return True
            except Exception as e2:
                # Log but don't raise - cleanup failures shouldn't crash the app
                print(f"    Warning: Failed to cleanup Selenium driver: {e2}")
                return False
    
    @staticmethod
    def cleanup_pipeline_resources(pipeline) -> bool:
        """
        Cleanup all resources associated with a StreamingPipeline instance.
        
        Args:
            pipeline: StreamingPipeline instance (or None)
            
        Returns:
            bool: True if cleanup succeeded, False otherwise
        """
        if pipeline is None:
            return True
        
        try:
            # Cleanup ContentCollector's Selenium driver
            if hasattr(pipeline, 'content_collector') and pipeline.content_collector:
                if hasattr(pipeline.content_collector, 'driver'):
                    ResourceCleanup.cleanup_selenium_driver(pipeline.content_collector.driver)
                    pipeline.content_collector.driver = None
            
            # Cleanup any other resources if needed
            # (Add more cleanup logic here as needed)
            
            return True
        except Exception as e:
            print(f"    Warning: Failed to cleanup pipeline resources: {e}")
            return False


def cleanup_on_exit(pipeline=None, driver=None):
    """
    Convenience function to cleanup resources on exit.
    Can be used in finally blocks or atexit handlers.
    
    Args:
        pipeline: Optional StreamingPipeline instance to cleanup
        driver: Optional Selenium WebDriver instance to cleanup
    """
    if pipeline:
        ResourceCleanup.cleanup_pipeline_resources(pipeline)
    if driver:
        ResourceCleanup.cleanup_selenium_driver(driver)

