#!/usr/bin/env python3
"""
Stress test for pipeline resource management.

Simulates long-running conditions to validate:
- No thread accumulation
- Proper resource cleanup
- Selenium driver recycling
- Garbage collection effectiveness

Run with: python3 stress_test.py
"""

import os
import sys
import time
import gc
import threading
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Try to import psutil for resource monitoring (optional)
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("⚠️  psutil not available - resource monitoring disabled")

from Pipeline import StreamingPipeline

def get_resource_stats():
    """Get current resource usage statistics."""
    stats = {
        'threads': threading.active_count(),
        'memory_mb': 0,
        'cpu_percent': 0
    }
    
    if HAS_PSUTIL:
        process = psutil.Process()
        stats['memory_mb'] = process.memory_info().rss / 1024 / 1024
        stats['cpu_percent'] = process.cpu_percent(interval=0.1)
    
    return stats

def print_resource_stats(label=""):
    """Print current resource statistics."""
    stats = get_resource_stats()
    print(f"\n{'='*60}")
    print(f"Resource Stats {label}")
    print(f"{'='*60}")
    print(f"Active Threads: {stats['threads']}")
    if HAS_PSUTIL:
        print(f"Memory Usage: {stats['memory_mb']:.1f} MB")
        print(f"CPU Usage: {stats['cpu_percent']:.1f}%")
    print(f"{'='*60}\n")

def stress_test_iterations(num_iterations=10, counties_per_iteration=3):
    """
    Run multiple pipeline iterations to simulate long-running conditions.
    
    Args:
        num_iterations: Number of times to run the pipeline
        counties_per_iteration: Number of counties to process per iteration
    """
    print(f"\n{'='*70}")
    print(f"STRESS TEST: {num_iterations} iterations × {counties_per_iteration} counties each")
    print(f"{'='*70}\n")
    
    # Test counties (using small subset for speed)
    test_counties = ["Autauga", "Baldwin", "Barbour", "Bibb", "Blount", 
                     "Bullock", "Butler", "Calhoun", "Chambers", "Cherokee"]
    
    # Get API keys from environment
    google_api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    
    if not google_api_key or not openai_api_key:
        print("⚠️  Warning: API keys not set. Some functionality may be limited.")
        print("   Set GOOGLE_PLACES_API_KEY and OPENAI_API_KEY environment variables")
    
    initial_stats = get_resource_stats()
    print_resource_stats("(Initial)")
    
    all_iteration_stats = []
    
    for iteration in range(1, num_iterations + 1):
        print(f"\n{'#'*70}")
        print(f"ITERATION {iteration}/{num_iterations}")
        print(f"{'#'*70}\n")
        
        iteration_start_time = time.time()
        iteration_start_stats = get_resource_stats()
        
        # Select counties for this iteration
        counties = test_counties[:counties_per_iteration]
        
        try:
            # Create pipeline
            pipeline = StreamingPipeline(
                google_api_key=google_api_key,
                openai_api_key=openai_api_key,
                global_max_api_calls=None,
                max_pages_per_school=2,
                state="alabama"
            )
            
            # Create temporary output file
            output_csv = f"/tmp/stress_test_iteration_{iteration}.csv"
            
            # Run pipeline for selected counties
            print(f"Processing counties: {', '.join(counties)}")
            pipeline.run(
                counties=counties,
                batch_size=0,
                output_csv=output_csv
            )
            
            # Cleanup pipeline (this should recycle the driver)
            print(f"\n[Iteration {iteration}] Cleaning up pipeline...")
            pipeline.cleanup()
            
            # Explicit garbage collection
            collected = gc.collect()
            print(f"[Iteration {iteration}] Garbage collection: {collected} objects collected")
            
            # Get stats after iteration
            iteration_end_stats = get_resource_stats()
            iteration_time = time.time() - iteration_start_time
            
            # Calculate resource delta
            thread_delta = iteration_end_stats['threads'] - iteration_start_stats['threads']
            memory_delta = iteration_end_stats['memory_mb'] - iteration_start_stats['memory_mb'] if HAS_PSUTIL else 0
            
            print(f"\n[Iteration {iteration}] Completed in {iteration_time:.1f}s")
            print(f"  Thread delta: {thread_delta:+d} (current: {iteration_end_stats['threads']})")
            if HAS_PSUTIL:
                print(f"  Memory delta: {memory_delta:+.1f} MB (current: {iteration_end_stats['memory_mb']:.1f} MB)")
            
            all_iteration_stats.append({
                'iteration': iteration,
                'time': iteration_time,
                'threads_start': iteration_start_stats['threads'],
                'threads_end': iteration_end_stats['threads'],
                'thread_delta': thread_delta,
                'memory_start': iteration_start_stats['memory_mb'],
                'memory_end': iteration_end_stats['memory_mb'],
                'memory_delta': memory_delta
            })
            
            # Warning if threads are accumulating
            if thread_delta > 2:
                print(f"  ⚠️  WARNING: Thread accumulation detected! (+{thread_delta} threads)")
            
            # Clean up output file
            if os.path.exists(output_csv):
                os.remove(output_csv)
            
        except Exception as e:
            print(f"\n❌ [Iteration {iteration}] FAILED: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        # Small delay between iterations
        time.sleep(1)
    
    # Final statistics
    final_stats = get_resource_stats()
    print_resource_stats("(Final)")
    
    print(f"\n{'='*70}")
    print("STRESS TEST SUMMARY")
    print(f"{'='*70}\n")
    
    # Calculate overall deltas
    total_thread_delta = final_stats['threads'] - initial_stats['threads']
    total_memory_delta = final_stats['memory_mb'] - initial_stats['memory_mb'] if HAS_PSUTIL else 0
    
    print(f"Total Iterations: {num_iterations}")
    print(f"Initial Threads: {initial_stats['threads']}")
    print(f"Final Threads: {final_stats['threads']}")
    print(f"Thread Delta: {total_thread_delta:+d}")
    
    if HAS_PSUTIL:
        print(f"Initial Memory: {initial_stats['memory_mb']:.1f} MB")
        print(f"Final Memory: {final_stats['memory_mb']:.1f} MB")
        print(f"Memory Delta: {total_memory_delta:+.1f} MB")
    
    # Check for resource leaks
    print(f"\n{'='*70}")
    print("LEAK DETECTION")
    print(f"{'='*70}\n")
    
    if total_thread_delta > 5:
        print(f"❌ THREAD LEAK DETECTED: {total_thread_delta} threads accumulated")
        print("   This indicates threads are not being properly released.")
    elif total_thread_delta > 2:
        print(f"⚠️  Minor thread accumulation: {total_thread_delta} threads")
        print("   Monitor this in production.")
    else:
        print(f"✓ Thread management OK: {total_thread_delta:+d} threads")
    
    if HAS_PSUTIL:
        if total_memory_delta > 500:  # 500 MB threshold
            print(f"❌ MEMORY LEAK DETECTED: {total_memory_delta:.1f} MB accumulated")
        elif total_memory_delta > 200:
            print(f"⚠️  Minor memory growth: {total_memory_delta:.1f} MB")
        else:
            print(f"✓ Memory management OK: {total_memory_delta:+.1f} MB")
    
    # Per-iteration analysis
    if all_iteration_stats:
        print(f"\n{'='*70}")
        print("PER-ITERATION ANALYSIS")
        print(f"{'='*70}\n")
        
        avg_thread_delta = sum(s['thread_delta'] for s in all_iteration_stats) / len(all_iteration_stats)
        max_thread_delta = max(s['thread_delta'] for s in all_iteration_stats)
        
        print(f"Average thread delta per iteration: {avg_thread_delta:+.2f}")
        print(f"Maximum thread delta in single iteration: {max_thread_delta:+d}")
        
        if max_thread_delta > 2:
            print(f"\n⚠️  Iteration with highest thread accumulation:")
            worst = max(all_iteration_stats, key=lambda x: x['thread_delta'])
            print(f"   Iteration {worst['iteration']}: {worst['thread_delta']:+d} threads")
    
    print(f"\n{'='*70}\n")
    
    return total_thread_delta <= 5  # Pass if thread delta is reasonable

if __name__ == "__main__":
    print("\n" + "="*70)
    print("PIPELINE STRESS TEST")
    print("="*70)
    print("\nThis test simulates long-running pipeline conditions to validate")
    print("resource management, thread cleanup, and driver recycling.\n")
    
    
    # Run stress test
    # Use fewer iterations and counties for faster testing
    # Increase for more thorough testing
    success = stress_test_iterations(
        num_iterations=5,  # Reduced for faster testing
        counties_per_iteration=2  # Process 2 counties per iteration
    )
    
    if success:
        print("✓ Stress test PASSED - Resource management looks good!")
        sys.exit(0)
    else:
        print("❌ Stress test FAILED - Resource leaks detected!")
        sys.exit(1)

