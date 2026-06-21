"""
FlightTracker — Pipeline Orchestrator
Runs the entire pipeline in one command.

Usage:
    python run_pipeline.py                    # Run with defaults (10 min streaming)
    python run_pipeline.py --stream-minutes 5 # Run streaming for 5 minutes
    python run_pipeline.py --skip-streaming   # Only run batch pipeline

What this does:
    1. Setup GCP infrastructure (if not exists)
    2. Run batch ingestion (historical data)
    3. Run streaming (producer + subscriber) for N minutes
    4. Run data cleaning and transformation
    5. Print summary
"""
import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_script(script_path, description, timeout=None):
    """
    Run a Python script as a subprocess.
    
    Returns:
        (success: bool, output: str, duration: float)
    """
    logger.info(f"{'='*50}")
    logger.info(f"Running: {description}")
    logger.info(f"Script: {script_path}")
    logger.info(f"{'='*50}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"✓ {description} completed in {duration:.1f}s")
            return True, result.stdout, duration
        else:
            logger.error(f"✗ {description} failed (exit code {result.returncode})")
            logger.error(f"  stderr: {result.stderr[-500:]}")  # Last 500 chars
            return False, result.stderr, duration
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        logger.info(f"✓ {description} stopped after {duration:.1f}s (timeout reached)")
        return True, "Timeout reached", duration
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"✗ {description} error: {e}")
        return False, str(e), duration


def run_streaming(duration_minutes):
    """
    Run the streaming producer and subscriber in parallel.
    
    Both scripts run as background processes.
    After `duration_minutes`, we stop them.
    """
    logger.info(f"{'='*50}")
    logger.info(f"Starting Streaming Pipeline ({duration_minutes} minutes)")
    logger.info(f"{'='*50}")
    
    start_time = time.time()
    timeout_seconds = duration_minutes * 60
    
    # Start both scripts as background processes
    producer_proc = subprocess.Popen(
        [sys.executable, "src/stream_ingestion.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    logger.info(f"  ✓ Producer started (PID: {producer_proc.pid})")
    
    subscriber_proc = subprocess.Popen(
        [sys.executable, "src/subscriber.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    logger.info(f"  ✓ Subscriber started (PID: {subscriber_proc.pid})")
    
    # Wait for the duration
    logger.info(f"  Streaming for {duration_minutes} minutes... (Ctrl+C to stop early)")
    
    try:
        time.sleep(timeout_seconds)
    except KeyboardInterrupt:
        logger.info("  Stopped early by user")
    
    # Stop both processes
    logger.info("  Stopping streaming processes...")
    producer_proc.terminate()
    subscriber_proc.terminate()
    
    # Wait for them to finish
    producer_proc.wait(timeout=10)
    subscriber_proc.wait(timeout=10)
    
    duration = time.time() - start_time
    logger.info(f"  ✓ Streaming completed in {duration:.1f}s")
    
    return True, duration


def main():
    parser = argparse.ArgumentParser(description="FlightTracker Pipeline Orchestrator")
    parser.add_argument(
        "--stream-minutes",
        type=int,
        default=10,
        help="How long to run the streaming pipeline (default: 10 minutes)"
    )
    parser.add_argument(
        "--skip-streaming",
        action="store_true",
        help="Skip the streaming pipeline (only run batch)"
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip the GCP setup step"
    )
    
    args = parser.parse_args()
    
    # Track results
    results = {}
    pipeline_start = time.time()
    
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║     FlightTracker — Full Pipeline Execution      ║")
    logger.info("╚══════════════════════════════════════════════════╝")
    logger.info("")
    
    # ──────────────────────────────────────────────
    # Step 1: Setup GCP Infrastructure
    # ──────────────────────────────────────────────
    if not args.skip_setup:
        success, output, duration = run_script("setup.py", "GCP Setup")
        results["setup"] = {"success": success, "duration": duration}
        
        if not success:
            logger.error("Setup failed. Cannot continue.")
            sys.exit(1)
    else:
        logger.info("Skipping GCP setup (--skip-setup)")
    
    # ──────────────────────────────────────────────
    # Step 2: Batch Ingestion
    # ──────────────────────────────────────────────
    success, output, duration = run_script(
        "src/batch_ingestion.py",
        "Batch Ingestion (Historical Data)"
    )
    results["batch"] = {"success": success, "duration": duration}
    
    if not success:
        logger.warning("Batch ingestion failed. Continuing with streaming...")
    
    # ──────────────────────────────────────────────
    # Step 3: Streaming Pipeline
    # ──────────────────────────────────────────────
    if not args.skip_streaming:
        success, duration = run_streaming(args.stream_minutes)
        results["streaming"] = {"success": success, "duration": duration}
    else:
        logger.info("Skipping streaming (--skip-streaming)")
    
    # ──────────────────────────────────────────────
    # Step 4: Data Cleaning
    # ──────────────────────────────────────────────
    success, output, duration = run_script(
        "src/data_cleaning.py",
        "Data Cleaning & Transformation"
    )
    results["cleaning"] = {"success": success, "duration": duration}
    
    # ──────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────
    total_duration = time.time() - pipeline_start
    
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║              Pipeline Summary                    ║")
    logger.info("╚══════════════════════════════════════════════════╝")
    
    for step, result in results.items():
        status = "✓" if result["success"] else "✗"
        logger.info(f"  {status} {step}: {result['duration']:.1f}s")
    
    logger.info(f"  ─────────────────────────────")
    logger.info(f"  Total: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
    logger.info("")
    
    # Next steps
    logger.info("Next steps:")
    logger.info("  1. Open BigQuery console and run sql/queries.sql")
    logger.info("  2. Run sql/ml_model.sql for anomaly detection")
    logger.info("  3. Create dashboard in Looker Studio")
    logger.info("  4. Take screenshots for your report")
    
    # Exit with appropriate code
    all_success = all(r["success"] for r in results.values())
    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
