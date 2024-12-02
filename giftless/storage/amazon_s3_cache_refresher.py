import sys
import time
import logging
import argparse
import json
import os
import signal

import boto3
import botocore
import redis

# Ensure the directory for logs exists
# TODO: This is so bad
log_dir = '/home/admin/giftless/giftless/logs'
os.makedirs(log_dir, exist_ok=True)  # Create the directory if it doesn't exist

# Configure logging to log to a file
logging.basicConfig(
    level=logging.INFO,  # Set the log level
    filename=os.path.join(log_dir, 'amazon_s3_cache_refresher.log'),  # Path to the log file
    filemode='a',  # Append mode
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log message format
    datefmt='%Y-%m-%d %H:%M:%S'  # Date format
)

logger = logging.getLogger(__name__)

def is_process_alive(pid: int) -> bool:
    """Check if a process with the given PID is alive."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def populate_ground_truth_cache(s3_client, redis_client, bucket_name, path_prefix):
    """Populate or refresh the ground truth cache with all objects in the S3 bucket."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pipeline = redis_client.pipeline(transaction=False)

    pipeline.delete("ground_truth_cache")  # Clear existing cache

    for page in paginator.paginate(Bucket=bucket_name, Prefix=path_prefix or ""):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                size = obj["Size"]
                pipeline.hset("ground_truth_cache", key, size)

    pipeline.execute()
    logger.info("Ground truth cache refreshed successfully.")

def reset_temporary_outgoing_cache(redis_client):
    """Reset the temporary outgoing cache."""
    try:
        redis_client.delete("temporary_outgoing_cache")
        logger.info("Temporary outgoing cache reset.")
    except Exception as e:
        logger.error(f"Error resetting temporary outgoing cache: {e}")

def main():
    parser = argparse.ArgumentParser(description="S3 Cache Refresher Process")
    parser.add_argument("--bucket", required=True, help="S3 Bucket Name")
    parser.add_argument("--prefix", default="", help="S3 Path Prefix")
    parser.add_argument("--parent-pid", type=int, required=True, help="Parent Process PID")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis URL")
    parser.add_argument("--interval", type=int, default=300, help="Cache refresh interval in seconds")

    args = parser.parse_args()

    bucket_name = args.bucket
    path_prefix = args.prefix
    parent_pid = args.parent_pid
    redis_url = args.redis_url
    cache_refresh_interval = args.interval

    # Initialize Redis client
    try:
        redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        redis_client.ping()
    except redis.RedisError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    # Initialize S3 client
    try:
        s3_client = boto3.client("s3")
    except botocore.exceptions.BotoCoreError as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        sys.exit(1)

    # Set the 'cache_refresh_process' key with the current PID
    current_pid = os.getpid()
    try:
        # Attempt to set the key only if it doesn't exist
        was_set = redis_client.setnx("cache_refresh_process", current_pid)
        if not was_set:
            logger.info("Another cache refresh process is already running. Exiting.")
            sys.exit(0)
        logger.info(f"Cache refresh process started with PID {current_pid}.")
    except redis.RedisError as e:
        logger.error(f"Failed to set cache refresh process key in Redis: {e}")
        sys.exit(1)

    def handle_exit(signum, frame):
        logger.info("Received termination signal. Exiting...")
        redis_client.delete("cache_refresh_process")
        sys.exit(0)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    try:
        while True:
            start_time = time.time()
            # Check if parent process is alive
            if not is_process_alive(parent_pid):
                logger.info("Parent process is not alive. Terminating cache refresher.")
                break

            try:
                logger.info("Refreshing ground truth cache from S3...")
                populate_ground_truth_cache(s3_client, redis_client, bucket_name, path_prefix)
            except Exception as e:
                logger.error(f"Error refreshing ground truth cache: {e}")

            try:
                reset_temporary_outgoing_cache(redis_client)
            except Exception as e:
                logger.error(f"Error resetting temporary outgoing cache: {e}")

            elapsed = time.time() - start_time
            remaining_time = cache_refresh_interval - elapsed

            if remaining_time <= 0:
                continue  # Immediately proceed to the next iteration

            # Sleep in 1-second increments to allow prompt termination
            for _ in range(int(remaining_time)):
                time.sleep(1)
                if not is_process_alive(parent_pid):
                    logger.info("Parent process is not alive during sleep. Terminating cache refresher.")
                    raise SystemExit
    finally:
        # Cleanup: remove the Redis key
        try:
            redis_client.delete("cache_refresh_process")
            logger.info("Cache refresh process key removed from Redis.")
        except redis.RedisError as e:
            logger.error(f"Failed to delete cache refresh process key from Redis: {e}")

if __name__ == "__main__":
    main()
