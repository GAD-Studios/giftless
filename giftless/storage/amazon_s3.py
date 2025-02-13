import base64
import binascii
import posixpath
import json
import threading
import time
import logging
import atexit
import subprocess
import sys
import os
import shutil
from typing import Any, BinaryIO, Iterable, List, NamedTuple, Optional, TypedDict

import boto3
from botocore.config import Config
import botocore
import redis

from giftless.storage import ExternalStorage, StreamingStorage, MultipartStorage, guess_mime_type_from_filename
from giftless.storage.exc import ObjectNotFoundError
from giftless.storage.exc import ObjectNotFoundError, BandwidthLimitError
from giftless.util import safe_filename

logger = logging.getLogger(__name__)
MAXIMUM_ALLOWED_DOWNLOAD_BYTES = 10 * 1024 * 1024 * 1024 * 1024  # 10TB download limit

class Block(NamedTuple):
    """Convenience wrapper for S3 block."""
    id: int
    start: int
    size: int

class UploadPartAction(TypedDict):
    href: str
    headers: Optional[dict[str, str]]
    method: str
    pos: int
    size: int
    expires_in: int

class UploadBasicAction(TypedDict):
    href: str
    headers: Optional[dict[str, str]]
    method: str
    size: int
    expires_in: int

class CommitAction(TypedDict):
    href: str
    headers: Optional[dict[str, str]]
    method: str
    expires_in: int

class AbortAction(TypedDict):
    href: str
    headers: Optional[dict[str, str]]
    method: str
    expires_in: int

class ListPartsAction(TypedDict):
    href: str
    headers: Optional[dict[str, str]]
    method: str
    expires_in: int

class MultipartUploadMetadata(TypedDict):
    parts: List[UploadPartAction]
    commit: CommitAction
    list_parts: ListPartsAction
    abort: AbortAction

class AmazonS3Storage(StreamingStorage, ExternalStorage, MultipartStorage):
    """AWS S3 Blob Storage backend with Redis caching and separate refresh process."""
    
    GROUND_TRUTH_CACHE = "ground_truth_cache"
    REFRESH_PROCESS_KEY = "cache_refresh_process"
    
    def __init__(
        self,
        bucket_name: str,
        path_prefix: str | None = None,
        endpoint: str | None = None,
        cache_refresh_interval: int = 300,  # 5 minutes
        redis_url: str = "redis://localhost:6379/0",
        cache_script_path: str = "giftless/storage/amazon_s3_cache.py",
        **_: Any,
    ) -> None:
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3", config=Config(s3={"use_accelerate_endpoint": True}))
        


        self.redis_url = redis_url
        self.python_executable = self._find_virtualenv_python()

        # Initialize Redis client
        try:
            self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

        # Initialize cache synchronization process
        self.cache_script_path = cache_script_path
        self.cache_refresh_interval = cache_refresh_interval

        # Attempt to spawn the cache refresh process
        self._spawn_cache_refresh_process()


    def _find_virtualenv_python(self) -> str:
        """Locate the Python executable within the virtual environment."""
        venv = os.environ.get('VIRTUAL_ENV')
        if not venv:
            logger.critical("VIRTUAL_ENV environment variable is not set. "
                            "Ensure the application is running within a virtual environment.")
            raise EnvironmentError("VIRTUAL_ENV is not set.")

        # Construct the path to the Python executable based on the operating system
        if os.name == 'nt':  # Windows
            python_path = os.path.join(venv, 'Scripts', 'python.exe')
        else:  # Unix/Linux/MacOS
            python_path = os.path.join(venv, 'bin', 'python')

        # Verify that the Python executable exists and is executable
        if not os.path.isfile(python_path):
            logger.critical(f"Python executable not found in virtual environment at: {python_path}")
            raise EnvironmentError(f"Python executable not found in virtual environment at: {python_path}")

        if not os.access(python_path, os.X_OK):
            logger.critical(f"Python executable at {python_path} is not executable.")
            raise EnvironmentError(f"Python executable at {python_path} is not executable.")

        logger.info(f"Using Python executable from virtual environment: {python_path}")
        return python_path

    def _spawn_cache_refresh_process(self) -> None:
        """Spawn the cache refresh process if it's not already running."""
        try:
            existing_pid = self.redis_client.get(self.REFRESH_PROCESS_KEY)
            if existing_pid:
                existing_pid = int(existing_pid)
                if is_process_alive(existing_pid):
                    logger.info(f"Cache refresh process is already running with PID {existing_pid}.")
                    return
                else:
                    logger.info(f"Stale cache refresh process detected with PID {existing_pid}. Cleaning up.")
                    self.redis_client.delete(self.REFRESH_PROCESS_KEY)

            parent_pid = os.getpid()

            # Prepare the environment variables
            env = os.environ.copy()
            env['VIRTUAL_ENV'] = os.environ.get('VIRTUAL_ENV', '')
            env['PATH'] = f"{os.path.join(env['VIRTUAL_ENV'], 'bin')}:" + env.get('PATH', '')

            logger.info("Spawning cache refresh process...")
            process = subprocess.Popen(
                [
                    self.python_executable,  # Path to the Python interpreter
                    self.cache_script_path,
                    "--bucket", self.bucket_name,
                    "--prefix", self.path_prefix or "",
                    "--parent-pid", str(parent_pid),
                    "--redis-url", self.redis_url,
                    "--interval", str(self.cache_refresh_interval)
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,
                env=env
            )

            stdout, stderr = process.communicate(timeout=5)  # Wait briefly for output
            if stdout:
                logger.info(f"Cache refresher stdout: {stdout.decode().strip()}")
            if stderr:
                logger.error(f"Cache refresher stderr: {stderr.decode().strip()}")

            if process.poll() is not None:
                logger.error("Cache refresh process terminated unexpectedly.")
            else:
                logger.info(f"Cache refresh process started with PID {process.pid}.")

        except subprocess.TimeoutExpired:
            logger.warning("Cache refresh process did not output within timeout. Continuing.")
        except Exception as e:
            logger.error(f"Failed to spawn cache refresh process: {e}")

    def get(self, prefix: str, oid: str) -> Iterable[bytes]:
        key = self._get_blob_path(prefix, oid)
        if not self.exists(prefix, oid):
            raise ObjectNotFoundError
        result: Iterable[bytes] = self._s3_object(prefix, oid).get()["Body"]
        return result

    def put(self, prefix: str, oid: str, data_stream: BinaryIO) -> int:
        completed: list[int] = []
        key = self._get_blob_path(prefix, oid)

        def upload_callback(size: int) -> None:
            completed.append(size)

        bucket = self.s3.Bucket(self.bucket_name)
        bucket.upload_fileobj(
            data_stream,
            key,
            Callback=upload_callback,
        )

        size = sum(completed)
        return size

    def exists(self, prefix: str, oid: str) -> bool:
        try:
            self.get_size(prefix, oid)
        except ObjectNotFoundError:
            return False
        return True

    def get_size(self, prefix: str, oid: str) -> int:
        """Get the size of an object using the ground truth cache."""
        key = self._get_blob_path(prefix, oid)
        size = self.redis_client.hget(self.GROUND_TRUTH_CACHE, key)
        if size is not None:
            return int(size)
        else:
            try:
                # Verify existence and get actual size
                actual_size: int = self._s3_object(prefix, oid).content_length
                # Update ground truth cache
                self.redis_client.hset(self.GROUND_TRUTH_CACHE, key, actual_size)
                return actual_size
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise ObjectNotFoundError from None
                raise

    def get_upload_action(
        self,
        prefix: str,
        oid: str,
        size: int,
        expires_in: int,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # NOTE: This function is unused. All requests are directed to the multipart upload endpoint.
        # Basic uploads (non multipart) are also handled inside there.
        base64_oid = base64.b64encode(binascii.a2b_hex(oid)).decode("ascii")
        key = self._get_blob_path(prefix, oid)
        params = {
            "Bucket": self.bucket_name,
            "Key": key,
            "ContentType": "application/octet-stream",
            "ChecksumSHA256": base64_oid,
        }
        response = self.s3_client.generate_presigned_url(
            "put_object", Params=params, ExpiresIn=expires_in
        )

        return {
            "actions": {
                "upload": {
                    "href": response,
                    "header": {
                        "Content-Type": "application/octet-stream",
                        "x-amz-checksum-sha256": base64_oid,
                    },
                    "expires_in": expires_in,
                }
            }
        }

    def get_download_action(
        self,
        prefix: str,
        oid: str,
        size: int,
        expires_in: int,
        extra: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        # Rate limiting
        new_total = self.redis_client.incrby("TOTAL_DOWNLOAD_BYTES", size)
        if new_total > MAXIMUM_ALLOWED_DOWNLOAD_BYTES:
            raise BandwidthLimitError("Maximum allowed download bandwidth exceeded. Contact administrator.")
        params = {
            "Bucket": self.bucket_name,
            "Key": self._get_blob_path(prefix, oid),
        }

        filename = extra.get("filename") if extra else None
        disposition = (
            extra.get("disposition", "attachment") if extra else "attachment"
        )

        if filename and disposition:
            filename = safe_filename(filename)
            params[
                "ResponseContentDisposition"
            ] = f'attachment; filename="{filename}"'
        elif disposition:
            params["ResponseContentDisposition"] = disposition

        response = self.s3_client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires_in
        )
        return {
            "actions": {
                "download": {
                    "href": response,
                    "header": {},
                    "expires_in": expires_in,
                }
            }
        }

    def get_multipart_actions(
        self,
        prefix: str,
        oid: str,
        size: int,
        part_size: int,
        expires_in: int,
        extra: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Get actions for a multipart upload."""
        
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB
        MAX_PARTS = 10000
        
        base64_oid = base64.b64encode(binascii.a2b_hex(oid)).decode("ascii")
        key = self._get_blob_path(prefix, oid)

        if size < MIN_PART_SIZE:
            # Fallback to basic upload
            # Note that s3 will ignore SHA256 checksums in presigned urls
            # Oddly, this doesn't seem to happen here. Their docs are unclear.
            params = {
                "Bucket": self.bucket_name,
                "Key": key,
                "ContentType": "application/octet-stream",
                "ChecksumSHA256": base64_oid,
            }
            presigned_url = self.s3_client.generate_presigned_url(
                "put_object", Params=params, ExpiresIn=expires_in
            )
            action = {
                "href": presigned_url,
                "headers": {"Content-Type": "application/octet-stream"},
                "method": "PUT",
                "expires_in": expires_in,
            }
            metadata = {
                "type": "basic",
                "upload": action,
            }
            response_json = json.dumps(metadata)
            encoded_href = base64.b64encode(response_json.encode()).hex()
            return {
                "actions": {
                    "upload": {
                        "href": encoded_href,
                    }
                }
            }
        
        blocks = _calculate_blocks(size, MIN_PART_SIZE)
        
        if len(blocks) >= MAX_PARTS:
            raise RuntimeError(f"Requested multipart file size is too large: {len(blocks) * 5} MB") 

        # Prepare multipart upload parameters
        init_params = {
            "Bucket": self.bucket_name,
            "Key": key,
            "ContentType": "application/octet-stream",
        }

        if extra:
            filename = extra.get("filename")
            if filename:
                init_params["ContentDisposition"] = f'attachment; filename="{safe_filename(filename)}"'
                mime_type = guess_mime_type_from_filename(filename)
                if mime_type:
                    init_params["ContentType"] = mime_type

        # Create the multipart upload
        response = self.s3_client.create_multipart_upload(**init_params)
        upload_id = response["UploadId"]

        part_actions: List[UploadPartAction] = []
        for block in blocks:
            part_params = {
                "Bucket": self.bucket_name,
                "Key": key,
                "PartNumber": block.id + 1,  # 1-based
                "UploadId": upload_id,
                "ContentLength": block.size,
            }

            part_url = self.s3_client.generate_presigned_url(
                "upload_part",
                Params=part_params,
                ExpiresIn=expires_in,
            )

            part_actions.append(UploadPartAction(
                href=part_url,
                headers={},
                method="PUT",
                pos=block.id + 1,
                size=block.size,
                expires_in=expires_in,
            ))

        # Generate commit and abort URLs
        commit_url = self.s3_client.generate_presigned_url(
            "complete_multipart_upload",
            Params={
                "Bucket": self.bucket_name,
                "Key": key,
                "UploadId": upload_id,
            },
            ExpiresIn=expires_in,
        )

        list_parts_url = self.s3_client.generate_presigned_url(
            "list_parts",
            Params={
                "Bucket": self.bucket_name,
                "Key": key,
                "MaxParts": 1000,
                "UploadId": upload_id,
            },
            ExpiresIn=expires_in,
        )

        abort_url = self.s3_client.generate_presigned_url(
            "abort_multipart_upload",
            Params={
                "Bucket": self.bucket_name,
                "Key": key,
                "UploadId": upload_id,
            },
            ExpiresIn=expires_in,
        )

        # Construct the upload metadata
        # Note that s3 will ignore SHA256 checksums in presigned urls
        # Additionally, SHA256 is not supported for multipart uploads.
        # We will have the client to verify using the Content-MD5 header.
        metadata: MultipartUploadMetadata = {
            "type": "multipart",
            "parts": part_actions,
            "commit": CommitAction(
                href=commit_url,
                headers={},
                method="POST",
                expires_in=expires_in,
            ),
            "list_parts": ListPartsAction(
                href=list_parts_url,
                headers={},
                method="GET",
                expires_in=expires_in,
            ),
            "abort": AbortAction(
                href=abort_url,
                headers={},
                method="DELETE",
                expires_in=expires_in,
            ),
        }

        response_json = json.dumps(metadata)
        return {
            "actions": {
                "upload": {
                    "href": base64.b64encode(response_json.encode()).hex(),
                }
            }
        }

    def _get_blob_path(self, prefix: str, oid: str) -> str:
        """Get the path to a blob in storage."""
        if not self.path_prefix:
            storage_prefix = ""
        elif self.path_prefix.startswith("/"):
            storage_prefix = self.path_prefix[1:]
        else:
            storage_prefix = self.path_prefix
        return posixpath.join(storage_prefix, prefix, oid)

    def _s3_object(self, prefix: str, oid: str) -> Any:
        return self.s3.Object(
            self.bucket_name, self._get_blob_path(prefix, oid)
        )

def is_process_alive(pid: int) -> bool:
    """Check if a process with the given PID is alive."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def _calculate_blocks(file_size: int, part_size: int) -> list[Block]:
    """Calculate the list of blocks in a blob."""
    if file_size == 0:
        return []
    full_blocks = file_size // part_size
    last_block_size = file_size % part_size
    blocks = [
        Block(id=i, start=i * part_size, size=part_size)
        for i in range(full_blocks)
    ]

    if last_block_size:
        blocks.append(
            Block(
                id=full_blocks,
                start=full_blocks * part_size,
                size=last_block_size,
            )
        )

    return blocks
