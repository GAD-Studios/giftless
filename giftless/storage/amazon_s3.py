import base64
import binascii
import posixpath
import json
import threading
import time
import logging
from typing import Any, BinaryIO, Iterable, List, NamedTuple, Optional, TypedDict

import boto3
import botocore
import redis

from giftless.storage import ExternalStorage, StreamingStorage, MultipartStorage, guess_mime_type_from_filename
from giftless.storage.exc import ObjectNotFoundError
from giftless.util import safe_filename

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    """AWS S3 Blob Storage backend with Redis caching."""

    GROUND_TRUTH_CACHE = "ground_truth_cache"
    TEMPORARY_OUTGOING_CACHE = "temporary_outgoing_cache"

    def __init__(
        self,
        bucket_name: str,
        path_prefix: str | None = None,
        endpoint: str | None = None,
        cache_refresh_interval: int = 300,  # 5 minutes
        redis_url: str = "redis://localhost:6379/0",
        **_: Any,
    ) -> None:
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.s3 = boto3.resource("s3", endpoint_url=endpoint)
        self.s3_client = boto3.client("s3", endpoint_url=endpoint)

        # Initialize Redis client with connection pooling for performance
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)

        # Background synchronization parameters
        self.cache_refresh_interval = cache_refresh_interval
        self._stop_event = threading.Event()
        self._cache_thread = threading.Thread(
            target=self._cache_refresh_loop,
            daemon=True
        )
        self._cache_thread.start()
        logger.info("Started Redis cache synchronization thread.")

    def _cache_refresh_loop(self) -> None:
        """Background thread that refreshes the ground truth cache periodically."""
        while not self._stop_event.is_set():
            start_time = time.time()
            try:
                logger.info("Refreshing ground truth cache from S3...")
                self._populate_ground_truth_cache()
                logger.info("Ground truth cache refreshed successfully.")
            except Exception as e:
                logger.error(f"Error refreshing ground truth cache: {e}")

            # Reset temporary outgoing cache
            try:
                self.redis_client.delete(self.TEMPORARY_OUTGOING_CACHE)
                logger.info("Temporary outgoing cache reset.")
            except Exception as e:
                logger.error(f"Error resetting temporary outgoing cache: {e}")

            elapsed = time.time() - start_time
            sleep_time = max(0, self.cache_refresh_interval - elapsed)
            time.sleep(sleep_time)

    def stop_cache_refresh(self) -> None:
        """Stop the background cache refresh thread."""
        self._stop_event.set()
        self._cache_thread.join()
        logger.info("Stopped Redis cache synchronization thread.")

    def _populate_ground_truth_cache(self) -> None:
        """Populate or refresh the ground truth cache with all objects in the bucket."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pipeline = self.redis_client.pipeline(transaction=False)

        pipeline.delete(self.GROUND_TRUTH_CACHE)  # Clear existing cache

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.path_prefix or ""):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    size = obj["Size"]
                    pipeline.hset(self.GROUND_TRUTH_CACHE, key, size)

        pipeline.execute()

    def _add_to_temporary_outgoing_cache(self, key: str, size: int) -> None:
        """Add an object to the temporary outgoing cache."""
        try:
            self.redis_client.hset(self.TEMPORARY_OUTGOING_CACHE, key, size)
        except Exception as e:
            logger.error(f"Error adding key to temporary outgoing cache: {e}")

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

        # After initiating the upload, add to temporary outgoing cache
        # Note: This does not guarantee the upload succeeded
        size = sum(completed)
        self._add_to_temporary_outgoing_cache(key, size)

        return size

    def exists(self, prefix: str, oid: str) -> bool:
        """Check if an object exists using the ground truth and temporary outgoing caches."""
        key = self._get_blob_path(prefix, oid)
        if self.redis_client.hexists(self.GROUND_TRUTH_CACHE, key):
            return True
        elif self.redis_client.hexists(self.TEMPORARY_OUTGOING_CACHE, key):
            # Potentially exists; verify with S3
            try:
                self._s3_object(prefix, oid).load()  # This checks if the object exists
                # Optionally, update ground truth cache
                size = self._s3_object(prefix, oid).content_length
                self.redis_client.hset(self.GROUND_TRUTH_CACHE, key, size)
                # Remove from temporary outgoing cache as it's confirmed
                self.redis_client.hdel(self.TEMPORARY_OUTGOING_CACHE, key)
                return True
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                else:
                    logger.error(f"Error checking object existence for {key}: {e}")
                    return False
        else:
            return False

    def get_size(self, prefix: str, oid: str) -> int:
        """Get the size of an object using the ground truth and temporary outgoing caches."""
        key = self._get_blob_path(prefix, oid)
        size = self.redis_client.hget(self.GROUND_TRUTH_CACHE, key)
        if size is not None:
            return int(size)
        size = self.redis_client.hget(self.TEMPORARY_OUTGOING_CACHE, key)
        if size is not None:
            try:
                # Verify existence and get actual size
                response = self._s3_object(prefix, oid).get()
                actual_size = response["ContentLength"]
                # Update ground truth cache
                self.redis_client.hset(self.GROUND_TRUTH_CACHE, key, actual_size)
                # Remove from temporary outgoing cache
                self.redis_client.hdel(self.TEMPORARY_OUTGOING_CACHE, key)
                return actual_size
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise ObjectNotFoundError from None
                else:
                    logger.error(f"Error getting size for {key}: {e}")
                    raise
        raise ObjectNotFoundError

    def get_upload_action(
        self,
        prefix: str,
        oid: str,
        size: int,
        expires_in: int,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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

        # Add to temporary outgoing cache
        self._add_to_temporary_outgoing_cache(key, size)

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
                "headers": {
                    "Content-Type": "application/octet-stream",
                    "x-amz-checksum-sha256": base64_oid,
                },
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
                "ChecksumSHA256": base64_oid
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

        # Add to temporary outgoing cache
        self._add_to_temporary_outgoing_cache(key, size)

        # Construct the upload metadata
        metadata: MultipartUploadMetadata = {
            "type": "multipart",
            "parts": part_actions,
            "commit": CommitAction(
                href=commit_url,
                headers={
                    "x-amz-checksum-sha256": base64_oid,
                },
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
