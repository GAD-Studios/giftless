"""Amazon S3 backend."""
import base64
import binascii
import posixpath
from collections.abc import Iterable
from typing import Any, BinaryIO, NamedTuple, TypedDict, List, Optional

import boto3
import botocore
import json

from giftless.storage import ExternalStorage, StreamingStorage, MultipartStorage, guess_mime_type_from_filename
from giftless.storage.exc import ObjectNotFoundError
from giftless.util import safe_filename

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
    """AWS S3 Blob Storage backend."""

    def __init__(
        self,
        bucket_name: str,
        path_prefix: str | None = None,
        endpoint: str | None = None,
        **_: Any,
    ) -> None:
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.s3 = boto3.resource("s3", endpoint_url=endpoint)
        self.s3_client = boto3.client("s3", endpoint_url=endpoint)

    def get(self, prefix: str, oid: str) -> Iterable[bytes]:
        if not self.exists(prefix, oid):
            raise ObjectNotFoundError
        result: Iterable[bytes] = self._s3_object(prefix, oid).get()["Body"]
        return result

    def put(self, prefix: str, oid: str, data_stream: BinaryIO) -> int:
        completed: list[int] = []

        def upload_callback(size: int) -> None:
            completed.append(size)

        bucket = self.s3.Bucket(self.bucket_name)
        bucket.upload_fileobj(
            data_stream,
            self._get_blob_path(prefix, oid),
            Callback=upload_callback,
        )
        return sum(completed)

    def exists(self, prefix: str, oid: str) -> bool:
        try:
            self.get_size(prefix, oid)
        except ObjectNotFoundError:
            return False
        return True

    def get_size(self, prefix: str, oid: str) -> int:
        try:
            result: int = self._s3_object(prefix, oid).content_length
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise ObjectNotFoundError from None
            raise
        return result

    def get_upload_action(
        self,
        prefix: str,
        oid: str,
        size: int,
        expires_in: int,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        base64_oid = base64.b64encode(binascii.a2b_hex(oid)).decode("ascii")
        params = {
            "Bucket": self.bucket_name,
            "Key": self._get_blob_path(prefix, oid),
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
    extra: Optional[dict[str, any]] = None,
) -> dict[str, any]:
        """Get actions for a multipart upload.

        Implements the S3 multipart upload scheme and returns actions formatted
        for Git LFS.
        """
        
        # We will not use part_size param, always use 5MB as this is the minimum for S3 multipart upload spec
        MIN_PART_SIZE = 5 * 1024 * 1024
        # Defined by S3 multipart spec
        MAX_PARTS = 10000
        
        blocks = _calculate_blocks(size, MIN_PART_SIZE)
        
        if len(blocks) >= MAX_PARTS:
            raise RuntimeError(f"Requested multipart file size is too large: {len(blocks) * 5} MB") 

        # Prepare multipart upload parameters
        filename = extra.get("filename") if extra else None
        init_params = {
            "Bucket": self.bucket_name,
            "Key": self._get_blob_path(prefix, oid),
            "ContentType": "application/octet-stream",
        }

        # Add optional parameters for the multipart upload
        if filename:
            init_params["ContentDisposition"] = f'attachment; filename="{safe_filename(filename)}"'
            mime_type = guess_mime_type_from_filename(filename)
            if mime_type:
                init_params["ContentType"] = mime_type

        # Create the multipart upload
        base64_oid = base64.b64encode(binascii.a2b_hex(oid)).decode("ascii")
        
        response = self.s3_client.create_multipart_upload(**init_params)
        upload_id = response["UploadId"]

        part_actions: List[UploadPartAction] = []
        for block in blocks:
            part_params = {
                "Bucket": self.bucket_name,
                "Key": self._get_blob_path(prefix, oid),
                "PartNumber": block.id + 1,  # S3 part numbers are 1-based
                "UploadId": upload_id,
                "ContentLength": block.size,
            }

            part_url = self.s3_client.generate_presigned_url(
                "upload_part",
                Params=part_params,
                ExpiresIn=expires_in,
            )

            # Initialize UploadPartAction with keyword arguments
            part_actions.append(UploadPartAction(
                href=part_url,
                headers={},
                method="PUT",
                pos=block.id + 1,  # Match the 1-based part numbers
                size=block.size,
                expires_in=expires_in,
            ))

        # Generate commit and abort URLs
        commit_url = self.s3_client.generate_presigned_url(
            "complete_multipart_upload",
            Params={
                "Bucket": self.bucket_name,
                "Key": self._get_blob_path(prefix, oid),
                "UploadId": upload_id,
                # "ChecksumSHA256": base64_oid
            },
            ExpiresIn=expires_in,
        )

        list_parts_url = self.s3_client.generate_presigned_url(
            "list_parts",
            Params={
                "Bucket": self.bucket_name,
                "Key": self._get_blob_path(prefix, oid),
                "MaxParts": 1000,
                "UploadId": upload_id,
            },
            ExpiresIn=expires_in,
        )

        abort_url = self.s3_client.generate_presigned_url(
            "abort_multipart_upload",
            Params={
                "Bucket": self.bucket_name,
                "Key": self._get_blob_path(prefix, oid),
                "UploadId": upload_id,
            },
            ExpiresIn=expires_in,
        )

        # Construct the upload metadata using keyword arguments
        metadata: MultipartUploadMetadata = {
            "parts": part_actions,
            "commit": CommitAction(
                href=commit_url,
                headers={
                    # "Content-Type": "application/xml",
                    # "x-amz-checksum-sha256": base64_oid,
                },
                method="POST",
                expires_in=expires_in,
            ),
            "list_parts": ListPartsAction(
                href=list_parts_url,
                headers={},  # Assuming headers are empty; adjust if necessary
                method="GET",
                expires_in=expires_in,
            ),
            "abort": AbortAction(
                href=abort_url,
                headers={},  # Assuming headers are empty; adjust if necessary
                method="DELETE",
                expires_in=expires_in,
            ),
        }

        # Compress the required info into the href of the upload request. A custom transfer will be required on the clientside to decode this.
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
        elif self.path_prefix[0] == "/":
            storage_prefix = self.path_prefix[1:]
        else:
            storage_prefix = self.path_prefix
        return posixpath.join(storage_prefix, prefix, oid)

    def _s3_object(self, prefix: str, oid: str) -> Any:
        return self.s3.Object(
            self.bucket_name, self._get_blob_path(prefix, oid)
        )

def _calculate_blocks(file_size: int, part_size: int) -> list[Block]:
    """Calculate the list of blocks in a blob.

    >>> _calculate_blocks(30, 10)
    [Block(id=0, start=0, size=10), Block(id=1, start=10, size=10), Block(id=2, start=20, size=10)]

    >>> _calculate_blocks(28, 10)
    [Block(id=0, start=0, size=10), Block(id=1, start=10, size=10), Block(id=2, start=20, size=8)]

    >>> _calculate_blocks(7, 10)
    [Block(id=0, start=0, size=7)]

    >>> _calculate_blocks(0, 10)
    []
    """  # noqa: E501
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
