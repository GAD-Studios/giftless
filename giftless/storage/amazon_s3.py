"""Amazon S3 backend."""
import base64
import binascii
import posixpath
from collections.abc import Iterable
from typing import Any, BinaryIO, NamedTuple

import boto3
import botocore

from giftless.storage import ExternalStorage, StreamingStorage, MultipartStorage, guess_mime_type_from_filename
from giftless.storage.exc import ObjectNotFoundError
from giftless.util import safe_filename

class Block(NamedTuple):
    """Convenience wrapper for Azure block."""

    id: int
    start: int
    size: int

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
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get actions for a multipart upload.

        This implements the S3 multipart upload scheme according to both git-lfs and S3 specs.
        """
        # Adjust part size to meet S3 minimums (5MB) and maximums (10000 parts)
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB
        MAX_PARTS = 10000

        adjusted_part_size = max(MIN_PART_SIZE, part_size)
        if size > adjusted_part_size * MAX_PARTS:
            adjusted_part_size = (size + MAX_PARTS - 1) // MAX_PARTS

        # Calculate number of parts needed
        blocks = _calculate_blocks(size, part_size)

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

        # Specify the checksum algorithm (do not include ChecksumSHA256)
        # init_params["ChecksumAlgorithm"] = "SHA256"

        # Create the multipart upload
        response = self.s3_client.create_multipart_upload(**init_params)
        upload_id = response["UploadId"]

        # Generate presigned URLs for each part
        parts = []
        for block in blocks:

            # Generate presigned URL for this part
            part_params = {
                "Bucket": self.bucket_name,
                "Key": self._get_blob_path(prefix, oid),
                "PartNumber": block.id,
                "UploadId": upload_id,
                "ContentLength": block.size,
                "ChecksumAlgorithm": "SHA256",
            }

            part_url = self.s3_client.generate_presigned_url(
                "upload_part",
                Params=part_params,
                ExpiresIn=expires_in,
            )

            parts.append({
                "href": part_url,
                "pos": block.id,
                "size": block.size,
                "expires_in": expires_in,
                "want_digest": "sha256",
            })

        # Generate commit URL (CompleteMultipartUpload)
        complete_params = {
            "Bucket": self.bucket_name,
            "Key": self._get_blob_path(prefix, oid),
            "UploadId": upload_id,
        }

        commit_url = self.s3_client.generate_presigned_url(
            "complete_multipart_upload",
            Params=complete_params,
            ExpiresIn=expires_in,
        )

        # Generate abort URL
        abort_params = {
            "Bucket": self.bucket_name,
            "Key": self._get_blob_path(prefix, oid),
            "UploadId": upload_id,
        }

        abort_url = self.s3_client.generate_presigned_url(
            "abort_multipart_upload",
            Params=abort_params,
            ExpiresIn=expires_in,
        )

        # Construct the complete response
        actions: dict[str, Any] = {
            "actions": {
                "parts": parts,
                "commit": {
                    "href": commit_url,
                    "expires_in": expires_in,
                    "method": "POST",
                    "header": {
                        "Content-Type": "application/xml",
                    },
                },
                "abort": {
                    "href": abort_url,
                    "expires_in": expires_in,
                    "method": "DELETE",
                }
            }
        }

        return actions

    
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

