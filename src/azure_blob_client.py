from __future__ import annotations

import json
import logging
import os

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from src.data_models import PipelineBlobStatus

CONTAINER_NAME = "users-also-buy"


class AzureBlobClient:
    def __init__(
        self,
    ) -> None:
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError(
                "Missing Azure Storage connection string. "
                "Set AZURE_STORAGE_CONNECTION_STRING."
            )

        self._service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self._container_client = self._service_client.get_container_client(
            CONTAINER_NAME
        )
        self._ensure_container()

    def _ensure_container(self) -> None:
        """Create the container if it does not yet exist."""
        try:
            self._container_client.create_container()
        except ResourceExistsError:
            return

    def write_pipeline_status(
        self, blob_name: str, pipeline_status: PipelineBlobStatus
    ) -> None:
        body = pipeline_status.model_dump_json()
        self._container_client.upload_blob(
            name=blob_name,
            data=body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    def read_json(self, blob_name: str) -> PipelineBlobStatus | None:
        try:
            data = self._container_client.download_blob(blob_name).readall()
        except ResourceNotFoundError:
            logging.warning(
                f"Blob '{blob_name}' not found in container "
                f"'{self._container_client.container_name}'."
            )
            return None
        if not isinstance(data, bytes):
            raise ValueError(f"Blob '{blob_name}' has no data.")
        return PipelineBlobStatus(**json.loads(data.decode("utf-8")))
