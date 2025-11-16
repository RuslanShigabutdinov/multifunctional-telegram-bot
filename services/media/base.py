from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
import logging

import httpx

logger = logging.getLogger(__name__)


@dataclass
class MediaInfo:
    url: str
    extension: str = ".mp4"


class MediaDownloader(ABC):
    """Base helper for RapidAPI-powered media downloaders."""

    api_url: str
    api_host: str

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError(f"{self.__class__.__name__} API key is missing")
        self.api_key = api_key

    def build_headers(self) -> dict:
        return {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host,
        }

    def build_query(self, link: str) -> dict:
        return {"url": link}

    @abstractmethod
    def extract_media(self, payload: dict) -> Optional[MediaInfo]:
        """Convert provider response into a downloadable media descriptor."""

    async def download(self, link: str) -> Optional[MediaInfo]:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    self.api_url,
                    headers=self.build_headers(),
                    params=self.build_query(link),
                )
        except httpx.HTTPError as exc:
            logger.error("%s request failed: %s", self.__class__.__name__, exc)
            return None

        if response.status_code != 200:
            logger.error(
                "%s request failed: %s %s",
                self.__class__.__name__,
                response.status_code,
                response.text,
            )
            return None

        payload = response.json()
        media_info = self.extract_media(payload)
        if not media_info:
            logger.error(
                "%s did not return downloadable media: %s",
                self.__class__.__name__,
                payload,
            )
            return None

        return media_info
