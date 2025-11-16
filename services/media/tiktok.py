import logging
import re
from os import remove

from services.media.base import MediaDownloader, MediaInfo
from services.usage import UsageTracker
from utils.settings import get_settings

def findLink(text):
    answer = re.search(r'[(http://)|\w]*?[\w]*\.[-/\w]*\.\w*[(/{1})]?[#-\./\w]*[(/{1,})]?', text)
    if answer is not None:
        return answer.group()
    return None

def deleteVideo(fileName):
    remove(fileName)


class TikTokDownloader(MediaDownloader):
    api_url = "https://tiktok-video-no-watermark2.p.rapidapi.com/"
    api_host = "tiktok-video-no-watermark2.p.rapidapi.com"

    def __init__(self, api_key: str, hd: bool = True) -> None:
        super().__init__(api_key)
        self.hd = hd

    def build_query(self, link: str) -> dict:
        query = super().build_query(link)
        if self.hd:
            query["hd"] = 1
        return query

    def extract_media(self, payload: dict):
        data = payload.get("data") or {}
        if self.hd:
            hd_url = data.get("hdplay")
            if hd_url:
                return MediaInfo(url=hd_url)
        play_url = data.get("play")
        if play_url:
            return MediaInfo(url=play_url)
        return None


logger = logging.getLogger(__name__)

settings = get_settings()
tiktok_downloader = TikTokDownloader(settings.tiktok_api_key)
usage_tracker = UsageTracker(key="tiktok_requests_remaining", limit=150)


async def downloadTikTok(link):
    if not usage_tracker.consume():
        logger.warning("TikTok request limit reached; skipping download.")
        return None
    return await tiktok_downloader.download(link)
