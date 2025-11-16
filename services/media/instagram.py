from typing import Optional

from services.media.base import MediaDownloader, MediaInfo
from utils.settings import get_settings


class InstagramDownloader(MediaDownloader):
    api_url = "https://instagram-post-reels-stories-downloader.p.rapidapi.com/instagram/"
    api_host = "instagram-post-reels-stories-downloader.p.rapidapi.com"

    def extract_media(self, payload: dict):
        if not payload.get('status'):
            return None
        results = payload.get('result') or []
        if not results:
            return None
        first_result = results[0]
        media_type = first_result.get('type', '')
        extension = '.jpeg' if media_type == 'image/jpeg' else '.mp4'
        media_url = first_result.get('url')
        if not media_url:
            return None
        return MediaInfo(url=media_url, extension=extension)


settings = get_settings()
instagram_downloader = InstagramDownloader(settings.instagram_api_key)


async def downloadInstagram(link) -> Optional[MediaInfo]:
    return await instagram_downloader.download(link)
