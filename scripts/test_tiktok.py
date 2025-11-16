import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.media.tiktok import downloadTikTok

VIDEO_URL = "https://vt.tiktok.com/ZSygmCma1/"


async def main() -> None:
    print(f"Downloading TikTok video from {VIDEO_URL} ...")
    file_name = await downloadTikTok(VIDEO_URL)
    if not file_name:
        raise SystemExit("Failed to download video.")
    print(f"Video saved to: {file_name}")


if __name__ == "__main__":
    asyncio.run(main())
