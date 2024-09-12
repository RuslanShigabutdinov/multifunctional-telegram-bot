import requests
from time import time
import re
from os import remove
from videoDownloader import downloadVideo
from env import TIKTOK_KEY

def findLink(text):
    answer = re.search(r'[(http://)|\w]*?[\w]*\.[-/\w]*\.\w*[(/{1})]?[#-\./\w]*[(/{1,})]?', text)
    if answer is not None:
        return answer.group()
    return None

def deleteVideo(fileName):
    remove(fileName)

async def downloadTikTok(link):
    apiUrl = "https://tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com/vid/index"

    querystring = {"url":link}

    headers = {
        "X-RapidAPI-Key": TIKTOK_KEY,
        "X-RapidAPI-Host": "tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com"
    }

    response = requests.get(apiUrl, headers=headers, params=querystring)
    if response.status_code == 200:
        videoUrl = response.json()['video'][0]
        return await downloadVideo(videoUrl)
    return False

def main():
    LINK = 'https://www.tiktok.com/@crklxwz/video/7347058077532802305'
    downloadTikTok(LINK)

if __name__ == '__main__':
    startTime = time()
    main()
    print("--- %s seconds ---" % (time() - startTime))