import requests
from videoDownloader import downloadVideo
import asyncio
from env import INSTAGRAM_KEY


async def downloadInstagram(link):
    url = "https://instagram-post-reels-stories-downloader.p.rapidapi.com/instagram/"

    querystring = {"url":link}

    headers = {
        "X-RapidAPI-Key": INSTAGRAM_KEY,
        "X-RapidAPI-Host": "instagram-post-reels-stories-downloader.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    json = response.json()
    if json['status']:
        if json['result'][0]['type'] == 'image/jpeg':
            videoUrl = json['result'][0]['url']
            return await downloadVideo(videoUrl, 'jpeg')
        videoUrl = json['result'][0]['url']
        return await downloadVideo(videoUrl)
    return False

async def main():
    link = 'https://www.instagram.com/lustra.bar/'
    fileName = await downloadInstagram(link)
    print(fileName)

if __name__ == '__main__':
    asyncio.run(main())