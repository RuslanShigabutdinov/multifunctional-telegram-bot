from time import time
import requests

def _normalize_extension(extension: str) -> str:
    return extension if extension.startswith('.') else f'.{extension}'


async def downloadVideo(link, extention='.mp4'):
    normalized_extension = _normalize_extension(extention)
    fileName = f'{time()}{normalized_extension}'
    response = requests.get(link, stream=True)
    if response.status_code == 200:
        with open(fileName, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    file.write(chunk)
        print(f'Скачал файл - {fileName}')
        return fileName
    else:
        return False
