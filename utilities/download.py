from io import BytesIO
import re
import datetime
from uuid import uuid4
import requests
import pandas as pd
import os
import lzma  # Import lzma for handling .xz files

def download_and_read_json_xz(url, extract_path='temp', lines=False):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {url}")

    file_like_object = lzma.open(BytesIO(response.content))
    file_content = file_like_object.read()
    file_like_object.close()

    temp_file_path = os.path.join(extract_path, f'temp_file_{uuid4()}_.json')
    os.makedirs(extract_path, exist_ok=True)
    with open(temp_file_path, 'wb') as temp_file:
        temp_file.write(file_content)

    df = pd.read_json(temp_file_path, encoding='utf-8', lines=lines)
    string_date = re.search(r'(\d{4}_\d{2}_\d{2})', url)
    date = datetime.datetime.strptime(string_date.group(1), '%Y_%m_%d').date()
    df['DATE'] = date

    os.remove(temp_file_path)

    return df