import requests
from bs4 import BeautifulSoup
import os
import zipfile
from io import BytesIO

url = 'https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/bajas-automoviles-mensual.html'

path = os.path.join("Data", "DGT", "bajas")
os.makedirs(path, exist_ok=True)

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

for li in soup.find_all('li', class_='list-group-item'):
    a_tag = li.find('a')
    if a_tag and a_tag['href'].endswith('.zip'):
        file_url = a_tag['href']
        if file_url.startswith('/'):
            file_url = 'https://www.dgt.es' + file_url

        file_name = file_url.split('/')[-1]
        print(f'Downloading and extracting {file_name}...')

        zip_response = requests.get(file_url)
        zip_file = zipfile.ZipFile(BytesIO(zip_response.content))

        zip_file.extractall(path)