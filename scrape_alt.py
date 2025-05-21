import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

def download_file(collector, url, href):
  try:
    file_url = url + href
    print('Downloading:', file_url)
    file_response = requests.get(file_url, stream=True)

    os.makedirs(f"bgpdata/{collector}", exist_ok=True)
    with open(f"bgpdata/{collector}/{href}", 'wb') as f:
      for chunk in file_response.iter_content(chunk_size=8192):
        f.write(chunk)

    print('Download complete:', href)
  except Exception as e:
    print(f"Error downloading {file_url}: {e}")

def process_month(collector, year, month, base_url):
  ym = f"{year}.{month:02d}"
  url = f"{base_url}/{collector}/bgpdata/{ym}/RIBS/"
  print("Checking:", url)

  try:
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    all_links = soup.find_all('a')

    for link in all_links:
      href = link.get('href')
      if href and href.endswith('.bz2'):
        download_file(collector, url, href)
        break

  except Exception as e:
    print(f"Error for {url}: {e}")

def main():
  with open("collectors.dat", "r") as collectors_file:
    collectors = [line.strip() for line in collectors_file]

  base_url = "http://archive.routeviews.org"
  years = range(2014, 2025)

  with ThreadPoolExecutor(max_workers=5) as executor:
    for collector in collectors:
      collector_path = collector.replace(".", "_")
      os.makedirs(f"bgpdata/{collector}", exist_ok=True)

      for year in years:
        for month in range(1, 13):  # Fixed range to include December
          executor.submit(process_month, collector, year, month, base_url)

if __name__ == "__main__":
  main()
