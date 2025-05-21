from collections import defaultdict
import glob
import os
import pickle


parent_dir = 'bgpdata'

years = range(2014, 2025)
months = range(1,13)
keys = []
for year in years:
  for month in months:
    keys.append((year,month))
my_dict = {key: 0 for key in keys}


for subdir in os.listdir(parent_dir):
  subdir_path = os.path.join(parent_dir, subdir)
  if not os.path.isdir(subdir_path):
    continue
  
  matching_files = glob.glob(os.path.join(subdir_path, 'rib.*.bz2'))
  for filepath in matching_files:
    filename = filepath.split('/')[len(filepath.split('/'))-1]
    date = filename.split('.')[1]
    try:
      month = int(date[4:6])
      year = int(date[:4])
      my_dict[(year, month)] += 1

    except Exception:
      print(f"error: {filepath} {date[:4]}{date[4:6]}")



counts = defaultdict(lambda: {'ipv4': 0, 'ipv6': 0})
if os.path.exists("counts.pkl"):
  with open("counts.pkl", "rb") as f:
    data = pickle.load(f)
    counts = defaultdict(lambda: {'ipv4': 0, 'ipv6': 0}, data)


with open('output_normalized.dat', 'w') as f:
  for (year, month) in sorted(counts):
    ipv4 = counts[(year, month)]['ipv4']/my_dict[(year, month)]
    ipv6 = counts[(year, month)]['ipv6']/my_dict[(year, month)]
    f.write(f"{month:02} {year} {ipv4} {ipv6}\n")


