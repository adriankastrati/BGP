import os
import glob
import subprocess
from datetime import datetime
from collections import defaultdict
import pickle

# Load existing data
seen_prefixes = defaultdict(set)
if os.path.exists("seen_prefixes.pkl"):
  with open("seen_prefixes.pkl", "rb") as f:
    data = pickle.load(f)
    seen_prefixes = defaultdict(set, data)
print("seen_prefixes")
counts = defaultdict(lambda: {'ipv4': 0, 'ipv6': 0})
if os.path.exists("counts.pkl"):
  with open("counts.pkl", "rb") as f:
    data = pickle.load(f)
    counts = defaultdict(lambda: {'ipv4': 0, 'ipv6': 0}, data)
print("counts")

processed_files = set()
if os.path.exists("processed_files.log"):
  with open("processed_files.log") as f:
    processed_files = set(line.strip() for line in f)
pending_files = []
print(len(processed_files))

i = 0
parent_dir = 'bgpdata'
for subdir in os.listdir(parent_dir):
  subdir_path = os.path.join(parent_dir, subdir)
  print(f"subdir: {subdir_path}")

  if not os.path.isdir(subdir_path):
    continue
  
  matching_files = glob.glob(os.path.join(subdir_path, 'rib.*.bz2'))
  for filepath in matching_files:

    if filepath in processed_files:
      print(f"filepath already processed: {filepath}")

      continue

    print(f"filepath: {filepath}")

    try:
      print(f"BGP dump of file: {filepath} loading...")

      proc = subprocess.Popen(
        ['bgpdump', '-M', '-t', 'dump', filepath],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
      )
      key = None
      print(f"BGP dump ready: {filepath} ")

      for line in proc.stdout:
        parts = line.strip().split('|')
        if len(parts) < 7:
          continue
        
        if key is None:
          try:
            dt = datetime.strptime(parts[1].strip(), "%m/%d/%y %H:%M:%S")
            key = (dt.year, dt.month)
          except:
            continue

        prefix = parts[3]
        if prefix in seen_prefixes[key]:
          continue
        
        seen_prefixes[key].add(prefix)

        if ':' in prefix:
          counts[(dt.year, dt.month)]['ipv6'] += 1
        elif '.' in prefix:
          counts[(dt.year, dt.month)]['ipv4'] += 1
        
      print(f"BGP dump proccessed: {filepath} ")

      
      proc.wait()  # Ensure process completes
      pending_files.append(filepath)

      if i > 0 and i % 12 == 0:

      # Save state after each file
        with open("seen_prefixes.pkl", "wb") as f:
          pickle.dump(dict(seen_prefixes), f)
        with open("counts.pkl", "wb") as f:
          pickle.dump(dict(counts), f)
        with open("processed_files.log", "a") as f:
          for p in pending_files:
            f.write(f"{p}\n")
            processed_files.add(p)
          pending_files.clear()
          

      i += 1

    except Exception as e:
      print(f"Error processing {filepath}: {e}")
      
if pending_files:
  with open("processed_files.log", "a") as f:
    for p in pending_files:
      f.write(f"{p}\n")
      processed_files.add(p)
  pending_files.clear()

# Write the final results
with open('output_false.dat', 'w') as f:
  f.write("Month Year IPv4 IPv6\n")
  for (year, month) in sorted(counts):
    ipv4 = counts[(year, month)]['ipv4']
    ipv6 = counts[(year, month)]['ipv6']
    f.write(f"{month:02} {year} {ipv4} {ipv6}\n")

