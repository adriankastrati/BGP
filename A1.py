import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as md
from scipy.interpolate import make_interp_spline
import numpy as np

# Optional: enable log scale or not
LOG = False
ipv4_fp = 'output_normalized.dat'

# Load and preprocess data
df = pd.read_csv(ipv4_fp, delim_whitespace=True)
df['date'] = pd.to_datetime(df['Month'].astype(str) + df['Year'].astype(str), format='%m%Y')

# Rename columns for clarity
df = df.rename(columns={'IPv4': 'ipv4', 'IPv6': 'ipv6'})

# Create separate DataFrames
ipv4_new = df[['date', 'ipv4']].rename(columns={'ipv4': 'count'})
ipv6_new = df[['date', 'ipv6']].rename(columns={'ipv6': 'count'})

# Merge and compute ratio
new = pd.merge(ipv4_new, ipv6_new, on='date', suffixes=('', '_6'))
new['ratio'] = new['count_6'] / new['count']

## Interpolate ratio
xnew = pd.date_range(new["date"].min(), new["date"].max(), periods=60)
spl = make_interp_spline(new["date"], new["ratio"], k=7)
smooth = spl(xnew)


# Plotting
fig = plt.figure(figsize=(8, 5))
ax1 = fig.subplots()

# Ticks for year
ticks = pd.date_range(df['date'].min(), df['date'].max(), freq='YS')

# Plot data
ax1.scatter(ipv4_new['date'], ipv4_new['count'], color="C0", label='IPv4', marker=".")
ax1.scatter(ipv6_new['date'], ipv6_new['count'], color="C1", label='IPv6', marker=".")

# Axis formatting
plt.xticks(ticks, rotation=45)
ax1.xaxis.set_major_formatter(md.DateFormatter('%Y'))
ax1.set_ylabel('Prefix Advertisements')
ax1.set_xlim(left=pd.Timestamp("2014-01-01"), right=pd.Timestamp("2024-12-31"))

# Vertical line

# Adjust layout
plt.subplots_adjust(bottom=0.2)

# Ratio on secondary axis
ax2 = ax1.twinx()
ax2.plot(xnew, smooth, color="C4", label='IPv6/IPv4 Ratio')
ax2.set_ylabel('Ratio')



# Fix legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
if LOG:
    ax1.set_yscale('log')
    fig.legend(bbox_to_anchor=(0.89, 0.4), loc='upper right')
else:
    fig.legend(bbox_to_anchor=(0.4, 0.9), loc='upper right')

# Grid and layout
plt.grid()
plt.tight_layout()


# Show the plot
plt.show()
