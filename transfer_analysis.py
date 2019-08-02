import itertools

import pandas
import numpy as np
import matplotlib.pyplot as plt


from condor_necropsy import stats
from condor_necropsy.events import get_events

data = stats.extract_data(get_events("events_combined"))

df = data.as_dataframe()

MB = 1024 ** 2
GB = 1024 ** 3

input_size = 50 * GB
df["transfer_input_rate"] = (input_size / MB) / df["transfer_input_time"]
max_rate = df["transfer_input_rate"].max()
num_bins = 20

hist = df.hist(
    column="transfer_input_rate",
    by="note",
    sharex=True,
    sharey=True,
    bins=np.linspace(0, max_rate, num_bins),
    figsize=(8, 8),
)

plt.suptitle(
    f"Transfer Rate Histograms for {int(len(df) / 4)} Test Runs per Facility", y=1.0
)
for ax in itertools.chain(*hist):
    ax.set_xlabel("Input Transfer Rate (MB/s)")
    ax.set_ylabel("# of Occurrences")
    ax.tick_params(labelleft=True, labelbottom=True)
    ax.set_xlim(0, None)

plt.tight_layout()
plt.savefig("transfer_input_rate.png")
