import numpy as np
import matplotlib.pyplot as plt from scipy.stats import norm

np.random.seed(0)
mu = 90 # Mean of distribution
sigma = 25 # Standard deviation of distribution x = mu + sigma * np.random.randn(5000)

num_bins = 25


fig, ax = plt.subplots()
n, bins, patches = ax.hist(x, num_bins, density=True)


y = norm.pdf(bins, mu, sigma) ax.plot(bins, y, '--') ax.set_xlabel('Example Data') ax.set_ylabel('Probability density')

sTitle = f'Histogram {len(x)} entries into {num_bins} Bins: $\\mu={mu}$, $\\sigma={sigma}$' ax.set_title(sTitle)

fig.tight_layout()
sPathFig = 'D:/Dinesh/DS/3/DU-Histogram.png' fig.savefig(sPathFig)
plt.show()
