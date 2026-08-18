
import pstats
p = pstats.Stats('profile.stats')
p.strip_dirs().sort_stats('cumulative').print_stats(20)
