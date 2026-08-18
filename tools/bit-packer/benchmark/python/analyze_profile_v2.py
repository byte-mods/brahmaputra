
import pstats
p = pstats.Stats('profile_v2.stats')
p.strip_dirs().sort_stats('cumulative').print_stats(30)
