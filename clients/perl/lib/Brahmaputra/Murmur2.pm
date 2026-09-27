package Brahmaputra::Murmur2;

# Kafka's 32-bit murmur2, so a key lands on the same partition as it would
# from any other Brahmaputra (or Kafka) client. murmur2("") == 275646681.
#
# Transcribed rather than imported. Every intermediate is kept as an
# unsigned 32-bit value by masking: the product of two such values stays
# below 2^63 (0xFFFFFFFF * 0x5BD1E995 < 6.7e18), so Perl's multiply never
# falls back to a floating-point result before the mask truncates it, and
# `>>` on a masked value is the logical shift Java's `>>>` performs.

use strict;
use warnings;
use Exporter 'import';
use Brahmaputra::Protocol qw(to_bytes);
our @EXPORT_OK = qw(murmur2 murmur2_partition);

use constant {
    SEED => 0x9747b28c,
    M    => 0x5bd1e995,
    MASK => 0xffffffff,
};

sub murmur2 {
    my ($data) = @_;
    $data = to_bytes($data);
    my $length = length $data;
    my $h = (SEED ^ $length) & MASK;
    my $chunks = int($length / 4);
    my @words = unpack('V' . $chunks, $data);
    for my $k (@words) {
        $k = ($k * M) & MASK;
        $k ^= $k >> 24;
        $k = ($k * M) & MASK;
        $h = ($h * M) & MASK;
        $h ^= $k;
    }
    my $tail = $chunks * 4;
    my $left = $length - $tail;
    if ($left >= 3) { $h ^= ord(substr($data, $tail + 2, 1)) << 16 }
    if ($left >= 2) { $h ^= ord(substr($data, $tail + 1, 1)) << 8 }
    if ($left >= 1) {
        $h ^= ord(substr($data, $tail, 1));
        $h = ($h * M) & MASK;
    }
    $h ^= $h >> 13;
    $h = ($h * M) & MASK;
    $h ^= $h >> 15;
    return $h;
}

# Kafka's default partitioner: positive(murmur2(key)) % count.
sub murmur2_partition {
    my ($key, $count) = @_;
    return (murmur2($key) & 0x7fffffff) % $count;
}

1;
