package Brahmaputra::Crc32c;

# CRC32C (Castagnoli, reflected polynomial 0x82F63B78). Record batches use
# it rather than zlib's CRC32, so Compress::Zlib::crc32 is the wrong
# function. Pure Perl, table driven.

use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(crc32c);

my @TABLE;
for my $index (0 .. 255) {
    my $crc = $index;
    for (1 .. 8) {
        $crc = ($crc & 1) ? (($crc >> 1) ^ 0x82f63b78) : ($crc >> 1);
    }
    push @TABLE, $crc;
}

sub crc32c {
    my ($data) = @_;
    my $crc = 0xffffffff;
    my $length = length $data;
    # Unpacked in slices so a large batch does not become one huge list.
    for (my $at = 0; $at < $length; $at += 65536) {
        for my $byte (unpack 'C*', substr($data, $at, 65536)) {
            $crc = $TABLE[($crc ^ $byte) & 0xff] ^ ($crc >> 8);
        }
    }
    return $crc ^ 0xffffffff;
}

1;
