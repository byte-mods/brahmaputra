package Brahmaputra::Compression;

# Batch compression codecs.
#
# `none` and `gzip` are built in (gzip through the core IO::Compress::Gzip
# and IO::Uncompress::Gunzip, in the RFC 1952 container the broker's
# flate2 GzEncoder/GzDecoder use). lz4, zstd and snappy are opt-in through
# register(), so an application that does not want those CPAN modules does
# not need them:
#
#   Brahmaputra::Compression::register(
#       Brahmaputra::Compression::ZSTD,
#       sub { Compress::Zstd::compress($_[0]) },
#       sub { Compress::Zstd::decompress($_[0]) },
#   );
#
# If you register lz4, the broker expects lz4_flex's compress_prepend_size
# layout: a little-endian uint32 of the uncompressed length, then a raw
# LZ4 *block* -- not the LZ4 frame format.

use strict;
use warnings;
use Carp qw(croak);
use IO::Compress::Gzip qw(gzip $GzipError);
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Brahmaputra::Error;

use constant {
    NONE   => 0,
    LZ4    => 1,
    ZSTD   => 2,
    SNAPPY => 3,
    GZIP   => 4,
};

# Caps decompressed output so a corrupt batch cannot exhaust memory.
use constant MAX_DECOMPRESSED_BYTES => 256 * 1024 * 1024;

my %NAMES = (none => NONE, lz4 => LZ4, zstd => ZSTD, snappy => SNAPPY, gzip => GZIP);
my %CODECS;

# Plug in a codec this driver does not carry: two code refs, each taking
# and returning a byte string.
sub register {
    my ($codec, $compress, $decompress) = @_;
    croak 'register needs a codec id and two code refs'
        unless defined $codec && ref $compress eq 'CODE' && ref $decompress eq 'CODE';
    $CODECS{$codec} = [$compress, $decompress];
    return;
}

sub parse {
    my ($name) = @_;
    my $codec = $NAMES{ lc($name // '') };
    croak "unknown compression.type $name (" . join(', ', sort keys %NAMES) . ')' unless defined $codec;
    return $codec;
}

sub name {
    my ($codec) = @_;
    my %by_id = reverse %NAMES;
    return $by_id{$codec} // "unknown($codec)";
}

sub compress {
    my ($codec, $payload) = @_;
    return $CODECS{$codec}[0]->($payload) if $CODECS{$codec};
    return $payload if $codec == NONE;
    if ($codec == GZIP) {
        my $out;
        gzip(\$payload => \$out, -Level => 6, Minimal => 1)
            or Brahmaputra::Error->throw("gzip compression failed: $GzipError");
        return $out;
    }
    Brahmaputra::Error->throw(name($codec)
        . ' compression is not available; register it with Brahmaputra::Compression::register or use none/gzip');
}

sub decompress {
    my ($codec, $payload) = @_;
    return $CODECS{$codec}[1]->($payload) if $CODECS{$codec};
    return $payload if $codec == NONE;
    if ($codec == GZIP) {
        my $out;
        gunzip(\$payload => \$out, MultiStream => 1)
            or Brahmaputra::Error::Protocol->throw("gzip batch payload failed to decompress: $GunzipError");
        Brahmaputra::Error::Protocol->throw('gzip batch payload decompresses past the size cap')
            if length($out) > MAX_DECOMPRESSED_BYTES;
        return $out;
    }
    Brahmaputra::Error->throw(name($codec)
        . ' decompression is not available; register it with Brahmaputra::Compression::register');
}

1;
