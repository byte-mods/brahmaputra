package Brahmaputra::Protocol;

# Wire constants, varint/zigzag primitives and the frame codec.
#
# Three encodings share one connection and none agrees with the others:
#
#  - the frame header is fixed big-endian (int32 length, int16 api key,
#    int16 api version, int32 correlation id, int16-prefixed client id);
#  - the request/response body is BitPacker: zigzag varints, varint-counted
#    strings and arrays, prefixed with the schema version (Writer/Reader);
#  - a record batch is big-endian header fields plus *plain* varints
#    (RecordBatch).
#
# Perl integers are 64-bit, but the bit operators work on unsigned values
# unless `use integer` is in force: `-1 << 1` is 0xFFFF_FFFF_FFFF_FFFE and
# `>>` is a logical shift. That is exactly what zigzag encoding needs, so
# encoding stays on unsigned patterns, and decoding converts back to a
# signed value with plain arithmetic that cannot overflow.

use strict;
use warnings;
use Exporter 'import';
use Brahmaputra::Error;

our @EXPORT_OK = qw(
    SCHEMA_VERSION API_VERSION READ_UNCOMMITTED READ_COMMITTED
    API_PRODUCE API_FETCH API_LIST_OFFSETS API_METADATA API_JOIN_GROUP
    API_SYNC_GROUP API_HEARTBEAT API_OFFSET_COMMIT API_OFFSET_FETCH
    API_LIST_GROUPS API_DESCRIBE_GROUP API_API_VERSIONS API_AUTHENTICATE
    API_LEAVE_GROUP
    encode_uvarint decode_uvarint zigzag32 unzigzag32 zigzag64 unzigzag64
    encode_frame decode_frame_payload signed32 to_bytes
);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

use constant {
    # The BitPacker schema version every body carries as its first field.
    SCHEMA_VERSION => '1.0.0',
    # Wire version this client speaks. The broker requires an exact match.
    API_VERSION => 4,

    READ_UNCOMMITTED => 0,
    READ_COMMITTED   => 1,

    API_PRODUCE                  => 0,
    API_FETCH                    => 1,
    API_LIST_OFFSETS             => 2,
    API_METADATA                 => 3,
    API_REPLICA_FETCH            => 4,
    API_OFFSETS_FOR_LEADER_EPOCH => 5,
    API_INIT_PRODUCER_ID         => 6,
    API_JOIN_GROUP               => 7,
    API_SYNC_GROUP               => 8,
    API_HEARTBEAT                => 9,
    API_OFFSET_COMMIT            => 10,
    API_OFFSET_FETCH             => 11,
    API_LIST_GROUPS              => 12,
    API_DESCRIBE_GROUP           => 13,
    API_API_VERSIONS             => 14,
    API_PRODUCE_MULTI            => 15,
    API_FETCH_MULTI              => 16,
    API_AUTHENTICATE             => 17,
    API_LEAVE_GROUP              => 18,
};

use constant {
    INT32_MIN => -2147483648,
    INT32_MAX => 2147483647,
};

# Byte string for the wire. A character string (UTF-8 flag on) is encoded
# as UTF-8; a byte string is passed through unchanged.
sub to_bytes {
    my ($value) = @_;
    return $value unless defined $value && utf8::is_utf8($value);
    my $copy = $value;
    utf8::encode($copy);
    return $copy;
}

# Unsigned LEB128 of a non-negative value (or an unsigned 64-bit pattern).
sub encode_uvarint {
    my ($value) = @_;
    my $out = '';
    while ($value >= 0x80) {
        $out .= chr(($value & 0x7f) | 0x80);
        $value >>= 7;
    }
    return $out . chr($value);
}

# Decode an unsigned varint from $$data at $$pos, advancing $$pos. The
# result is the unsigned 64-bit pattern.
sub decode_uvarint {
    my ($data, $pos) = @_;
    my $length = length $$data;
    my $result = 0;
    my $shift = 0;
    while (1) {
        Brahmaputra::Error::Protocol->throw('truncated varint') if $$pos >= $length;
        my $byte = ord substr($$data, $$pos++, 1);
        $result |= ($byte & 0x7f) << $shift;
        return $result unless $byte & 0x80;
        $shift += 7;
        Brahmaputra::Error::Protocol->throw('varint overflows 64 bits') if $shift > 63;
    }
}

sub zigzag64 {
    my ($value) = @_;
    # Unsigned shift, then flip every bit for negatives: the arithmetic
    # shift `value >> 63` of the textbook formula.
    return ($value << 1) ^ ($value < 0 ? ~0 : 0);
}

sub unzigzag64 {
    my ($raw) = @_;
    my $half = $raw >> 1;    # < 2^63, so it fits a signed integer
    return ($raw & 1) ? -1 - $half : $half;
}

sub zigzag32 {
    my ($value) = @_;
    if ($value < INT32_MIN || $value > INT32_MAX || $value != int $value) {
        die "$value does not fit an int32\n";
    }
    return (($value << 1) ^ ($value < 0 ? 0xffffffff : 0)) & 0xffffffff;
}

sub unzigzag32 {
    my ($raw) = @_;
    $raw &= 0xffffffff;
    my $half = $raw >> 1;
    return ($raw & 1) ? -1 - $half : $half;
}

sub signed32 {
    my ($value) = @_;
    return $value >= 0x80000000 ? $value - 4294967296 : $value;
}

# One complete frame, length prefix included.
sub encode_frame {
    my ($api_key, $correlation_id, $client_id, $body) = @_;
    my $client = defined $client_id
        ? pack('n', length $client_id) . $client_id
        : pack('n', 0xffff);
    my $payload = pack('n n N', $api_key, API_VERSION, $correlation_id & 0xffffffff) . $client;
    return pack('N', length($payload) + length($body)) . $payload . $body;
}

# Split a frame payload (length prefix stripped) into (correlation id, body).
sub decode_frame_payload {
    my ($payload) = @_;
    Brahmaputra::Error::Protocol->throw('frame payload shorter than its header') if length($payload) < 10;
    my (undef, undef, $correlation, $client_len) = unpack('n n N n', $payload);
    $client_len = 0 if $client_len >= 0x8000;    # -1: null client id
    my $offset = 10 + $client_len;
    Brahmaputra::Error::Protocol->throw('frame client id runs past the payload') if $offset > length $payload;
    return (signed32($correlation), substr($payload, $offset));
}

1;
