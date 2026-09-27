package Brahmaputra::RecordBatch;

# Record batch codec.
#
# The broker never re-encodes a batch: it validates the header, stamps
# base_offset and leader_epoch in place (both precede the CRC, so it stays
# valid) and writes these exact bytes to disk. An encoding slip here
# corrupts the log rather than failing a request.
#
# Layout (big-endian): base_offset i64, batch_length i32, leader_epoch
# i32, magic i8, crc u32 (CRC32C over everything after it), attributes
# u16, last_offset_delta i32, max_timestamp i64, [v2: producer_id i64,
# producer_epoch i16, base_sequence i32], then the (possibly compressed)
# records. Inside each record varints are *plain*, not zigzag, except the
# timestamp delta.
#
# A record is a hash: { key, value, timestamp_delta, headers => [[k, v], ...] }.
# undef key/value/header value is null; '' is empty, and the two stay
# distinct through a round trip.

use strict;
use warnings;
use Brahmaputra::Error;
use Brahmaputra::Protocol qw(encode_uvarint decode_uvarint zigzag64 unzigzag64);
use Brahmaputra::Crc32c qw(crc32c);
use Brahmaputra::Compression;

use constant {
    HEADER_LEN             => 12,
    MIN_BATCH_LENGTH       => 4 + 1 + 4 + 2 + 4 + 8,
    PRODUCER_EXTENSION_LEN => 8 + 2 + 4,
    MAGIC_V1               => 1,
    MAGIC_V2               => 2,
    COMPRESSION_MASK       => 0x0007,
    HEADERS_BIT            => 0x0008,
    # Some record in the batch has a null value (a tombstone). Set only
    # when one is present; it widens value lengths to length+1, 0 = null.
    NULL_VALUE_BIT => 0x0040,
};

sub encode {
    my ($records, $max_timestamp, $codec) = @_;
    $codec //= Brahmaputra::Compression::NONE;
    my ($has_headers, $has_null_values) = (0, 0);
    for my $record (@$records) {
        $has_headers = 1 if $record->{headers} && @{ $record->{headers} };
        $has_null_values = 1 unless defined $record->{value};
    }

    my $payload = '';
    for my $record (@$records) {
        my $key = $record->{key};
        my $rec = defined $key ? encode_uvarint(length($key) + 1) . $key : "\x00";
        my $value = $record->{value};
        if ($has_null_values) {
            $rec .= defined $value ? encode_uvarint(length($value) + 1) . $value : "\x00";
        } else {
            $rec .= encode_uvarint(length $value) . $value;
        }
        $rec .= encode_uvarint(zigzag64($record->{timestamp_delta}));
        if ($has_headers) {
            my $headers = $record->{headers} || [];
            $rec .= encode_uvarint(scalar @$headers);
            for my $header (@$headers) {
                my ($name, $header_value) = @$header;
                $rec .= encode_uvarint(length $name) . $name;
                $rec .= defined $header_value
                    ? encode_uvarint(length($header_value) + 1) . $header_value
                    : "\x00";
            }
        }
        $payload .= encode_uvarint(length $rec) . $rec;
    }

    my $compressed = Brahmaputra::Compression::compress($codec, $payload);
    my $attributes = $codec & COMPRESSION_MASK;
    $attributes |= HEADERS_BIT if $has_headers;
    $attributes |= NULL_VALUE_BIT if $has_null_values;

    my $last_delta = @$records ? @$records - 1 : 0;
    my $after_crc = pack('n N q>', $attributes, $last_delta, $max_timestamp) . $compressed;
    return pack('q>', 0)                                    # base_offset, stamped by the broker
        . pack('N', MIN_BATCH_LENGTH + length $compressed)
        . pack('N', 0)                                      # leader_epoch, likewise
        . chr(MAGIC_V1)
        . pack('N', crc32c($after_crc))
        . $after_crc;
}

# Decode one batch starting at $offset in $$data. Returns
# ({ base_offset, max_timestamp, records => [...] }, $next_offset).
sub decode {
    my ($data, $offset) = @_;
    my $length = length $$data;
    Brahmaputra::Error::Protocol->throw('truncated batch header') if $length - $offset < HEADER_LEN;
    my ($base_offset, $batch_length) = unpack('q> l>', substr($$data, $offset, HEADER_LEN));
    # Also rejects a negative length, which would otherwise walk backwards.
    Brahmaputra::Error::Protocol->throw("batch_length $batch_length too small") if $batch_length < MIN_BATCH_LENGTH;
    my $body_at = $offset + HEADER_LEN;
    my $end = $body_at + $batch_length;
    Brahmaputra::Error::Protocol->throw('truncated batch body') if $end > $length;

    my $magic = ord substr($$data, $body_at + 4, 1);
    Brahmaputra::Error::Protocol->throw("unsupported magic $magic") if $magic != MAGIC_V1 && $magic != MAGIC_V2;
    my $crc_at = $body_at + 5;
    my $stored = unpack('N', substr($$data, $crc_at, 4));
    my $computed = crc32c(substr($$data, $crc_at + 4, $end - $crc_at - 4));
    if ($stored != $computed) {
        Brahmaputra::Error::Protocol->throw(sprintf('crc mismatch: stored 0x%08x, computed 0x%08x', $stored, $computed));
    }

    my $cursor = $crc_at + 4;
    my ($attributes, undef, $max_timestamp) = unpack('n N q>', substr($$data, $cursor, 14));
    $cursor += 14;
    $cursor += PRODUCER_EXTENSION_LEN if $magic == MAGIC_V2;
    Brahmaputra::Error::Protocol->throw('batch header runs past its batch') if $cursor > $end;

    my $payload = Brahmaputra::Compression::decompress($attributes & COMPRESSION_MASK,
        substr($$data, $cursor, $end - $cursor));
    my $records = _decode_records(\$payload, ($attributes & HEADERS_BIT) ? 1 : 0,
        ($attributes & NULL_VALUE_BIT) ? 1 : 0);
    return ({ base_offset => $base_offset, max_timestamp => $max_timestamp, records => $records }, $end);
}

sub _take {
    my ($data, $pos, $length, $end) = @_;
    if ($length < 0 || $$pos + $length > $end) {
        Brahmaputra::Error::Protocol->throw('record field runs past its record');
    }
    my $value = substr($$data, $$pos, $length);
    $$pos += $length;
    return $value;
}

sub _decode_records {
    my ($payload, $has_headers, $has_null_values) = @_;
    my @records;
    my $pos = 0;
    my $total = length $$payload;
    while ($pos < $total) {
        my $size = decode_uvarint($payload, \$pos);
        Brahmaputra::Error::Protocol->throw('truncated record') if $size > $total - $pos;
        my $end = $pos + $size;

        my $key_plus_one = decode_uvarint($payload, \$pos);
        my $key = $key_plus_one == 0 ? undef : _take($payload, \$pos, $key_plus_one - 1, $end);

        my $raw_value_len = decode_uvarint($payload, \$pos);
        my $value;
        if ($has_null_values && $raw_value_len == 0) {
            $value = undef;    # a tombstone, distinct from an empty value
        } else {
            $value = _take($payload, \$pos, $has_null_values ? $raw_value_len - 1 : $raw_value_len, $end);
        }

        my $timestamp_delta = unzigzag64(decode_uvarint($payload, \$pos));

        my @headers;
        if ($has_headers) {
            my $count = decode_uvarint($payload, \$pos);
            # A count larger than the bytes left is corrupt; allocating on it
            # would let a two-byte record ask for gigabytes.
            Brahmaputra::Error::Protocol->throw('record header count exceeds record') if $count > $end - $pos;
            for (1 .. $count) {
                my $name = _take($payload, \$pos, decode_uvarint($payload, \$pos), $end);
                my $value_plus_one = decode_uvarint($payload, \$pos);
                my $header_value = $value_plus_one == 0 ? undef : _take($payload, \$pos, $value_plus_one - 1, $end);
                push @headers, [$name, $header_value];
            }
        }
        Brahmaputra::Error::Protocol->throw('trailing bytes in record') if $pos != $end;
        push @records, { key => $key, value => $value, timestamp_delta => $timestamp_delta, headers => \@headers };
    }
    return \@records;
}

1;
