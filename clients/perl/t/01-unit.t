#!/usr/bin/env perl
# Offline unit checks of the encodings; no broker needed.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Test::More;

use Brahmaputra;
use Brahmaputra::Protocol qw(:all);
use Brahmaputra::Murmur2 qw(murmur2);
use Brahmaputra::Crc32c qw(crc32c);

# zigzag varints at the int64 and int32 edges
for my $value (0, 1, -1, 63, -64, 2**31 - 1, -2**31, 1_700_000_000_000,
    9223372036854775807, -9223372036854775807 - 1) {
    my $encoded = encode_uvarint(zigzag64($value));
    my $pos = 0;
    my $back = unzigzag64(decode_uvarint(\$encoded, \$pos));
    is("$back", "$value", "int64 $value round-trips");
    is($pos, length $encoded, "int64 $value consumes its bytes");
}
is(length encode_uvarint(zigzag64(-9223372036854775807 - 1)), 10, 'int64 min takes ten bytes');
is(unzigzag32(zigzag32($_)), $_, "int32 $_ round-trips") for 0, -1, 2147483647, -2147483648;
ok(!eval { zigzag32(2147483648); 1 }, 'int32 overflow is refused');

# hashes
is(murmur2(''), 275646681, 'murmur2("")');
is(crc32c('123456789'), 0xE3069283, 'crc32c check value');

# BitPacker body
my $body = Brahmaputra::Writer->body->int32(-5)->int64(-1)->string('hé')->bool(1)->bytes;
my $reader = Brahmaputra::Reader->body($body);
is($reader->int32, -5, 'reader int32');
is($reader->int64, -1, 'reader int64');
is($reader->string, "h\xc3\xa9", 'strings go out as UTF-8');
is($reader->bool, 1, 'reader bool');
ok(!eval { Brahmaputra::Reader->body("\x0a2.0.0")->int32; 1 }, 'schema version mismatch is refused');

# record batch: null vs empty survives, headers keep order
for my $codec (Brahmaputra::Compression::NONE, Brahmaputra::Compression::GZIP) {
    my $batch = Brahmaputra::RecordBatch::encode([
        { key => undef, value => '',    timestamp_delta => -3, headers => [] },
        { key => '',    value => undef, timestamp_delta => 0,  headers => [['a', undef], ['b', '']] },
    ], 1_700_000_000_000, $codec);
    my ($decoded, $next) = Brahmaputra::RecordBatch::decode(\$batch, 0);
    is($next, length $batch, "codec $codec: batch consumed");
    my ($first, $second) = @{ $decoded->{records} };
    ok(!defined $first->{key} && defined $first->{value} && $first->{value} eq '', "codec $codec: null key, empty value");
    ok(defined $second->{key} && $second->{key} eq '' && !defined $second->{value}, "codec $codec: empty key, tombstone");
    is_deeply($second->{headers}, [['a', undef], ['b', '']], "codec $codec: headers");
    is($first->{timestamp_delta}, -3, "codec $codec: negative timestamp delta");
    is($decoded->{max_timestamp}, 1_700_000_000_000, "codec $codec: max timestamp");
}
my $bad = pack('q> l>', 0, -1) . ("\0" x 40);
ok(!eval { Brahmaputra::RecordBatch::decode(\$bad, 0); 1 }, 'negative batch length is a decode error');

# assignors compare partitions as integers, never as strings
my $members = [{ id => 'm1', topics => ['t'] }, { id => 'm2', topics => ['t'] }];
my $parts = { t => [0 .. 11] };
my $previous = { m1 => [map { Brahmaputra::TopicPartition->new('t', $_) } 2, 10] };
my $sticky = Brahmaputra::Assignor::sticky($members, $parts, $previous);
is_deeply([map { $_->partition } @{ $sticky->{m1} }], [0, 1, 2, 3, 4, 10], 'sticky keeps claims, sorts numerically');
is(scalar @{ $sticky->{m2} }, 6, 'sticky balances');
my $range = Brahmaputra::Assignor::range($members, { t => [0 .. 4] });
is_deeply([map { $_->partition } @{ $range->{m1} }], [0, 1, 2], 'range gives the first member the extra');
my $rr = Brahmaputra::Assignor::round_robin($members, { t => [0 .. 3] });
is_deeply([map { $_->partition } @{ $rr->{m2} }], [1, 3], 'roundrobin deals alternately');

done_testing;
