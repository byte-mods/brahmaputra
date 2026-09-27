#!/usr/bin/env perl
# End-to-end suite for the Perl driver against a live broker. A port of
# clients/go/cmd/manualtest/main.go with the same sections and checks.
#
#   brahmaputra-server --data-dir ./data --default-partitions 4
#   perl t/manual_test.pl 127.0.0.1 9092
#
# Every check asserts a property of the system, not that a function ran:
# records come back byte-identical, keys pin partitions, headers survive,
# offsets are contiguous. Exits non-zero on any failure.
#
# The source is deliberately not `use utf8`: its non-ASCII literals are
# UTF-8 byte strings, which is exactly what comes back from the broker.

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use IO::Socket::INET;
use IO::Select;
use POSIX ();
use Time::HiRes ();
use Data::Dumper ();
use Scalar::Util qw(blessed);

use Brahmaputra;
use Brahmaputra::Consumer qw(EARLIEST LATEST);
use Brahmaputra::Murmur2 qw(murmur2 murmur2_partition);

$| = 1;
my ($passed, $failed) = (0, 0);

sub check {
    my ($name, $ok, $detail) = @_;
    if ($ok) {
        $passed++;
        print "  ok   $name\n";
        return;
    }
    $failed++;
    print defined $detail && length $detail ? "  FAIL $name: $detail\n" : "  FAIL $name\n";
}

sub section { print "\n$_[0]\n" }

my $unique_counter = 0;
sub unique {
    my ($prefix) = @_;
    return sprintf('%s-%d-%d', $prefix, int(Time::HiRes::time() * 1e6) % 1_000_000_000, $unique_counter++);
}

sub now_ms { int(Time::HiRes::time() * 1000) }
sub sleep_ms { Time::HiRes::sleep($_[0] / 1000) }
sub dump1 { local $Data::Dumper::Terse = 1; local $Data::Dumper::Indent = 0; Data::Dumper::Dumper($_[0]) }
sub msg { my $e = shift; return defined $e ? "$e" =~ s/\s+$//r : 'nil' }

$SIG{__DIE__} = sub {
    return if $^S;    # inside eval
    print '  FATAL ' . (blessed($_[0]) ? ref($_[0]) . ': ' : '') . "$_[0]\n";
    exit 2;
};

my $host = $ARGV[0] // '127.0.0.1';
my $port = $ARGV[1] // '9092';
my $bootstrap = "$host:$port";

sub producer_config { return { 'bootstrap.servers' => $bootstrap, 'linger.ms' => 0, @_ } }
sub consumer_config { return { 'bootstrap.servers' => $bootstrap } }
sub group_config {
    my ($group_id, %overrides) = @_;
    return { 'bootstrap.servers' => $bootstrap, 'group.id' => $group_id, 'enable.auto.commit' => 0, %overrides };
}

# Run $work in a forked child, which ends with _exit so it never runs
# destructors on its copies of the parent's clients (which would send
# LeaveGroup or commits on shared sockets).
sub run_in_child {
    my ($work) = @_;
    my $pid = fork;
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        local $SIG{__DIE__};
        eval { $work->(); 1 } or print STDERR "child failed: $@\n";
        POSIX::_exit(0);
    }
    return $pid;
}

section('connection and metadata');
{
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my $answer = eval { $consumer->router->seed->api_versions };
    my $error = $@;
    check('ApiVersions answers', $answer && @{ $answer->{versions} } > 0, msg($error));
    check('broker reports a version', $answer && length($answer->{broker_version} // ''), $answer ? $answer->{broker_version} : '');
    my $metadata = $consumer->router->metadata([], 1);
    check('metadata lists brokers', @{ $metadata->{brokers} } >= 1, scalar(@{ $metadata->{brokers} }) . ' brokers');
    $consumer->close;
}

section('produce and consume round trip');
my $topic = unique('perl-roundtrip');
my @payloads = map { "record-$_" } 0 .. 49;
{
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $topic, value => $_, partition => 0) for @payloads;
    $producer->flush;
    $producer->close;
}
{
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($topic, 0, 0, 500);
    check('every record comes back', @got == @payloads, 'got ' . scalar @got);
    my $identical = @got == @payloads;
    for my $i (0 .. $#got) {
        last unless $identical;
        $identical = 0 if !defined $got[$i]->value || $got[$i]->value ne $payloads[$i] || $got[$i]->offset != $i;
    }
    check('values byte-identical and offsets contiguous', $identical);
    $consumer->close;
}

section('compression codecs');
# Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
# Brahmaputra::Compression::register.
for my $codec (qw(none gzip)) {
    my $codec_topic = unique("perl-$codec");
    my $body = 'the same line over and over. ' x 40;
    my $producer = Brahmaputra::Producer->new(producer_config('compression.type' => $codec));
    $producer->send(topic => $codec_topic, value => $body . chr(ord('0') + $_ % 10), partition => 0) for 0 .. 19;
    $producer->flush;
    $producer->close;

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($codec_topic, 0, 0, 500);
    check("$codec: round trips", @got == 20 && index($got[0]->value // '', $body) == 0, 'got ' . scalar(@got) . ' records');
    $consumer->close;
}

section('keys, partitioning and ordering');
{
    my $key_topic = unique('perl-keys');
    my $producer = Brahmaputra::Producer->new(producer_config());
    my @partitions = $producer->router->partitions($key_topic);
    $producer->send(topic => $key_topic, value => "v$_", key => 'user-7') for 0 .. 29;
    $producer->flush;
    $producer->close;

    my $target = $partitions[ murmur2_partition('user-7', scalar @partitions) ];
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @on_target = $consumer->fetch($key_topic, $target, 0, 500);
    check('a key pins every record to one partition', @on_target == 30,
        "partition $target holds " . scalar(@on_target) . ' of 30');
    my $ordered = @on_target == 30;
    for my $i (0 .. $#on_target) {
        $ordered = 0 if ($on_target[$i]->value // '') ne "v$i";
    }
    check('per-key order is preserved', $ordered);
    my $strays = 0;
    for my $partition (@partitions) {
        next if $partition == $target;
        $strays += () = $consumer->fetch($key_topic, $partition, 0, 200);
    }
    check('no keyed record landed elsewhere', $strays == 0, "$strays strays");
    $consumer->close;
}

section("murmur2 agrees with the broker's partitioner");
check('murmur2("") is stable', murmur2('') == 275646681, murmur2(''));
check('murmur2 is deterministic', murmur2('user-7') == murmur2('user-7'));
check('different keys hash differently', murmur2('user-7') != murmur2('user-8'));

section('record headers and timestamps');
{
    my $header_topic = unique('perl-headers');
    my $before = now_ms() - 1000;
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $header_topic, value => 'annotated', partition => 0, headers => [
        ['trace-id', 'abc-123'],
        ['content-type', 'application/json'],
        ['tombstone-reason', undef],
    ]);
    $producer->send(topic => $header_topic, value => 'plain', partition => 0);
    $producer->flush;
    $producer->close;
    my $after = now_ms() + 1000;

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($header_topic, 0, 0, 500);
    check('both records arrive', @got == 2, 'got ' . scalar @got);
    if (@got == 2) {
        my ($annotated, $plain) = @got;
        check('headers survive the round trip', @{ $annotated->headers } == 3, scalar(@{ $annotated->headers }) . ' headers');
        check('header values are exact', ($annotated->header('trace-id') // '') eq 'abc-123');
        check('a null header value stays null', @{ $annotated->headers } == 3 && !defined $annotated->headers->[2][1]);
        check('a record with no headers gains none from its batch', @{ $plain->headers } == 0,
            scalar(@{ $plain->headers }) . ' headers');
        my $in_window = !grep { $_->timestamp < $before || $_->timestamp > $after } @got;
        check('timestamps are real wall-clock values', $in_window,
            sprintf('%d,%d outside %d..%d', $got[0]->timestamp, $got[1]->timestamp, $before, $after));
    }
    $consumer->close;
}

section('tombstones');
{
    my $tomb_topic = unique('perl-tombstones');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $tomb_topic, value => 'set', key => 'k1', partition => 0);
    $producer->send(topic => $tomb_topic, value => '', key => 'k2', partition => 0);
    # A null value is a deletion, and must stay distinguishable from the
    # empty value above all the way through the round trip.
    $producer->send(topic => $tomb_topic, value => undef, key => 'k3', partition => 0);
    $producer->flush;
    $producer->close;

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($tomb_topic, 0, 0, 500);
    check('all three records arrive', @got == 3, 'got ' . scalar @got);
    if (@got == 3) {
        check('an ordinary value round-trips', ($got[0]->value // '') eq 'set');
        check('an empty value is empty, not null', defined $got[1]->value && $got[1]->value eq '', dump1($got[1]->value));
        check('a tombstone arrives as a null value', !defined $got[2]->value, dump1($got[2]->value));
    }
    $consumer->close;
}

section('offsets');
{
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my $earliest = $consumer->list_offsets($topic, 0, EARLIEST);
    my $latest = $consumer->list_offsets($topic, 0, LATEST);
    check('earliest is 0 on a fresh topic', $earliest == 0, $earliest);
    check('latest equals the record count', $latest == 50, $latest);
    $consumer->close;
}

section('acks');
for my $acks (0, 1, -1) {
    my $acks_topic = unique("perl-acks$acks");
    my $producer = Brahmaputra::Producer->new(producer_config(acks => $acks));
    $producer->send(topic => $acks_topic, value => 'durable', partition => 0);
    $producer->flush;
    $producer->close;
    sleep_ms(400);

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($acks_topic, 0, 0, 500);
    check("acks=$acks stores the record", @got == 1, 'got ' . scalar @got);
    $consumer->close;
}

section('consumer group: assignment, commit, resume');
{
    my $group_topic = unique('perl-group');
    my $group_id = unique('perl-billing');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $group_topic, value => "g$_") for 0 .. 39;
    $producer->flush;
    $producer->close;

    my $consumer = Brahmaputra::GroupConsumer->new(group_config($group_id));
    $consumer->subscribe($group_topic);
    my @seen;
    my $deadline = now_ms() + 30_000;
    push @seen, $consumer->poll(500) while @seen < 40 && now_ms() < $deadline;
    check('the group consumes every record', @seen == 40, 'got ' . scalar @seen);
    my %distinct = map { ($_->partition . '-' . $_->offset) => 1 } @seen;
    check('no record is delivered twice', keys(%distinct) == @seen);

    $consumer->commit;
    my $total = 0;
    $total += $_->offset for $consumer->committed;
    check('commit records a position', $total == 40, $total);
    $consumer->close;

    # A second consumer in the same group must resume, not replay.
    my $rejoined = Brahmaputra::GroupConsumer->new(group_config($group_id));
    $rejoined->subscribe($group_topic);
    my @replayed;
    my $until = now_ms() + 5_000;
    push @replayed, $rejoined->poll(300) while now_ms() < $until;
    check('a rejoining group resumes from its commit', @replayed == 0,
        'replayed ' . scalar(@replayed) . ' records it had already committed');
    $rejoined->close;
}

section('auto.offset.reset');
{
    my $reset_topic = unique('perl-reset');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $reset_topic, value => "r$_") for 0 .. 9;
    $producer->flush;
    $producer->close;

    my $consumer = Brahmaputra::GroupConsumer->new(group_config(unique('perl-latest'), 'auto.offset.reset' => 'latest'));
    $consumer->subscribe($reset_topic);
    my @skipped;
    my $until = now_ms() + 4_000;
    push @skipped, $consumer->poll(300) while now_ms() < $until;
    check('latest skips records produced before the group existed', @skipped == 0, 'saw ' . scalar @skipped);
    $consumer->close;

    my $strict = Brahmaputra::GroupConsumer->new(group_config(unique('perl-none'), 'auto.offset.reset' => 'none'));
    $strict->subscribe($reset_topic);
    my $raised = 0;
    $until = now_ms() + 5_000;
    while (now_ms() < $until && !$raised) {
        eval { $strict->poll(300); 1 } and next;
        my $error = $@;
        $raised = 1 if blessed($error) && $error->isa('Brahmaputra::Error::NoOffset');
    }
    check('none refuses to guess a position', $raised);
    $strict->close;
}

section('assignors');
for my $assignor (Brahmaputra::Assignor::RANGE, Brahmaputra::Assignor::ROUNDROBIN, Brahmaputra::Assignor::STICKY) {
    my $assignor_topic = unique("perl-$assignor");
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $assignor_topic, value => "a$_") for 0 .. 19;
    $producer->flush;
    $producer->close;

    my $consumer = Brahmaputra::GroupConsumer->new(
        group_config(unique("perl-grp-$assignor"), 'partition.assignment.strategy' => $assignor));
    $consumer->subscribe($assignor_topic);
    my @collected;
    my $deadline = now_ms() + 20_000;
    while (@collected < 20 && now_ms() < $deadline) {
        push @collected, eval { $consumer->poll(500) };
    }
    check("$assignor: consumes every record", @collected == 20, 'got ' . scalar @collected);
    $consumer->close;
}

section('bounded client buffer');
{
    my $buffer_topic = unique('perl-buffer');
    my $producer = Brahmaputra::Producer->new({
        'bootstrap.servers' => $bootstrap,
        'linger.ms'         => 10_000,    # never flush on time during this check
        'buffer.memory'     => 2048,
        'max.block.ms'      => 300,
    });
    my $blocked = 0;
    for (1 .. 500) {
        last if $blocked;
        eval { $producer->send(topic => $buffer_topic, value => 'x' x 256, partition => 0); 1 } and next;
        my $error = $@;
        $blocked = blessed($error) && $error->isa('Brahmaputra::Error::BufferFull') && $error =~ /buffer full/;
    }
    check('a full buffer blocks and then reports', $blocked);
}

section('wire edge cases');
{
    my $edge_topic = unique('perl-edge');
    my $producer = Brahmaputra::Producer->new(producer_config());
    my $large = pack('C*', map { ($_ * 7) & 0xff } 0 .. (1 << 20) - 1);
    my $unicode_key = "\xd0\xba\xd0\xbb\xd1\x8e\xd1\x87-\xe2\x9c\x93-\xf0\x9f\x94\x91";    # ключ-✓-🔑 as UTF-8
    my $unicode_value = 'значение — 数据 — 🚀';
    my $unicode_header = 'ünïcødé-🏷';
    $producer->send(topic => $edge_topic, value => $large, partition => 0);
    $producer->send(topic => $edge_topic, value => $unicode_value, key => $unicode_key, partition => 0,
        headers => [[$unicode_header, '✓']]);
    # An empty key and an empty header value are values, not nulls.
    $producer->send(topic => $edge_topic, value => 'empty-key', key => '', partition => 0,
        headers => [['empty', ''], ['null', undef]]);
    $producer->send(topic => $edge_topic, value => 'null-key', partition => 0);
    $producer->close;

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got;
    my $offset = 0;
    while (@got < 4) {
        my @batch = eval { $consumer->fetch($edge_topic, 0, $offset, 500) };
        last unless @batch;
        push @got, @batch;
        $offset = $batch[-1]->offset + 1;
    }
    check('edge records all arrive', @got == 4, 'got ' . scalar @got);
    if (@got == 4) {
        check('a 1 MiB value round-trips byte-identical', ($got[0]->value // '') eq $large,
            length($got[0]->value // '') . ' bytes');
        check('unicode key, value and header key round-trip',
            ($got[1]->key // '') eq $unicode_key && ($got[1]->value // '') eq $unicode_value
                && @{ $got[1]->headers } == 1 && $got[1]->headers->[0][0] eq $unicode_header);
        check('an empty key stays empty, not null', defined $got[2]->key && $got[2]->key eq '', dump1($got[2]->key));
        check('an empty header value stays empty, not null',
            @{ $got[2]->headers } == 2 && defined $got[2]->headers->[0][1] && $got[2]->headers->[0][1] eq ''
                && !defined $got[2]->headers->[1][1],
            dump1($got[2]->headers));
        check('a null key stays null', !defined $got[3]->key, dump1($got[3]->key));
    }
    $consumer->close;
}

section('ordering under linger flushes');
{
    my $order_topic = unique('perl-order');
    my $producer = Brahmaputra::Producer->new({ 'bootstrap.servers' => $bootstrap, 'linger.ms' => 1, 'batch.size' => 256 });
    my $total = 5000;
    $producer->send(topic => $order_topic, value => "$_", partition => 0) for 0 .. $total - 1;
    $producer->close;
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @values;
    my $offset = 0;
    while (@values < $total) {
        my @batch = eval { $consumer->fetch($order_topic, 0, $offset, 500) };
        last unless @batch;
        push @values, map { 0 + ($_->value // -1) } @batch;
        $offset = $batch[-1]->offset + 1;
    }
    my $inversions = grep { $values[$_] < $values[$_ - 1] } 1 .. $#values;
    check('every record of a partition arrives', @values == $total, 'got ' . scalar @values);
    check("a partition's records keep send order", $inversions == 0, "$inversions inversions");
    $consumer->close;
}

section('background flush failures are reported');
{
    my $producer = Brahmaputra::Producer->new({ 'bootstrap.servers' => $bootstrap, 'linger.ms' => 20 });
    # Partition 999 does not exist. There is no sender thread, so the
    # "background" flush is the linger-expired one poll() performs; its
    # failure must not vanish, and must not be thrown from poll() either.
    my ($send_error, $poll_error, $flush_error);
    eval { $producer->send(topic => unique('perl-bgfail'), value => 'lost', partition => 999); 1 } or $send_error = $@;
    sleep_ms(300);
    eval { $producer->poll(0); 1 } or $poll_error = $@;
    eval { $producer->flush; 1 } or $flush_error = $@;
    check('a failed linger flush surfaces on the next Flush',
        !defined $send_error && !defined $poll_error && defined $flush_error,
        sprintf('send=%s poll=%s flush=%s', msg($send_error), msg($poll_error), msg($flush_error)));
    my $started = now_ms();
    eval { $producer->close };
    check('Close returns after a failed flush', now_ms() - $started < 5000, 'hung');
}

section('connection failures');
{
    # A broker that accepts and never answers must cost an error, not a
    # process blocked forever. The kernel completes the handshake from the
    # listen backlog, so this socket never needs to accept().
    my $silent = IO::Socket::INET->new(Listen => 8, LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', ReuseAddr => 1)
        or die "silent listener: $!";
    my $conn = Brahmaputra::Connection->open(host => '127.0.0.1', port => $silent->sockport,
        client_id => 'perl-test', connect_timeout_ms => 1000);
    $conn->set_request_timeout(300);
    my $started = now_ms();
    my $request_error;
    eval { $conn->api_versions; 1 } or $request_error = $@;
    check('a request to an unresponsive broker times out',
        defined $request_error && now_ms() - $started < 3000, msg($request_error // 'no error'));
    check('a timed-out connection is not reused', $conn->broken);
    $conn->close;
    close $silent;

    # A connection the broker drops is redialled, not kept forever.
    my $proxy = TestProxy->start($host, $port);
    my $drop_topic = unique('perl-drop');
    my $producer = Brahmaputra::Producer->new({ 'bootstrap.servers' => $proxy->address, 'linger.ms' => 0 });
    $producer->send(topic => $drop_topic, value => 'before', partition => 0);
    $proxy->drop_all;
    my $recovered = 'not attempted';
    for (1 .. 3) {
        last unless defined $recovered;
        $recovered = eval { $producer->send(topic => $drop_topic, value => 'after', partition => 0); 1 } ? undef : msg($@);
    }
    check('a producer recovers after its connection drops', !defined $recovered, $recovered // '');
    eval { $producer->close };

    my $consumer = Brahmaputra::Consumer->new({ 'bootstrap.servers' => $proxy->address });
    $consumer->fetch($drop_topic, 0, 0, 100);
    $proxy->drop_all;
    my $fetch_error = 'not attempted';
    my @fetched;
    for (1 .. 3) {
        last unless defined $fetch_error;
        @fetched = eval { $consumer->fetch($drop_topic, 0, 0, 100) };
        $fetch_error = $@ ? msg($@) : undef;
    }
    check('a consumer recovers after its connection drops', !defined $fetch_error && @fetched >= 1, $fetch_error // '');
    $consumer->close;
    $proxy->close;
}

section('consumer group: max.poll.interval and rejoin');
{
    my $slow_topic = unique('perl-slow');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $slow_topic, value => "s$_") for 0 .. 9;
    my $consumer = Brahmaputra::GroupConsumer->new(group_config(unique('perl-slow-grp'), 'max.poll.interval.ms' => 1500));
    $consumer->subscribe($slow_topic);
    my @first;
    my $deadline = now_ms() + 15_000;
    while (@first < 10 && now_ms() < $deadline) {
        my @records = eval { $consumer->poll(300) };
        last if $@;
        push @first, @records;
    }
    $consumer->commit;
    # Stall past max.poll.interval.ms: the member leaves the group.
    sleep_ms(2500);
    $producer->send(topic => $slow_topic, value => "s$_") for 10 .. 19;
    $producer->close;
    my @second;
    my $poll_error;
    $deadline = now_ms() + 15_000;
    while (@second < 10 && now_ms() < $deadline) {
        my @records = eval { $consumer->poll(300) };
        if ($@) {
            $poll_error = $@;
            last;
        }
        push @second, @records;
    }
    check('a member that stalled rejoins on its next poll',
        @first == 10 && @second == 10 && !defined $poll_error,
        sprintf('first=%d second=%d err=%s', scalar @first, scalar @second, msg($poll_error)));
    $consumer->close;
}

section('consumer group: time inside poll does not count against max.poll.interval');
{
    my $join_topic = unique('perl-inpoll');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->router->partitions($join_topic);
    $producer->close;
    # Far shorter than the poll below, which spends ~1s joining (the
    # broker's initial rebalance delay) and then waits for data.
    my $consumer = Brahmaputra::GroupConsumer->new(group_config(unique('perl-inpoll-grp'), 'max.poll.interval.ms' => 600));
    $consumer->subscribe($join_topic);
    # Single-threaded client: a forked child produces while the parent polls.
    my $child = run_in_child(sub {
        sleep_ms(2000);
        my $late = Brahmaputra::Producer->new({ 'bootstrap.servers' => $bootstrap, 'linger.ms' => 0 });
        $late->send(topic => $join_topic, value => "j$_") for 0 .. 9;
        $late->close;
    });
    my (@got, $poll_error, $commit_error);
    # One long poll: it joins, then waits for the records above.
    @got = eval { $consumer->poll(4000) };
    $poll_error = $@ if $@;
    # Committed straight away, before another poll could quietly rejoin:
    # this fails if the member left the group mid-poll.
    eval { $consumer->commit; 1 } or $commit_error = $@;
    check('a member is still in its group after a long poll',
        !defined $poll_error && @got > 0 && !defined $commit_error,
        sprintf('got=%d poll=%s commit=%s', scalar @got, msg($poll_error), msg($commit_error)));
    waitpid($child, 0);
    $consumer->close;
}

# ---------------------------------------------------------------------------
# Checks beyond the Go suite's 54: one per feature of the client contract
# that those do not already exercise.
# ---------------------------------------------------------------------------

sub mono_ms { int(Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) * 1000) }

# Polls until $want records arrive or $limit_ms passes. With $largest, also
# records the biggest single poll.
sub poll_until {
    my ($consumer, $want, $limit_ms, $largest) = @_;
    my @seen;
    my $deadline = mono_ms() + $limit_ms;
    while (@seen < $want && mono_ms() < $deadline) {
        my @batch = eval { $consumer->poll(300) };
        $$largest = @batch if $largest && @batch > $$largest;
        push @seen, @batch;
    }
    return @seen;
}

sub committed_total {
    my ($consumer) = @_;
    my $total = 0;
    $total += $_->offset for $consumer->committed;
    return $total;
}

section('producer: explicit partition, timestamp and synchronous send');
{
    my $t = unique('perl-sync');
    my $producer = Brahmaputra::Producer->new(producer_config());
    my @offsets = map { $producer->send_sync(topic => $t, value => "sync-$_", partition => 0) } 0 .. 2;
    check("send_sync returns each record's offset", "@offsets" eq '0 1 2', "@offsets");
    $producer->send(topic => $t, value => 'stamped', partition => 2, timestamp => 1_600_000_000_123);
    $producer->close;
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @on_two = $consumer->fetch($t, 2, 0, 500);
    my @on_zero = $consumer->fetch($t, 0, 0, 500);
    check('an explicit partition is honoured', @on_two == 1 && @on_zero == 3, 'partition 2 holds ' . scalar @on_two);
    check('an explicit timestamp survives the round trip',
        @on_two == 1 && $on_two[0]->timestamp == 1_600_000_000_123, @on_two ? $on_two[0]->timestamp : 'no record');
    $consumer->close;
}

section('producer: round-robin for records without a key');
{
    my $t = unique('perl-rr');
    my $producer = Brahmaputra::Producer->new(producer_config());
    my @partitions = $producer->router->partitions($t);
    $producer->send(topic => $t, value => "rr$_") for 0 .. 7;
    $producer->close;
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @counts = map { scalar(my @r = $consumer->fetch($t, $_, 0, 300)) } @partitions;
    check('unkeyed records are spread evenly over every partition', "@counts" eq '2 2 2 2', "@counts");
    $consumer->close;
}

section('producer: batch.size, linger.ms and close');
{
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my $full = unique('perl-batchfull');
    my $eager = Brahmaputra::Producer->new(producer_config('linger.ms' => 60_000, 'batch.size' => 64));
    $eager->send(topic => $full, value => 'b' x 100, partition => 0);
    check('a batch that reaches batch.size is sent without waiting for linger.ms',
        scalar(my @f = $consumer->fetch($full, 0, 0, 300)) == 1);

    # No sender thread here: a lingering batch goes out from the next call
    # into the producer once linger.ms has passed (poll() in a worker loop).
    my $lingering = unique('perl-linger');
    my $lazy = Brahmaputra::Producer->new(producer_config('linger.ms' => 100, 'batch.size' => 1 << 20));
    $lazy->send(topic => $lingering, value => 'waits', partition => 0);
    $lazy->poll(0);
    my $held_back = !scalar(my @h = $consumer->fetch($lingering, 0, 0, 0));
    sleep_ms(300);
    $lazy->poll(0);
    check('linger.ms holds a partial batch, then sends it once linger.ms has passed',
        $held_back && scalar(my @l = $consumer->fetch($lingering, 0, 0, 300)) == 1,
        $held_back ? 'never sent' : 'sent before linger.ms');

    my $closing = unique('perl-close');
    my $closer = Brahmaputra::Producer->new(producer_config('linger.ms' => 60_000, 'batch.size' => 1 << 20));
    $closer->send(topic => $closing, value => "c$_", partition => 0) for 0 .. 4;
    $closer->close;
    check('close flushes what is still buffered', scalar(my @c = $consumer->fetch($closing, 0, 0, 300)) == 5);
    $eager->close;
    $lazy->close;
    $consumer->close;
}

section('producer: retries, request.timeout.ms and delivery.timeout.ms');
{
    my $proxy = FaultProxy->start($host, $port);
    my $t = unique('perl-retry');
    my %base = ('bootstrap.servers' => $proxy->address, 'linger.ms' => 0, 'acks' => 'all', 'request.timeout.ms' => 4321);
    my $producer = Brahmaputra::Producer->new({ %base, retries => 3, 'retry.backoff.ms' => 50 });
    $producer->router->partitions($t);
    $proxy->fail_produces(2, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER);
    my $error;
    eval { $producer->send(topic => $t, value => 'persistent', partition => 0); 1 } or $error = $@;
    my ($produces, $acks, $timeout) = $proxy->stats;
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    check('a retriable error is retried until the send succeeds',
        !defined $error && $produces == 3 && scalar(my @r = $consumer->fetch($t, 0, 0, 300)) == 1,
        "attempts=$produces " . msg($error));
    check('request.timeout.ms and acks travel on the produce request', $timeout == 4321 && $acks == -1, "$timeout/$acks");
    $consumer->close;

    my $bounded = Brahmaputra::Producer->new({ %base, retries => 2, 'retry.backoff.ms' => 150 });
    $bounded->router->partitions($t);
    $proxy->fail_produces(1000, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER);
    my $started = mono_ms();
    my $failed_right = !eval { $bounded->send(topic => $t, value => 'doomed', partition => 0); 1 }
        && blessed($@) && $@->isa('Brahmaputra::Error::Server') && $@->code == Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER;
    my $took = mono_ms() - $started;
    ($produces) = $proxy->stats;
    check('retries are bounded and spaced by retry.backoff.ms', $failed_right && $produces == 3 && $took >= 300,
        "attempts=$produces took ${took}ms");

    $proxy->fail_produces(1000, Brahmaputra::ErrorCode::INVALID_REQUEST);
    eval { $bounded->send(topic => $t, value => 'malformed', partition => 0); 1 };
    ($produces) = $proxy->stats;
    check('a non-retriable error is not retried', $produces == 1, "attempts=$produces");
    eval { $bounded->close };

    my $capped = Brahmaputra::Producer->new({ %base, retries => 1000, 'retry.backoff.ms' => 50, 'delivery.timeout.ms' => 400 });
    $capped->router->partitions($t);
    $proxy->fail_produces(100_000, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER);
    $started = mono_ms();
    my $gave_up = !eval { $capped->send(topic => $t, value => 'late', partition => 0); 1 };
    $took = mono_ms() - $started;
    ($produces) = $proxy->stats;
    check('delivery.timeout.ms caps the whole retry loop', $gave_up && $took < 3000, "took ${took}ms, attempts=$produces");
    $proxy->fail_produces(0, 0);
    eval { $capped->close };
    $producer->close;
    $proxy->close;
}

section('compression: registering a codec');
{
    my $refused = !eval {
        Brahmaputra::Producer->new({ 'bootstrap.servers' => $bootstrap, 'compression.type' => 'snappy' })->close;
        1;
    };
    check('an unregistered codec is refused up front', $refused);

    # A toy reversible codec: enough to prove the hook is used on both the
    # produce and the fetch path. The broker stores batches as-is.
    my $flip = sub { scalar reverse($_[0] ^ ("\x5a" x length $_[0])) };
    Brahmaputra::Compression::register(Brahmaputra::Compression::SNAPPY, $flip, $flip);
    my $t = unique('perl-codec');
    my $producer = Brahmaputra::Producer->new(producer_config('compression.type' => 'snappy'));
    $producer->send(topic => $t, value => 'through a registered codec', key => 'k', partition => 0, headers => [['h', 'v']]);
    $producer->close;
    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my @got = $consumer->fetch($t, 0, 0, 300);
    check('a registered codec compresses on produce and decompresses on fetch',
        @got == 1 && $got[0]->value eq 'through a registered codec' && $got[0]->key eq 'k' && @{ $got[0]->headers } == 1);
    $consumer->close;
    my $encoded = Brahmaputra::RecordBatch::encode([{ key => 'k', value => 'v', headers => [], timestamp_delta => 0 }],
        now_ms(), Brahmaputra::Compression::SNAPPY);
    my ($decoded) = Brahmaputra::RecordBatch::decode(\$encoded, 0);
    check('a batch encoded with it decodes offline',
        @{ $decoded->{records} } == 1 && $decoded->{records}[0]{value} eq 'v');
}

section('consumer: fetch limits, watermark, offsets by time, metadata');
{
    my $t = unique('perl-fetch');
    my $producer = Brahmaputra::Producer->new(producer_config());
    my $base = 1_700_000_000_000;
    $producer->send(topic => $t, value => chr(ord('a') + $_) x 1000, partition => 0, timestamp => $base + $_ * 1000) for 0 .. 19;
    $producer->close;

    my $limited = Brahmaputra::Consumer->new({ %{ consumer_config() }, 'fetch.max.bytes' => 2500 });
    my @capped = $limited->fetch($t, 0, 0, 300);
    check('fetch.max.bytes caps a response', @capped > 0 && @capped < 20, scalar(@capped) . ' records');
    $limited->close;

    my $consumer = Brahmaputra::Consumer->new(consumer_config());
    my $result = $consumer->fetch_verbose($t, 0, 0, 300);
    check('the high watermark is reported', $result->{high_watermark} == 20, $result->{high_watermark});

    my $waiter = Brahmaputra::Consumer->new({ %{ consumer_config() }, 'fetch.max.wait.ms' => 400, 'fetch.min.bytes' => 1 });
    my $started = mono_ms();
    my @none = $waiter->fetch($t, 0, 20, 10_000);
    my $took = mono_ms() - $started;
    check('fetch.max.wait.ms bounds a long poll at the end of the log', !@none && $took >= 250 && $took < 3000, "${took}ms");
    $waiter->close;

    my $by_time = $consumer->list_offsets($t, 0, $base + 5000);
    my $between = $consumer->list_offsets($t, 0, $base + 5500);
    check('list offsets by timestamp finds the first record at or after it', $by_time == 5 && $between == 6,
        "$by_time,$between");

    my $metadata = $consumer->router->metadata([$t], 1);
    my @infos = @{ $metadata->{topics}{$t} || [] };
    check("metadata lists a topic's partitions and their leaders",
        @infos == 4 && !grep({ $_->{leader} < 0 } @infos), scalar(@infos) . ' partitions');
    $consumer->close;

    my $group = Brahmaputra::GroupConsumer->new(group_config(unique('perl-maxpoll'), 'max.poll.records' => 3));
    $group->subscribe($t);
    my $largest = 0;
    my @seen = poll_until($group, 20, 20_000, \$largest);
    check('max.poll.records caps every poll', @seen == 20 && $largest == 3,
        scalar(@seen) . " records, largest poll $largest");
    $group->close;
}

section('decoding is bounds-checked');
{
    my $negative = !eval { Brahmaputra::Reader->body(Brahmaputra::Writer->body->int32(-5)->bytes)->string; 1 }
        && blessed($@) && $@->isa('Brahmaputra::Error::Protocol');
    check('a negative length is an error, not a read', $negative);
    my $oversized = !eval { Brahmaputra::Reader->body(Brahmaputra::Writer->body->int32(1 << 30)->bytes)->string; 1 }
        && blessed($@) && $@->isa('Brahmaputra::Error::Protocol');
    my $batch = Brahmaputra::RecordBatch::encode([{ key => 'k', value => 'v', headers => [], timestamp_delta => 0 }], now_ms());
    substr($batch, 8, 1) = "\x7f";    # batch_length far past the buffer
    my $truncated = !eval { Brahmaputra::RecordBatch::decode(\$batch, 0); 1 }
        && blessed($@) && $@->isa('Brahmaputra::Error::Protocol');
    check('an oversized length is an error, not a read', $oversized && $truncated);
}

section('consumer groups: auto commit, several topics, heartbeats');
{
    my ($t1, $t2) = (unique('perl-multi-a'), unique('perl-multi-b'));
    my $producer = Brahmaputra::Producer->new(producer_config());
    for my $i (0 .. 5) {
        $producer->send(topic => $t1, value => "a$i");
        $producer->send(topic => $t2, value => "b$i");
    }
    $producer->close;

    my $consumer = Brahmaputra::GroupConsumer->new(group_config(unique('perl-multi'),
        'enable.auto.commit' => 1, 'auto.commit.interval.ms' => 200));
    $consumer->subscribe($t1, $t2);
    my @seen = poll_until($consumer, 12, 20_000);
    my %topics = map { $_->topic => 1 } @seen;
    check('one member subscribed to two topics consumes both', @seen == 12 && keys %topics == 2,
        scalar(@seen) . ' records');
    sleep_ms(300);
    eval { $consumer->poll(300) };
    my $total = committed_total($consumer);
    check('enable.auto.commit commits on poll after auto.commit.interval.ms', $total == 12, "committed $total");
    $consumer->close;

    # Single-threaded: heartbeats run inside poll(), commit() and
    # heartbeat(). An application busy between polls calls heartbeat().
    my $idle = unique('perl-idle');
    my $seeder = Brahmaputra::Producer->new(producer_config());
    $seeder->send(topic => $idle, value => 'x');
    $seeder->close;
    my $quiet = Brahmaputra::GroupConsumer->new(group_config(unique('perl-heartbeat'),
        'session.timeout.ms' => 1500, 'heartbeat.interval.ms' => 300));
    $quiet->subscribe($idle);
    poll_until($quiet, 1, 15_000);
    my $generation = $quiet->generation;
    my $until = mono_ms() + 4000;
    while (mono_ms() < $until) {
        sleep_ms(300);
        eval { $quiet->heartbeat };
    }
    my $error;
    eval { $quiet->commit; 1 } or $error = $@;
    check('heartbeats keep an idle member in its group past session.timeout.ms',
        !defined $error && $quiet->generation == $generation, msg($error));
    $quiet->close;
}

section('consumer groups: fencing, rejoin, leave and static membership');
{
    my $t = unique('perl-fence');
    my $producer = Brahmaputra::Producer->new(producer_config());
    $producer->send(topic => $t, value => "f$_") for 0 .. 7;

    my $group_id = unique('perl-fence-grp');
    my %config = ('max.poll.interval.ms' => 60_000, 'heartbeat.interval.ms' => 200);
    my $first = Brahmaputra::GroupConsumer->new(group_config($group_id, %config));
    $first->subscribe($t);
    poll_until($first, 8, 15_000);

    # The coordinator forgets this member behind its back, as it does when
    # a session expires.
    my $leave = Brahmaputra::Writer->body->string($group_id)->string($first->member_id)->bytes;
    $first->consumer->router->seed->request(Brahmaputra::Protocol::API_LEAVE_GROUP, $leave);
    my ($old_member, $old_generation) = ($first->member_id, $first->generation);
    sleep_ms(1000);
    $producer->send(topic => $t, value => "f$_") for 8 .. 11;
    my @after = poll_until($first, 4, 15_000);
    my $commit_error;
    eval { $first->commit; 1 } or $commit_error = $@;
    check('a member the coordinator forgot rejoins on its next poll',
        @after == 4 && $first->generation > $old_generation && !defined $commit_error,
        sprintf('%d records, %s@%d -> %s@%d %s', scalar @after, $old_member, $old_generation,
            $first->member_id, $first->generation, msg($commit_error)));

    # A second member joins; the first sits out the rebalance and its
    # generation goes stale.
    my $stale_generation = $first->generation;
    my $second = Brahmaputra::GroupConsumer->new(group_config($group_id, %config));
    $second->subscribe($t);
    poll_until($second, 1000, 8000);
    my $fenced_code = eval { $first->commit; 0 } // (blessed($@) && $@->can('code') ? $@->code : -1);
    check('a commit from a stale generation is fenced',
        $fenced_code == Brahmaputra::ErrorCode::ILLEGAL_GENERATION || $fenced_code == Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID,
        "generation $stale_generation -> code $fenced_code");
    $second->close;
    $first->close;

    # Close sends LeaveGroup: the next member gets every partition at once
    # instead of waiting out a long session.
    my %slow = (%config, 'session.timeout.ms' => 30_000, 'rebalance.timeout.ms' => 30_000);
    my $leaver = Brahmaputra::GroupConsumer->new(group_config("$group_id-leave", %slow));
    $leaver->subscribe($t);
    poll_until($leaver, 12, 15_000);
    $leaver->close;
    my $successor = Brahmaputra::GroupConsumer->new(group_config("$group_id-leave", %slow));
    $successor->subscribe($t);
    my $started = mono_ms();
    $producer->send(topic => $t, value => "f$_") for 12 .. 13;
    my @handed_over = poll_until($successor, 2, 15_000);
    my $took = mono_ms() - $started;
    my @held = $successor->assignment;
    check('close leaves the group so partitions move without a session timeout',
        @handed_over == 2 && @held == 4 && $took < 10_000, scalar(@handed_over) . " records after ${took}ms");
    $successor->close;

    my $static_group = unique('perl-static');
    my %static = (%config, 'group.instance.id' => 'perl-instance-1');
    my $original = Brahmaputra::GroupConsumer->new(group_config($static_group, %static));
    $original->subscribe($t);
    poll_until($original, 14, 15_000);
    my ($original_member, $original_generation) = ($original->member_id, $original->generation);
    my $restarted = Brahmaputra::GroupConsumer->new(group_config($static_group, %static));
    $restarted->subscribe($t);
    poll_until($restarted, 1000, 3000);
    check('a static member reclaims its member id without a rebalance',
        length($original_member) && $restarted->member_id eq $original_member && $restarted->generation == $original_generation,
        sprintf('%s@%d vs %s@%d', $original_member, $original_generation, $restarted->member_id, $restarted->generation));
    $restarted->close;
    $original->close;
    $producer->close;
}

section('assignors: sticky keeps what members hold');
{
    my $tp = sub { Brahmaputra::TopicPartition->new('t', $_[0]) };
    my $members = [{ id => 'm1', topics => ['t'] }, { id => 'm2', topics => ['t'] }];
    my $topics = { t => [0 .. 11] };
    my $previous = { m1 => [map { $tp->($_) } 2, 10, 11], m2 => [map { $tp->($_) } 0, 1] };
    my $sticky = Brahmaputra::Assignor::assign('sticky', $members, $topics, $previous);
    my $holds = sub {
        my ($id, $p) = @_;
        return grep { $_->partition == $p } @{ $sticky->{$id} };
    };
    check('sticky leaves every held partition where it was',
        $holds->('m1', 2) && $holds->('m1', 10) && $holds->('m1', 11) && $holds->('m2', 0) && $holds->('m2', 1)
            && @{ $sticky->{m1} } == 6 && @{ $sticky->{m2} } == 6);
    my @ids = map { $_->partition } @{ $sticky->{m1} };
    my $numeric = @ids > 1;
    for my $i (1 .. $#ids) { $numeric = 0 unless $ids[ $i - 1 ] < $ids[$i] }
    check('sticky orders partitions as numbers, not strings', $numeric, "@ids");
    my $range = Brahmaputra::Assignor::assign('range', $members, $topics);
    my $rr = Brahmaputra::Assignor::assign('roundrobin', $members, $topics);
    check('range and roundrobin split twelve partitions six and six',
        @{ $range->{m1} } == 6 && @{ $range->{m2} } == 6 && @{ $rr->{m1} } == 6 && $rr->{m1}[1]->partition == 2);
}

print "\n$passed passed, $failed failed\n";
exit($failed > 0 ? 1 : 0);

# Forwards TCP to the broker from a forked child and can sever every live
# connection (SIGUSR1), which is how a broker restart or an idle timeout
# looks to a client.
package TestProxy;

use strict;
use warnings;

sub start {
    my ($class, $host, $port) = @_;
    my $server = IO::Socket::INET->new(Listen => 16, LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', ReuseAddr => 1)
        or die "proxy: $!";
    my $address = '127.0.0.1:' . $server->sockport;
    my $pid = main::run_in_child(sub {
        my $drop = 0;
        local $SIG{USR1} = sub { $drop = 1 };
        my %peer;    # fileno => peer socket
        my %sock;    # fileno => socket
        my $select = IO::Select->new($server);
        while (1) {
            if ($drop) {
                for my $fd (keys %sock) {
                    $select->remove($sock{$fd});
                    CORE::close $sock{$fd};
                }
                %sock = ();
                %peer = ();
                $drop = 0;
            }
            my @ready = $select->can_read(0.05);
            for my $socket (@ready) {
                if ($socket == $server) {
                    my $client = $server->accept or next;
                    my $upstream = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Proto => 'tcp', Timeout => 2);
                    unless ($upstream) {
                        CORE::close $client;
                        next;
                    }
                    $sock{ fileno $client } = $client;
                    $sock{ fileno $upstream } = $upstream;
                    $peer{ fileno $client } = $upstream;
                    $peer{ fileno $upstream } = $client;
                    $select->add($client, $upstream);
                    next;
                }
                my $fd = fileno $socket;
                next unless defined $fd && $sock{$fd};
                my $n = sysread($socket, my $data, 1 << 16);
                my $other = $peer{$fd};
                if (!$n) {
                    for my $s ($socket, $other) {
                        next unless $s;
                        my $sfd = fileno $s;
                        $select->remove($s);
                        delete $sock{$sfd} if defined $sfd;
                        delete $peer{$sfd} if defined $sfd;
                        CORE::close $s;
                    }
                    next;
                }
                local $SIG{PIPE} = 'IGNORE';
                my $off = 0;
                while ($off < length $data) {
                    my $w = syswrite($other, $data, length($data) - $off, $off);
                    last unless $w;
                    $off += $w;
                }
            }
        }
    });
    CORE::close $server;
    return bless { address => $address, pid => $pid }, $class;
}

sub address { $_[0]{address} }

sub drop_all {
    my ($self) = @_;
    kill 'USR1', $self->{pid};
    Time::HiRes::sleep(0.1);
    return;
}

sub close {
    my ($self) = @_;
    kill 'KILL', $self->{pid};
    waitpid($self->{pid}, 0);
    return;
}

# A proxy that understands frames, in a forked child. It forwards every
# request to the broker except Produce, which it can answer itself with an
# error code for the next N requests -- how a leader move or an
# under-replicated partition looks to a producer -- and it records what
# each Produce asked for. Driven over a control socket, one line per
# command: "fail N CODE" and "stats".
package FaultProxy;

use strict;
use warnings;

sub start {
    my ($class, $host, $port) = @_;
    my $server = IO::Socket::INET->new(Listen => 16, LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', ReuseAddr => 1)
        or die "fault proxy: $!";
    my $control = IO::Socket::INET->new(Listen => 4, LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', ReuseAddr => 1)
        or die "fault proxy control: $!";
    my $address = '127.0.0.1:' . $server->sockport;
    my $control_port = $control->sockport;
    my $pid = main::run_in_child(sub { _serve($server, $control, $host, $port) });
    CORE::close $server;
    CORE::close $control;
    my $line = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $control_port, Proto => 'tcp')
        or die "fault proxy control connect: $!";
    $line->autoflush(1);
    return bless { address => $address, pid => $pid, control => $line }, $class;
}

sub _serve {
    my ($server, $control, $host, $port) = @_;
    local $SIG{PIPE} = 'IGNORE';
    my ($failures, $code, $produces, $acks, $timeout) = (0, 0, 0, 0, 0);
    my $select = IO::Select->new($server, $control);
    my (%peer, %is_client, %buffer, %controls);
    my $drop = sub {
        for my $s (@_) {
            next unless $s;
            my $fd = fileno $s;
            $select->remove($s);
            if (defined $fd) { delete $peer{$fd}; delete $is_client{$fd}; delete $buffer{$fd}; delete $controls{$fd} }
            CORE::close $s;
        }
    };
    my $write = sub {
        my ($to, $data) = @_;
        my $off = 0;
        while ($off < length $data) {
            my $w = syswrite($to, $data, length($data) - $off, $off);
            return 0 unless $w;
            $off += $w;
        }
        return 1;
    };
    while (1) {
        for my $socket ($select->can_read(0.5)) {
            if ($socket == $server) {
                my $client = $server->accept or next;
                my $upstream = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Proto => 'tcp', Timeout => 2);
                unless ($upstream) { CORE::close $client; next }
                $peer{ fileno $client } = $upstream;
                $peer{ fileno $upstream } = $client;
                $is_client{ fileno $client } = 1;
                $buffer{ fileno $client } = '';
                $select->add($client, $upstream);
                next;
            }
            if ($socket == $control) {
                my $c = $control->accept or next;
                $controls{ fileno $c } = $c;
                $select->add($c);
                next;
            }
            my $fd = fileno $socket;
            next unless defined $fd;
            if ($controls{$fd}) {
                my $n = sysread($socket, my $data, 4096);
                unless ($n) { $drop->($socket); next }
                $buffer{$fd} .= $data;
                while ($buffer{$fd} =~ s/^([^\n]*)\n//) {
                    my @words = split ' ', $1;
                    if ($words[0] eq 'fail') {
                        ($failures, $code, $produces) = ($words[1], $words[2], 0);
                        $write->($socket, "ok\n");
                    } elsif ($words[0] eq 'stats') {
                        $write->($socket, "$produces $acks $timeout\n");
                    }
                }
                next;
            }
            my $other = $peer{$fd};
            next unless $other;
            my $n = sysread($socket, my $data, 1 << 16);
            unless ($n) { $drop->($socket, $other); next }
            unless ($is_client{$fd}) {
                $drop->($socket, $other) unless $write->($other, $data);
                next;
            }
            $buffer{$fd} .= $data;
            while (length $buffer{$fd} >= 4) {
                my $length = unpack('N', $buffer{$fd});
                last if length $buffer{$fd} < 4 + $length;
                my $frame = substr($buffer{$fd}, 0, 4 + $length, '');
                my $api_key = unpack('n', substr($frame, 4, 2));
                if ($api_key == Brahmaputra::Protocol::API_PRODUCE) {
                    my ($correlation, $body) = Brahmaputra::Protocol::decode_frame_payload(substr($frame, 4));
                    my $reader = Brahmaputra::Reader->body($body);
                    my $topic = $reader->string;
                    my $partition = $reader->int32;
                    $acks = $reader->int32;
                    $timeout = $reader->int32;
                    $produces++;
                    if ($failures > 0) {
                        $failures--;
                        my $reply = Brahmaputra::Writer->body->string($topic)->int32($partition)->int32($code)
                            ->int64(-1)->int64(-1)->bytes;
                        $write->($socket, Brahmaputra::Protocol::encode_frame($api_key, $correlation, '', $reply));
                        next;
                    }
                }
                $write->($other, $frame);
            }
        }
    }
}

sub address { $_[0]{address} }

sub _command {
    my ($self, $line) = @_;
    print { $self->{control} } "$line\n";
    my $reply = readline $self->{control};
    chomp $reply;
    return $reply;
}

# Answer the next $count Produce requests with $code.
sub fail_produces {
    my ($self, $count, $code) = @_;
    $self->_command("fail $count $code");
    return;
}

# (produce requests seen since the last fail_produces, their acks, their timeout)
sub stats { split ' ', $_[0]->_command('stats') }

sub close {
    my ($self) = @_;
    CORE::close $self->{control};
    kill 'KILL', $self->{pid};
    waitpid($self->{pid}, 0);
    return;
}
