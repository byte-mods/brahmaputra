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
