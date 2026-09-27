package Brahmaputra::Producer;

# A batching producer, single-threaded.
#
# Perl ithreads are optional in a perl build and heavy where present, so
# this client has no sender thread: batching happens in-process and
# batches go out from inside your calls. A partition's batch is sent
#
#  - by send() when it reaches batch.size bytes, or at once when
#    linger.ms is 0;
#  - by send(), poll() or flush() once its oldest record has waited
#    linger.ms -- whichever of those you call first after the deadline;
#  - by flush() and close() unconditionally.
#
# So a long-lived worker calls poll(0) from its loop, and every program
# calls flush() or close() before it ends (DESTROY flushes as a last resort
# and warns if it cannot).
#
# Errors. A batch the call itself had to send -- its record filled the
# batch, linger.ms is 0, send_sync(), flush() -- dies from that call. A
# batch sent only because its linger expired while you called send() or
# poll() for something else is a "background" flush: its failure is not
# thrown from the unrelated call but held and thrown by the next flush()
# or close(), so it is never dropped. With delivery.report.callback set,
# every outcome goes to the callback instead and nothing is held.
#
# Ordering. A partition has at most one open batch and batches are sent
# synchronously, one at a time, so there is never more than one batch in
# flight per partition and a partition's records reach the broker in send
# order whichever path sends them. send_sync() sends the partition's open
# batch first for the same reason.
#
# Retries. A batch refused with a retriable code (returned before the
# broker appends, so no duplicate is possible) is retried up to `retries`
# times, retry.backoff.ms apart, within delivery.timeout.ms of its oldest
# record. A connection failure mid-request is retried too, as Kafka's
# non-idempotent producer does: the broker may have appended the batch
# before the connection dropped, so that case is at-least-once.

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(blessed refaddr weaken);
use List::Util qw(min max);
use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Router;
use Brahmaputra::Compression;
use Brahmaputra::RecordBatch;
use Brahmaputra::DeliveryReport;
use Brahmaputra::Murmur2 qw(murmur2_partition);
use Brahmaputra::Protocol qw(API_PRODUCE to_bytes);
use Brahmaputra::Writer;
use Brahmaputra::Reader;

sub defaults {
    return {
        'bootstrap.servers' => undef,
        'client.id'         => 'brahmaputra-perl',
        # 0 fire-and-forget, 1 leader append, -1 / "all" every in-sync replica.
        'acks'       => 1,
        'batch.size' => 16384,
        # Kafka's default is 0; 5 because an unbatched producer is slow
        # enough to look broken.
        'linger.ms'           => 5,
        'compression.type'    => 'none',
        # The broker-side wait for acknowledgements; the socket round trip
        # is bounded by this plus a margin.
        'request.timeout.ms'  => 30000,
        'retries'             => 5,
        'retry.backoff.ms'    => 100,
        'delivery.timeout.ms' => 120000,
        'buffer.memory'       => 32 * 1024 * 1024,
        'max.block.ms'        => 60000,
        'socket.connection.setup.timeout.ms' => 10000,
        # sub { my ($report) = @_; ... } (a Brahmaputra::DeliveryReport), or
        # undef to throw failures instead.
        'delivery.report.callback' => undef,
    };
}

# Margin on top of request.timeout.ms before the socket gives up on an
# answer: the broker itself may take request.timeout.ms to say "timed out".
use constant ROUND_TRIP_MARGIN_MS => 5000;

# Open producers (weak references), flushed at program exit.
my %LIVE;

sub new {
    my ($class, $config) = @_;
    my $self = bless {}, $class;
    $self->{config} = Brahmaputra::Config::resolve(defaults(), $config, 'producer');
    my $c = $self->{config};
    my $acks = $c->{acks};
    $acks = -1 if defined $acks && $acks eq 'all';
    croak "acks must be 0, 1, -1 or \"all\", got $c->{acks}" unless defined $acks && $acks =~ /^(?:0|1|-1)$/;
    $self->{acks} = 0 + $acks;
    $self->{codec} = Brahmaputra::Compression::parse($c->{'compression.type'});
    croak 'delivery.report.callback must be a code ref'
        if defined $c->{'delivery.report.callback'} && ref $c->{'delivery.report.callback'} ne 'CODE';
    $self->{router} = Brahmaputra::Router->new(
        bootstrap_servers  => $c->{'bootstrap.servers'},
        client_id          => $c->{'client.id'},
        request_timeout_ms => $c->{'request.timeout.ms'} + ROUND_TRIP_MARGIN_MS,
        connect_timeout_ms => $c->{'socket.connection.setup.timeout.ms'},
    );
    $self->{slots} = {};           # "topic\0partition" => { topic, partition, records, bytes, first_ms }
    $self->{slot_order} = [];      # slot names, oldest first
    $self->{buffered_bytes} = 0;
    $self->{round_robin} = 0;
    $self->{closed} = 0;
    $self->{pending_error} = undef;
    $self->{pending_failures} = 0;
    # A forked child inherits this object but not the right to flush or
    # leave on the parent's behalf over the parent's sockets.
    $self->{pid} = $$;
    weaken($LIVE{ refaddr $self } = $self);
    return $self;
}

# Producers still open when the program ends are flushed from this END
# block, before global destruction starts tearing down the sockets and
# modules a flush needs. DESTROY covers producers that go out of scope
# earlier.
END {
    local ($@, $!);
    my $status = $?;
    for my $producer (grep { defined && $_->{pid} == $$ } values %LIVE) {
        next if $producer->{closed};
        eval { $producer->close; 1 }
            or warn "brahmaputra producer lost buffered records at exit: $@";
    }
    $? = $status;
}

sub router { $_[0]{router} }
sub config { $_[0]{config} }

# Unflushed record bytes currently held client-side.
sub buffered_bytes { $_[0]{buffered_bytes} }

# Buffer one record for delivery.
#
#   $producer->send(topic => 'orders', value => '{"id":1}', key => 'user-7',
#       headers => [['trace-id', 'abc']], partition => 3, timestamp => $ms);
#
# value undef is a tombstone (deletes key on a compacted topic); '' is an
# ordinary empty value. Without partition a keyed record goes to
# murmur2(key) % partitions and a keyless one round-robins.
sub send {
    my ($self, %args) = @_;
    $self->_ensure_open;
    # Deliver whatever has lingered long enough before adding more.
    $self->_send_expired(1);

    my $topic = $args{topic} // croak 'send needs a topic';
    my $record = $self->_record(\%args);
    my $target = $args{partition} // $self->_choose_partition($topic, $record->{key});
    my $size = _estimate($record);
    $self->_reserve($size);

    my $name = "$topic\0$target";
    my $now = Brahmaputra::Config::now_ms();
    my $slot = $self->{slots}{$name};
    unless ($slot) {
        $slot = $self->{slots}{$name} = { topic => $topic, partition => 0 + $target, records => [], bytes => 0, first_ms => $now };
        push @{ $self->{slot_order} }, $name;
    }
    $slot->{first_ms} = $now unless @{ $slot->{records} };
    push @{ $slot->{records} }, $record;
    $slot->{bytes} += $size;

    my $c = $self->{config};
    if ($c->{'linger.ms'} <= 0 || $slot->{bytes} >= $c->{'batch.size'}) {
        $self->_flush_slots([$name], 0);
    }
    return;
}

# Send one record on its own, bypassing the buffer, and return its offset
# (-1 with acks=0). A full round trip per record: correct, and slow.
sub send_sync {
    my ($self, %args) = @_;
    $self->_ensure_open;
    my $topic = $args{topic} // croak 'send_sync needs a topic';
    my $record = $self->_record(\%args);
    my $target = $args{partition} // $self->_choose_partition($topic, $record->{key});
    # Records already buffered for this partition were sent first, so they
    # must reach the broker first.
    $self->_flush_slots(["$topic\0$target"], 0);
    return $self->_produce($topic, $target, [$record]);
}

# Send every batch whose linger.ms has elapsed. With $timeout_ms > 0, wait
# up to that long for further batches to fall due and send them too.
# Returns the number of batches sent.
sub poll {
    my ($self, $timeout_ms) = @_;
    $self->_ensure_open;
    $timeout_ms //= 0;
    my $sent = $self->_send_expired(1);
    if ($timeout_ms > 0) {
        my $deadline = Brahmaputra::Config::now_ms() + $timeout_ms;
        while ($self->_has_buffered && Brahmaputra::Config::now_ms() < $deadline) {
            my $now = Brahmaputra::Config::now_ms();
            Brahmaputra::Config::sleep_ms(max(1, min($self->_next_due_ms - $now, $deadline - $now)));
            $sent += $self->_send_expired(1);
        }
    }
    return $sent;
}

# Send every buffered record now and wait for the broker to acknowledge
# them. Also throws any failure a background (linger) flush has held since
# the last flush, so no failed batch goes unreported.
sub flush {
    my ($self) = @_;
    my $ok = eval { $self->_flush_slots([@{ $self->{slot_order} }], 0); 1 };
    my $error = $@;
    my $held = $self->_take_pending_error;
    die $error unless $ok;
    die $held if $held;
    return;
}

# Flush, then close every connection. Resources are released even when
# the flush fails; the failure is then rethrown.
sub close {
    my ($self) = @_;
    return if $self->{closed};
    my $ok = eval { $self->flush; 1 };
    my $error = $@;
    $self->{closed} = 1;
    delete $LIVE{ refaddr $self };
    $self->{router}->close;
    die $error unless $ok;
    return;
}

sub DESTROY {
    my ($self) = @_;
    return if $self->{closed} || ($self->{pid} // $$) != $$ || !$self->{router};
    local ($@, $!, $?);
    my $ok = eval { $self->close; 1 };
    warn "brahmaputra producer lost buffered records at shutdown: $@" unless $ok;
}

sub _ensure_open {
    Brahmaputra::Error->throw('producer is closed') if $_[0]{closed};
}

sub _record {
    my ($self, $args) = @_;
    my @headers;
    for my $header (@{ $args->{headers} || [] }) {
        croak 'a header is a [name, value] pair' unless ref $header eq 'ARRAY';
        push @headers, [to_bytes($header->[0] // ''), to_bytes($header->[1])];
    }
    return {
        key        => to_bytes($args->{key}),
        value      => to_bytes($args->{value}),
        headers    => \@headers,
        timestamp  => int($args->{timestamp} // Brahmaputra::Config::wall_ms()),
        created_ms => Brahmaputra::Config::now_ms(),
    };
}

sub _choose_partition {
    my ($self, $topic, $key) = @_;
    my @partitions = $self->{router}->partitions($topic);
    return $partitions[ $self->{round_robin}++ % @partitions ] unless defined $key;
    return $partitions[ murmur2_partition($key, scalar @partitions) ];
}

sub _estimate {
    my ($record) = @_;
    my $size = length($record->{value} // '') + length($record->{key} // '') + 16;
    $size += length($_->[0]) + length($_->[1] // '') + 4 for @{ $record->{headers} };
    return $size;
}

# Wait until $size more bytes may be buffered.
#
# This is what makes buffer.memory real: a producer faster than its broker
# is held here instead of growing without limit. With no background sender
# the only thing that can drain the buffer while we wait is a batch whose
# linger.ms falls due, so that is what the loop sends; if nothing does
# within max.block.ms, send() dies with Brahmaputra::Error::BufferFull.
sub _reserve {
    my ($self, $size) = @_;
    my $limit = $self->{config}{'buffer.memory'};
    if ($limit <= 0 || $size >= $limit) {
        # A record larger than the whole budget is admitted rather than
        # waiting on a condition that can never hold; refusing oversized
        # records is the broker's job.
        $self->{buffered_bytes} += $size;
        return;
    }
    my $max_block = $self->{config}{'max.block.ms'};
    my $deadline = Brahmaputra::Config::now_ms() + $max_block;
    while ($self->{buffered_bytes} + $size > $limit) {
        $self->_send_expired(1);
        last if $self->{buffered_bytes} + $size <= $limit;
        my $now = Brahmaputra::Config::now_ms();
        if ($now >= $deadline) {
            Brahmaputra::Error::BufferFull->throw(sprintf(
                'producer buffer full: %d of %d bytes unflushed after max.block.ms=%d',
                $self->{buffered_bytes}, $limit, $max_block));
        }
        Brahmaputra::Config::sleep_ms(max(1, min(20, $deadline - $now, $self->_next_due_ms - $now)));
    }
    $self->{buffered_bytes} += $size;
    return;
}

sub _has_buffered {
    my ($self) = @_;
    for my $slot (values %{ $self->{slots} }) {
        return 1 if @{ $slot->{records} };
    }
    return 0;
}

sub _next_due_ms {
    my ($self) = @_;
    my $due;
    my $linger = $self->{config}{'linger.ms'};
    for my $slot (values %{ $self->{slots} }) {
        next unless @{ $slot->{records} };
        my $at = $slot->{first_ms} + $linger;
        $due = $at if !defined $due || $at < $due;
    }
    return $due // Brahmaputra::Config::now_ms() + 1000;
}

# $background: hold failures for the next flush()/close() instead of dying.
sub _send_expired {
    my ($self, $background) = @_;
    my $now = Brahmaputra::Config::now_ms();
    my $linger = $self->{config}{'linger.ms'};
    my @due = grep {
        my $slot = $self->{slots}{$_};
        $slot && @{ $slot->{records} } && $now - $slot->{first_ms} >= $linger
    } @{ $self->{slot_order} };
    $self->_flush_slots(\@due, $background) if @due;
    return scalar @due;
}

sub _take_pending_error {
    my ($self) = @_;
    my $error = $self->{pending_error};
    my $failures = $self->{pending_failures};
    $self->{pending_error} = undef;
    $self->{pending_failures} = 0;
    if ($error && $failures > 1) {
        return Brahmaputra::Error->new("$failures background batches failed to deliver; first: $error", cause => $error);
    }
    return $error;
}

# Send the named slots. Every slot is attempted even if one fails; the
# first failure is then thrown (or held, or each reported to the callback).
sub _flush_slots {
    my ($self, $names, $background) = @_;
    my $first_error;
    my $failures = 0;
    for my $name (@$names) {
        my $slot = $self->{slots}{$name};
        next unless $slot && @{ $slot->{records} };
        delete $self->{slots}{$name};
        @{ $self->{slot_order} } = grep { $_ ne $name } @{ $self->{slot_order} };
        $self->{buffered_bytes} = max(0, $self->{buffered_bytes} - $slot->{bytes});
        my $count = scalar @{ $slot->{records} };
        my $offset = eval { $self->_produce($slot->{topic}, $slot->{partition}, $slot->{records}) };
        if (defined $offset) {
            $self->_report(Brahmaputra::DeliveryReport->new(
                topic => $slot->{topic}, partition => $slot->{partition}, base_offset => $offset, record_count => $count));
            next;
        }
        my $error = $@ || Brahmaputra::Error->new('produce failed');
        $failures++;
        my $reported = $self->_report(Brahmaputra::DeliveryReport->new(
            topic => $slot->{topic}, partition => $slot->{partition}, base_offset => -1,
            record_count => $count, error => $error));
        $first_error //= $error unless $reported;
    }
    return unless $first_error;
    if ($background) {
        $self->{pending_error} //= $first_error;
        $self->{pending_failures} += $failures;
        return;
    }
    die Brahmaputra::Error->new("$failures batches failed to deliver; first: $first_error", cause => $first_error)
        if $failures > 1;
    die $first_error;
}

# True when a callback consumed the report.
sub _report {
    my ($self, $report) = @_;
    my $callback = $self->{config}{'delivery.report.callback'} or return 0;
    $callback->($report);
    return 1;
}

# Encode and deliver one batch; returns its base offset (-1 for acks=0).
sub _produce {
    my ($self, $topic, $partition, $buffered) = @_;
    my $c = $self->{config};
    # One base timestamp per batch plus a delta per record; the base is the
    # newest record's time, so max_timestamp truthfully answers "how recent
    # is this batch".
    my $max_timestamp = max(map { $_->{timestamp} } @$buffered);
    my $oldest_created = min(map { $_->{created_ms} } @$buffered);
    my @records = map {
        { key => $_->{key}, value => $_->{value}, headers => $_->{headers},
          timestamp_delta => $_->{timestamp} - $max_timestamp }
    } @$buffered;
    my $encoded = Brahmaputra::RecordBatch::encode(\@records, $max_timestamp, $self->{codec});
    my $request_timeout = $c->{'request.timeout.ms'};
    my $body = Brahmaputra::Writer->body
        ->string($topic)
        ->int32($partition)
        ->int32($self->{acks})
        ->int32($request_timeout)
        ->int64(length $encoded)
        ->raw($encoded)
        ->bytes;

    my $deadline = $oldest_created + $c->{'delivery.timeout.ms'};
    my $attempts_left = $c->{retries};
    while (1) {
        if (Brahmaputra::Config::now_ms() >= $deadline) {
            Brahmaputra::Error::Timeout->throw("delivery.timeout.ms expired for $topic-$partition");
        }
        my $response = eval {
            my $connection = $self->{router}->connection_for($topic, $partition);
            if ($self->{acks} == 0) {
                $connection->send_oneway(API_PRODUCE, $body);
                '';
            } else {
                $connection->request(API_PRODUCE, $body, $request_timeout + ROUND_TRIP_MARGIN_MS);
            }
        };
        unless (defined $response) {
            my $error = $@;
            die $error unless blessed($error) && $error->isa('Brahmaputra::Error::Connection');
            die $error if $attempts_left-- <= 0 || Brahmaputra::Config::now_ms() >= $deadline;
            Brahmaputra::Config::sleep_ms($c->{'retry.backoff.ms'});
            next;
        }
        return -1 if $self->{acks} == 0;
        my $reader = Brahmaputra::Reader->body($response);
        $reader->string;    # topic
        $reader->int32;     # partition
        my $code = $reader->int32;
        my $base_offset = $reader->int64;
        $reader->int64;     # log_append_time_ms
        return $base_offset if $code == Brahmaputra::ErrorCode::NONE;
        if (!Brahmaputra::ErrorCode::is_retriable($code) || $attempts_left-- <= 0
            || Brahmaputra::Config::now_ms() >= $deadline) {
            Brahmaputra::Error::Server->throw($code, "produce to $topic-$partition");
        }
        # Resending to the same broker would repeat a stale-route error.
        $self->{router}->refresh($topic) if Brahmaputra::ErrorCode::is_stale_route($code);
        Brahmaputra::Config::sleep_ms($c->{'retry.backoff.ms'});
    }
}

1;
