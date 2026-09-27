package Brahmaputra::GroupConsumer;

# A consumer that shares its topics' partitions with the rest of its group.
#
# The group's coordinator is the leader of __consumer_offsets partition
# crc32c(group.id) % partitions; every group request goes there.
#
# Heartbeats run inside poll(). This client is single-threaded (no
# ithreads), so there is no background heartbeat thread as in the Java
# client: poll() heartbeats every heartbeat.interval.ms while it waits,
# and commit() heartbeats too. Processing between two poll() calls must
# therefore stay under session.timeout.ms, or the coordinator evicts this
# member and rebalances; for longer work call heartbeat() from your loop.
#
# max.poll.interval.ms bounds the time the *application* spends between
# polls -- from one poll() returning to the next being called. It is
# enforced at the next poll() (or heartbeat()): if that gap exceeded it,
# this member leaves the group (as Java's heartbeat thread would have when
# the deadline passed), drops its uncommitted positions and rejoins. Time
# spent *inside* poll() -- joining, syncing, waiting for records -- never
# counts: the clock is stamped on entry and again on return.
#
# One GroupConsumer per process: two members in one single-threaded
# process cannot both answer a rebalance at once.

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(blessed refaddr weaken);
use List::Util qw(min max);
use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Consumer;
use Brahmaputra::Assignor;
use Brahmaputra::TopicPartition;
use Brahmaputra::Crc32c qw(crc32c);
use Brahmaputra::Protocol qw(API_JOIN_GROUP API_SYNC_GROUP API_HEARTBEAT API_OFFSET_COMMIT
    API_OFFSET_FETCH API_LEAVE_GROUP);
use Brahmaputra::Writer;
use Brahmaputra::Reader;

use constant {
    OFFSETS_TOPIC        => '__consumer_offsets',
    COORDINATOR_ATTEMPTS => 4,
    JOIN_ATTEMPTS        => 4,
};

# Open group consumers (weak references). Those still open at program exit
# leave their group from an END block, before global destruction tears
# down the sockets that needs.
my %LIVE;

END {
    local ($@, $!);
    my $status = $?;
    for my $consumer (grep { defined && $_->{pid} == $$ } values %LIVE) {
        eval { $consumer->close; 1 };
    }
    $? = $status;
}

sub defaults {
    my %consumer = %{ Brahmaputra::Consumer::defaults() };
    return {
        %consumer,
        'group.id' => undef,
        # Kafka's default is 45 s; 10 s matches the Rust client.
        'session.timeout.ms' => 10000,
        # 0 means session.timeout.ms / 3.
        'heartbeat.interval.ms'   => 0,
        'rebalance.timeout.ms'    => 3000,
        'max.poll.interval.ms'    => 300000,
        'enable.auto.commit'      => 1,
        'auto.commit.interval.ms' => 5000,
        # earliest, latest or none.
        'auto.offset.reset' => 'earliest',
        # range, roundrobin or sticky.
        'partition.assignment.strategy' => Brahmaputra::Assignor::RANGE,
        # Static membership (KIP-345); empty for a dynamic member.
        'group.instance.id' => '',
    };
}

sub new {
    my ($class, $config) = @_;
    my $self = bless {}, $class;
    my $c = $self->{config} = Brahmaputra::Config::resolve(defaults(), $config, 'group consumer');
    croak 'group consumer config needs group.id' unless defined $c->{'group.id'} && length $c->{'group.id'};
    croak 'auto.offset.reset must be earliest, latest or none'
        unless $c->{'auto.offset.reset'} =~ /^(?:earliest|latest|none)$/;
    croak 'partition.assignment.strategy must be one of ' . join(', ', Brahmaputra::Assignor::all())
        unless grep { $_ eq $c->{'partition.assignment.strategy'} } Brahmaputra::Assignor::all();
    $self->{group_id} = $c->{'group.id'};
    my %consumer_config = map { $_ => $c->{$_} } keys %{ Brahmaputra::Consumer::defaults() };
    $self->{consumer} = Brahmaputra::Consumer->new(\%consumer_config);
    $self->{subscribed} = [];
    $self->{member_id} = '';
    $self->{generation} = -1;
    $self->{joined} = 0;
    $self->{assignment} = [];
    $self->{positions} = {};          # key => [tp, next offset to deliver] -- what gets committed
    $self->{fetch_positions} = {};    # key => next offset to fetch; runs ahead by the buffer
    $self->{buffered} = [];
    $self->{last_poll_ms} = undef;
    $self->{last_commit_ms} = Brahmaputra::Config::now_ms();
    $self->{last_heartbeat_ms} = 0;
    $self->{closed} = 0;
    $self->{in_poll} = 0;
    # A forked child inherits this object but not the right to flush or
    # leave on the parent's behalf over the parent's sockets.
    $self->{pid} = $$;
    weaken($LIVE{ refaddr $self } = $self);
    return $self;
}

sub subscribe {
    my ($self, @topics) = @_;
    @topics = @{ $topics[0] } if @topics == 1 && ref $topics[0] eq 'ARRAY';
    my %seen;
    $self->{subscribed} = [grep { !$seen{$_}++ } @topics];
    $self->{joined} = 0;
    return;
}

sub assignment { @{ $_[0]{assignment} } }
sub member_id  { $_[0]{member_id} }
sub generation { $_[0]{generation} }
sub consumer   { $_[0]{consumer} }

# Up to max.poll.records records (Brahmaputra::Record), waiting up to
# $timeout_ms for some to arrive. Joins (or rejoins) the group, heartbeats
# and auto-commits as needed while it waits.
sub poll {
    my ($self, $timeout_ms) = @_;
    $timeout_ms //= 1000;
    Brahmaputra::Error->throw('group consumer is closed') if $self->{closed};
    Brahmaputra::Error->throw('subscribe to at least one topic before polling') unless @{ $self->{subscribed} };
    $self->_enforce_poll_interval;
    # Stamped on entry and again on return: the interval bounds how long
    # the *application* goes without asking for records, and a poll that
    # spends its time joining or waiting is the consumer working normally.
    $self->{last_poll_ms} = Brahmaputra::Config::now_ms();
    $self->{in_poll} = 1;
    my @records = eval { $self->_poll_inner($self->{last_poll_ms} + $timeout_ms) };
    my $error = $@;
    $self->{in_poll} = 0;
    $self->{last_poll_ms} = Brahmaputra::Config::now_ms();
    die $error if $error;
    return @records;
}

sub _poll_inner {
    my ($self, $deadline) = @_;
    while (1) {
        $self->_maybe_heartbeat;
        $self->_join unless $self->{joined};
        return $self->_take_buffered if @{ $self->{buffered} };
        unless (@{ $self->{assignment} }) {
            my $now = Brahmaputra::Config::now_ms();
            return () if $now >= $deadline;
            Brahmaputra::Config::sleep_ms(min(50, max(1, $deadline - $now)));
            next;
        }

        my $got_any = 0;
        for my $tp (@{ $self->{assignment} }) {
            last unless $self->{joined};    # a heartbeat below saw a rebalance
            # Once something is buffered, do not sit in a long poll on the
            # remaining partitions.
            my $wait = $got_any ? 0
                : min(max(0, $deadline - Brahmaputra::Config::now_ms()), $self->_heartbeat_interval_ms, 500);
            my $offset = $self->{fetch_positions}{ $tp->key } // 0;
            my @records = eval { $self->{consumer}->fetch($tp->topic, $tp->partition, $offset, $wait) };
            if (my $error = $@) {
                die $error unless blessed($error) && $error->isa('Brahmaputra::Error::Server');
                if ($error->code == Brahmaputra::ErrorCode::OFFSET_OUT_OF_RANGE) {
                    # The position fell off the log (retention); restart
                    # where auto.offset.reset says.
                    my $reset = $self->_reset_offset($tp);
                    $self->{fetch_positions}{ $tp->key } = $reset;
                    $self->{positions}{ $tp->key } = [$tp, $reset];
                    $self->{buffered} = [grep {
                        $_->topic ne $tp->topic || $_->partition != $tp->partition
                    } @{ $self->{buffered} }];
                    next;
                }
                if (Brahmaputra::ErrorCode::is_stale_route($error->code)) {
                    $self->{consumer}->router->refresh($tp->topic);
                    next;
                }
                die $error;
            }
            if (@records) {
                $got_any = 1;
                $self->{fetch_positions}{ $tp->key } = $records[-1]->offset + 1;
                push @{ $self->{buffered} }, @records;
            }
            $self->_maybe_heartbeat;
        }

        $self->_maybe_auto_commit;
        return $self->_take_buffered if @{ $self->{buffered} };
        return () if Brahmaputra::Config::now_ms() >= $deadline;
    }
}

# Commit the positions of records poll() has returned (at-least-once: call
# it after processing them).
sub commit {
    my ($self) = @_;
    my @entries = sort { Brahmaputra::TopicPartition::compare($a->[0], $b->[0]) } values %{ $self->{positions} };
    return unless @entries;
    my $writer = Brahmaputra::Writer->body
        ->string($self->{group_id})
        ->int32($self->{generation})
        ->string($self->{member_id})
        ->int32(scalar @entries);
    $writer->string($_->[0]->topic)->int32($_->[0]->partition)->int64($_->[1]) for @entries;
    my $reader = Brahmaputra::Reader->body($self->_coordinator_request(API_OFFSET_COMMIT, $writer->bytes));
    my $code = $reader->int32;
    if ($code != Brahmaputra::ErrorCode::NONE) {
        # Generation fencing: this member's view is stale, so its commit is
        # refused. Rejoin on the next poll.
        $self->{joined} = 0 if _is_fencing($code);
        Brahmaputra::Error::Server->throw($code, 'offset_commit');
    }
    $self->{last_commit_ms} = Brahmaputra::Config::now_ms();
    $self->_maybe_heartbeat;
    return;
}

# The group's committed offsets as TopicPartitions carrying ->offset. With
# no arguments, every partition the group has committed.
sub committed {
    my ($self, @partitions) = @_;
    my $writer = Brahmaputra::Writer->body->string($self->{group_id})->int32(scalar @partitions);
    $writer->string($_->topic)->int32($_->partition) for @partitions;
    my $reader = Brahmaputra::Reader->body($self->_coordinator_request(API_OFFSET_FETCH, $writer->bytes));
    my $code = $reader->int32;
    Brahmaputra::Error::Server->throw($code, 'offset_fetch') if $code != Brahmaputra::ErrorCode::NONE;
    my @out;
    for (1 .. $reader->count) {
        my $topic = $reader->string;
        my $partition = $reader->int32;
        push @out, Brahmaputra::TopicPartition->new($topic, $partition, $reader->int64);
    }
    return @out;
}

# Heartbeat now. poll() does this itself; call it from a long processing
# loop to stay in the group without polling. Returns false when the
# coordinator reports a rebalance (the next poll() rejoins).
sub heartbeat {
    my ($self) = @_;
    return 0 unless $self->_enforce_poll_interval;
    return 0 unless $self->{joined} && length $self->{member_id};
    my ($member_id, $generation) = @$self{qw(member_id generation)};
    my $body = Brahmaputra::Writer->body->string($self->{group_id})->int32($generation)->string($member_id)->bytes;
    my $reader = Brahmaputra::Reader->body($self->_coordinator_request(API_HEARTBEAT, $body));
    my $code = $reader->int32;
    $self->{last_heartbeat_ms} = Brahmaputra::Config::now_ms();
    return 1 if $code == Brahmaputra::ErrorCode::NONE;
    if (_is_fencing($code)) {
        # A reply about a membership this consumer no longer has (it
        # rejoined since the heartbeat was sent) says nothing about the
        # current one.
        return 1 if $self->{member_id} ne $member_id || $self->{generation} != $generation;
        # Evicted: the next join gets a fresh member id.
        $self->{member_id} = '' if $code == Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID;
        $self->{joined} = 0;
        return 0;
    }
    Brahmaputra::Error::Server->throw($code, 'heartbeat');
}

# Commit, leave the group and close connections. Leaving is what separates
# a clean shutdown from a crash: without it the coordinator must wait out
# session.timeout.ms before reassigning.
sub close {
    my ($self) = @_;
    return if $self->{closed};
    $self->{closed} = 1;
    delete $LIVE{ refaddr $self };
    # A failed final commit shows up as the next member resuming from an
    # older position, not as a crash on the shutdown path.
    eval { $self->commit if $self->{joined}; 1 };
    # Best effort: failing to leave costs only the session timeout.
    eval { $self->_leave if length $self->{member_id}; 1 };
    $self->{consumer}->close;
    return;
}

sub DESTROY {
    my ($self) = @_;
    return if $self->{closed} || ($self->{pid} // $$) != $$ || !$self->{consumer};
    local ($@, $!, $?);
    eval { $self->close; 1 };
}

sub _is_fencing {
    my ($code) = @_;
    return $code == Brahmaputra::ErrorCode::REBALANCE_IN_PROGRESS
        || $code == Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID
        || $code == Brahmaputra::ErrorCode::ILLEGAL_GENERATION;
}

sub _heartbeat_interval_ms {
    my ($self) = @_;
    my $interval = $self->{config}{'heartbeat.interval.ms'};
    return $interval > 0 ? $interval : max(1, int($self->{config}{'session.timeout.ms'} / 3));
}

sub _maybe_heartbeat {
    my ($self) = @_;
    if ($self->{joined} && Brahmaputra::Config::now_ms() - $self->{last_heartbeat_ms} >= $self->_heartbeat_interval_ms) {
        $self->heartbeat;
    }
    return;
}

# Leave if the application went longer than max.poll.interval.ms between
# polls. Returns false when it did. Never fires while inside poll().
sub _enforce_poll_interval {
    my ($self) = @_;
    return 1 if $self->{in_poll} || !defined $self->{last_poll_ms} || !$self->{joined};
    my $idle = Brahmaputra::Config::now_ms() - $self->{last_poll_ms};
    return 1 if $idle < $self->{config}{'max.poll.interval.ms'};
    # The coordinator evicts us after the session timeout anyway.
    eval { $self->_leave; 1 };
    # What was delivered but not committed is abandoned, exactly as when
    # Kafka's heartbeat thread leaves on this deadline: another member may
    # already own these partitions.
    $self->{positions} = {};
    $self->{fetch_positions} = {};
    $self->{buffered} = [];
    $self->{assignment} = [];
    $self->{member_id} = '';
    $self->{joined} = 0;
    $self->{last_poll_ms} = undef;
    return 0;
}

sub _take_buffered {
    my ($self) = @_;
    my $limit = max(1, $self->{config}{'max.poll.records'});
    my @delivered = splice @{ $self->{buffered} }, 0, $limit;
    for my $record (@delivered) {
        # The committed position advances only over records actually handed
        # to the caller; committing what was merely fetched would skip
        # records nobody processed.
        my $tp = Brahmaputra::TopicPartition->new($record->topic, $record->partition);
        $self->{positions}{ $tp->key } = [$tp, $record->offset + 1];
    }
    return @delivered;
}

sub _maybe_auto_commit {
    my ($self) = @_;
    my $c = $self->{config};
    return unless Brahmaputra::Config::bool_value($c->{'enable.auto.commit'}) && $c->{'auto.commit.interval.ms'} > 0;
    return unless %{ $self->{positions} };
    return if Brahmaputra::Config::now_ms() - $self->{last_commit_ms} < $c->{'auto.commit.interval.ms'};
    # A failure is retried on the next poll; an explicit commit() is what a
    # caller relies on.
    my $ok = eval { $self->commit; 1 };
    unless ($ok) {
        my $error = $@;
        die $error unless blessed($error)
            && ($error->isa('Brahmaputra::Error::Server') || $error->isa('Brahmaputra::Error::Connection'));
    }
    return;
}

sub _reset_offset {
    my ($self, $tp) = @_;
    my $reset = $self->{config}{'auto.offset.reset'};
    return $self->{consumer}->list_offsets($tp->topic, $tp->partition, Brahmaputra::Consumer::EARLIEST)
        if $reset eq 'earliest';
    return $self->{consumer}->list_offsets($tp->topic, $tp->partition, Brahmaputra::Consumer::LATEST)
        if $reset eq 'latest';
    Brahmaputra::Error::NoOffset->throw("no committed offset for $tp and auto.offset.reset=none");
}

sub _join {
    my ($self) = @_;
    my $c = $self->{config};
    # A member that rejoins after a rebalance commits what it has delivered
    # first, while its generation may still be accepted.
    if ($self->{generation} >= 0 && %{ $self->{positions} } && Brahmaputra::Config::bool_value($c->{'enable.auto.commit'})) {
        eval { $self->commit; 1 };
    }
    for (1 .. JOIN_ATTEMPTS) {
        my $body = Brahmaputra::Writer->body
            ->string($self->{group_id})
            ->int32($c->{'session.timeout.ms'})
            ->int32($c->{'rebalance.timeout.ms'})
            ->string($self->{member_id})
            ->string_array($self->{subscribed})
            ->string($c->{'group.instance.id'})
            ->bytes;
        my $reader = Brahmaputra::Reader->body(
            $self->_coordinator_request(API_JOIN_GROUP, $body, $c->{'rebalance.timeout.ms'}));
        my $code = $reader->int32;
        if ($code == Brahmaputra::ErrorCode::REBALANCE_IN_PROGRESS) {
            Brahmaputra::Config::sleep_ms(100);
            next;
        }
        if ($code == Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID) {
            # Our id is gone: rejoin as a new member.
            $self->{member_id} = '';
            next;
        }
        Brahmaputra::Error::Server->throw($code, 'join_group') if $code != Brahmaputra::ErrorCode::NONE;

        my $generation = $reader->int32;
        my $member_id = $reader->string;
        my $leader_id = $reader->string;
        my (@members, %previous);
        for (1 .. $reader->count) {
            my $id = $reader->string;
            my $topics = $reader->string_array;
            my @held;
            for (1 .. $reader->count) {
                my $topic = $reader->string;
                push @held, Brahmaputra::TopicPartition->new($topic, $reader->int32);
            }
            push @members, { id => $id, topics => $topics };
            $previous{$id} = \@held;
        }

        $self->{member_id} = $member_id;
        $self->{generation} = $generation;
        $self->{last_heartbeat_ms} = Brahmaputra::Config::now_ms();

        my $assignments = $member_id eq $leader_id ? $self->_compute_assignments(\@members, \%previous) : {};
        if ($self->_sync($assignments)) {
            $self->{joined} = 1;
            return;
        }
    }
    Brahmaputra::Error->throw('consumer group failed to stabilise after ' . JOIN_ATTEMPTS . ' join attempts');
}

sub _sync {
    my ($self, $assignments) = @_;
    my $writer = Brahmaputra::Writer->body
        ->string($self->{group_id})
        ->int32($self->{generation})
        ->string($self->{member_id})
        ->int32(scalar keys %$assignments);
    for my $member_id (sort keys %$assignments) {
        my $partitions = $assignments->{$member_id};
        $writer->string($member_id)->int32(scalar @$partitions);
        $writer->string($_->topic)->int32($_->partition) for @$partitions;
    }
    my $reader = Brahmaputra::Reader->body(
        $self->_coordinator_request(API_SYNC_GROUP, $writer->bytes, $self->{config}{'rebalance.timeout.ms'}));
    my $code = $reader->int32;
    return 0 if $code == Brahmaputra::ErrorCode::REBALANCE_IN_PROGRESS || $code == Brahmaputra::ErrorCode::ILLEGAL_GENERATION;
    if ($code == Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID) {
        # Evicted between join and sync: rejoin under a fresh id.
        $self->{member_id} = '';
        return 0;
    }
    Brahmaputra::Error::Server->throw($code, 'sync_group') if $code != Brahmaputra::ErrorCode::NONE;
    my @assignment;
    for (1 .. $reader->count) {
        my $topic = $reader->string;
        push @assignment, Brahmaputra::TopicPartition->new($topic, $reader->int32);
    }
    $self->_apply_assignment(\@assignment);
    return 1;
}

sub _apply_assignment {
    my ($self, $assignment) = @_;
    $self->{assignment} = $assignment;
    my %owned = map { $_->key => 1 } @$assignment;
    delete $self->{positions}{$_} for grep { !$owned{$_} } keys %{ $self->{positions} };
    # Buffered records sit ahead of the delivered position and were never
    # handed out, so a new assignment simply drops them.
    $self->{buffered} = [];

    my @needed = grep { !exists $self->{positions}{ $_->key } } @$assignment;
    if (@needed) {
        my %committed = map { $_->key => $_->offset } $self->committed(@needed);
        for my $tp (@needed) {
            my $offset = $committed{ $tp->key } // -1;
            $offset = $self->_reset_offset($tp) if $offset < 0;
            $self->{positions}{ $tp->key } = [$tp, $offset];
        }
    }
    $self->{fetch_positions} = { map { $_ => $self->{positions}{$_}[1] } keys %{ $self->{positions} } };
    return;
}

sub _compute_assignments {
    my ($self, $members, $previous) = @_;
    my %topic_partitions;
    for my $member (@$members) {
        for my $topic (@{ $member->{topics} }) {
            $topic_partitions{$topic} //= [$self->{consumer}->partitions($topic)];
        }
    }
    return Brahmaputra::Assignor::assign($self->{config}{'partition.assignment.strategy'},
        $members, \%topic_partitions, $previous);
}

sub _leave {
    my ($self) = @_;
    my $body = Brahmaputra::Writer->body->string($self->{group_id})->string($self->{member_id})->bytes;
    $self->{joined} = 0;
    my $reader = Brahmaputra::Reader->body($self->_coordinator_request(API_LEAVE_GROUP, $body));
    my $code = $reader->int32;
    if ($code != Brahmaputra::ErrorCode::NONE && $code != Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID) {
        Brahmaputra::Error::Server->throw($code, 'leave_group');
    }
    return;
}

sub _coordinator_partition {
    my ($self) = @_;
    my @partitions = $self->{consumer}->partitions(OFFSETS_TOPIC);
    return crc32c($self->{group_id}) % @partitions;
}

# Send to the group's coordinator, following moves and waiting out loads.
sub _coordinator_request {
    my ($self, $api_key, $body, $extra_wait_ms) = @_;
    my $timeout = $self->{config}{'request.timeout.ms'} + ($extra_wait_ms // 0)
        + Brahmaputra::Consumer::ROUND_TRIP_MARGIN_MS;
    my $last_error;
    for (1 .. COORDINATOR_ATTEMPTS) {
        my $response = eval {
            my $connection = $self->{consumer}->router->connection_for(OFFSETS_TOPIC, $self->_coordinator_partition);
            $connection->request($api_key, $body, $timeout);
        };
        unless (defined $response) {
            my $error = $@;
            die $error unless blessed($error) && $error->isa('Brahmaputra::Error::Connection');
            $last_error = $error;
            Brahmaputra::Config::sleep_ms(100);
            next;
        }
        my $code = _peek_error_code($response);
        if ($code == Brahmaputra::ErrorCode::COORDINATOR_LOAD_IN_PROGRESS) {
            Brahmaputra::Config::sleep_ms(100);
            next;
        }
        if ($code == Brahmaputra::ErrorCode::NOT_COORDINATOR || $code == Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER) {
            $self->{consumer}->router->refresh(OFFSETS_TOPIC);
            next;
        }
        return $response;
    }
    Brahmaputra::Error::Connection->throw(
        'group coordinator unavailable after ' . COORDINATOR_ATTEMPTS . ' attempts'
            . ($last_error ? ": $last_error" : ''),
        cause => $last_error);
}

# Every group response starts with an error code; read it without
# consuming the body.
sub _peek_error_code {
    my ($body) = @_;
    my $code = eval { Brahmaputra::Reader->body($body)->int32 };
    return $code // Brahmaputra::ErrorCode::NONE;
}

1;
