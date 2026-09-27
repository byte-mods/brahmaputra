package Brahmaputra::Consumer;

# Reads partitions directly, with no group coordination.

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(blessed);
use List::Util qw(min max);
use Exporter 'import';
use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Router;
use Brahmaputra::Record;
use Brahmaputra::RecordBatch;
use Brahmaputra::Protocol qw(API_FETCH API_LIST_OFFSETS READ_UNCOMMITTED READ_COMMITTED);
use Brahmaputra::Writer;
use Brahmaputra::Reader;

our @EXPORT_OK = qw(EARLIEST LATEST);

# Sentinels for list_offsets(); any other value is a unix-ms timestamp.
use constant {
    EARLIEST => -2,    # the oldest retained offset
    LATEST   => -1,    # the next offset to be written (the log end)
};

# Margin on top of request.timeout.ms (+ the fetch wait) before the socket
# gives up on an answer.
use constant ROUND_TRIP_MARGIN_MS => 5000;

sub defaults {
    return {
        'bootstrap.servers' => undef,
        'client.id'         => 'brahmaputra-perl',
        'fetch.max.bytes'   => 8 * 1024 * 1024,
        'fetch.min.bytes'   => 1,
        'fetch.max.wait.ms' => 500,
        'max.poll.records'  => 500,
        # read_uncommitted, or read_committed (stops at the last stable offset).
        'isolation.level' => 'read_uncommitted',
        # This consumer's failure domain; with it set the leader may point
        # reads at a same-rack replica.
        'client.rack'        => '',
        'request.timeout.ms' => 30000,
        'socket.connection.setup.timeout.ms' => 10000,
    };
}

sub new {
    my ($class, $config) = @_;
    my $self = bless {}, $class;
    $self->{config} = Brahmaputra::Config::resolve(defaults(), $config, 'consumer');
    my $level = $self->{config}{'isolation.level'};
    $self->{isolation} = $level eq 'read_uncommitted' || $level eq '0' ? READ_UNCOMMITTED
        : $level eq 'read_committed' || $level eq '1' ? READ_COMMITTED
        : croak 'isolation.level must be read_uncommitted or read_committed';
    $self->{router} = Brahmaputra::Router->new(
        bootstrap_servers  => $self->{config}{'bootstrap.servers'},
        client_id          => $self->{config}{'client.id'},
        request_timeout_ms => $self->{config}{'request.timeout.ms'} + ROUND_TRIP_MARGIN_MS,
        connect_timeout_ms => $self->{config}{'socket.connection.setup.timeout.ms'},
    );
    return $self;
}

sub router { $_[0]{router} }
sub config { $_[0]{config} }

sub close {
    my ($self) = @_;
    $self->{router}->close if $self->{router};
    return;
}

# The topic's partition ids, ascending.
sub partitions {
    my ($self, $topic) = @_;
    return $self->{router}->partitions($topic);
}

# Resolve EARLIEST, LATEST or a unix-ms timestamp to an offset (for a
# timestamp: the first offset whose record time is at or after it).
sub list_offsets {
    my ($self, $topic, $partition, $timestamp) = @_;
    my $body = Brahmaputra::Writer->body->string($topic)->int32($partition)->int64($timestamp)->bytes;
    my $reader = Brahmaputra::Reader->body($self->_request_leader($topic, $partition, API_LIST_OFFSETS, $body));
    $reader->string;    # topic
    $reader->int32;     # partition
    my $code = $reader->int32;
    my $offset = $reader->int64;
    $reader->int64;     # timestamp
    Brahmaputra::Error::Server->throw($code, "list_offsets $topic-$partition") if $code != Brahmaputra::ErrorCode::NONE;
    return $offset;
}

# The partition's high watermark: the offset the next record will get.
sub high_watermark {
    my ($self, $topic, $partition) = @_;
    return $self->list_offsets($topic, $partition, LATEST);
}

# Records (Brahmaputra::Record) from $offset on, waiting up to $max_wait_ms
# (capped by fetch.max.wait.ms) for fetch.min.bytes to accumulate.
sub fetch {
    my ($self, @args) = @_;
    return @{ $self->fetch_verbose(@args)->{records} };
}

# Fetch, also returning the partition's high watermark:
# { records => [...], high_watermark => N }.
sub fetch_verbose {
    my ($self, $topic, $partition, $offset, $max_wait_ms) = @_;
    my $c = $self->{config};
    my $configured = $c->{'fetch.max.wait.ms'};
    my $wait = max(0, min($max_wait_ms // $configured, $configured));
    my $body = Brahmaputra::Writer->body
        ->string($topic)
        ->int32($partition)
        ->int64($offset)
        ->int32($c->{'fetch.max.bytes'})
        ->int32($wait)
        ->int32($c->{'fetch.min.bytes'})
        ->int32($self->{isolation})
        ->string($c->{'client.rack'})
        ->bytes;
    # The broker may hold the request for $wait before answering.
    my $timeout = $c->{'request.timeout.ms'} + $wait + ROUND_TRIP_MARGIN_MS;

    my $result = _decode_fetch($self->_request_leader($topic, $partition, API_FETCH, $body, $timeout));
    if ($result->{code} == Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER) {
        $self->{router}->refresh($topic);
        $result = _decode_fetch($self->_request_leader($topic, $partition, API_FETCH, $body, $timeout));
    }
    Brahmaputra::Error::Server->throw($result->{code}, "fetch $topic-$partition")
        if $result->{code} != Brahmaputra::ErrorCode::NONE;

    my @records;
    for my $batch (@{ $result->{batches} }) {
        my $index = 0;
        for my $record (@{ $batch->{records} }) {
            my $record_offset = $batch->{base_offset} + $index++;
            # A batch can start before the requested offset; skip what the
            # caller has already seen.
            next if $record_offset < $offset;
            push @records, Brahmaputra::Record->new(
                topic     => $topic,
                partition => $partition,
                offset    => $record_offset,
                key       => $record->{key},
                value     => $record->{value},
                timestamp => $batch->{max_timestamp} + $record->{timestamp_delta},
                headers   => $record->{headers},
            );
        }
    }
    return { records => \@records, high_watermark => $result->{high_watermark} };
}

# Send a read to the partition leader. Reads are idempotent, so a
# connection that dropped (broker restart, idle timeout) is redialled and
# the request sent once more before the error reaches the caller. A
# timeout is not retried: it already cost the caller its full budget.
sub _request_leader {
    my ($self, $topic, $partition, $api_key, $body, $timeout_ms) = @_;
    my $response = eval { $self->{router}->connection_for($topic, $partition)->request($api_key, $body, $timeout_ms) };
    return $response if defined $response;
    my $error = $@;
    die $error unless blessed($error) && $error->isa('Brahmaputra::Error::Connection')
        && !$error->isa('Brahmaputra::Error::Timeout');
    return $self->{router}->connection_for($topic, $partition)->request($api_key, $body, $timeout_ms);
}

sub _decode_fetch {
    my ($body) = @_;
    my $reader = Brahmaputra::Reader->body($body);
    $reader->string;    # topic
    $reader->int32;     # partition
    my $code = $reader->int32;
    my $high_watermark = $reader->int64;
    $reader->int64;     # last_stable_offset
    my $batches_length = $reader->int64;
    # Read even though unused: the batches trail the whole struct, so
    # skipping a field would take them from the wrong offset.
    $reader->int32;     # preferred_read_replica
    my $trailing = $reader->rest;
    if ($batches_length < 0 || $batches_length > length $trailing) {
        Brahmaputra::Error::Protocol->throw('fetch response claims more batch bytes than it carries');
    }
    my $raw = substr($trailing, 0, $batches_length);
    my @batches;
    my $pos = 0;
    while ($pos < length $raw) {
        (my $batch, $pos) = Brahmaputra::RecordBatch::decode(\$raw, $pos);
        push @batches, $batch;
    }
    return { code => $code, high_watermark => $high_watermark, batches => \@batches };
}

1;
