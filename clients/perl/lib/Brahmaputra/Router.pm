package Brahmaputra::Router;

# Keeps one connection per broker and routes each request to its
# partition's leader.
#
# Metadata is cached and refreshed only when a request says the route was
# stale (or the topic is not yet known): refreshing per request would put
# the control plane on the data path. A connection that broke (timeout,
# I/O error, desync) is never handed out again; the next request for that
# broker -- the seed included -- dials a fresh one.
#
# The metadata image is a hash:
#   { brokers => [{ node_id, host, port, rack }, ...], controller_id,
#     topics  => { name => [{ partition, leader, replicas, isr, leader_epoch }, ...] } }

use strict;
use warnings;
use Scalar::Util qw(blessed);
use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Connection;
use Brahmaputra::Protocol qw(API_METADATA);
use Brahmaputra::Writer;
use Brahmaputra::Reader;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        bootstrap          => [Brahmaputra::Config::parse_bootstrap($args{bootstrap_servers})],
        client_id          => $args{client_id} // 'brahmaputra-perl',
        request_timeout_ms => $args{request_timeout_ms} // Brahmaputra::Connection::DEFAULT_REQUEST_TIMEOUT_MS,
        connect_timeout_ms => $args{connect_timeout_ms} // Brahmaputra::Connection::DEFAULT_CONNECT_TIMEOUT_MS,
        seed               => undef,
        connections        => {},
        metadata           => undef,
    }, $class;
    $self->seed;
    return $self;
}

sub _dial {
    my ($self, $host, $port) = @_;
    return Brahmaputra::Connection->open(
        host               => $host,
        port               => $port,
        client_id          => $self->{client_id},
        connect_timeout_ms => $self->{connect_timeout_ms},
        request_timeout_ms => $self->{request_timeout_ms},
    );
}

# The bootstrap connection, redialled (trying each bootstrap server) if it
# broke.
sub seed {
    my ($self) = @_;
    return $self->{seed} if $self->{seed} && !$self->{seed}->broken;
    my $last;
    for my $server (@{ $self->{bootstrap} }) {
        my $connection = eval { $self->_dial(@$server) };
        if ($connection) {
            $self->{seed} = $connection;
            return $connection;
        }
        $last = $@;
    }
    die $last // Brahmaputra::Error::Connection->new('no bootstrap server reachable');
}

sub close {
    my ($self) = @_;
    $_->close for values %{ $self->{connections} };
    $self->{connections} = {};
    $self->{seed}->close if $self->{seed};
    $self->{seed} = undef;
    return;
}

sub _is_connection_error {
    my ($error) = @_;
    return blessed($error) && $error->isa('Brahmaputra::Error::Connection');
}

# Cluster metadata. Topics named here are merged into the cached image;
# an empty list asks the broker for every topic.
sub metadata {
    my ($self, $topics, $refresh) = @_;
    $topics ||= [];
    if (!$refresh && $self->{metadata}) {
        my @missing = grep { !exists $self->{metadata}{topics}{$_} } @$topics;
        return $self->{metadata} unless @missing;
    }
    my $body = Brahmaputra::Writer->body->string_array($topics)->bytes;
    my $response = eval { $self->seed->request(API_METADATA, $body) };
    unless (defined $response) {
        my $error = $@;
        die $error unless _is_connection_error($error);
        # One redial: a broker restart or a dropped socket should not fail
        # the caller.
        $response = $self->seed->request(API_METADATA, $body);
    }
    my $fresh = _decode_metadata(Brahmaputra::Reader->body($response));
    if ($self->{metadata} && @$topics) {
        $fresh->{topics} = { %{ $self->{metadata}{topics} }, %{ $fresh->{topics} } };
    }
    $self->{metadata} = $fresh;
    return $fresh;
}

sub refresh {
    my ($self, $topic) = @_;
    return $self->metadata([$topic], 1);
}

# The topic's partition ids, ascending.
sub partitions {
    my ($self, $topic) = @_;
    my $metadata = $self->metadata([$topic]);
    unless (@{ $metadata->{topics}{$topic} || [] }) {
        # A topic auto-created on first use is not in the cached image yet;
        # one refresh distinguishes "new" from "absent".
        $metadata = $self->refresh($topic);
    }
    my $infos = $metadata->{topics}{$topic} || [];
    Brahmaputra::Error->throw("topic $topic has no partitions") unless @$infos;
    return sort { $a <=> $b } map { $_->{partition} } @$infos;
}

sub _leader_of {
    my ($metadata, $topic, $partition) = @_;
    for my $info (@{ $metadata->{topics}{$topic} || [] }) {
        return $info->{leader} if $info->{partition} == $partition;
    }
    return -1;
}

# The connection to the leader of $topic-$partition.
sub connection_for {
    my ($self, $topic, $partition) = @_;
    my $metadata = $self->metadata([$topic]);
    my $leader = _leader_of($metadata, $topic, $partition);
    if ($leader < 0) {
        $metadata = $self->refresh($topic);
        $leader = _leader_of($metadata, $topic, $partition);
    }
    Brahmaputra::Error->throw("no leader for $topic-$partition") if $leader < 0;
    return $self->_connection_to($leader, $metadata);
}

sub _connection_to {
    my ($self, $node_id, $metadata) = @_;
    my $existing = $self->{connections}{$node_id};
    return $existing if $existing && !$existing->broken;
    # A single-broker cluster advertises the address it was configured
    # with, which may not be the one we dialled (a proxy, a NAT); reuse the
    # seed rather than opening a second connection to ourselves.
    if (@{ $metadata->{brokers} } == 1) {
        return $self->{connections}{$node_id} = $self->seed;
    }
    my ($broker) = grep { $_->{node_id} == $node_id } @{ $metadata->{brokers} };
    Brahmaputra::Error->throw("broker $node_id is not in the metadata") unless $broker;
    return $self->{connections}{$node_id} = $self->_dial($broker->{host}, $broker->{port});
}

sub _decode_metadata {
    my ($reader) = @_;
    # Schema order: error_code, brokers, controller_id, topics. The leading
    # code is request-level and distinct from the per-topic one.
    my $request_error = $reader->int32;
    Brahmaputra::Error::Server->throw($request_error, 'metadata') if $request_error != Brahmaputra::ErrorCode::NONE;
    my @brokers;
    for (1 .. $reader->count) {
        push @brokers, {
            node_id => $reader->int32,
            host    => $reader->string,
            port    => $reader->int32,
            rack    => $reader->string,
        };
    }
    my $controller_id = $reader->int32;
    my %topics;
    for (1 .. $reader->count) {
        my $name = $reader->string;
        my $topic_error = $reader->int32;
        my @partitions;
        for (1 .. $reader->count) {
            my $partition = $reader->int32;
            my $leader = $reader->int32;
            my @replicas = map { $reader->int32 } 1 .. $reader->count;
            my @isr = map { $reader->int32 } 1 .. $reader->count;
            push @partitions, {
                partition    => $partition,
                leader       => $leader,
                replicas     => \@replicas,
                isr          => \@isr,
                leader_epoch => $reader->int32,
            };
        }
        if ($topic_error != Brahmaputra::ErrorCode::NONE
            && $topic_error != Brahmaputra::ErrorCode::UNKNOWN_TOPIC_OR_PARTITION) {
            Brahmaputra::Error::Server->throw($topic_error, "metadata for $name");
        }
        $topics{$name} = \@partitions;
    }
    return { brokers => \@brokers, controller_id => $controller_id, topics => \%topics };
}

1;
