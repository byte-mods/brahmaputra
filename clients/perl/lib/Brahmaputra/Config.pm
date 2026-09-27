package Brahmaputra::Config;

# Kafka-style configuration (a flat hash of dotted keys) and clocks.
#
# Unknown keys are rejected rather than ignored: a misspelled `linger.ms`
# that silently falls back to its default is only found in production.

use strict;
use warnings;
use Carp qw(croak);
use Time::HiRes ();
use POSIX ();

sub resolve {
    my ($defaults, $given, $what) = @_;
    $given ||= {};
    croak "$what config must be a hash reference" unless ref $given eq 'HASH';
    my @unknown = sort grep { !exists $defaults->{$_} } keys %$given;
    if (@unknown) {
        croak sprintf('unknown %s config %s; known keys: %s',
            $what, join(', ', @unknown), join(', ', sort keys %$defaults));
    }
    my %config = (%$defaults, %$given);
    my $servers = $config{'bootstrap.servers'};
    croak qq{$what config needs bootstrap.servers ("host:port[,host:port]")}
        unless defined $servers && !ref $servers && length $servers;
    return \%config;
}

# "host:port,host:port" -> ([host, port], ...). A bare host means :9092.
sub parse_bootstrap {
    my ($servers) = @_;
    my @out;
    for my $server (split /,/, $servers) {
        $server =~ s/^\s+|\s+$//g;
        next unless length $server;
        if ($server =~ /^\[(.+)\]:(\d+)$/ || $server =~ /^([^:]+):(\d+)$/) {
            push @out, [$1, 0 + $2];
        } else {
            push @out, [$server =~ s/^\[|\]$//gr, 9092];
        }
    }
    croak 'bootstrap.servers is empty' unless @out;
    return @out;
}

sub bool_value {
    my ($value) = @_;
    return 0 if !defined $value;
    return 0 if $value =~ /^\s*(?:0|false|no|off|)\s*$/i;
    return 1;
}

# Monotonic milliseconds, for deadlines.
sub now_ms {
    return int(Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) * 1000);
}

# Wall-clock unix milliseconds, for record timestamps.
sub wall_ms {
    return int(Time::HiRes::time() * 1000);
}

sub sleep_ms {
    my ($ms) = @_;
    Time::HiRes::sleep($ms / 1000) if $ms > 0;
    return;
}

1;
