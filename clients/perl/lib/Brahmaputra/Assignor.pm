package Brahmaputra::Assignor;

# Partition assignment strategies (partition.assignment.strategy).
#
# The group leader computes the assignment client-side and hands it to the
# coordinator in SyncGroup. Each strategy mirrors the Rust client's, so a
# Perl leader and a Rust leader produce the same assignment for the same
# group.
#
#   $members          [{ id => 'member', topics => ['t', ...] }, ...]
#   $topic_partitions { topic => [partition ids, ascending] }
#   $previous         { member id => [Brahmaputra::TopicPartition, ...] }
#   returns           { member id => [Brahmaputra::TopicPartition, ...] }
#
# Partitions are always compared as (topic string, partition integer).

use strict;
use warnings;
use Carp qw(croak);
use Brahmaputra::TopicPartition;

use constant {
    RANGE      => 'range',
    ROUNDROBIN => 'roundrobin',
    # Keeps members on the partitions they already hold; prefer it when
    # consumers carry per-partition state.
    STICKY => 'sticky',
};

sub all { return (RANGE, ROUNDROBIN, STICKY) }

sub assign {
    my ($strategy, $members, $topic_partitions, $previous) = @_;
    return range($members, $topic_partitions) if $strategy eq RANGE;
    return round_robin($members, $topic_partitions) if $strategy eq ROUNDROBIN;
    return sticky($members, $topic_partitions, $previous || {}) if $strategy eq STICKY;
    croak "unknown partition.assignment.strategy $strategy";
}

sub _empty {
    my ($members) = @_;
    return { map { $_->{id} => [] } @$members };
}

sub _subscribes {
    my ($member, $topic) = @_;
    return scalar grep { $_ eq $topic } @{ $member->{topics} };
}

# Contiguous ranges per topic; the first (n % members) take one extra.
sub range {
    my ($members, $topic_partitions) = @_;
    my $assignment = _empty($members);
    for my $topic (sort keys %$topic_partitions) {
        my @partitions = @{ $topic_partitions->{$topic} };
        my @subscribers = sort map { $_->{id} } grep { _subscribes($_, $topic) } @$members;
        next unless @subscribers;
        my $base = int(@partitions / @subscribers);
        my $extra = @partitions % @subscribers;
        my $cursor = 0;
        for my $index (0 .. $#subscribers) {
            my $take = $base + ($index < $extra ? 1 : 0);
            for my $partition (@partitions[$cursor .. $cursor + $take - 1]) {
                push @{ $assignment->{ $subscribers[$index] } }, Brahmaputra::TopicPartition->new($topic, $partition);
            }
            $cursor += $take;
        }
    }
    return $assignment;
}

# Deal every partition around the circle of members sorted by id.
sub round_robin {
    my ($members, $topic_partitions) = @_;
    my $assignment = _empty($members);
    my @circle = sort { $a->{id} cmp $b->{id} } @$members;
    return $assignment unless @circle;
    my $cursor = 0;
    for my $topic (sort keys %$topic_partitions) {
        for my $partition (@{ $topic_partitions->{$topic} }) {
            my $start = $cursor;
            while (1) {
                my $member = $circle[ $cursor % @circle ];
                $cursor++;
                if (_subscribes($member, $topic)) {
                    push @{ $assignment->{ $member->{id} } }, Brahmaputra::TopicPartition->new($topic, $partition);
                    last;
                }
                last if $cursor - $start >= @circle;    # nobody subscribes
            }
        }
    }
    return $assignment;
}

# Keep members on what they hold; move only what balance requires.
sub sticky {
    my ($members, $topic_partitions, $previous) = @_;
    my $assignment = _empty($members);
    return $assignment unless @$members;
    my %by_id = map { $_->{id} => $_ } @$members;
    my $subscribes = sub {
        my ($id, $topic) = @_;
        return $by_id{$id} && _subscribes($by_id{$id}, $topic);
    };

    # Every partition that needs an owner, and who has a valid claim on it.
    my (@unassigned, @claimed);
    for my $topic (sort keys %$topic_partitions) {
        PARTITION: for my $partition (@{ $topic_partitions->{$topic} }) {
            my $tp = Brahmaputra::TopicPartition->new($topic, $partition);
            for my $id (sort keys %$previous) {
                for my $held (@{ $previous->{$id} }) {
                    if ($held->topic eq $topic && $held->partition == $partition && $subscribes->($id, $topic)) {
                        push @claimed, [$tp, $id];
                        next PARTITION;
                    }
                }
            }
            push @unassigned, $tp;
        }
    }

    # Fair share among members subscribed to at least one live topic.
    my @eligible = sort map { $_->{id} }
        grep { my $m = $_; grep { exists $topic_partitions->{$_} } @{ $m->{topics} } } @$members;
    return $assignment unless @eligible;
    my $total = 0;
    $total += @{ $topic_partitions->{$_} } for keys %$topic_partitions;
    my $base = int($total / @eligible);
    my $extra = $total % @eligible;
    my %quota;
    $quota{ $eligible[$_] } = $base + ($_ < $extra ? 1 : 0) for 0 .. $#eligible;

    # Honour claims up to quota; the overflow joins the pool.
    my %kept;
    for my $claim (@claimed) {
        my ($tp, $id) = @$claim;
        $kept{$id} ||= [];
        if (@{ $kept{$id} } < ($quota{$id} // 0)) {
            push @{ $kept{$id} }, $tp;
        } else {
            push @unassigned, $tp;
        }
    }
    for my $id (keys %kept) {
        $assignment->{$id} = $kept{$id} if exists $assignment->{$id};
    }

    for my $tp (sort { Brahmaputra::TopicPartition::compare($a, $b) } @unassigned) {
        my ($taker) = grep { $subscribes->($_, $tp->topic) && @{ $assignment->{$_} } < ($quota{$_} // 0) } @eligible;
        # Quotas exhausted (uneven subscriptions): an unassigned partition
        # is a stalled one, so fall back to any subscriber.
        ($taker) = grep { $subscribes->($_, $tp->topic) } @eligible unless defined $taker;
        push @{ $assignment->{$taker} }, $tp if defined $taker;
    }
    for my $id (keys %$assignment) {
        $assignment->{$id} = [sort { Brahmaputra::TopicPartition::compare($a, $b) } @{ $assignment->{$id} }];
    }
    return $assignment;
}

1;
