package Brahmaputra::ErrorCode;

# Broker error codes.

use strict;
use warnings;

my @NAMES = qw(
    NONE UNKNOWN_TOPIC_OR_PARTITION OFFSET_OUT_OF_RANGE INVALID_REQUEST
    UNSUPPORTED_VERSION INTERNAL NOT_LEADER_OR_FOLLOWER FENCED_BROKER_EPOCH
    FENCED_LEADER_EPOCH UNKNOWN_LEADER_EPOCH NOT_ENOUGH_REPLICAS
    FENCED_PRODUCER_EPOCH OUT_OF_ORDER_SEQUENCE UNKNOWN_MEMBER_ID
    REBALANCE_IN_PROGRESS NOT_COORDINATOR ILLEGAL_GENERATION
    COORDINATOR_LOAD_IN_PROGRESS SASL_AUTHENTICATION_FAILED AUTHORIZATION_FAILED
);

use constant {
    NONE                         => 0,
    UNKNOWN_TOPIC_OR_PARTITION   => 1,
    OFFSET_OUT_OF_RANGE          => 2,
    INVALID_REQUEST              => 3,
    UNSUPPORTED_VERSION          => 4,
    INTERNAL                     => 5,
    NOT_LEADER_OR_FOLLOWER       => 6,
    FENCED_BROKER_EPOCH          => 7,
    FENCED_LEADER_EPOCH          => 8,
    UNKNOWN_LEADER_EPOCH         => 9,
    NOT_ENOUGH_REPLICAS          => 10,
    FENCED_PRODUCER_EPOCH        => 11,
    OUT_OF_ORDER_SEQUENCE        => 12,
    UNKNOWN_MEMBER_ID            => 13,
    REBALANCE_IN_PROGRESS        => 14,
    NOT_COORDINATOR              => 15,
    ILLEGAL_GENERATION           => 16,
    COORDINATOR_LOAD_IN_PROGRESS => 17,
    SASL_AUTHENTICATION_FAILED   => 18,
    AUTHORIZATION_FAILED         => 19,
};

# Codes the broker only returns *before* it appends anything, so a retry
# cannot duplicate a record.
my %RETRIABLE = map { $_ => 1 } (
    NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH,
    NOT_ENOUGH_REPLICAS, COORDINATOR_LOAD_IN_PROGRESS, INTERNAL,
);

sub is_retriable { return $RETRIABLE{ $_[0] } ? 1 : 0 }

# True when the error means the cached leader route is stale.
sub is_stale_route {
    my $code = shift;
    return ($code == NOT_LEADER_OR_FOLLOWER || $code == FENCED_LEADER_EPOCH || $code == UNKNOWN_LEADER_EPOCH) ? 1 : 0;
}

sub name {
    my $code = shift;
    return (defined $code && $code >= 0 && $code < @NAMES) ? $NAMES[$code] : 'UNKNOWN';
}

1;
