package Brahmaputra::Record;

# One record as a consumer sees it. value() undef is a tombstone and ''
# an empty value; key() and header values keep the same distinction.
# headers() is an array ref of [name, value] pairs, in order, possibly
# repeating. Keys, values and headers are byte strings.

use strict;
use warnings;

sub new {
    my ($class, %fields) = @_;
    $fields{headers} ||= [];
    return bless \%fields, $class;
}

sub topic     { $_[0]{topic} }
sub partition { $_[0]{partition} }
sub offset    { $_[0]{offset} }
sub key       { $_[0]{key} }
sub value     { $_[0]{value} }
sub timestamp { $_[0]{timestamp} }
sub headers   { $_[0]{headers} }

# The value of the first header named $name: undef when absent or null.
sub header {
    my ($self, $name) = @_;
    for my $header (@{ $self->{headers} }) {
        return $header->[1] if $header->[0] eq $name;
    }
    return undef;
}

1;
