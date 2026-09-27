package Brahmaputra::TopicPartition;

# A topic partition, optionally carrying an offset (as committed() returns it).

use strict;
use warnings;
use overload '""' => sub { "$_[0]{topic}-$_[0]{partition}" }, fallback => 1;

sub new {
    my ($class, $topic, $partition, $offset) = @_;
    return bless { topic => $topic, partition => 0 + $partition, offset => $offset }, $class;
}

sub topic     { $_[0]{topic} }
sub partition { $_[0]{partition} }
sub offset    { $_[0]{offset} }

# A hash key for this partition.
sub key { "$_[0]{topic}\0$_[0]{partition}" }

# Orders by topic, then partition *number* (never as strings) -- the Rust
# assignors' (String, i32) tuple order.
sub compare {
    my ($x, $y) = @_;
    return ($x->{topic} cmp $y->{topic}) || ($x->{partition} <=> $y->{partition});
}

1;
