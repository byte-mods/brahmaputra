package Brahmaputra::Writer;

# Builds a BitPacker request body: every integer a zigzag varint, every
# string and array a varint count then its contents. Methods chain.

use strict;
use warnings;
use Brahmaputra::Protocol qw(SCHEMA_VERSION encode_uvarint zigzag32 zigzag64 to_bytes);

sub new {
    my ($class) = @_;
    my $buffer = '';
    return bless \$buffer, $class;
}

# A writer already carrying the schema version every body starts with.
sub body {
    my ($class) = @_;
    return $class->new->string(SCHEMA_VERSION);
}

sub raw {
    my ($self, $bytes) = @_;
    $$self .= $bytes;
    return $self;
}

sub int32 {
    my ($self, $value) = @_;
    $$self .= encode_uvarint(zigzag32($value));
    return $self;
}

sub int64 {
    my ($self, $value) = @_;
    $$self .= encode_uvarint(zigzag64($value));
    return $self;
}

sub bool {
    my ($self, $value) = @_;
    $$self .= $value ? "\x01" : "\x00";
    return $self;
}

sub string {
    my ($self, $value) = @_;
    $value = to_bytes(defined $value ? $value : '');
    $self->int32(length $value);
    $$self .= $value;
    return $self;
}

sub string_array {
    my ($self, $values) = @_;
    $self->int32(scalar @$values);
    $self->string($_) for @$values;
    return $self;
}

sub bytes { ${ $_[0] } }

1;
