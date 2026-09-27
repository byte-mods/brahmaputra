package Brahmaputra::Reader;

# Reads a BitPacker response body. The encoding is positional, so a field
# this client ignores must still be *read* or everything after misaligns.
# Every length is bounds-checked against the bytes that remain.

use strict;
use warnings;
use Brahmaputra::Error;
use Brahmaputra::Protocol qw(SCHEMA_VERSION decode_uvarint unzigzag32 unzigzag64);

sub new {
    my ($class, $data) = @_;
    return bless { data => $data, pos => 0 }, $class;
}

# A reader positioned past the schema version, which is verified: decoding
# garbage into plausible fields is worse than failing loudly.
sub body {
    my ($class, $data) = @_;
    my $reader = $class->new($data);
    my $version = $reader->string;
    if ($version ne SCHEMA_VERSION) {
        Brahmaputra::Error::Protocol->throw(sprintf(
            'schema version mismatch: broker speaks %s, this client speaks %s', $version, SCHEMA_VERSION));
    }
    return $reader;
}

sub remaining { length($_[0]{data}) - $_[0]{pos} }

sub int32 {
    my ($self) = @_;
    return unzigzag32(decode_uvarint(\$self->{data}, \$self->{pos}));
}

sub int64 {
    my ($self) = @_;
    return unzigzag64(decode_uvarint(\$self->{data}, \$self->{pos}));
}

sub bool {
    my ($self) = @_;
    Brahmaputra::Error::Protocol->throw('truncated bool') if $self->{pos} >= length $self->{data};
    return substr($self->{data}, $self->{pos}++, 1) ne "\x00" ? 1 : 0;
}

sub string {
    my ($self) = @_;
    my $length = $self->int32;
    if ($length < 0 || $length > $self->remaining) {
        Brahmaputra::Error::Protocol->throw('truncated string');
    }
    my $value = substr($self->{data}, $self->{pos}, $length);
    $self->{pos} += $length;
    return $value;
}

# An array length, bounded by the bytes left so garbage cannot allocate
# gigabytes.
sub count {
    my ($self) = @_;
    my $count = $self->int32;
    if ($count < 0 || $count > $self->remaining) {
        Brahmaputra::Error::Protocol->throw("implausible array count $count");
    }
    return $count;
}

sub string_array {
    my ($self) = @_;
    my @out;
    push @out, $self->string for 1 .. $self->count;
    return \@out;
}

sub rest {
    my ($self) = @_;
    my $value = substr($self->{data}, $self->{pos});
    $self->{pos} = length $self->{data};
    return $value;
}

1;
