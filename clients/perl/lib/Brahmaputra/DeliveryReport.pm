package Brahmaputra::DeliveryReport;

# The outcome of one produced batch, handed to `delivery.report.callback`.
# base_offset is -1 when acks=0 (the broker does not answer) or on error.

use strict;
use warnings;

sub new {
    my ($class, %fields) = @_;
    return bless \%fields, $class;
}

sub topic        { $_[0]{topic} }
sub partition    { $_[0]{partition} }
sub base_offset  { $_[0]{base_offset} }
sub record_count { $_[0]{record_count} }
sub error        { $_[0]{error} }
sub ok           { !defined $_[0]{error} }

1;
