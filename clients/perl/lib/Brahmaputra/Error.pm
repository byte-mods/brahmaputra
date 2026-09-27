package Brahmaputra::Error;

# Exception classes. Every error this driver raises is a blessed object
# that stringifies to its message, so `die`/`eval` and `$@ =~ /.../` both
# work, and `$@->isa('Brahmaputra::Error::Server')` tells the kinds apart.

use strict;
use warnings;

use overload
    '""'     => sub { $_[0]->message },
    'bool'   => sub { 1 },
    fallback => 1;

sub new {
    my ($class, $message, %fields) = @_;
    return bless { message => $message, %fields }, $class;
}

sub throw {
    my $class = shift;
    die $class->new(@_);
}

sub message { $_[0]{message} }

# The error this one wraps, if any (for example the first failure behind
# "3 batches failed to deliver").
sub cause { $_[0]{cause} }

package Brahmaputra::Error::Server;
# The broker answered with a non-zero error code (->code).
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error');

sub new {
    my ($class, $code, $context) = @_;
    require Brahmaputra::ErrorCode;
    my $name = Brahmaputra::ErrorCode::name($code);
    my $message = "broker returned ${name}[$code]" . (defined $context && length $context ? " ($context)" : '');
    return bless { message => $message, code => $code }, $class;
}

sub code { $_[0]{code} }

sub is_retriable {
    require Brahmaputra::ErrorCode;
    return Brahmaputra::ErrorCode::is_retriable($_[0]{code});
}

package Brahmaputra::Error::Connection;
# A socket could not be opened, or failed mid-request.
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error');

package Brahmaputra::Error::Timeout;
# A round trip (or delivery.timeout.ms) ran out. A kind of connection error.
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error::Connection');

package Brahmaputra::Error::Protocol;
# Bytes from the broker that do not decode.
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error');

package Brahmaputra::Error::BufferFull;
# buffer.memory stayed full for max.block.ms.
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error');

package Brahmaputra::Error::NoOffset;
# auto.offset.reset=none and a partition has no committed offset.
use strict;
use warnings;
our @ISA = ('Brahmaputra::Error');

1;
