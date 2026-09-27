package Brahmaputra;

use strict;
use warnings;

our $VERSION = '0.1.0';

use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Protocol;
use Brahmaputra::Writer;
use Brahmaputra::Reader;
use Brahmaputra::Crc32c;
use Brahmaputra::Murmur2;
use Brahmaputra::Compression;
use Brahmaputra::RecordBatch;
use Brahmaputra::Connection;
use Brahmaputra::Router;
use Brahmaputra::Record;
use Brahmaputra::TopicPartition;
use Brahmaputra::DeliveryReport;
use Brahmaputra::Producer;
use Brahmaputra::Consumer;
use Brahmaputra::Assignor;
use Brahmaputra::GroupConsumer;

1;

__END__

=head1 NAME

Brahmaputra - pure-Perl client for the Brahmaputra log broker

=head1 SYNOPSIS

    use Brahmaputra;

    my $producer = Brahmaputra::Producer->new({
        'bootstrap.servers' => '127.0.0.1:9092',
        'acks'              => 'all',
        'linger.ms'         => 5,
    });
    $producer->send(topic => 'orders', key => 'user-7', value => '{"id":1}');
    $producer->close;

    my $consumer = Brahmaputra::Consumer->new({ 'bootstrap.servers' => '127.0.0.1:9092' });
    for my $record ($consumer->fetch('orders', 0, 0)) {
        printf "%d %s\n", $record->offset, $record->value;
    }

    my $group = Brahmaputra::GroupConsumer->new({
        'bootstrap.servers' => '127.0.0.1:9092',
        'group.id'          => 'billing',
    });
    $group->subscribe('orders');
    while (1) {
        handle($_) for $group->poll(500);
        $group->commit;
    }

=head1 DESCRIPTION

Speaks Brahmaputra's own wire protocol directly over TCP, using only core
modules. The client is single-threaded: producer batches are sent from
inside C<send>, C<poll>, C<flush> and C<close>, and group heartbeats run
inside C<poll>. See F<README.md> for the details and the configuration
reference.

=cut
