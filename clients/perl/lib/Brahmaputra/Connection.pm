package Brahmaputra::Connection;

# One TCP connection to one broker.
#
# Single-threaded: a request is written and its response read before the
# call returns, so there is at most one request in flight per connection.
# The socket is non-blocking and every wait goes through select() against
# a deadline, so no request can block forever: each round trip is bounded
# by the request timeout (120 s unless set_request_timeout or a per-call
# timeout says otherwise).
#
# Any I/O failure, timeout or correlation-id mismatch leaves the byte
# stream at an unknown position -- a partial frame may have been written,
# or a late response may still arrive -- so the connection is closed and
# marked broken() rather than reused. The Router notices and redials.

use strict;
use warnings;
use Errno ();
use IO::Socket::IP;
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use MIME::Base64 qw(encode_base64 decode_base64);
use Digest::SHA qw(sha256 hmac_sha256);
use Brahmaputra::Error;
use Brahmaputra::ErrorCode;
use Brahmaputra::Config;
use Brahmaputra::Protocol qw(encode_frame decode_frame_payload API_API_VERSIONS API_AUTHENTICATE);
use Brahmaputra::Writer;
use Brahmaputra::Reader;

use constant {
    # Bounds one request/response round trip. It must exceed the longest
    # the broker may legitimately hold a request (a fetch long-poll, an
    # acks=all wait, a JoinGroup waiting out a rebalance), so it is
    # generous; its job is to turn a wedged broker into an error.
    DEFAULT_REQUEST_TIMEOUT_MS => 120_000,
    DEFAULT_CONNECT_TIMEOUT_MS => 10_000,
    MAX_FRAME                  => 512 * 1024 * 1024,
};

# Brahmaputra::Connection->open(host => ..., port => ..., client_id => ...,
#     connect_timeout_ms => ..., request_timeout_ms => ...)
sub open {
    my ($class, %args) = @_;
    my $host = $args{host};
    my $port = $args{port};
    my $connect_ms = $args{connect_timeout_ms} // DEFAULT_CONNECT_TIMEOUT_MS;
    my $socket = IO::Socket::IP->new(
        PeerHost => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => $connect_ms / 1000,
    );
    unless ($socket) {
        my $reason = $IO::Socket::errstr || $@ || $! || 'unknown error';
        Brahmaputra::Error::Connection->throw("connect to $host:$port failed: $reason");
    }
    # Responses are small and latency matters more than packet count;
    # without this every request pays Nagle plus the peer's delayed ACK.
    setsockopt($socket, IPPROTO_TCP, TCP_NODELAY, 1);
    $socket->blocking(0);
    return bless {
        socket     => $socket,
        host       => $host,
        port       => $port,
        client_id  => $args{client_id} // 'brahmaputra-perl',
        timeout_ms => $args{request_timeout_ms} // DEFAULT_REQUEST_TIMEOUT_MS,
        next       => 0,
        broken     => 0,
    }, $class;
}

sub host { $_[0]{host} }
sub port { $_[0]{port} }
sub address { "$_[0]{host}:$_[0]{port}" }

# True once an I/O failure, timeout or close() has retired this connection.
# A broken connection is never reused: the Router dials a fresh one.
sub broken { $_[0]{broken} }

# Change the default bound on one round trip (milliseconds). Zero or
# negative disables it.
sub set_request_timeout {
    my ($self, $ms) = @_;
    $self->{timeout_ms} = $ms;
    return;
}

sub request_timeout { $_[0]{timeout_ms} }

sub close {
    my ($self) = @_;
    $self->{broken} = 1;
    if (my $socket = delete $self->{socket}) {
        CORE::close($socket);
    }
    return;
}

sub DESTROY { $_[0]->close }

# Send one request and return the matching response body. $timeout_ms
# bounds the round trip; it defaults to the connection's request timeout.
sub request {
    my ($self, $api_key, $body, $timeout_ms) = @_;
    $self->_require_open;
    my $correlation_id = $self->_next_correlation;
    my $deadline = $self->_deadline($timeout_ms);
    $self->_write_all(encode_frame($api_key, $correlation_id, $self->{client_id}, $body), $deadline);
    my $payload = $self->_read_frame($deadline);
    my ($got, $response) = eval { decode_frame_payload($payload) };
    unless (defined $got) {
        my $error = $@;
        $self->close;
        die $error;
    }
    if ($got != $correlation_id) {
        # A response to a request we are not waiting on means the stream has
        # desynchronised; carrying on would pair every later response with
        # the wrong request.
        $self->close;
        Brahmaputra::Error::Protocol->throw(
            "correlation id mismatch from $self->{host}:$self->{port}: expected $correlation_id, got $got");
    }
    return $response;
}

# Send without waiting for a response (acks=0).
sub send_oneway {
    my ($self, $api_key, $body) = @_;
    $self->_require_open;
    $self->_write_all(encode_frame($api_key, $self->_next_correlation, $self->{client_id}, $body),
        $self->_deadline(undef));
    return;
}

sub _require_open {
    my ($self) = @_;
    if ($self->{broken} || !$self->{socket}) {
        Brahmaputra::Error::Connection->throw(
            "connection to $self->{host}:$self->{port} is broken; the router will redial");
    }
}

sub _deadline {
    my ($self, $timeout_ms) = @_;
    $timeout_ms //= $self->{timeout_ms};
    return undef if !defined $timeout_ms || $timeout_ms <= 0;
    return Brahmaputra::Config::now_ms() + $timeout_ms;
}

sub _next_correlation {
    my ($self) = @_;
    $self->{next} = ($self->{next} + 1) & 0x7fffffff;
    return $self->{next};
}

# Wait until the socket is ready; false on deadline.
sub _wait {
    my ($self, $for_write, $deadline) = @_;
    while (1) {
        my $timeout;
        if (defined $deadline) {
            my $remaining = $deadline - Brahmaputra::Config::now_ms();
            return 0 if $remaining <= 0;
            $timeout = $remaining / 1000;
        }
        my $bits = '';
        vec($bits, fileno($self->{socket}), 1) = 1;
        my ($read, $write) = $for_write ? (undef, $bits) : ($bits, undef);
        my $ready = select($read, $write, undef, $timeout);
        return 1 if $ready > 0;
        next if $ready < 0 && $!{EINTR};
        if ($ready < 0) {
            my $reason = "$!";
            $self->close;
            Brahmaputra::Error::Connection->throw("select on $self->{host}:$self->{port} failed: $reason");
        }
    }
}

sub _timed_out {
    my ($self, $what) = @_;
    $self->close;
    Brahmaputra::Error::Timeout->throw("$what $self->{host}:$self->{port} timed out");
}

sub _write_all {
    my ($self, $frame, $deadline) = @_;
    # A peer that closed its end raises SIGPIPE on write; turn it into EPIPE.
    local $SIG{PIPE} = 'IGNORE';
    my $length = length $frame;
    my $offset = 0;
    while ($offset < $length) {
        $self->_wait(1, $deadline) or $self->_timed_out('write to');
        my $written = syswrite($self->{socket}, $frame, $length - $offset, $offset);
        if (!defined $written) {
            next if $!{EAGAIN} || $!{EWOULDBLOCK} || $!{EINTR};
            my $reason = "$!";
            $self->close;
            Brahmaputra::Error::Connection->throw("write to $self->{host}:$self->{port} failed: $reason");
        }
        $offset += $written;
    }
    return;
}

sub _read_exact {
    my ($self, $length, $deadline) = @_;
    my $buffer = '';
    while (length($buffer) < $length) {
        $self->_wait(0, $deadline) or $self->_timed_out('request to');
        my $n = sysread($self->{socket}, $buffer, $length - length($buffer), length $buffer);
        if (!defined $n) {
            next if $!{EAGAIN} || $!{EWOULDBLOCK} || $!{EINTR};
            my $reason = "$!";
            $self->close;
            Brahmaputra::Error::Connection->throw("read from $self->{host}:$self->{port} failed: $reason");
        }
        if ($n == 0) {
            $self->close;
            Brahmaputra::Error::Connection->throw("connection to $self->{host}:$self->{port} closed by broker");
        }
    }
    return $buffer;
}

sub _read_frame {
    my ($self, $deadline) = @_;
    my $length = unpack('l>', $self->_read_exact(4, $deadline));
    if ($length < 0 || $length > MAX_FRAME) {
        $self->close;
        Brahmaputra::Error::Protocol->throw("implausible frame length $length");
    }
    return $self->_read_exact($length, $deadline);
}

# Ask the broker what it speaks. Returns
# { versions => [{ api_key, min_version, max_version }, ...], broker_version }.
sub api_versions {
    my ($self) = @_;
    my $body = Brahmaputra::Writer->body->string('brahmaputra-perl')->string($Brahmaputra::VERSION // '0.1.0')->bytes;
    my $reader = Brahmaputra::Reader->body($self->request(API_API_VERSIONS, $body));
    my $code = $reader->int32;
    Brahmaputra::Error::Server->throw($code, 'api_versions') if $code != Brahmaputra::ErrorCode::NONE;
    my @versions;
    for (1 .. $reader->count) {
        push @versions, { api_key => $reader->int32, min_version => $reader->int32, max_version => $reader->int32 };
    }
    return { versions => \@versions, broker_version => $reader->string };
}

# Bind a principal to this connection with SCRAM-SHA-256 (RFC 5802). The
# password never crosses the wire, only a proof derived from it. The
# broker refuses credentials on a plaintext listener, so this is usable
# only once TLS lands. Returns { principal, role }.
sub authenticate {
    my ($self, $username, $password) = @_;
    my $client_nonce = encode_base64(join('', map { chr int rand 256 } 1 .. 18), '');
    $client_nonce =~ tr/,/./;
    my $bare = "n=$username,r=$client_nonce";
    my $first = $self->_authenticate_step($username, '', 'SCRAM-SHA-256', "n,,$bare");
    Brahmaputra::Error::Protocol->throw('broker ended the SCRAM exchange before it began') if $first->{done};
    my $server_first = $first->{payload};
    my %field = map { /^(\w)=(.*)$/s ? ($1 => $2) : () } split /,/, $server_first;
    my ($nonce, $salt, $iterations) = @field{qw(r s i)};
    if (!defined $nonce || !defined $salt || !$iterations || $iterations <= 0) {
        Brahmaputra::Error::Protocol->throw('malformed SCRAM server-first message');
    }
    # The server must extend this client's nonce, which is what makes the
    # exchange this one rather than a replay.
    if (index($nonce, $client_nonce) != 0) {
        Brahmaputra::Error::Protocol->throw('SCRAM server nonce does not extend the client nonce');
    }
    my $without_proof = "c=biws,r=$nonce";
    my $auth_message = "$bare,$server_first,$without_proof";
    my $salted = _pbkdf2_sha256($password, decode_base64($salt), $iterations);
    my $client_key = hmac_sha256('Client Key', $salted);
    my $signature = hmac_sha256($auth_message, sha256($client_key));
    my $proof = encode_base64($client_key ^ $signature, '');
    my $final = $self->_authenticate_step($username, '', 'SCRAM-SHA-256', "$without_proof,p=$proof");
    return { principal => $final->{principal}, role => $final->{role} };
}

# SASL/PLAIN: sends the password itself; refused on a plaintext listener.
sub authenticate_plain {
    my ($self, $username, $password) = @_;
    my $result = $self->_authenticate_step($username, $password, 'PLAIN', '');
    return { principal => $result->{principal}, role => $result->{role} };
}

sub _authenticate_step {
    my ($self, $username, $password, $mechanism, $payload) = @_;
    my $body = Brahmaputra::Writer->body->string($username)->string($password)
        ->string($mechanism)->string($payload)->bytes;
    my $reader = Brahmaputra::Reader->body($self->request(API_AUTHENTICATE, $body));
    my $code = $reader->int32;
    my %result = (principal => $reader->string, role => $reader->string, payload => $reader->string, done => $reader->bool);
    Brahmaputra::Error::Server->throw($code, 'authenticate') if $code != Brahmaputra::ErrorCode::NONE;
    return \%result;
}

# PBKDF2-HMAC-SHA256 with a 32-byte output: exactly one block.
sub _pbkdf2_sha256 {
    my ($password, $salt, $iterations) = @_;
    my $u = hmac_sha256($salt . pack('N', 1), $password);
    my $out = $u;
    for (2 .. $iterations) {
        $u = hmac_sha256($u, $password);
        $out ^= $u;
    }
    return $out;
}

1;
