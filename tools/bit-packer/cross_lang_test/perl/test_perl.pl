#!/usr/bin/env perl
# Cross-language conformance test for the BitPacker Perl target.
# Usage: perl -I<generated dir> test_perl.pl <cross_lang_test dir>
use strict;
use warnings;
use utf8;
use BenchComplex;
use Edge;
use F32;

my $dir = shift // '..';
my ($passed, $failed) = (0, 0);

sub check {
    my ($ok, $name, $detail) = @_;
    if ($ok) { $passed++; print "  ok   $name\n" }
    else     { $failed++; print "  FAIL $name (" . ($detail // '') . ")\n" }
}

# run a block; an exception is a failure with its message
sub expect {
    my ($name, $code) = @_;
    my $ok = eval { $code->() };
    if ($@) { check(0, $name, "died: $@") } else { check($ok, $name, 'mismatch') }
}

sub slurp {
    my ($path) = @_;
    open my $fh, '<:raw', $path or die "open $path: $!";
    local $/;
    return scalar <$fh>;
}

sub same_list {
    my ($got, $want) = @_;
    return 0 unless ref $got eq 'ARRAY' && @$got == @$want;
    for my $i (0 .. $#$want) { return 0 unless $got->[$i] == $want->[$i] }
    return 1;
}

sub same_strs {
    my ($got, $want) = @_;
    return 0 unless ref $got eq 'ARRAY' && @$got == @$want;
    for my $i (0 .. $#$want) { return 0 unless $got->[$i] eq $want->[$i] }
    return 1;
}

# ---------------- bench ----------------

sub build_world {
    my $hero = BenchComplex::Character->new(
        name => 'TestHero', level => 99, hp => 1000, mp => 500, is_alive => 1,
        position => BenchComplex::Vec3->new(x => 10, y => -20, z => 30),
        skills => [1, 2, 3, 100],
        inventory => [BenchComplex::Item->new(id => 1, name => 'Excalibur', value => 9999, weight => 15, rarity => 'Legendary')],
    );
    my $guild = BenchComplex::Guild->new(name => 'TestGuild', description => 'A test guild for cross-language', members => [$hero]);
    return BenchComplex::WorldState->new(
        world_id => 42, seed => 'cross_lang_test', guilds => [$guild],
        loot_table => [BenchComplex::Item->new(id => 2, name => 'HealthPotion', value => 50, weight => 1, rarity => 'Common')],
    );
}

sub verify_world {
    my ($d) = @_;
    return 0 unless $d->world_id == 42 && $d->seed eq 'cross_lang_test' && @{ $d->guilds } == 1;
    my $g = $d->guilds->[0];
    return 0 unless $g->name eq 'TestGuild' && $g->description eq 'A test guild for cross-language' && @{ $g->members } == 1;
    my $h = $g->members->[0];
    return 0 unless $h->name eq 'TestHero' && $h->level == 99 && $h->hp == 1000 && $h->mp == 500 && $h->is_alive;
    return 0 unless $h->position->x == 10 && $h->position->y == -20 && $h->position->z == 30;
    return 0 unless same_list($h->skills, [1, 2, 3, 100]) && @{ $h->inventory } == 1;
    my $s = $h->inventory->[0];
    return 0 unless $s->id == 1 && $s->name eq 'Excalibur' && $s->value == 9999 && $s->weight == 15 && $s->rarity eq 'Legendary';
    return 0 unless @{ $d->loot_table } == 1;
    my $p = $d->loot_table->[0];
    return $p->id == 2 && $p->name eq 'HealthPotion' && $p->value == 50 && $p->weight == 1 && $p->rarity eq 'Common';
}

my $ref = slurp("$dir/test_data.bin");
my $out = build_world()->encode;
{
    open my $fh, '>:raw', "$dir/test_data_perl.bin" or die $!;
    print $fh $out;
    close $fh or die $!;
}
check(!utf8::is_utf8($out), 'bench: encode returns bytes');
check($out eq $ref, 'bench: encode == test_data.bin', length($out) . ' vs ' . length($ref) . ' bytes');
my $decoded;
expect('bench: decode test_data.bin', sub { $decoded = BenchComplex::WorldState->decode($ref); 1 });
expect('bench: decoded fields', sub { verify_world($decoded) });
expect('bench: re-encode decoded == ref', sub { $decoded->encode eq $ref });
expect('bench: round-trip own encoding', sub { verify_world(BenchComplex::WorldState->decode($out)) });

# ---------------- edge ----------------

my $UNICODE = "h\x{e9}llo w\x{f6}rld \x{2713} \x{65e5}\x{672c} \x{1F680}";
my $IMIN = -2147483648;
my $IMAX = 2147483647;
my $LMIN = -9223372036854775808;
my $LMAX = 9223372036854775807;

sub build_edge {
    return Edge::Edge->new(
        i_min => $IMIN, i_max => $IMAX, i_zero => 0, i_neg => -1,
        l_min => $LMIN, l_max => $LMAX, l_neg => -300,
        f => -1.25, d => 1234.5625, d_neg => -0.5, yes => 1, no => 0,
        empty => '', unicode => $UNICODE,
        ints => [0, -1, 1, -64, 64, $IMIN, $IMAX],
        longs => [0, -1, $LMAX, $LMIN, 4294967296],
        floats => [0.0, 0.5, -2.25], doubles => [0.0, 3.5, -1000000.25],
        bools => [1, 0, 1], strings => ['', 'a', '日本語'], no_ints => [],
        inner => Edge::Inner->new(big => 1099511627776, label => 'inner'),
        inners => [Edge::Inner->new(big => -1, label => ''), Edge::Inner->new(big => 0, label => 'x')],
        no_inners => [],
    );
}

check($LMIN == -9223372036854775807 - 1 && "$LMIN" eq '-9223372036854775808', 'edge: perl has exact 64-bit IVs');

my $eref = slurp("$dir/edge/edge_ref.bin");
my $eout = build_edge()->encode;
check($eout eq $eref, 'edge: encode == edge_ref.bin', length($eout) . ' vs ' . length($eref) . ' bytes');
my $e;
expect('edge: decode edge_ref.bin', sub { $e = Edge::Edge->decode($eref); 1 });
if ($e) {
    my @checks = (
        [i_min => sub { $e->i_min == $IMIN && "$e->{i_min}" eq '-2147483648' }],
        [i_max => sub { $e->i_max == $IMAX }],
        [i_zero => sub { $e->i_zero == 0 }],
        [i_neg => sub { $e->i_neg == -1 }],
        [l_min => sub { "$e->{l_min}" eq '-9223372036854775808' }],
        [l_max => sub { "$e->{l_max}" eq '9223372036854775807' }],
        [l_neg => sub { $e->l_neg == -300 }],
        [f => sub { $e->f == -1.25 }],
        [d => sub { $e->d == 1234.5625 }],
        [d_neg => sub { $e->d_neg == -0.5 }],
        [yes => sub { $e->yes && $e->yes == 1 }],
        [no => sub { !$e->no }],
        [empty => sub { defined $e->empty && $e->empty eq '' }],
        [unicode => sub { $e->unicode eq $UNICODE && length($e->unicode) == 18 }],
        [ints => sub { same_list($e->ints, [0, -1, 1, -64, 64, $IMIN, $IMAX]) }],
        [longs => sub { same_strs($e->longs, ['0', '-1', '9223372036854775807', '-9223372036854775808', '4294967296']) }],
        [floats => sub { same_list($e->floats, [0.0, 0.5, -2.25]) }],
        [doubles => sub { same_list($e->doubles, [0.0, 3.5, -1000000.25]) }],
        [bools => sub { my $b = $e->bools; @$b == 3 && $b->[0] && !$b->[1] && $b->[2] }],
        [strings => sub { same_strs($e->strings, ['', 'a', '日本語']) }],
        [no_ints => sub { ref $e->no_ints eq 'ARRAY' && !@{ $e->no_ints } }],
        [inner => sub { $e->inner->big == 1099511627776 && $e->inner->label eq 'inner' }],
        [inners => sub { my $l = $e->inners; @$l == 2 && $l->[0]->big == -1 && $l->[0]->label eq '' && $l->[1]->big == 0 && $l->[1]->label eq 'x' }],
        [no_inners => sub { ref $e->no_inners eq 'ARRAY' && !@{ $e->no_inners } }],
    );
    expect("edge: field $_->[0]", $_->[1]) for @checks;
}
expect('edge: re-encode decoded == ref', sub { $e->encode eq $eref });

{
    my $bad = $eref;
    substr($bad, 1, 1) = '3';
    my $ok = eval { Edge::Edge->decode($bad); 1 };
    check(!$ok && $@ =~ /version/, 'edge: wrong version rejected', $ok ? 'decoded' : $@);
}

{
    my $first_bad;
    for my $n (0 .. length($eref) - 1) {
        my $ok = eval { Edge::Edge->decode(substr($eref, 0, $n)); 1 };
        if ($ok) { $first_bad = "prefix of $n bytes decoded"; last }
        if ($@ !~ /^Edge: /) { $first_bad = "prefix of $n bytes: unexpected error $@"; last }
    }
    check(!defined $first_bad, 'edge: every truncation rejected', $first_bad);
}

sub rejects {
    my $bytes = pack('C*', @_);
    return !eval { Edge::Edge->decode($bytes); 1 };
}
my @ver = (10, map { ord } split //, '2.1.0');
check(rejects(@ver, (0) x 17, 0xfe, 0xff, 0xff, 0xff, 0x0f), 'edge: huge array count rejected');
check(rejects(@ver, (0) x 12, 0xfe, (0xff) x 7, 0x7f, 97), 'edge: huge string length rejected');
check(rejects(@ver, (0) x 13, 1), 'edge: negative string length rejected');
check(rejects(@ver, (0xff) x 10, 1), 'edge: 11-byte varint rejected');
check(rejects(@ver, (0) x 12, 2, 0xff), 'edge: invalid UTF-8 rejected');
check(!eval { Edge::Edge->decode("\x{100}"); 1 }, 'edge: wide-character input rejected');

# wrapping of out-of-range integers, like the fixed-width targets
expect('edge: long UV 2**64-1 wraps to -1', sub { Edge::Inner->decode(Edge::Inner->new(big => 18446744073709551615)->encode)->big == -1 });
expect('edge: int wraps to 32 bits', sub { Edge::Edge->decode(Edge::Edge->new(i_min => 2147483648)->encode)->i_min == $IMIN });
expect('edge: unknown constructor field croaks', sub { !eval { Edge::Inner->new(bigg => 1); 1 } && $@ =~ /unknown field/ });

# ---------------- float32 ----------------
{
    my $ref = pack('H*', '0a312e302e30b06d06a82db06d808080a00100');
    my $f32 = sub { unpack('f', pack('f', $_[0])) };
    my $out = F32::F32->new(f => 0.7, fs => [0.29, 0.7, 16777.217])->encode;
    check($out eq $ref, 'f32: 0.7, [0.29, 0.7, 16777.217] -> 7000, [2900, 7000, 167772160]', unpack('H*', $out));
    expect('f32: decodes to the nearest float32 values', sub {
        my $v = F32::F32->decode($ref);
        $v->f == $f32->(0.7)
            && same_list($v->fs, [$f32->(0.29), $f32->(0.7), $f32->(16777.217)])
            && same_list($v->fs, [0.28999999165534973, 0.699999988079071, 16777.216796875]);
    });
}

print "perl: $passed passed, $failed failed\n";
exit($failed ? 1 : 0);
