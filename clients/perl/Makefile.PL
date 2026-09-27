use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Brahmaputra',
    AUTHOR           => 'Brahmaputra contributors',
    VERSION_FROM     => 'lib/Brahmaputra.pm',
    ABSTRACT         => 'Pure-Perl client for the Brahmaputra log broker',
    LICENSE          => 'apache_2',
    MIN_PERL_VERSION => '5.020',
    # Core modules only.
    PREREQ_PM => {
        'IO::Socket::IP'         => 0,
        'IO::Compress::Gzip'     => 0,
        'IO::Uncompress::Gunzip' => 0,
        'Time::HiRes'            => 0,
        'Digest::SHA'            => 0,
        'MIME::Base64'           => 0,
        'List::Util'             => 0,
        'Scalar::Util'           => 0,
    },
    TEST_REQUIRES => { 'Test::More' => 0 },
    # t/manual_test.pl needs a live broker; `make test` runs only t/*.t.
    test => { TESTS => 't/*.t' },
);
