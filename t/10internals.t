#!/usr/bin/perl -w
use strict;

#----------------------------------------------------------------------------
# TODO List

# 1. add regenrate tests
# 2. mock AWS connection and return pre-prepared report data

#----------------------------------------------------------------------------
# Libraries

use Config::IniFiles;
use CPAN::Testers::Common::DBUtils;
use CPAN::Testers::Data::Generator;
use CPAN::Testers::Metabase::AWS;
#use Data::Dumper;
use File::Path;
use IO::File;
use JSON;
use Test::More tests => 23;

#----------------------------------------------------------------------------
# Test Variables

my (%options,$meta);
my $config = 't/_DBDIR/test-config.ini';

#----------------------------------------------------------------------------
# Test Data

my @test_stat_rows = (
[ '1', '2', 'a58945f6-3510-11df-89c9-1bb9c3681c0d', 'pass', '201003', 'chris@bingosnet.co.uk', 'Sub-Exporter-ForMethods', '0.100050', 'i686-linux-thread-multi-64int', '5.8.8', 'Linux', '2.6.28-11-generic', '201003211739' ],
[ '2', '2', 'ad3189d0-3510-11df-89c9-1bb9c3681c0d', 'pass', '201003', 'chris@bingosnet.co.uk', 'Algorithm-Diff', '1.1902', 'i686-linux-thread-multi-64int', '5.8.8', 'Linux', '2.6.28-11-generic', '201003211739' ],
[ '3', '2', 'af820e12-3510-11df-89c9-1bb9c3681c0d', 'pass', '201003', 'chris@bingosnet.co.uk', 'Text-Diff', '1.37', 'i686-linux-thread-multi-64int', '5.8.8', 'Linux', '2.6.28-11-generic', '201003211739' ],
[ '4', '2', 'b248f71e-3510-11df-89c9-1bb9c3681c0d', 'pass', '201003', 'chris@bingosnet.co.uk', 'Test-Differences', '0.500', 'i686-linux-thread-multi-64int', '5.8.8', 'Linux', '2.6.28-11-generic', '201003211739' ],
[ '5', '2', 'b77e7132-3510-11df-89c9-1bb9c3681c0d', 'pass', '201003', 'chris@bingosnet.co.uk', 'namespace-autoclean', '0.09', 'i686-linux-thread-multi-64int', '5.8.8', 'Linux', '2.6.28-11-generic', '201003211739' ]
);

my @test_meta_rows = (
[ 1, 'a58945f6-3510-11df-89c9-1bb9c3681c0d', '2010-03-21T17:39:05Z' ],
[ 2, 'ad3189d0-3510-11df-89c9-1bb9c3681c0d', '2010-03-21T17:39:18Z' ],
[ 3, 'af820e12-3510-11df-89c9-1bb9c3681c0d', '2010-03-21T17:39:22Z' ],
[ 4, 'b248f71e-3510-11df-89c9-1bb9c3681c0d', '2010-03-21T17:39:27Z' ],
[ 5, 'b77e7132-3510-11df-89c9-1bb9c3681c0d', '2010-03-21T17:39:35Z' ]
);

#----------------------------------------------------------------------------
# Test Main

# TEST INTERNALS

SKIP: {
    skip "Test::Database required for DB testing", 21 unless(-f $config);

    # prep test directory
    my $directory = './test';
    rmtree($directory);
    mkpath($directory) or die "cannot create directory";

    testCpanstatsRecords();
    testMetabaseRecords();

    my $c1 = getMetabaseCount();
    is($c1,5,'Internal Tests, metabase contains 5 reports');

    my $t;
    eval {
        $t = CPAN::Testers::Data::Generator->new(
            config      => $config,
            logfile     => $directory . '/cpanstats.log',
            localonly   => 1
        );
    };

    isa_ok($t,'CPAN::Testers::Data::Generator');

    #diag(Dumper($@))    if($@);

    my @test_dates = (
        [ undef, '', '' ],
        [ undef, 'xxx', '' ],
        [ undef, '', 'xxx' ],
        [ '2000-01-01T00:00:00Z', '', '2000-01-01T00:00:00Z' ],
        [ '2010-09-13T03:20:00Z', undef, '2010-09-13T03:20:00Z' ],
        [ '2010-03-21T17:39:05Z', 'a58945f6-3510-11df-89c9-1bb9c3681c0d', '' ],
    );

    for my $test (@test_dates) {
        is($t->_get_createdate($test->[1],$test->[2]),$test->[0], ".. test date [".($test->[0]||'undef')."]"); 
    }

    is($t->already_saved('a58945f7-3510-11df-89c9-1bb9c3681c0d'),0,'.. missing metabase guid');
    is($t->already_saved('a58945f6-3510-11df-89c9-1bb9c3681c0d'),'2010-03-21T17:39:05Z','.. found metabase guid');

    is($t->retrieve_report('a58945f7-3510-11df-89c9-1bb9c3681c0d'),undef,'.. missing cpanstats guid');
    my $r = $t->retrieve_report('a58945f6-3510-11df-89c9-1bb9c3681c0d');
    is($r->{guid},'a58945f6-3510-11df-89c9-1bb9c3681c0d','.. found cpanstats guid');

    $options{CPANSTATS} ||= config_db('CPANSTATS');
    my @rows = $options{CPANSTATS}->{dbh}->get_query('array','SELECT count(id) FROM osname');
    is($rows[0]->[0],25,'.. all OS names');

    is($t->_platform_to_osname('linux'),'linux',        '.. known OS');
    is($t->_platform_to_osname('linuxThis'),'linux',    '.. known mispelling');
    is($t->_platform_to_osname('unknown'),'',           '.. unknown OS');

    is($t->_osname('LINUX'),'Linux',                    '.. known OS fixed case');
    is($t->_osname('Unknown'),'UNKNOWN',                '.. save unknown OS');
    is($t->_platform_to_osname('unknown'),'unknown',    '.. unknown is now known OS');

    my $json;
    my $fh = IO::File->new("t/data/ad3189d0-3510-11df-89c9-1bb9c3681c0d.json") or die diag("$!");
    while(<$fh>) { $json .= $_ }
    $fh->close;

    my $text = decode_json($json);
    $t->{report}{metabase} = $text;
    $t->_check_arch_os();
    is($t->{report}{osname},'linux','.. set OS');
    is($t->{report}{platform},'i686-linux-thread-multi-64int','.. set platform');
}

#----------------------------------------------------------------------------
# Test Functions

sub config_db {
    # Loads Test::Database instances

    my $db = shift;

    # load config file
    my $cfg = Config::IniFiles->new( -file => $config );

    # configure databases
    die "No configuration for $db database\n"   unless($cfg->SectionExists($db));
    my %opts = map {$_ => ($cfg->val($db,$_)||undef);} qw(driver database dbfile dbhost dbport dbuser dbpass);
    unlink $opts{database}  if($opts{driver} eq 'SQLite' && -f $opts{database});

    # need to store new configuration details here

    my $dbh = CPAN::Testers::Common::DBUtils->new(%opts);
    die "Cannot configure $db database\n" unless($dbh);

    my %hash = ( opts => \%opts, dbh => $dbh );
    return \%hash;
}

sub testCpanstatsRecords {
    $options{CPANSTATS} ||= config_db('CPANSTATS');
    my @rows = $options{CPANSTATS}->{dbh}->get_query('array','SELECT * FROM cpanstats');
    is_deeply(\@rows,\@test_stat_rows,'.. test cpanstats rows');
}

sub testMetabaseRecords {
    $options{METABASE} ||= config_db('METABASE');
    my @rows = $options{METABASE}->{dbh}->get_query('array','SELECT id,guid,updated FROM metabase');
    is_deeply(\@rows,\@test_meta_rows,'.. test metabase rows');
}

sub getMetabaseCount {
    $options{METABASE} ||= config_db('METABASE');
    my @rows = $options{METABASE}->{dbh}->get_query('array','SELECT count(id) FROM metabase');
    return 0	unless(@rows);
    return $rows[0]->[0] || 0;
}

sub deleteMetabase {
    my $id = shift;
    $options{METABASE} ||= config_db('METABASE');
    my @rows = $options{METABASE}->{dbh}->get_query('array','SELECT * FROM metabase WHERE id = ?',$id);
    $options{METABASE}->{dbh}->do_query('DELETE FROM metabase WHERE id = ?',$id)    if(@rows);
}

sub countRequests {
    $options{CPANSTATS} ||= config_db('CPANSTATS');
    my @rows = $options{CPANSTATS}->{dbh}->get_query('array','SELECT * FROM page_requests');
#    diag(Dumper($_))    for(@rows);
    return scalar(@rows);
}

sub countSummaries {
    $options{CPANSTATS} ||= config_db('CPANSTATS');
    my @rows = $options{CPANSTATS}->{dbh}->get_query('array','SELECT * FROM release_summary');
#    diag(Dumper($_))    for(@rows);
    return scalar(@rows);
}

sub countReleases {
    $options{CPANSTATS} ||= config_db('CPANSTATS');
    my @rows = $options{CPANSTATS}->{dbh}->get_query('array','SELECT * FROM release_data');
#    diag(Dumper($_))    for(@rows);
    return scalar(@rows);
}
