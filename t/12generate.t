#!/usr/bin/perl -w
use strict;

use Config::IniFiles;
use CPAN::Testers::Common::DBUtils;
use CPAN::Testers::Data::Generator;
use CPAN::Testers::Metabase::AWS;
use Data::Dumper;
use File::Path;
use IO::File;
use Test::More;

#----------------------------------------------------------------------------
# Test Variables

my (%options,$meta);
my $config = './t/test-config.ini';
my $TESTS = 43;

#----------------------------------------------------------------------------
# Test Conditions

BEGIN {
    my $meta = CPAN::Testers::Metabase::AWS->new(
        bucket      => 'cpantesters',
        namespace   => 'beta2',
    );

    if($meta) {
        # check whether tester has a valid access key
        $meta = undef   unless($meta->access_key_id());
    }
}

#----------------------------------------------------------------------------
# Test Data

my @create_sqlite = (
            'PRAGMA auto_vacuum = 1',
            'CREATE TABLE cpanstats (
                id          INTEGER PRIMARY KEY,
                type        INTEGER,
                guid        TEXT,
                state       TEXT,
                postdate    TEXT,
                tester      TEXT,
                dist        TEXT,
                version     TEXT,
                platform    TEXT,
                perl        TEXT,
                osname      TEXT,
                osvers      TEXT,
                fulldate    TEXT,
                date        TEXT)',

            'CREATE INDEX distverstate ON cpanstats (dist, version, state)',
            'CREATE INDEX ixperl ON cpanstats (perl)',
            'CREATE INDEX ixplat ON cpanstats (platform)',
            'CREATE INDEX ixdate ON cpanstats (postdate)',

            'CREATE TABLE release_data (
                dist        TEXT,
                version     TEXT,
                id          INTEGER PRIMARY KEY,
                guid        TEXT,
                oncpan      INTEGER,
                distmat     INTEGER,
                perlmat     INTEGER,
                patched     INTEGER,
                pass        INTEGER,
                fail        INTEGER,
                na          INTEGER,
                unknown     INTEGER
            )',

            'CREATE INDEX reldist ON release_data (dist,version)',
            'CREATE INDEX relguid ON release_data (guid)',

            'CREATE TABLE release_summary (
                dist        TEXT,
                version     TEXT,
                id          INTEGER,
                oncpan      INTEGER,
                distmat     INTEGER,
                perlmat     INTEGER,
                patched     INTEGER,
                pass        INTEGER,
                fail        INTEGER,
                na          INTEGER,
                unknown     INTEGER
            )',

            'CREATE TABLE uploads (
                type        TEXT,
                author      TEXT,
                dist        TEXT,
                version     TEXT,
                filename    TEXT,
                released    INTEGER
            )',

            'CREATE INDEX uploaded ON uploads (author, dist, version)',

            'CREATE TABLE ixlatest (
                dist        TEXT PRIMARY KEY,
                version     TEXT,
                released    INTEGER,
                author      TEXT
            )',

            'CREATE TABLE page_requests (
                type        TEXT,
                name        TEXT,
                weight      INTEGER
            )',

            'CREATE TABLE osname (
                id          INTEGER PRIMARY KEY,
                osname      TEXT,
                ostitle     TEXT
            )',

            "INSERT INTO osname VALUES (1,'linux','Linux')"
);

my @create_mysql = (
            'DROP TABLE IF EXISTS cpanstats',
            'CREATE TABLE cpanstats (
                id         int(10) unsigned NOT NULL,
                type       tinyint(4) default 0,
                guid       varchar(32),
                state      varchar(32),
                postdate   varchar(8),
                tester     varchar(255),
                dist       varchar(255),
                version    varchar(255),
                platform   varchar(255),
                perl       varchar(255),
                osname     varchar(255),
                osvers     varchar(255),
                fulldate   varchar(32),
                PRIMARY KEY (id))',

            'DROP TABLE IF EXISTS page_requests',
            'CREATE TABLE page_requests (
                type        varchar(8)   NOT NULL,
                name        varchar(255) NOT NULL,
                weight      int(2) unsigned NOT NULL
            )',

            'DROP TABLE IF EXISTS release_data',
            'CREATE TABLE release_data (
                dist        varchar(255) NOT NULL,
                version     varchar(255) NOT NULL,
                id          int(10) unsigned NOT NULL,
                guid        char(36) NOT NULL,
                oncpan      tinyint(4) default 0,
                distmat     tinyint(4) default 0,
                perlmat     tinyint(4) default 0,
                patched     tinyint(4) default 0,
                pass        int(10) default 0,
                fail        int(10) default 0,
                na          int(10) default 0,
                unknown     int(10) default 0,
                PRIMARY KEY (id,guid),
                INDEX (dist,version)
            )',

            'DROP TABLE IF EXISTS release_summary',
            'CREATE TABLE release_summary (
                dist        varchar(255) NOT NULL,
                version     varchar(255) NOT NULL,
                id          int(10) unsigned NOT NULL,
                oncpan      tinyint(4) default 0,
                distmat     tinyint(4) default 0,
                perlmat     tinyint(4) default 0,
                patched     tinyint(4) default 0,
                pass        int(10)    default 0,
                fail        int(10)    default 0,
                na          int(10)    default 0,
                unknown     int(10)    default 0
            )',

            'DROP TABLE IF EXISTS uploads',
            'CREATE TABLE uploads (
                type        varchar(10)  NOT NULL,
                author      varchar(32)  NOT NULL,
                dist        varchar(100) NOT NULL,
                version     varchar(100) NOT NULL,
                filename    varchar(255) NOT NULL,
                released    int(16)	     NOT NULL,
                PRIMARY KEY (author,dist,version)
            )',

            'DROP TABLE IF EXISTS ixlatest',
            'CREATE TABLE ixlatest (
                dist        varchar(100) NOT NULL,
                version     varchar(100) NOT NULL,
                released    int(16)		 NOT NULL,
                author      varchar(32)  NOT NULL,
                PRIMARY KEY (dist)
            )',

            'DROP TABLE IF EXISTS osname',
            'CREATE TABLE osname (
                id          int(10) unsigned NOT NULL auto_increment,
                osname      varchar(255) NOT NULL,
                ostitle     varchar(255) NOT NULL,
                PRIMARY KEY (id)
            )',

            "INSERT INTO osname VALUES (1,'linux','Linux')"
);

my @create_meta_sqlite = (
            'PRAGMA auto_vacuum = 1',
            'CREATE TABLE metabase (
                id          INTEGER PRIMARY KEY,
                guid        INTEGER,
                report      TEXT)',
            'CREATE INDEX guid ON metabase (guid)',

            'CREATE TABLE `testers_email` (
                id          INTEGER PRIMARY KEY,
                resource    TEXT,
                fullname    TEXT,
                email       TEXT
            )',
            'CREATE INDEX resource ON testers_email (resource)'
);

my @create_meta_mysql = (
            'CREATE TABLE metabase (
                id          int(10) unsigned NOT NULL,
                guid        char(36) NOT NULL,
                report      blob,
                PRIMARY KEY (id),
                INDEX guid (guid)
            )',

            'CREATE TABLE `testers_email` (
              id            int(10) unsigned NOT NULL auto_increment,
              resource      varchar(64) NOT NULL,
              fullname      varchar(255) NOT NULL,
              email         varchar(255) default NULL,
              PRIMARY KEY  (id),
              KEY resource (resource)
            )'
);

my @delete_sqlite = (
            'DELETE FROM cpanstats',
            'DELETE FROM page_requests',
            'DELETE FROM release_summary',
            'DELETE FROM uploads',
            'DELETE FROM ixlatest'
);

my @delete_mysql = (
            'DELETE FROM cpanstats',
            'DELETE FROM page_requests',
            'DELETE FROM release_summary',
            'DELETE FROM uploads',
            'DELETE FROM ixlatest'
);

my @delete_meta_sqlite = (
            'DELETE FROM metabase'
);

my @delete_meta_mysql = (
            'DELETE FROM metabase'
);

#----------------------------------------------------------------------------
# Test Main

# prep test directory
my $directory = './test';
rmtree($directory);
mkpath($directory) or die "cannot create directory";

if(create_db(0)) {
    plan skip_all => 'Cannot create temporary databases';
} else {
    plan tests => $TESTS;
}

# continue with testing

rmtree($directory);
mkpath($directory);

ok(!-f $directory . '/cpanstats.db', '.. dbs not created yet');
ok(!-f $directory . '/litestats.db');
ok(!-f $directory . '/metabase.db');

is(create_db(0), 0, '.. dbs created');

ok(-f $directory . '/cpanstats.db', '.. dbs created');
ok(-f $directory . '/litestats.db');
ok(-f $directory . '/metabase.db');

is(create_db(2), 0, '.. dbs prepped');

## Test we can generate

SKIP: {
    skip "Valid S3 access key required for live tests", 7 unless $meta;
    #diag "Testing generate()";

    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    isa_ok($t,'CPAN::Testers::Data::Generator');

    # nothing should be created yet
    ok(!-f $directory . '/cpanstats.log');

    # first update should build all databases
    $t->generate;

    # just check they were created, if it ever becomes an issue we can
    # interrogate the contents at a later date :)
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/metabase.db');

    my $size = -s $directory . '/metabase.db';

    # second update should do nothing
    $t->generate;

    is(-s $directory . '/metabase.db', $size,'.. db should not change size');

    is(countRequests(),1,'.. page requests added');
    is(countReleases(),1,'.. release data entries added');
}

# FROM HERE WE DON'T NEED THE INTERNET

# refresh the databases

rmtree($directory);
mkpath($directory);

ok(!-f $directory . '/cpanstats.db', '.. dbs not created yet');
ok(!-f $directory . '/litestats.db');
ok(!-f $directory . '/metabase.db');

is(create_db(0), 0, '.. dbs created');

ok(-f $directory . '/cpanstats.db', '.. dbs created');
ok(-f $directory . '/litestats.db');
ok(-f $directory . '/metabase.db');

is(create_db(2), 0, '.. dbs prepped');

# build test metabase

is(create_metabase(0), 0, '.. metabase created');

## Test we can rebuild

{
    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/metabase.db');

    my $size = -s $directory . '/cpanstats.db';

    # remove stats database entries
    #create_db(1);

    # recreate the stats database
    $t->rebuild;

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');

    # recreate the stats database for specific entries
    $t->rebuild(1,4);

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');
    ok(-f $directory . '/cpanstats.log');
}

## Test we can reparse

{
    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/metabase.db');

    my $size = -s $directory . '/cpanstats.db';

    # recreate the stats database
    $t->reparse({localonly => 1},1,4);

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');
}

## Test we don't reparse anything that doesn't already exist

{
    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/metabase.db');

    my $size = -s $directory . '/cpanstats.db';

    my $c1 = getMetabaseCount();
    deleteMetabase(1);
    my $c2 = getMetabaseCount();
    is($c1-1,$c2,'... removed 1 article');

    # recreate the stats database locally
    $t->reparse({localonly => 1},1,2);
    my $c3 = getMetabaseCount();
    is($c2,$c3,'... no more or less articles');

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');
}

# now clean up!
rmtree($directory);


#----------------------------------------------------------------------------
# Test Functions

sub config_db {
    # Unfortunately Test::Database is not stable enough to use,
    # so this test script cannot be used to reliably test this 
    # distribution. As soon as Test::Database does become stable
    # further work to complete testing will be done.

    # last Test-Database install attempt was version 1.07, which still doesn't 
    # pass its own tests on my linux distros :(

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

sub create_db {
    my $type = shift || 0;

    if($type == 0) {
        $options{CPANSTATS} = config_db('CPANSTATS')    or return 1;
        $options{LITESTATS} = config_db('LITESTATS')    or return 1;
        $options{METABASE}  = config_db('METABASE')     or return 1;

        if($options{CPANSTATS}->{opts}{driver} =~ /sqlite/i)    { create_file('CPANSTATS')                  and return 1;
                                                                  dosql('CPANSTATS',\@create_sqlite)        and return 1; }
        else                                                    { dosql('CPANSTATS',\@create_mysql)         and return 1; }
        if($options{LITESTATS}->{opts}{driver} =~ /sqlite/i)    { create_file('LITESTATS')                  and return 1;
                                                                  dosql('LITESTATS',\@create_sqlite)        and return 1; }
        else                                                    { dosql('LITESTATS',\@create_mysql)         and return 1; }
        if($options{METABASE}->{opts}{driver} =~ /sqlite/i)     { create_file('METABASE')                   and return 1;
                                                                  dosql('METABASE', \@create_meta_sqlite)   and return 1; }
        else                                                    { dosql('METABASE', \@create_meta_mysql)    and return 1; }
    }
    
    if($type < 3) {
        if($options{CPANSTATS}->{opts}{driver} =~ /sqlite/i)    { dosql('CPANSTATS',\@delete_sqlite)        and return 1; }
        else                                                    { dosql('CPANSTATS',\@delete_mysql)         and return 1; }
        if($options{LITESTATS}->{opts}{driver} =~ /sqlite/i)    { dosql('LITESTATS',\@delete_sqlite)        and return 1; }
        else                                                    { dosql('LITESTATS',\@delete_mysql)         and return 1; }
    }

    if($type > 1) {
        if($options{METABASE}->{opts}{driver} =~ /sqlite/i)     { dosql('METABASE', \@delete_meta_sqlite)   and return 1; }
        else                                                    { dosql('METABASE', \@delete_meta_mysql)    and return 1; }
    }

    return 0;
}

sub create_file {
    my $db = shift;
    my $fh = IO::File->new($options{$db}->{opts}{database},'w+')    or return 1;
    $fh->close;
    return 0;
}

sub dosql {
    my ($db,$sql) = @_;

    for(@$sql) {
        #diag "SQL: [$db] $_";
        eval { $options{$db}->{dbh}->do_query($_); };
        if($@) {
            diag $@;
            for my $i (1..5) {
                my @calls = caller($i);
                last    unless(@calls);
                diag " => CALLER($calls[1],$calls[2])";
            }
            return 1;
        }
    }

    return 0;
}

sub create_metabase {
    my @guids = map {s!.*/(.*?).json$!$1!; $_} glob('t/data/*.json');
    #diag "create_metabase: guids=@guids";

    for my $guid (@guids) {
        #diag "create_metabase: guid=$guid";

        my $text;
        my $fh = IO::File->new("t/data/$guid.json") or return 1;
        while(<$fh>) { $text .= $_ }
        $fh->close;

        $options{'METABASE'}->{dbh}->do_query('INSERT INTO metabase (guid,report) VALUES (?,?)',$guid,$text);
    }

    my $fh = IO::File->new("t/data/testers.csv") or return 1;
    while(<$fh>) {
        chomp;
        my @fields = split(',',$_);
        $options{'METABASE'}->{dbh}->do_query('INSERT INTO testers_email (id,resource,fullname,email) VALUES (?,?,?,?)',@fields);
    }
    $fh->close;

    return 0;
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
