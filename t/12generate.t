#!/usr/bin/perl -w
use strict;

use Config::IniFiles;
use CPAN::Testers::Common::DBUtils;
use CPAN::Testers::Data::Generator;
use Data::Dumper;
use File::Path;
use IO::File;
use Test::More;

#----------------------------------------------------------------------------
# Test Conditions

eval "use Test::MockObject";
if($@) {
    plan skip_all => 'Need Test::MockObject to do full tests';
}

#----------------------------------------------------------------------------
# Test Variables

my (%options,$nomock,$mock1);
my $config = './t/test-config.ini';
my %articles = (
    1 => 't/nntp/126015.txt',
    2 => 't/nntp/125106.txt',
    3 => 't/nntp/1804993.txt',
    4 => 't/nntp/1805500.txt',
);

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
                fulldate    TEXT)',

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

my @create_arts_sqlite = (
            'PRAGMA auto_vacuum = 1',
            'CREATE TABLE articles (
                id            INTEGER PRIMARY KEY,
                article       TEXT)'
);

my @create_arts_mysql = (
            'CREATE TABLE articles (
                id          int(10) unsigned NOT NULL,
                article     blob,
                PRIMARY KEY (id)'
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

my @delete_arts_sqlite = (
            'DELETE FROM articles'
);

my @delete_arts_mysql = (
            'DELETE FROM articles'
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
    plan tests => 50;
}

# continue with testing

BEGIN {
    eval "use Test::MockObject";
    $nomock = $@;

    unless($nomock) {
        $mock1 = Test::MockObject->new();
        $mock1->fake_module( 'Net::NNTP',
                    'group' => \&group,
                    'article' => \&getArticle);
        $mock1->fake_new( 'Net::NNTP' );
        $mock1->mock( 'group', \&group );
        $mock1->mock( 'article', \&getArticle );
    }
}

rmtree($directory);
mkpath($directory);

ok(!-f $directory . '/cpanstats.db', '.. dbs not created yet');
ok(!-f $directory . '/litestats.db');
ok(!-f $directory . '/articles.db');

is(create_db(0), 0, '.. dbs created');

ok(-f $directory . '/cpanstats.db', '.. dbs files found');
ok(-f $directory . '/litestats.db');
ok(-f $directory . '/articles.db');

is(create_db(2), 0, '.. dbs prepped');

## Test we can generate

SKIP: {
    skip "Test::MockObject required for testing", 7 if $nomock;
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
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/articles.db';

    # second update should do nothing
    $t->generate;

    is(-s $directory . '/articles.db', $size,'.. db should not change size');

    is(countRequests(),2,'.. page requests added');
    is(countReleases(),2,'.. release data entries added');
}

## Test we can rebuild

SKIP: {
    skip "Test::MockObject required for testing", 9 if $nomock;
    #diag "Testing rebuild()";

    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/cpanstats.db';

    # delete stats database records
    create_db(1);

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
}


## Test we can reparse

SKIP: {
    skip "Test::MockObject required for testing", 7 if $nomock;
    #diag "Testing reparse()";

    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/cpanstats.db';

    # recreate the stats database
    $t->reparse({localonly => 1},1);

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');

    # recreate the stats database for specific entries
    $t->reparse({},4);

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');
}


## Test we don't reparse anything that doesn't already exist

SKIP: {
    skip "Test::MockObject required for testing", 10 if $nomock;
    #diag "Testing reparse()";

    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log'
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/cpanstats.db';

    my $c1 = getArticleCount();
    deleteArticle(1);
    my $c2 = getArticleCount();
    is($c1-1,$c2,'... removed 1 article');

    # recreate the stats database locally
    $t->reparse({localonly => 1},1,2);
    my $c3 = getArticleCount();
    is($c2,$c3,'... no more or less articles');

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');

    # recreate the stats database for specific entries
    $t->reparse({},0,6,20,100);
    my $c4 = getArticleCount();
    is($c2,$c4,'... no more or less articles again');

    # check stats database is again the same size as before
    ok(-f $directory . '/cpanstats.db');
    is(-s $directory . '/cpanstats.db', $size,'.. db should be same size');
}


## Test we don't store articles

SKIP: {
    skip "Test::MockObject required for testing", 10 if $nomock;
    #diag "Testing nostore()";

    # set to not store articles
    my $t = CPAN::Testers::Data::Generator->new(
        config  => $config,
        logfile => $directory . '/cpanstats.log',
	    nostore     => 1,
	    ignore      => 1
    );

    # everything should still be there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/articles.db';
    my $count = getArticleCount();
    is($count,2,'.. should be 2 records');

    # update should just reduce articles database
    $t->generate;

    # check everything is still there
    ok(-f $directory . '/cpanstats.db','.. dbs still there');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $msize = -s $directory . '/articles.db';
    my $mcount = getArticleCount();

    cmp_ok($msize, '<=', $size,'.. db should be a smaller size');
    cmp_ok($mcount, '<=', $count,'.. db should have fewer records');
    is($mcount,1,'.. should be 1 record');
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
        $options{LITEARTS}  = config_db('LITEARTS')     or return 1;

        if($options{CPANSTATS}->{opts}{driver} =~ /sqlite/i)    { create_file('CPANSTATS')                  and return 1;
                                                                  dosql('CPANSTATS',\@create_sqlite)        and return 1; }
        else                                                    { dosql('CPANSTATS',\@create_mysql)         and return 1; }
        if($options{LITESTATS}->{opts}{driver} =~ /sqlite/i)    { create_file('LITESTATS')                  and return 1;
                                                                  dosql('LITESTATS',\@create_sqlite)        and return 1; }
        else                                                    { dosql('LITESTATS',\@create_mysql)         and return 1; }
        if($options{LITEARTS}->{opts}{driver} =~ /sqlite/i)     { create_file('LITEARTS')                   and return 1;
                                                                  dosql('LITEARTS', \@create_arts_sqlite)   and return 1; }
        else                                                    { dosql('LITEARTS', \@create_arts_mysql)    and return 1; }
    }
    
    if($type < 3) {
        if($options{CPANSTATS}->{opts}{driver} =~ /sqlite/i)    { dosql('CPANSTATS',\@delete_sqlite)        and return 1; }
        else                                                    { dosql('CPANSTATS',\@delete_mysql)         and return 1; }
        if($options{LITESTATS}->{opts}{driver} =~ /sqlite/i)    { dosql('LITESTATS',\@delete_sqlite)        and return 1; }
        else                                                    { dosql('LITESTATS',\@delete_mysql)         and return 1; }
    }

    if($type > 1) {
        if($options{LITEARTS}->{opts}{driver} =~ /sqlite/i)     { dosql('LITEARTS', \@delete_arts_sqlite)   and return 1; }
        else                                                    { dosql('LITEARTS', \@delete_arts_mysql)    and return 1; }
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
            return 1;
        }
    }

    return 0;
}

sub group {
    return(4,1,4);
}

sub getArticle {
    my ($self,$id) = @_;
    my @text;

    my $fh = IO::File->new($articles{$id}) or return \@text;
    while(<$fh>) { push @text, $_ }
    $fh->close;

    return \@text;
}

sub getArticleCount {
    $options{LITEARTS} ||= config_db('LITEARTS');

    my @rows = $options{LITEARTS}->{dbh}->get_query('array','SELECT count(id) FROM articles');
    return 0	unless(@rows);
    return $rows[0]->[0] || 0;
}

sub deleteArticle {
    my $id = shift;

    $options{LITEARTS} ||= config_db('LITEARTS');
    my @rows = $options{LITEARTS}->{dbh}->get_query('array','SELECT * FROM articles WHERE id = ?',$id);
    $options{LITEARTS}->{dbh}->do_query('DELETE FROM articles WHERE id = ?',$id)    if(@rows);
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