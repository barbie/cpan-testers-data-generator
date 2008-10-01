#!/usr/bin/perl -w
use strict;

use Cwd;
use File::Path;
use IO::File;
use Test::More tests => 48;

use CPAN::WWW::Testers::Generator;
use CPAN::WWW::Testers::Generator::Database;

my ($mock,$nomock);

BEGIN {
    eval "use Test::MockObject";
    $nomock = $@;

    if(!$nomock) {
        $mock = Test::MockObject->new();
        $mock->fake_module( 'Net::NNTP',
                    'group' => \&group,
                    'article' => \&getArticle);
        $mock->fake_new( 'Net::NNTP' );
        $mock->mock( 'group', \&group );
        $mock->mock( 'article', \&getArticle );
    }
}

my %articles = (
    1 => 't/nntp/126015.txt',
    2 => 't/nntp/125106.txt',
    3 => 't/nntp/1804993.txt',
    4 => 't/nntp/1805500.txt',
);

my $directory = cwd() . '/test';
rmtree($directory);



## Test we can generate

SKIP: {
    skip "Test::MockObject required for testing", 11 if $nomock;

    my $t = CPAN::WWW::Testers::Generator->new(
        directory   => $directory,
        logfile     => $directory . '/cpanstats.log'
    );

    is($t->directory, $directory);
    is($t->articles,  $directory . '/articles.db');
    is($t->database,  $directory . '/cpanstats.db');
    is($t->logfile,   $directory . '/cpanstats.log');

    # nothing should be created yet
    ok(!-f $directory . '/cpanstats.db');
    ok(!-f $directory . '/cpanstats.log');
    ok(!-f $directory . '/articles.db');

    # first update should build all databases
    $t->generate;

    # just check they were created, if it ever becomes an issue we can
    # interrogate the contents at a later date :)
    ok(-f $directory . '/cpanstats.db');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/articles.db';

    # second update should do nothing
    $t->generate;

    is(-s $directory . '/articles.db', $size,'.. db should not change size');
}


## Test we can rebuild

SKIP: {
    skip "Test::MockObject required for testing", 12 if $nomock;

    my $t = CPAN::WWW::Testers::Generator->new(
        directory   => $directory,
        logfile     => $directory . '/cpanstats.log'
    );

    is($t->directory, $directory);
    is($t->articles,  $directory . '/articles.db');
    is($t->database,  $directory . '/cpanstats.db');
    is($t->logfile,   $directory . '/cpanstats.log');

    # everything should still be there
    ok(-f $directory . '/cpanstats.db');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/cpanstats.db';

    # remove stats database
    unlink $directory . '/cpanstats.db';
    ok(!-f $directory . '/cpanstats.db');

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
    skip "Test::MockObject required for testing", 11 if $nomock;

    my $t = CPAN::WWW::Testers::Generator->new(
        directory   => $directory,
        logfile     => $directory . '/cpanstats.log'
    );

    is($t->directory, $directory);
    is($t->articles,  $directory . '/articles.db');
    is($t->database,  $directory . '/cpanstats.db');
    is($t->logfile,   $directory . '/cpanstats.log');

    # everything should still be there
    ok(-f $directory . '/cpanstats.db');
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


## Test we don't store articles

SKIP: {
    skip "Test::MockObject required for testing", 14 if $nomock;

    # set to not store articles
    my $t = CPAN::WWW::Testers::Generator->new(
        directory   => $directory,
        logfile     => $directory . '/cpanstats.log',
	nostore     => 1,
	ignore      => 1
    );

    is($t->directory, $directory);
    is($t->articles,  $directory . '/articles.db');
    is($t->database,  $directory . '/cpanstats.db');
    is($t->logfile,   $directory . '/cpanstats.log');

    # everything should still be there
    ok(-f $directory . '/cpanstats.db');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $size = -s $directory . '/articles.db';
    my $count = getCount($directory . '/articles.db');
    is($count,4,'.. should one be multiple records');

    # update should just reduce articles database
    $t->generate;

    # check everything is still there
    ok(-f $directory . '/cpanstats.db');
    ok(-f $directory . '/cpanstats.log');
    ok(-f $directory . '/articles.db');

    my $msize = -s $directory . '/articles.db';
    my $mcount = getCount($directory . '/articles.db');

    cmp_ok($msize, '<=', $size,'.. db should be a smaller size');
    cmp_ok($mcount, '<=', $count,'.. db should have fewer records');
    is($mcount,1,'.. should one be 1 record');
}


# now clean up!
rmtree($directory);


sub getArticle {
    my ($self,$id) = @_;
    my @text;

    my $fh = IO::File->new($articles{$id}) or return \@text;
    while(<$fh>) { push @text, $_ }
    $fh->close;

    return \@text;
}

sub group {
    return(4,1,4);
}

sub getCount {
    my $db = shift;
    my $a = CPAN::WWW::Testers::Generator::Database->new(
        database   => $db
    );
    my @rows = $a->get_query('SELECT count(id) FROM articles');
    return 0	unless(@rows);
    return $rows[0]->[0] || 0;
}
