#!/usr/bin/perl -w
use strict;

use Cwd;
use File::Path;
use IO::File;
use Test::More tests => 9;

use CPAN::WWW::Testers::Generator::Database;

my %articles = (
    1 => 't/nntp/126015.txt',
    2 => 't/nntp/125106.txt',
    3 => 't/nntp/1804993.txt',
    4 => 't/nntp/1805500.txt',
);

my $directory = cwd() . '/test';
rmtree($directory);



## Test we can create databases

{
    my $t = CPAN::WWW::Testers::Generator::Database->new();
    is($t, undef);

    # nothing should be created yet
    ok(!-f $directory . '/cpanstats.db');
    ok(!-f $directory . '/articles.db');

    my $a = CPAN::WWW::Testers::Generator::Database->new(
        database   => $directory . '/articles.db'
    );
    isa_ok($a, 'CPAN::WWW::Testers::Generator::Database');

    # only articles should be created
    ok(!-f $directory . '/cpanstats.db');
    ok( -f $directory . '/articles.db');

    my $s = CPAN::WWW::Testers::Generator::Database->new(
        database   => $directory . '/cpanstats.db',
	AutoCommit => 1
    );
    isa_ok($s, 'CPAN::WWW::Testers::Generator::Database');

    # now both should be created
    ok(-f $directory . '/cpanstats.db');
    ok(-f $directory . '/articles.db');


    # TODO add tests for methods
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
