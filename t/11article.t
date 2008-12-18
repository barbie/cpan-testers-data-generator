#!/usr/bin/perl -w
use strict;

use lib 'lib';
use Test::More tests => 39;
use IO::File;

use_ok('CPAN::Testers::Data::Generator::Article');

# PASS report
my $article = readfile('t/nntp/126015.txt');
my $a = CPAN::Testers::Data::Generator::Article->new($article);
isa_ok($a,'CPAN::Testers::Data::Generator::Article');
ok($a->parse_report());
is($a->from, 'Jost.Krieger+perl@rub.de (Jost Krieger+Perl)');
is($a->postdate, '200403');
is($a->date, '200403081025');
is($a->status, 'PASS');
ok($a->passed);
ok(!$a->failed);
is($a->distribution, 'AI-Perceptron');
is($a->version, '1.0');
is($a->perl, '5.8.3');
is($a->osname, 'solaris');
is($a->osvers, '2.8');
is($a->archname, 'sun4-solaris-thread-multi');

# FAIL report
$article = readfile('t/nntp/125106.txt');
$a = CPAN::Testers::Data::Generator::Article->new($article);
isa_ok($a,'CPAN::Testers::Data::Generator::Article');
ok($a->parse_report());
is($a->from, 'cpansmoke@alternation.net');
is($a->postdate, '200403');
is($a->date, '200403030607');
is($a->status, 'FAIL');
ok(!$a->passed);
ok($a->failed);
is($a->distribution, 'Net-IP-Route-Reject');
is($a->version, '0.5_1');
is($a->perl, '5.8.0');
is($a->osname, 'linux');
is($a->osvers, '2.4.22-4tr');
is($a->archname, 'i586-linux');

ok(!$a->parse_upload());


# upload announcement
$article = readfile('t/nntp/1804993.txt');
$a = CPAN::Testers::Data::Generator::Article->new($article);
isa_ok($a,'CPAN::Testers::Data::Generator::Article');
ok($a->parse_upload());
is($a->from, 'upload@pause.perl.org (PAUSE)');
is($a->postdate, '200806');
is($a->date, '200806271438');
is($a->distribution, 'Test-CPAN-Meta');
is($a->version, '0.12');

ok(!$a->parse_report());


# in reply to
$article = readfile('t/nntp/1805500.txt');
$a = CPAN::Testers::Data::Generator::Article->new($article);
ok(!$a);

exit;


# base64
$article = readfile('t/nntp/1804993.txt');
$a = CPAN::Testers::Data::Generator::Article->new($article);
isa_ok($a,'CPAN::Testers::Data::Generator::Article');
ok(!$a->parse_upload());
ok($a->parse_report());
is($a->from, 'cpansmoke@alternation.net');
is($a->postdate, '200403');
is($a->date, '200403000000');
is($a->status, 'FAIL');
ok(!$a->passed);
ok($a->failed);
is($a->distribution, 'Net-IP-Route-Reject');
is($a->version, '0.5_1');
is($a->perl, '5.8.0');
is($a->osname, 'linux');
is($a->osvers, '2.4.22-4tr');
is($a->archname, 'i586-linux');



# quoted printable
$article = readfile('t/nntp/1804993.txt');
$a = CPAN::Testers::Data::Generator::Article->new($article);
isa_ok($a,'CPAN::Testers::Data::Generator::Article');
ok(!$a->parse_upload());
ok($a->parse_report());
is($a->from, 'cpansmoke@alternation.net');
is($a->postdate, '200403');
is($a->date, '200403000000');
is($a->status, 'FAIL');
ok(!$a->passed);
ok($a->failed);
is($a->distribution, 'Net-IP-Route-Reject');
is($a->version, '0.5_1');
is($a->perl, '5.8.0');
is($a->osname, 'linux');
is($a->osvers, '2.4.22-4tr');
is($a->archname, 'i586-linux');



sub readfile {
    my $file = shift;
    my $text;
    my $fh = IO::File->new($file)   or return;
    while(<$fh>) { $text .= $_ }
    $fh->close;
    return $text;
}