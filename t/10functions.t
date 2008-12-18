#!/usr/bin/perl -w
use strict;
use Test::More tests => 17;
use CPAN::Testers::Data::Generator;

my @perls = (
  {
    text => 'Summary of my perl5 (revision 5.0 version 6 subversion 1) configuration',
    perl => '5.6.1'
  },
  {
    text => 'Summary of my perl5 (revision a version b subversion c) configuration',
    perl => '0'
  },
  {
    text => 'Summary of my perl5 (revision 5.0 version 8 subversion 0 patch 17332) configuration',
    perl => '5.8.0 patch 17332',
  },
  {
    text => 'Summary of my perl5 (revision 5.0 version 8 subversion 1 RC3) configuration',
    perl => '5.8.1 RC3',
  },
#  {
#    text => '',
#    perl => '',
#  },
);

my $t = CPAN::Testers::Data::Generator->new(config  => './t/test-config.ini');
isa_ok($t,'CPAN::Testers::Data::Generator');

foreach (@perls) {
  my $text = $_->{text};
  my $perl = $_->{perl};

  my $version = CPAN::Testers::Data::Generator::Article->_extract_perl_version(\$text);
  is($version, $perl);
}

my @testdates = (
    ['Wed, 13 September 2004','200409','200409130000'],
    ['13 September 2004','200409','200409130000'],
    ['September 22, 1999 06:29','199909','199909220629'],

    ['Wed, 13 September 1990','000000','000000000000'],
    ['13 September 1990','000000','000000000000'],
    ['September 22, 1990 06:29','000000','000000000000'],
);

for my $row (@testdates) {
    my ($d1,$d2) = CPAN::Testers::Data::Generator::Article::_extract_date($row->[0]);
    is($d1,$row->[1],".. short date parse of '$row->[0]'");
    is($d2,$row->[2],".. long date parse of '$row->[0]'");
}
