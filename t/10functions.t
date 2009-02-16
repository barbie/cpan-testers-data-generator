#!/usr/bin/perl -w
use strict;
use Test::More tests => 1;
use CPAN::Testers::Data::Generator;

my $t = CPAN::Testers::Data::Generator->new(config  => './t/test-config.ini');
isa_ok($t,'CPAN::Testers::Data::Generator');

