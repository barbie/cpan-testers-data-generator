#!/usr/bin/perl -w
use strict;

use Test::More tests => 2;

BEGIN {
	use_ok( 'CPAN::Testers::Data::Generator' );
	use_ok( 'CPAN::Testers::Data::Generator::Article' );
}
