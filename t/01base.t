#!/usr/bin/perl -w
use strict;

use Test::More tests => 3;

BEGIN {
	use_ok( 'CPAN::WWW::Testers::Generator' );
	use_ok( 'CPAN::WWW::Testers::Generator::Article' );
	use_ok( 'CPAN::WWW::Testers::Generator::Database' );
}
