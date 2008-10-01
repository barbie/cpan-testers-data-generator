package CPAN::WWW::Testers::Generator::Database;

use warnings;
use strict;
use vars qw($VERSION);

$VERSION = '0.30';

#----------------------------------------------------------------------------

=head1 NAME

CPAN::WWW::Testers::Generator::Database - DB handling code.

=head1 SYNOPSIS

  my $dbi = CPAN::WWW::Testers::Generator::Database->new(database => $db);
  my @rows = $dbi->get_query($sql);
  $dbi->do_query($sql);

  my $iterator = $dbi->get_query_interator($sql);
  while(my $row = $iterator->()) {
    # do something
  }

=head1 DESCRIPTION

Database handling code for interacting with a local cpanstats database.

=cut

# -------------------------------------
# Library Modules

use DBI;
use File::Basename;
use File::Path;

# -------------------------------------
# Variables

use constant    DATABASE    => 'cpanstats.db';

# -------------------------------------
# Routines

=head1 INTERFACE

=head2 The Constructor

=over 4

=item new

=back

=cut

sub new {
    my ($class,%hash) = @_;
    return  unless($hash{database});

    my $self = {database => $hash{database}};
    bless $self, $class;

    $self->{AutoCommit} = $hash{AutoCommit} || 0;

    my $exists = -f $self->{database};

    mkpath(dirname($self->{database}))  unless($exists);

    $self->{dbh} = DBI->connect("DBI:SQLite:dbname=$self->{database}", "", "", {
        RaiseError => 1,
        AutoCommit => $self->{AutoCommit},
        sqlite_handle_binary_nulls => 1,
    });
    return  unless($self->{dbh});

    if(!$exists) {
        eval { $self->_dbh_create($self->{dbh},$self->{database}) };
        die "Failed to create database: $@"  if($@);
    }

    return $self;
}

sub DESTROY {
    my $self = shift;
    return      unless($self->{dbh});

    $self->{dbh}->commit    unless($self->{AutoCommit});
    $self->{dbh}->disconnect;
}

=head2 Methods

=over 4

=item do_commit

Force a commit if AutoCommit is off

=cut

sub do_commit {
    my $self = shift;
    $self->{dbh}->commit;
}

=item do_query

An SQL wrapper method to perform a non-returning request.

=cut

sub do_query {
    my ($self,$sql,@fields) = @_;
    my $sth;

    # prepare the sql statement for executing
    eval { $sth = $self->{dbh}->prepare($sql); };
    unless($sth) {
        die sprintf "ERROR: %s : %s\n", $self->{dbh}->errstr, $sql;
    }

    # execute the SQL using any values sent to the function
    # to be placed in the sql
    unless($sth->execute(@fields)) {
        die sprintf "ERROR: %s : %s : [%s]\n", $sth->errstr, $sql, join(',',@fields);
    }

    $sth->finish;
}

=item get_query

An SQL wrapper method to perform a returning request.

=cut

sub get_query {
    my ($self,$sql,@fields) = @_;
    my ($sth,@rows);

    eval { $sth = $self->{dbh}->prepare($sql); };
    unless($sth) {
        die sprintf "ERROR: %s : %s\n", $self->{dbh}->errstr, $sql;
    }

    unless($sth->execute(@fields)) {
        die sprintf "ERROR: %s : %s : [%s]\n", $sth->errstr, $sql, join(',',@fields);
    }

    while(my $row = $sth->fetchrow_arrayref) {
        push @rows, [@$row];
    }
    return @rows;
}

=item get_query_iterator

An SQL wrapper method to perform a returning request, via an iterator.

=cut

sub get_query_iterator {
    my ($self,$sql,@fields) = @_;
    my ($sth,@rows);

    eval { $sth = $self->{dbh}->prepare($sql); };
    unless($sth) {
        die sprintf "ERROR: %s : %s\n", $self->{dbh}->errstr, $sql;
    }

    unless($sth->execute(@fields)) {
        die sprintf "ERROR: %s : %s : [%s]\n", $sth->errstr, $sql, join(',',@fields);
    }

    return sub { return $sth->fetchrow_arrayref }
}

sub _dbh_create {
    my ($self,$dbh,$db) = @_;
    my @sql;

    if($db =~ /cpanstats.db$/) {
        push @sql,
            'PRAGMA auto_vacuum = 1',
            'CREATE TABLE cpanstats (
                          id            INTEGER PRIMARY KEY,
                          state         TEXT,
                          postdate      TEXT,
                          tester        TEXT,
                          dist          TEXT,
                          version       TEXT,
                          platform      TEXT,
                          perl          TEXT,
                          osname        TEXT,
                          osvers        TEXT,
                          date          TEXT)',

            'CREATE INDEX distverstate ON cpanstats (dist, version, state)',
            'CREATE INDEX ixperl ON cpanstats (perl)',
            'CREATE INDEX ixplat ON cpanstats (platform)',
            'CREATE INDEX ixdate ON cpanstats (postdate)';
    } else {
        push @sql,
            'PRAGMA auto_vacuum = 1',
            'CREATE TABLE articles (
                          id            INTEGER PRIMARY KEY,
                          article       TEXT)';
    }

    $dbh->do($_)    for(@sql);
}


__END__

=back

=head1 BUGS, PATCHES & FIXES

There are no known bugs at the time of this release. However, if you spot a
bug or are experiencing difficulties, that is not explained within the POD
documentation, please send bug reports and patches to the RT Queue (see below).

Fixes are dependant upon their severity and my availablity. Should a fix not
be forthcoming, please feel free to (politely) remind me.

RT Queue -
http://rt.cpan.org/Public/Dist/Display.html?Name=CPAN-WWW-Testers-Generator

=head1 SEE ALSO

L<CPAN::WWW::Testers>,
L<CPAN::Testers::WWW::Statistics>

F<http://www.cpantesters.org/>,
F<http://stats.cpantesters.org/>,
F<http://wiki.cpantesters.org/>

=head1 AUTHOR

  Barbie, <barbie@cpan.org>
  for Miss Barbell Productions <http://www.missbarbell.co.uk>.

=head1 COPYRIGHT AND LICENSE

  Copyright (C) 2008 Barbie for Miss Barbell Productions.

  This module is free software; you can redistribute it and/or
  modify it under the same terms as Perl itself.

=cut

