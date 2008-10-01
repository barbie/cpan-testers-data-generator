package CPAN::WWW::Testers::Generator;

use warnings;
use strict;
use vars qw($VERSION);

$VERSION = '0.30';

#----------------------------------------------------------------------------
# Library Modules

use DBI;
use File::Basename;
use File::Path;
use IO::File;
use Net::NNTP;

use CPAN::WWW::Testers::Generator::Article;
use CPAN::WWW::Testers::Generator::Database;

use base qw(Class::Accessor::Fast);

#----------------------------------------------------------------------------
# The Application Programming Interface

__PACKAGE__->mk_accessors(qw(articles database directory logfile));

sub new {
    my $class = shift;
    my %hash  = @_;

    $hash{directory} ||= '.';

    my $self = {};
    bless $self, $class;

    # continue when no article
    $self->{ignore} = $hash{ignore}     if($hash{ignore});

    # do not store old articles
    $self->{nostore} = $hash{nostore}   if($hash{nostore});

    # prime the logging
    $self->logfile($hash{logfile})      if($hash{logfile});

    # prime the databases
    $self->directory($hash{directory});
    $self->database("$hash{directory}/cpanstats.db");
    $self->articles("$hash{directory}/articles.db");

    return $self;
}

sub DESTROY {
    my $self = shift;
}

#----------------------------------------------------------------------------
# Public Methods

sub _init {
    my ($self,$nntp) = @_;

    $self->{stats} ||= CPAN::WWW::Testers::Generator::Database->new(database => $self->database);
    $self->{arts}  ||= CPAN::WWW::Testers::Generator::Database->new(database => $self->articles);
    return  unless($nntp);

    $self->{nntp}  ||= $self->nntp_connect();
}

sub generate {
    my $self = shift;

    $self->_init(1);

    my $start = $self->_get_lastid() +1;
    my $end   = $self->{nntp_last};
    die "Cannot access NNTP server\n"   unless($end);   # better to bail out than fade away!

    # starting from last retrieved article
    for(my $id = $start; $id <= $end; $id++) {

        $self->_log("ID [$id]");
        my $article = join "", @{$self->{nntp}->article($id) || []};

        # no article for that id!
        unless($article) {
            $self->_log(" ... no article\n");
            if($self->{ignore}) {
                next;
            } else {
                die "No article returned [$id]\n";
            }
        }

        $self->insert_article($id,$article);
        $self->parse_article($id,$article);
    }

    $self->cleanup  if($self->{nostore});
    $self->{stats}->do_commit;
    $self->{arts}->do_commit;
}


sub rebuild {
    my ($self,$start,$end) = @_;

    $self->_init(0);

    $start ||= 1;
    $end   ||= $self->_get_lastid();

    $self->{stats}->do_query("DELETE FROM cpanstats WHERE id >= $start AND id <= $end");

    my $iterator = $self->{arts}->get_query_iterator("SELECT * FROM articles WHERE id >= $start AND id <= $end ORDER BY id asc");
    while(my $row = $iterator->()) {
        my $id = $row->[0];
	    my $article = $row->[1];

        $self->_log("ID [$id]");

        # no article for that id!
        unless($article) {
            $self->_log(" ... no article\n");
            if($self->{ignore}) {
                next;
            } else {
                die "No article returned [$id]\n";
            }
        }

        $self->parse_article($id,$article);
    }

    $self->{stats}->do_commit;
    $self->{arts}->do_commit;
}

sub reparse {
    my ($self,$options,@ids) = @_;
    return  unless(@ids);

    my $flag = ($options && $options->{localonly}) ? 0 : 1;
    $self->_init($flag);

    my $last = $self->_get_lastid();

    for my $id (@ids) {
        next    if($id < 1 || $id > $last);

        my $article;
        my @rows = $self->{arts}->get_query('SELECT * FROM articles WHERE id = ?',$id);
        if(@rows) {
            $article = $rows[0]->[1];

        } elsif($options && $options->{localonly}) {
            next;

        } else {
            $article = join "", @{$self->{nntp}->article($id) || []};
        }

        next    unless($article);
        $self->_log("ID [$id]");

        $self->{stats}->do_query('DELETE FROM cpanstats WHERE id = ?',$id)  unless($options && $options->{check});
        $self->parse_article($id,$article,$options);
    }

    $self->{stats}->do_commit;
}

sub cleanup {
    my $self = shift;
    my $id = $self->_get_lastid();
    return  unless($id);

    $self->{arts}->do_query('DELETE FROM articles WHERE id < ?',$id);
    $self->{arts}->do_commit;
}

#----------------------------------------------------------------------------
# Private Methods

sub nntp_connect {
    my $self = shift;

    # connect to NNTP server
    my $nntp = Net::NNTP->new("nntp.perl.org") or die "Cannot connect to nntp.perl.org";
    ($self->{nntp_num}, $self->{nntp_first}, $self->{nntp_last}) = $nntp->group("perl.cpan.testers");

    return $nntp;
}

sub parse_article {
    my ($self,$id,$article,$options) = @_;
    my $object = CPAN::WWW::Testers::Generator::Article->new($article);

    unless($object) {
        $self->_log(" ... bad parse\n");
        return;
    }

    my $subject = $object->subject;
    my $from    = $object->from;
    $self->_log(" [$from] $subject\n");
    return    if($subject =~ /Re:/i);

    unless($subject =~ /(CPAN|FAIL|PASS|NA|UNKNOWN)\s+/i) {
        $self->_log(" . [$id] ... bad subject\n");
        return;
    }

    my $state = lc $1;
    my ($post,$date,$dist,$version,$platform,$perl,$osname,$osvers) = ();

    if($state eq 'cpan') {
        if($object->parse_upload()) {
            $dist      = $object->distribution;
            $version   = $object->version;
            $from      = $object->author;
        }

        return  unless($self->_valid_field($id, 'dist'    => $dist)     || ($options && $options->{exclude}{dist}));
        return  unless($self->_valid_field($id, 'version' => $version)  || ($options && $options->{exclude}{version}));
        return  unless($self->_valid_field($id, 'author'  => $from)     || ($options && $options->{exclude}{from}));

    } else {
        if($object->parse_report()) {
            $dist      = $object->distribution;
            $version   = $object->version;
            $from      = $object->from;
            $perl      = $object->perl;
            $platform  = $object->archname;
            $osname    = $object->osname;
            $osvers    = $object->osvers;

            $from      =~ s/'/''/g; #'
        }

        return  unless($self->_valid_field($id, 'dist'     => $dist)        || ($options && $options->{exclude}{dist}));
        return  unless($self->_valid_field($id, 'version'  => $version)     || ($options && $options->{exclude}{version}));
        return  unless($self->_valid_field($id, 'from'     => $from)        || ($options && $options->{exclude}{from}));
        return  unless($self->_valid_field($id, 'perl'     => $perl)        || ($options && $options->{exclude}{perl}));
        return  unless($self->_valid_field($id, 'platform' => $platform)    || ($options && $options->{exclude}{platform}));
        return  unless($self->_valid_field($id, 'osname'   => $osname)      || ($options && $options->{exclude}{osname}));
        return  unless($self->_valid_field($id, 'osvers'   => $osvers)      || ($options && $options->{exclude}{osname}));
    }

    $post = $object->postdate;
    $date = $object->date;
    $self->insert_stats($id,$state,$post,$from,$dist,$version,$platform,$perl,$osname,$osvers,$date)
        unless($options && $options->{check});
}

sub insert_stats {
    my $self = shift;

    my @fields = @_;
    $fields[$_] ||= 0   for(0);
    $fields[$_] ||= ''  for(1,2,3,4,5,6,8,9,10);
    $fields[$_] ||= '0' for(7);

    my $INSERT = 'INSERT INTO cpanstats VALUES (?,?,?,?,?,?,?,?,?,?,?)';

    $self->{stats}->do_query($INSERT,@fields);
    if((++$self->{stat_count} % 50) == 0) {
        $self->{stats}->do_commit;
    }
}

sub insert_article {
    my $self = shift;

    my @fields = @_;
    $fields[$_] ||= 0   for(0);
    $fields[$_] ||= ''  for(1);

    my $INSERT = 'INSERT INTO articles VALUES (?,?)';

    $self->{arts}->do_query($INSERT,@fields);
    if((++$self->{arts_count} % 50) == 0) {
        $self->{arts}->do_commit;
    }
}

#----------------------------------------------------------------------------
# Private Functions

sub _valid_field {
    my ($self,$id,$name,$value) = @_;
    return 1    if(defined $value);
    $self->_log(" . [$id] ... missing field: $name\n");
    return 0;
}

sub _get_lastid {
    my $self = shift;

    my @rows = $self->{arts}->get_query("SELECT max(id) FROM articles");
    return 0    unless(@rows);
    return $rows[0]->[0] || 0;
}

sub _log {
    my $self = shift;
    my $log = $self->logfile()  or return;
    mkpath(dirname($log))   unless(-f $log);
    my $fh = IO::File->new($log,'a+') or die "Cannot append to log file [$log]: $!\n";
    print $fh @_;
    $fh->close;
}


1;

__END__

=head1 NAME

CPAN::WWW::Testers::Generator - Download and summarize CPAN Testers data

=head1 SYNOPSIS

  % cpanstats
  # ... wait patiently, very patiently
  # ... then use cpanstats.db, an SQLite database

=head1 DESCRIPTION

This distribution was originally written by Leon Brocard to download and
summarize CPAN Testers data. However, all of the original code has been
rewritten to use the CPAN Testers Statistics database generation code. This
now means that all the CPAN Testers sites including the Reports site, the
Statistics site and the CPAN Dependencies site, can use the same database.

This module downloads articles from the cpan-testers newsgroup, generating or
updating an SQLite database containing all the most important information. You
can then query this database, or use CPAN::WWW::Testers to present it over the
web.

A good example query for Acme-Colour would be:

  SELECT version, status, count(*) FROM cpanstats WHERE
  distribution = "Acme-Colour" group by version, state;

To create a database from scratch can take several days, as there are now over
2 million articles in the newgroup. As such updating from a known copy of the
database is much more advisable. If you don't want to generate the database
yourself, you can obtain the latest official copy (compressed with gzip) at
http://devel.cpantesters.org/cpanstats.db.gz

With over 2 million articles in the archive, if you do plan to run this
software to generate the databases it is recommended you utilise a high-end
processor machine. Even with a reasonable processor it can takes days!

=head1 DATABASE SCHEMA

The cpanstats database schema is very straightforward, one main table with
several index tables to speed up searches. The main table is as below:

  +--------------------------------+
  | cpanstats                      |
  +----------+---------------------+
  | id       | INTEGER PRIMARY KEY |
  | state    | TEXT                |
  | postdate | TEXT                |
  | tester   | TEXT                |
  | dist     | TEXT                |
  | version  | TEXT                |
  | platform | TEXT                |
  | perl     | TEXT                |
  | osname   | TEXT                |
  | osvers   | TEXT                |
  | date     | TEXT                |
  +----------+---------------------+

It should be noted that 'postdate' refers to the YYYYMM formatted date, whereas
the 'date' field refers to the YYYYMMDDhhmm formatted date and time.

The articles database schema is again very straightforward, and consists of one
table, as below:

  +--------------------------------+
  | articles                       |
  +----------+---------------------+
  | id       | INTEGER PRIMARY KEY |
  | article  | TEXT                |
  +----------+---------------------+

=head1 INTERFACE

=head2 The Constructor

=over

=item * new

Instatiates the object CPAN::WWW::Testers::Generator. Accepts a hash containing
values to prepare the object. These are described as:

  my $obj = CPAN::WWW::Testers::Generator->new(
                logfile   => './here/logfile',
                directory => './here'
  );

Where 'logfile' is the location to write log messages. Log messages are only
written if a logfile entry is specified, and will always append to any existing
file. The 'directory' value is where all databases will be created.

=back

=head2 Accessors

=over

=item * articles

Accessor to set/get the database full path.

=item * database

Accessor to set/get the database full path.

=item * directory

Accessor to set/get the directory where the database is to be created.

=item * logfile

Accessor to set/get where the logging information is to be kept. Note that if
this not set, no logging occurs.

=back

=head2 Public Methods

=over

=item * generate

Starting from the last recorded article, retrieves all the more recent articles
from the NNTP server, parsing each and recording the articles that either
upload announcements or reports.

=item * rebuild

In the event that the cpanstats database needs regenerating, either in part or
for the whole database, this method allow you to do so. You may supply
parameters as to the 'start' and 'end' values (inclusive), where all records
are assumed by default. Note that the 'nostore' option is ignored and no
records are deleted from the articles database.

=item * reparse

Rather than a complete rebuild the option to selective reparse selected entries
is useful if there are posts which have since been identified as valid and now
have supporting parsing code within the codebase.

In addition there is the option to exclude fields from parsing checks, where
they may be corrupted, and can be later amended using the 'cpanstats-update'
tool.

=item * cleanup

In the event that you do not wish to store all the articles permanently in the
articles database, this method removes all but the most recent entry, which is
kept to ensure that subsequent runs will start from the correct article. To
enable this feature, specify 'nostore' within the has passed to new().

=back

=head2 Private Methods

=over

=item * nntp_connect

Sets up the connection to the NNTP server.

=item * parse_article

Parses an article extracting the metadata required for the stats database.

=item * insert_article

Inserts an article into the articles database.

=item * insert_stats

Inserts the components of a parsed article into the statistics database.

=back

=head1 HISTORY

The CPAN testers was conceived back in May 1998 by Graham Barr and Chris
Nandor as a way to provide multi-platform testing for modules. Today there
are over 2 million tester reports and more than 100 testers each month
giving valuable feedback for users and authors alike.

=head1 BECOME A TESTER

Whether you have a common platform or a very unusual one, you can help by
testing modules you install and submitting reports. There are plenty of
module authors who could use test reports and helpful feedback on their
modules and distributions.

If you'd like to get involved, please take a look at the CPAN Testers Wiki,
where you can learn how to install and configure one of the recommended
smoke tools.

For further help and advice, please subscribe to the the CPAN Testers
discussion mailing list.

  CPAN Testers Wiki
    - http://wiki.cpantesters.org
  CPAN Testers Discuss mailing list
    - http://lists.cpan.org/showlist.cgi?name=cpan-testers-discuss

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

  Original author:    Leon Brocard <acme@astray.com>   200?-2008
  Current maintainer: Barbie       <barbie@cpan.org>   2008-present

=head1 LICENSE

This code is distributed under the same license as Perl.
