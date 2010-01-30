package CPAN::Testers::Data::Generator;

use warnings;
use strict;
use vars qw($VERSION);

$VERSION = '0.39';

#----------------------------------------------------------------------------
# Library Modules

use Config::IniFiles;
use CPAN::Testers::Common::Article;
use CPAN::Testers::Common::DBUtils;
use CPAN::Testers::Common::Utils    qw(nntp_to_guid);
use File::Basename;
use File::Path;
use IO::File;
use Net::NNTP;

#----------------------------------------------------------------------------
# The Application Programming Interface

sub new {
    my $class = shift;
    my %hash  = @_;

    my $self = {};
    bless $self, $class;

    # load configuration
    my $cfg = Config::IniFiles->new( -file => $hash{config} );

    # configure databases
    for my $db (qw(CPANSTATS LITESTATS LITEARTS)) {
        die "No configuration for $db database\n"   unless($cfg->SectionExists($db));
        my %opts = map {$_ => ($cfg->val($db,$_)||undef);} qw(driver database dbfile dbhost dbport dbuser dbpass);
        $opts{AutoCommit} = 0;
        $self->{$db} = CPAN::Testers::Common::DBUtils->new(%opts);
        die "Cannot configure $db database\n" unless($self->{$db});
    }

    # command line swtiches override configuration settings
    for my $key (qw(ignore nostore logfile)) {
        $self->{$key} = $hash{$key} || $cfg->val('MAIN',$key);
    }

    my @rows = $self->{CPANSTATS}->get_query('array',q{SELECT osname,ostitle FROM osname});
    for my $row (@rows) {
        $self->{OSNAMES}{lc $row->[0]} = $row->[1];
    }

    if($cfg->SectionExists('DISABLE')) {
        my @values = $cfg->val('DISABLE','LIST');
        $self->{DISABLE}{$_} = 1    for(@values);
    }

    ($self->{nntp_num}, $self->{nntp_first}, $self->{nntp_last}) = (0,0,0);

    return $self;
}

sub DESTROY {
    my $self = shift;
}

#----------------------------------------------------------------------------
# Public Methods

sub generate {
    my $self = shift;

    $self->{nntp}  ||= $self->nntp_connect();

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
    $self->commit();
}


sub rebuild {
    my ($self,$start,$end) = @_;

    $start ||= 1;
    $end   ||= $self->_get_lastid();

    $self->{CPANSTATS}->do_query("DELETE FROM cpanstats WHERE id >= $start AND id <= $end");
    $self->{LITESTATS}->do_query("DELETE FROM cpanstats WHERE id >= $start AND id <= $end");

    my $iterator = $self->{LITEARTS}->iterator('array',"SELECT * FROM articles WHERE id >= $start AND id <= $end ORDER BY id asc");
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

    $self->commit();
}

sub reparse {
    my ($self,$options,@ids) = @_;
    return  unless(@ids);

    $self->{nntp}  ||= $self->nntp_connect()
        unless($options && $options->{localonly});

    my $last = $self->_get_lastid();

    for my $id (@ids) {
        #print STDERR "id=[$id], last=[$last]\n";
        next    if($id < 1 || ($id > $last && $id > $self->{nntp_last}));

        my $article;
        my @rows = $self->{LITEARTS}->get_query('array','SELECT * FROM articles WHERE id = ?',$id);
        if(@rows) {
            $article = $rows[0]->[1];
            #print STDERR "got article\n";

        } elsif($options && $options->{localonly}) {
            #print STDERR "no article locally\n";
            next;

        } else {
            $article = join "", @{$self->{nntp}->article($id) || []};
            $self->insert_article($id,$article) if($article);
            #print STDERR "got NNTP article\n";
        }


        next    unless($article);
        $self->_log("ID [$id]");

        unless($options && $options->{check}) {
            $self->{CPANSTATS}->do_query('DELETE FROM cpanstats WHERE id = ?',$id);
            $self->{LITESTATS}->do_query('DELETE FROM cpanstats WHERE id = ?',$id);
        }
        $self->parse_article($id,$article,$options);
    }

    $self->commit();
}

#----------------------------------------------------------------------------
# Private Methods

sub cleanup {
    my $self = shift;
    my $id = $self->_get_lastid();
    return  unless($id);

    $self->{LITEARTS}->do_query('DELETE FROM articles WHERE id < ?',$id);
}

sub commit {
    my $self = shift;
    for(qw(CPANSTATS LITESTATS LITEARTS)) {
        next    unless($self->{$_});
        $self->{$_}->do_commit;
    }
}

sub nntp_connect {
    my $self = shift;

    # connect to NNTP server
    my $nntp = Net::NNTP->new("nntp.perl.org") or die "Cannot connect to NNTP server [nntp.perl.org]\n";
    ($self->{nntp_num}, $self->{nntp_first}, $self->{nntp_last}) = $nntp->group("perl.cpan.testers");

    #print STDERR "NNTP: (num,first,last) = ($self->{nntp_num}, $self->{nntp_first}, $self->{nntp_last})\n";

    return $nntp;
}

sub parse_article {
    my ($self,$id,$article,$options) = @_;
    my $object = CPAN::Testers::Common::Article->new($article);

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
    my ($dist,$version,$platform,$perl,$osname,$osvers);

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
            $osname    = $self->_osname($object->osname);
            $osvers    = $object->osvers;

            $from      =~ s/'/''/g; #'
        }

        if($self->{DISABLE} && $self->{DISABLE}{$from}) {
            $state .= ':invalid';
        }

        return  unless($self->_valid_field($id, 'dist'     => $dist)        || ($options && $options->{exclude}{dist}));
        return  unless($self->_valid_field($id, 'version'  => $version)     || ($options && $options->{exclude}{version}));
        return  unless($self->_valid_field($id, 'from'     => $from)        || ($options && $options->{exclude}{from}));
        return  unless($self->_valid_field($id, 'perl'     => $perl)        || ($options && $options->{exclude}{perl}));
        return  unless($self->_valid_field($id, 'platform' => $platform)    || ($options && $options->{exclude}{platform}));
        return  unless($self->_valid_field($id, 'osname'   => $osname)      || ($options && $options->{exclude}{osname}));
        return  unless($self->_valid_field($id, 'osvers'   => $osvers)      || ($options && $options->{exclude}{osname}));
    }

    my $guid = nntp_to_guid($id);
    my $post = $object->postdate;
    my $date = $object->date;
    $self->insert_stats($id,$guid,$state,$post,$from,$dist,$version,$platform,$perl,$osname,$osvers,$date)
        unless($options && $options->{check});
}

sub insert_stats {
    my $self = shift;

    my @fields = @_;
    $fields[$_] ||= 0   for(0);
    $fields[$_] ||= ''  for(1,2,3,4,5,6,7,9,10,11);
    $fields[$_] ||= '0' for(8);

    my $INSERT = 'INSERT INTO cpanstats VALUES (?,?,?,?,?,?,?,?,?,?,?,?)';

    for my $db (qw(CPANSTATS LITESTATS)) {
        my @rows = $self->{$db}->get_query('array','SELECT * FROM cpanstats WHERE id=?',$fields[0]);
        next    if(@rows);
        $self->{$db}->do_query($INSERT,@fields);
    }

    # push page requests
    # - note we only update the author if this is the *latest* version of the distribution
    my $author = $fields[2] eq 'cpan' ? $fields[4] : $self->_get_author($fields[5],$fields[6]);
    $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('author',?,1)",$author)  if($author);
    $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('distro',?,1)",$fields[5]);

    if($fields[2] ne 'cpan') {
        $self->{CPANSTATS}->do_query(
                'INSERT INTO release_data ' . 
                '(dist,version,id,guid,oncpan,distmat,perlmat,patched,pass,fail,na,unknown) ' .
                'VALUES (?,?,?,?,?,?,?,?,?,?,?)',

            $fields[5],$fields[6],$fields[0],$fields[1],

            $self->_oncpan($fields[5],$fields[6]) ? 1 : 2,

            $fields[6] =~ /_/           ? 2 : 1,
            $fields[8] =~ /^5.(7|9|11)/ ? 2 : 1,
            $fields[8] =~ /patch/       ? 2 : 1,

            $fields[2] eq 'pass'    ? 1 : 0,
            $fields[2] eq 'fail'    ? 1 : 0,
            $fields[2] eq 'na'      ? 1 : 0,
            $fields[2] eq 'unknown' ? 1 : 0);
    }

    if((++$self->{stat_count} % 50) == 0) {
        $self->{CPANSTATS}->do_commit;
        $self->{LITESTATS}->do_commit;
    }
}

sub insert_article {
    my $self = shift;

    my @fields = @_;
    $fields[$_] ||= 0   for(0);
    $fields[$_] ||= ''  for(1);

    my $INSERT = 'INSERT INTO articles VALUES (?,?)';

    for my $db (qw(LITEARTS)) {
        my @rows = $self->{$db}->get_query('array','SELECT * FROM articles WHERE id=?',$fields[0]);
        next    if(@rows);
        $self->{$db}->do_query($INSERT,@fields);
    }

    if((++$self->{arts_count} % 50) == 0) {
        $self->{LITEARTS}->do_commit;
    }
}

#----------------------------------------------------------------------------
# Private Functions

sub _get_author {
    my ($self,$dist,$version) = @_;
    my @rows = $self->{CPANSTATS}->get_query('array','SELECT author FROM ixlatest WHERE dist=? AND version=? LIMIT 1',$dist,$version);
    return @rows ? $rows[0]->[0] : '';
}

sub _valid_field {
    my ($self,$id,$name,$value) = @_;
    return 1    if(defined $value);
    $self->_log(" . [$id] ... missing field: $name\n");
    return 0;
}

sub _get_lastid {
    my $self = shift;

    my @rows = $self->{LITEARTS}->get_query('array',"SELECT max(id) FROM articles");
    return 0    unless(@rows);
    return $rows[0]->[0] || 0;
}

sub _oncpan {
    my ($self,$dist,$vers) = @_;

    my @rows = $self->{CPANSTATS}->get_query('array','SELECT DISTINCT(type) FROM uploads WHERE dist=? AND version=?',$dist,$vers);
    my $type = @rows ? $rows[0]->[0] : undef;

    return 1    unless($type);          # assume it's a new release
    return 0    if($type eq 'backpan'); # on backpan only
    return 1;                           # on cpan or new upload
}

sub _osname {
    my ($self,$name) = @_;
    my $lname = lc $name;
    unless($self->{OSNAME}{$lname}) {
        $self->{OSNAMES}{$lname} = uc($name);
        $self->{CPANSTATS}->do_query(qq{INSERT INTO osname (osname,ostitle) VALUES ('$name','$self->{OSNAMES}{$lname}'});
    }
    return $name;
}

sub _log {
    my $self = shift;
    my $log = $self->{logfile} or return;
    mkpath(dirname($log))   unless(-f $log);
    my $fh = IO::File->new($log,'a+') or die "Cannot append to log file [$log]: $!\n";
    print $fh @_;
    $fh->close;
}


1;

__END__

=head1 NAME

CPAN::Testers::Data::Generator - Download and summarize CPAN Testers data

=head1 SYNOPSIS

  % cpanstats
  # ... wait patiently, very patiently
  # ... then use cpanstats.db, an SQLite database
  # ... or the MySQL database

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

=head1 SQLite DATABASE SCHEMA

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

=head1 v0.31 CHANGES

With the release of v0.31, a number of changes to the codebase were made as
a further move towards CPAN Testers 2.0. The first change is the name for this
distribution. Now titled 'CPAN-Testers-Data-Generator', this now fits more
appropriately within the CPAN-Testers namespace on CPAN.

The second significant change is to now reference a MySQL cpanstats database.
The SQLite version is still updated as before, as a number of other websites
and toolsets still rely on that database file format. However, in order to make
the CPAN Testers Reports website more dynamic, an SQLite database is not really
appropriate for a high demand website.

The database creation code is now available as a standalone program, in the
examples directory, and all the database communication is now handled by the
new distribution CPAN-Testers-Common-DBUtils.

=head1 INTERFACE

=head2 The Constructor

=over

=item * new

Instatiates the object CPAN::Testers::Data::Generator. Accepts a hash containing
values to prepare the object. These are described as:

  my $obj = CPAN::Testers::Data::Generator->new(
                logfile => './here/logfile',
                config  => './here/config.ini'
  );

Where 'logfile' is the location to write log messages. Log messages are only
written if a logfile entry is specified, and will always append to any existing
file. The 'config' should contain the path to the configuration file, used
to define the database access and general operation settings.

In addition the binary keys of 'ignore' and 'nostore' are available. 'ignore'
is used to ignore NNTP entries which return no article and continue processing
articles, while 'nostore' will delete all articles, except the last one
received, thus reducing space in the SQL database.

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

=back

=head2 Private Methods

=over

=item * cleanup

In the event that you do not wish to store all the articles permanently in the
articles database, this method removes all but the most recent entry, which is
kept to ensure that subsequent runs will start from the correct article. To
enable this feature, specify 'nostore' within the has passed to new().

=item * commit

To speed up the transaction process, a commit is performed every 50 inserts.
This method is used as part of the clean up process to ensure all transactions
are completed.

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
http://rt.cpan.org/Public/Dist/Display.html?Name=CPAN-Testers-Data-Generator

=head1 SEE ALSO

L<CPAN::WWW::Testers>,
L<CPAN::Testers::WWW::Statistics>

F<http://www.cpantesters.org/>,
F<http://stats.cpantesters.org/>,
F<http://wiki.cpantesters.org/>

=head1 AUTHOR

  Original author:    Leon Brocard <acme@astray.com>   (C) 2002-2008
  Current maintainer: Barbie       <barbie@cpan.org>   (C) 2008-2010

=head1 LICENSE

This code is distributed under the same license as Perl.
