package CPAN::Testers::Data::Generator;

use warnings;
use strict;
use vars qw($VERSION);

$VERSION = '0.41';

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
        $self->{OSNAMES}{lc $row->[0]} ||= $row->[1];
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

        $self->parse_article($id,$article);
        next    unless($self->{article}{guid});
        $self->cache_report();
        $self->store_report();
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
        next    unless($self->{article}{guid});
        $self->store_report();
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

        my $save_article = 0;
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
            #print STDERR "got NNTP article\n";
            $save_article = 1;
        }

        next    unless($article);
        $self->_log("ID [$id]");

        $self->parse_article($id,$article,$options);
        next    if($options && $options->{check});
        next    unless($self->{article}{guid});

        $self->{CPANSTATS}->do_query('DELETE FROM cpanstats WHERE guid = ?',$self->{article}{guid});
        $self->{LITESTATS}->do_query('DELETE FROM cpanstats WHERE guid = ?',$self->{article}{guid});
        $self->cache_report() if($save_article);
        $self->store_report();
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

    $self->{article} = { article => $article };
    my $object = CPAN::Testers::Common::Article->new($article);

    unless($object) {
        $self->_log(" ... bad parse\n");
        return;
    }

    $self->{article}{subject} = $object->subject;
    $self->{article}{from}    = $object->from;
    $self->_log(" [$self->{article}{from}] $self->{article}{subject}\n");
    return    if($self->{article}{subject} =~ /Re:/i);

    unless($self->{article}{subject} =~ /(CPAN|FAIL|PASS|NA|UNKNOWN)\s+/i) {
        $self->_log(" . [$id] ... bad subject\n");
        return;
    }

    $self->{article}{state} = lc $1;

    if($self->{article}{state} eq 'cpan') {
        $self->{article}{type} = 1;
        if($object->parse_upload()) {
            $self->{article}{dist}       = $object->distribution;
            $self->{article}{version}    = $object->version;
            $self->{article}{from}       = $object->author;
            $self->{article}{type}       = 1;
        }

        return  unless($self->_valid_field($id, 'dist'    => $self->{article}{dist})     || ($options && $options->{exclude}{dist}));
        return  unless($self->_valid_field($id, 'version' => $self->{article}{version})  || ($options && $options->{exclude}{version}));
        return  unless($self->_valid_field($id, 'author'  => $self->{article}{from})     || ($options && $options->{exclude}{from}));

    } else {
        $self->{article}{type} = 2;
        if($object->parse_report()) {
            $self->{article}{dist}       = $object->distribution;
            $self->{article}{version}    = $object->version;
            $self->{article}{from}       = $object->from;
            $self->{article}{perl}       = $object->perl;
            $self->{article}{platform}   = $object->archname;
            $self->{article}{osname}     = $self->_osname($object->osname);
            $self->{article}{osvers}     = $object->osvers;
            $self->{article}{from}       =~ s/'/''/g; #'
        }

        if($self->{DISABLE} && $self->{DISABLE}{$self->{article}{from}}) {
            $self->{article}{state} .= ':invalid';
            $self->{article}{type} = 3;
        }

        return  unless($self->_valid_field($id, 'dist'     => $self->{article}{dist})        || ($options && $options->{exclude}{dist}));
        return  unless($self->_valid_field($id, 'version'  => $self->{article}{version})     || ($options && $options->{exclude}{version}));
        return  unless($self->_valid_field($id, 'from'     => $self->{article}{from})        || ($options && $options->{exclude}{from}));
        return  unless($self->_valid_field($id, 'perl'     => $self->{article}{perl})        || ($options && $options->{exclude}{perl}));
        return  unless($self->_valid_field($id, 'platform' => $self->{article}{platform})    || ($options && $options->{exclude}{platform}));
        return  unless($self->_valid_field($id, 'osname'   => $self->{article}{osname})      || ($options && $options->{exclude}{osname}));
        return  unless($self->_valid_field($id, 'osvers'   => $self->{article}{osvers})      || ($options && $options->{exclude}{osname}));
    }

    $self->{article}{nntp} = $id;
    $self->{article}{guid} = nntp_to_guid($id);
    $self->{article}{post} = $object->postdate;
    $self->{article}{date} = $object->date;
}

sub store_report {
    my ($self) = @_;

    my @fields = map {$self->{article}{$_}} qw(guid state post from dist version platform perl osname osvers date type);
    $fields[$_] ||= 0   for(11);
    $fields[$_] ||= ''  for(0,1,2,3,4,5,6,7,8,9,10);
    $fields[$_] ||= '0' for(7);

    my %INSERT = (
        CPANSTATS => 'INSERT INTO cpanstats (guid,state,postdate,tester,dist,version,platform,perl,osname,osvers,fulldate,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        LITESTATS => 'INSERT INTO cpanstats (id,guid,state,postdate,tester,dist,version,platform,perl,osname,osvers,fulldate,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
    );

    my @rows = $self->{CPANSTATS}->get_query('array','SELECT * FROM cpanstats WHERE guid=?',$fields[0]);
    return  if(@rows);

    $self->{article}{id} = $self->{CPANSTATS}->id_query($INSERT{CPANSTATS},@fields);

    @rows = $self->{LITESTATS}->get_query('array','SELECT * FROM cpanstats WHERE guid=?',$fields[0]);
    $self->{LITESTATS}->do_query($INSERT{LITESTATS},$self->{article}{id},@fields)   unless(@rows);

    # only valid reports    
    if($self->{article}{type} == 2) {
        unshift @fields, $self->{article}{id};

        # push page requests
        # - note we only update the author if this is the *latest* version of the distribution
        my $author = $self->_get_author($fields[5],$fields[6]);
        $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('author',?,1)",$author)  if($author);
        $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('distro',?,1)",$fields[5]);

        $self->{CPANSTATS}->do_query(
            'INSERT INTO release_data ' . 
            '(dist,version,id,guid,oncpan,distmat,perlmat,patched,pass,fail,na,unknown) ' .
            'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',

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

sub cache_report {
    my $self = shift;
    return  unless($self->{article}{nntp} && $self->{article}{article});

    my @fields = map {$self->{article}{$_}} qw(nntp article);;
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
    unless($self->{OSNAMES}{$lname}) {
        $self->{OSNAMES}{$lname} = uc($name);
        $self->{CPANSTATS}->do_query(qq{INSERT INTO osname (osname,ostitle) VALUES ('$name','$self->{OSNAMES}{$lname}')});
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

With over 6 million articles in the archive, if you do plan to run this
software to generate the databases it is recommended you utilise a high-end
processor machine. Even with a reasonable processor it can take a week!

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
  | fulldate | TEXT                |
  | guid     | TEXT                |
  | type     | INTEGER             |
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

=head1 SIGNIFICANT CHANGES

=head2 v0.31 CHANGES

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

=head2 v0.41 CHANGES

In the next stage of development of CPAN Testers 2.0, the id field used within
the database schema above for the cpanstats table no longer matches the NNTP
ID value, although the id in the articles does still reference the NNTP ID.

In order to correctly reference the id in the articles table, you will need to
use the function guid_to_nntp() with CPAN::Testers::Common::Utils, using the
new guid field in the cpanstats table.

As of this release the cpanstats id field is a unique auto incrementing field.

The next release of this distribution will be focused on generation of stats
using the Metabase storage API.

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

=item * cache_report

Inserts an article into the articles database.

=item * store_report

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

L<CPAN::Testers::WWW::Statistics>

F<http://www.cpantesters.org/>,
F<http://stats.cpantesters.org/>,
F<http://wiki.cpantesters.org/>

=head1 AUTHOR

It should be noted that the original code for this distribution began life
under another name. The original distribution generated data for the original 
CPAN Testers website. However, in 2008 the code was reworked to generate data
in the format for the statistics data analysis, which in turn was reworked to 
drive the redesign of the all the CPAN Testers websites. To reflect the code 
changes, a new name was given to the distribution.

=head2 CPAN-WWW-Testers-Generator
  
  Original author:    Leon Brocard <acme@astray.com>   (C) 2002-2008
  Current maintainer: Barbie       <barbie@cpan.org>   (C) 2008-2010

=head2 CPAN-Testers-Data-Generator

  Original author:    Barbie       <barbie@cpan.org>   (C) 2008-2010

=head1 LICENSE

This code is distributed under the Artistic License 2.0.
