package CPAN::Testers::Data::Generator;

use warnings;
use strict;

use vars qw($VERSION);
$VERSION = '1.00';

#----------------------------------------------------------------------------
# Library Modules

use Config::IniFiles;
use CPAN::Testers::Common::DBUtils;
use File::Basename;
use File::Path;
use IO::File;
use JSON;
use Time::Local;

use Metabase    0.004;
use Metabase::Fact;
use CPAN::Testers::Fact::LegacyReport;
use CPAN::Testers::Fact::TestSummary;
use CPAN::Testers::Metabase::AWS;
use CPAN::Testers::Report;

#----------------------------------------------------------------------------
# Variables

my %testers;

my $FROM    = 'CPAN Tester Report Server <do_not_reply@cpantesters.org>';
my $HOW     = '/usr/sbin/sendmail -bm';
my $HEAD    = 'To: EMAIL
From: FROM
Date: DATE
Subject: CPAN Testers Generator Error Report

';

my $BODY    = '
The following reports failed to parse into the cpanstats database:

INVALID

Thanks,
CPAN Testers Server.
';

#----------------------------------------------------------------------------
# The Application Programming Interface

sub new {
    my $class = shift;
    my %hash  = @_;

    my $self = {
        meta_count  => 0,
        stat_count  => 0,
        last        => '',
    };
    bless $self, $class;

    # load configuration
    my $cfg = Config::IniFiles->new( -file => $hash{config} );

    # configure databases
    for my $db (qw(CPANSTATS LITESTATS METABASE)) {
        die "No configuration for $db database\n"   unless($cfg->SectionExists($db));
        my %opts = map {$_ => ($cfg->val($db,$_)||undef);} qw(driver database dbfile dbhost dbport dbuser dbpass);
        $opts{AutoCommit} = 0;
        $self->{$db} = CPAN::Testers::Common::DBUtils->new(%opts);
        die "Cannot configure $db database\n" unless($self->{$db});
        $self->{$db}->{'mysql_enable_utf8'} = 1 if($opts{driver} =~ /mysql/i);
    }

    if($cfg->SectionExists('ADMINISTRATION')) {
        my @admins = $cfg->val('ADMINISTRATION','admins');
        $self->{admins} = \@admins;
    }

    # command line swtiches override configuration settings
    for my $key (qw(logfile poll_limit stopfile offset)) {
        $self->{$key} = $hash{$key} || $cfg->val('MAIN',$key);
    }

    $self->{offset}     ||= 1;
    $self->{poll_limit} ||= 1000;

    my @rows = $self->{METABASE}->get_query('hash','SELECT * FROM testers_email');
    for my $row (@rows) {
        $testers{$row->{resource}} = $row->{email};
    }

    @rows = $self->{CPANSTATS}->get_query('array','SELECT osname,ostitle FROM osname');
    for my $row (@rows) {
        $self->{OSNAMES}{lc $row->[0]} ||= $row->[1];
    }

    if($cfg->SectionExists('DISABLE')) {
        my @values = $cfg->val('DISABLE','LIST');
        $self->{DISABLE}{$_} = 1    for(@values);
    }

    $self->{metabase} = CPAN::Testers::Metabase::AWS->new(
        bucket      => 'cpantesters',
        namespace   => 'beta2',
    );
    $self->{librarian} = $self->{metabase}->public_librarian;

    return $self;
}

sub DESTROY {
    my $self = shift;
}

#----------------------------------------------------------------------------
# Public Methods

sub generate {
    my $self    = shift;
    my $nonstop = shift || 0;

$self->_log("START GENERATE nonstop=$nonstop\n");

    do {

    my ($processed,$stored,$cached) = (0,0,0);
    my $start = localtime(time);

    my $guids = $self->get_next_guids();
    if($guids) {
        for my $guid (@$guids) {
            $self->_log("GUID [$guid]");
            $processed++;

            #if($self->already_saved($guid)) {
            #    $self->_log(".. already saved\n");
            #    next;
            #}

            if(my $report = $self->get_fact($guid)) {
                $self->{report}{guid}   = $guid;
                next    if($self->parse_report(report => $report));

                if($self->store_report()) { $self->_log(".. stored"); $stored++; }
                else                      {
                    if($self->{time} gt $self->{report}{updated}) {
                        $self->_log(".. FAIL: older than requested [$self->{time}]\n");
                        next;
                    }
                    $self->_log(".. already stored");
                }
                if($self->cache_report()) { $self->_log(".. cached\n"); $cached++; }
                else                      { $self->_log(".. already cached\n"); }
            } else {
                $self->_log(".. FAIL\n");
            }
        }
    }

    $self->commit();
    my $invalid = $self->{invalid} ? scalar(@{$self->{invalid}}) : 0;
    my $stop = localtime(time);
    $self->_log("MARKER: processed=$processed, stored=$stored, cached=$cached, invalid=$invalid, start=$start, stop=$stop\n");

    # only email invalid reports during the generate process
    $self->_send_email()    if($self->{invalid});

    $nonstop = 0	if($processed == 0);
    $nonstop = 0	if($self->{stopfile} && -f $self->{stopfile});

$self->_log("CHECK nonstop=$nonstop\n");
    } while($nonstop);
$self->_log("STOP GENERATE nonstop=$nonstop\n");
}

sub regenerate {
    my ($self,%hash) = @_;
    $self->{reparse} = 1;

$self->_log("START REGENERATE\n");

    $hash{dstart} = $self->_get_createdate( $hash{gstart}, $hash{dstart} );
    $hash{dend}   = $self->_get_createdate( $hash{gend},   $hash{dend} );

$self->_log("dstart=$hash{dstart}, dend=$hash{dend}\n");
print STDERR "#\ndstart=$hash{dstart}, dend=$hash{dend}\n";

    my @where;
    push @where, "updated >= $hash{dstart}"  if($hash{dstart});
    push @where, "updated <= $hash{dend}"    if($hash{dend});
    
    my $sql =   'SELECT guid FROM metabase' . 
                (@where ? ' WHERE ' . join(' AND ',@where) : '') .
                ' ORDER BY updated asc';

    my @guids = $self->{METABASE}->get_query('hash',$sql);
    my %guids = map {$_->{guid} => 1} @guids;

    my ($processed,$stored,$cached) = (0,0,0);
    my $start = $hash{dstart};

    my $last = $start;
    while($start le $hash{dend}) {
        my $guids = $self->get_next_guids($start);
        if($guids) {
            for my $guid (@$guids) {
                $self->_log("GUID [$guid]");
                $processed++;

                if($guids{$guid}) {
                    $self->_log(".. already saved\n");
                    next;
                }

                if(my $report = $self->get_fact($guid)) {
                    $start = $report->{metadata}{core}{update_time};
                    $self->{report}{guid}   = $guid;
                    next    if($self->parse_report(report => $report));

                    if($self->store_report()) { $self->_log(".. stored"); $stored++;    }
                    else                      { $self->_log(".. already stored");       }
                    if($self->cache_report()) { $self->_log(".. cached\n"); $cached++;  }
                    else                      { $self->_log(".. already cached\n");     }
                } else {
                    $self->_log(".. FAIL\n");
                }
            }
        }

        $self->commit();

        last    if($start eq $last);
        $last = $start;
    }

    $self->commit();
    my $invalid = $self->{invalid} ? scalar(@{$self->{invalid}}) : 0;
    my $stop = localtime(time);
    $self->_log("MARKER: processed=$processed, stored=$stored, cached=$cached, invalid=$invalid, start=$start, stop=$stop\n");

    # only email invalid reports during the generate process
    $self->_send_email()    if($self->{invalid});

$self->_log("STOP REGENERATE last=$last\n");

    return $last;
}

sub rebuild {
    my ($self,%hash) = @_;
    $self->{reparse} = 1;
    my ($processed,$stored,$cached) = (0,0,0);
    my $start = localtime(time);

$self->_log("START REBUILD\n");

    # selection choices:
    # 1) from guid [to guid]
    # 2) from date [to date]

    $hash{dstart} = $self->_get_createdate( $hash{gstart}, $hash{dstart} );
    $hash{dend}   = $self->_get_createdate( $hash{gend},   $hash{dend} );

    my @where;
    push @where, "updated >= $hash{dstart}"  if($hash{dstart});
    push @where, "updated <= $hash{dend}"    if($hash{dend});
    
    my $sql =   'SELECT * FROM metabase' . 
                (@where ? ' WHERE ' . join(' AND ',@where) : '') .
                ' ORDER BY updated asc';

#    $self->{CPANSTATS}->do_query("DELETE FROM cpanstats WHERE id >= $start AND id <= $end");
#    $self->{LITESTATS}->do_query("DELETE FROM cpanstats WHERE id >= $start AND id <= $end");

    my $iterator = $self->{METABASE}->iterator('hash',$sql);
    while(my $row = $iterator->()) {
        $self->_log("GUID [$row->{guid}]");
        $processed++;

        # no article for that id!
        unless($row->{report}) {
            $self->_log(" ... no report\n");
            warn "No report returned [$row->{id},$row->{guid}]\n";
            next;
        }

        $self->{report}{id}       = $row->{id};
        $self->{report}{guid}     = $row->{guid};
        $self->{report}{metabase} = decode_json($row->{report});
        $self->reparse_report();
        $self->store_report();
        $self->cache_update();
        $self->_log(".. stored\n");

        $stored++;
        $cached++;
    }

    my $invalid = $self->{invalid} ? scalar(@{$self->{invalid}}) : 0;
    my $stop = localtime(time);
    $self->_log("MARKER: processed=$processed, stored=$stored, cached=$cached, invalid=$invalid, start=$start, stop=$stop\n");

    $self->commit();
$self->_log("STOP REBUILD\n");
}

sub reparse {
    my ($self,$guid) = @_;
    return  unless($guid);

    $self->{reparse} = 1;

    if(my $report = $self->get_fact($guid)) {
        $self->{report}{guid} = $guid;
        if($self->parse_report(report => $report)) {
            $self->_log(".. cannot parse report\n");
            return;
        }

        if($self->store_report()) { $self->_log(".. stored"); }
        else                      {
            if($self->{time} gt $self->{report}{updated}) {
                $self->_log(".. FAIL: older than requested [$self->{time}]\n");
                return;
            }
            $self->_log(".. already stored");
        }
        if($self->cache_report()) { $self->_log(".. cached\n"); }
        else                      { $self->_log(".. already cached\n"); }
    } else {
        $self->_log(".. FAIL\n");
        return;
    }

    $self->commit();
}

#----------------------------------------------------------------------------
# Private Methods

sub commit {
    my $self = shift;
    for(qw(CPANSTATS LITESTATS)) {
        next    unless($self->{$_});
        $self->{$_}->do_commit;
    }
}

sub get_next_guids {
    my ($self,$dstart) = @_;
    my $guids;
    my $time;

    if($dstart) {
        $self->{time} = $dstart;
    } else {
        my @rows = $self->{METABASE}->get_query('array','SELECT max(updated) FROM metabase');
        $self->{time} = $rows[0]->[0]	if(@rows);

        $self->{time} ||= '1999-01-01T00:00:00Z';
        if($self->{last} ge $self->{time}) {
            my @ts = $self->{last} =~ /(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)Z/;
            $ts[1]--;
            my $ts = timelocal(reverse @ts);
            @ts = localtime($ts + $self->{offset}); # increment the offset for next time
            $self->{time} = sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ", $ts[5]+1900,$ts[4]+1,$ts[3], $ts[2],$ts[1],$ts[0];
        }
    }

    $self->_log("START time=[$self->{time}], last=[$self->{last}]\n");
    $self->{last} = $self->{time};

    eval {
    	$guids = $self->{librarian}->search(
        	'core.type'         => 'CPAN-Testers-Report',
        	'core.update_time'  => { ">=", $self->{time} },
        	'-asc'              => 'core.update_time',
        	'-limit'            => $self->{poll_limit},
    	);
    };

    $self->_log(" ... Metabase Search Failed [$@]\n") if($@);
    #if($guids) {
        $self->_log("START guids=[".scalar(@$guids)."]\n");
    #}

    return $guids;
}

sub already_saved {
    my ($self,$guid) = @_;
    my @rows = $self->{METABASE}->get_query('array','SELECT id FROM metabase WHERE guid=?',$guid);
    return 1	if(@rows);
    return 0;
}

sub get_fact {
    my ($self,$guid) = @_;
    my $fact;
    #print STDERR "guid=$guid\n";
    eval { $fact = $self->{librarian}->extract( $guid ) };
    return $fact    if($fact);

    $self->_log(" ... no report [$@]\n");
    return;
}

sub parse_report {
    my ($self,%hash) = @_;
    my $options = $hash{options};
    my $report  = $hash{report};
    my $guid    = $self->{report}{guid};
    my $invalid;

    $self->{report}{created} = $report->{metadata}{core}{creation_time};
    $self->{report}{updated} = $report->{metadata}{core}{update_time};

    my @facts = $report->facts();
    for my $fact (@facts) {
        if(ref $fact eq 'CPAN::Testers::Fact::TestSummary') {
            $self->{report}{metabase}{'CPAN::Testers::Fact::TestSummary'} = $fact->as_struct ;

            $self->{report}{state}      = lc $fact->{content}{grade};
            $self->{report}{platform}   = $fact->{content}{archname};
            $self->{report}{osname}     = $self->_osname($fact->{content}{osname});
            $self->{report}{osvers}     = $fact->{content}{osversion};
            $self->{report}{perl}       = $fact->{content}{perl_version};
            #$self->{report}{created}    = $fact->{metadata}{core}{creation_time};
            #$self->{report}{updated}    = $fact->{metadata}{core}{update_time};

            my $dist                    = Metabase::Resource->new( $fact->resource );
            $self->{report}{dist}       = $dist->metadata->{dist_name};
            $self->{report}{version}    = $dist->metadata->{dist_version};

            $self->{report}{from}       = $self->_get_tester( $fact->creator->resource );

            # alternative API
            #my $profile                 = $fact->creator->user;                                                                                                                                                                          
            #$self->{report}{from}       = $profile->{email};
            #$self->{report}{from}       =~ s/'/''/g; #'
            #$self->{report}{dist}       = $fact->resource->dist_name;                                                                                                                                                                 
            #$self->{report}{version}    = $fact->resource->dist_version;          

        } elsif(ref $fact eq 'CPAN::Testers::Fact::LegacyReport') {
            $self->{report}{metabase}{'CPAN::Testers::Fact::LegacyReport'} = $fact->as_struct;
            $invalid = 'missing textreport' if(length $fact->{content}{textreport} < 10);   # what is the smallest report?

            $self->{report}{perl}       = $fact->{content}{perl_version};
        }
    }

    if($invalid) {
        push @{$self->{invalid}}, {msg => $invalid, guid => $guid};
        return 1;
    }

    if($self->{report}{created}) {
        my @created = $self->{report}{created} =~ /(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)Z/; # 2010-02-23T20:33:52Z
        $self->{report}{postdate}   = sprintf "%04d%02d", $created[0], $created[1];
        $self->{report}{fulldate}   = sprintf "%04d%02d%02d%02d%02d", $created[0], $created[1], $created[2], $created[3], $created[4];
    } else {
        my @created = localtime(time);
        $self->{report}{postdate}   = sprintf "%04d%02d", $created[5]+1900, $created[4]+1;
        $self->{report}{fulldate}   = sprintf "%04d%02d%02d%02d%02d", $created[5]+1900, $created[4]+1, $created[3], $created[2], $created[1];
    }

$self->_log(".. time [$self->{report}{created}][$self->{report}{updated}]");

    $self->{report}{type}       = 2;
    if($self->{DISABLE} && $self->{DISABLE}{$self->{report}{from}}) {
        $self->{report}{state} .= ':invalid';
        $self->{report}{type}   = 3;
    }

    #use Data::Dumper;
    #print STDERR "\n====\nreport=".Dumper($self->{report});

    return 1  unless($self->_valid_field($guid, 'dist'     => $self->{report}{dist})     || ($options && $options->{exclude}{dist}));
    return 1  unless($self->_valid_field($guid, 'version'  => $self->{report}{version})  || ($options && $options->{exclude}{version}));
    return 1  unless($self->_valid_field($guid, 'from'     => $self->{report}{from})     || ($options && $options->{exclude}{from}));
    return 1  unless($self->_valid_field($guid, 'perl'     => $self->{report}{perl})     || ($options && $options->{exclude}{perl}));
    return 1  unless($self->_valid_field($guid, 'platform' => $self->{report}{platform}) || ($options && $options->{exclude}{platform}));
    return 1  unless($self->_valid_field($guid, 'osname'   => $self->{report}{osname})   || ($options && $options->{exclude}{osname}));
    return 1  unless($self->_valid_field($guid, 'osvers'   => $self->{report}{osvers})   || ($options && $options->{exclude}{osname}));

    return 0
}

sub reparse_report {
    my ($self,%hash) = @_;
    my $fact = 'CPAN::Testers::Fact::TestSummary';
    my $options = $hash{options};
    my $report  = CPAN::Testers::Fact::TestSummary->from_struct( $self->{report}{metabase}{$fact} );
    my $guid    = $self->{report}{guid};

    $self->{report}{state}      = lc $report->{content}{grade};
    $self->{report}{platform}   = $report->{content}{archname};
    $self->{report}{osname}     = $self->_osname($report->{content}{osname});
    $self->{report}{osvers}     = $report->{content}{osversion};
    $self->{report}{perl}       = $report->{content}{perl_version};
    $self->{report}{created}    = $report->{metadata}{core}{creation_time};

    my $dist                    = Metabase::Resource->new( $report->{metadata}{core}{resource} );
    $self->{report}{dist}       = $dist->metadata->{dist_name};
    $self->{report}{version}    = $dist->metadata->{dist_version};

    $self->{report}{from}       = $self->_get_tester( $report->{metadata}{core}{creator}{resource} );

    if($self->{report}{created}) {
        my @created = $self->{report}{created} =~ /(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)Z/; # 2010-02-23T20:33:52Z
        $self->{report}{postdate}   = sprintf "%04d%02d", $created[0], $created[1];
        $self->{report}{fulldate}   = sprintf "%04d%02d%02d%02d%02d", $created[0], $created[1], $created[2], $created[3], $created[4];
    } else {
        my @created = localtime(time);
        $self->{report}{postdate}   = sprintf "%04d%02d", $created[5]+1900, $created[4]+1;
        $self->{report}{fulldate}   = sprintf "%04d%02d%02d%02d%02d", $created[5]+1900, $created[4]+1, $created[3], $created[2], $created[1];
    }

    $self->{report}{type}       = 2;
    if($self->{DISABLE} && $self->{DISABLE}{$self->{report}{from}}) {
        $self->{report}{state} .= ':invalid';
        $self->{report}{type}   = 3;
    }

    return  unless($self->_valid_field($guid, 'dist'     => $self->{report}{dist})     || ($options && $options->{exclude}{dist}));
    return  unless($self->_valid_field($guid, 'version'  => $self->{report}{version})  || ($options && $options->{exclude}{version}));
    return  unless($self->_valid_field($guid, 'from'     => $self->{report}{from})     || ($options && $options->{exclude}{from}));
    return  unless($self->_valid_field($guid, 'perl'     => $self->{report}{perl})     || ($options && $options->{exclude}{perl}));
    return  unless($self->_valid_field($guid, 'platform' => $self->{report}{platform}) || ($options && $options->{exclude}{platform}));
    return  unless($self->_valid_field($guid, 'osname'   => $self->{report}{osname})   || ($options && $options->{exclude}{osname}));
    return  unless($self->_valid_field($guid, 'osvers'   => $self->{report}{osvers})   || ($options && $options->{exclude}{osname}));
}

sub retrieve_report {
    my $self = shift;
    my $guid = shift or return;

    my @rows = $self->{CPANSTATS}->get_query('hash','SELECT * FROM cpanstats WHERE guid=?',$guid);
    return $rows[0] if(@rows);
    return;
}

sub store_report {
    my $self = shift;

    my @fields = map {$self->{report}{$_}} qw(guid state postdate from dist version platform perl osname osvers fulldate type);
    $fields[$_] ||= 0   for(11);
    $fields[$_] ||= ''  for(0,1,2,3,4,5,6,7,8,9,10);
    $fields[$_] ||= '0' for(7);

    my %SQL = (
        'SELECT' => {
            CPANSTATS => 'SELECT id FROM cpanstats WHERE guid=?',
            LITESTATS => 'SELECT id FROM cpanstats WHERE guid=?',
            RELEASE   => 'SELECT id FROM release_data WHERE guid=?',
        },
        'INSERT' => {
            CPANSTATS => 'INSERT INTO cpanstats (guid,state,postdate,tester,dist,version,platform,perl,osname,osvers,fulldate,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            LITESTATS => 'INSERT INTO cpanstats (id,guid,state,postdate,tester,dist,version,platform,perl,osname,osvers,date,type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            RELEASE   => 'INSERT INTO release_data (id,guid,dist,version,oncpan,distmat,perlmat,patched,pass,fail,na,unknown) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        },
        'UPDATE' => {
            CPANSTATS => 'UPDATE cpanstats SET state=?,postdate=?,tester=?,dist=?,version=?,platform=?,perl=?,osname=?,osvers=?,fulldate=?,type=? WHERE guid=?',
            LITESTATS => 'UPDATE cpanstats SET state=?,postdate=?,tester=?,dist=?,version=?,platform=?,perl=?,osname=?,osvers=?,fulldate=?,type=? WHERE guid=?',
            RELEASE   => 'UPDATE release_data SET id=?,dist=?,version=?,oncpan=?,distmat=?,perlmat=?,patched=?,pass=?,fail=?,na=?,unknown=? WHERE guid=?',
        },
    );

    # update the mysql database
    my @rows = $self->{CPANSTATS}->get_query('array',$SQL{SELECT}{CPANSTATS},$fields[0]);
    if(@rows) {
        if($self->{reparse}) {
            my ($guid,@update) = @fields;
            $self->{CPANSTATS}->do_query($SQL{UPDATE}{CPANSTATS},@update,$guid);
        } else {
            $self->{report}{id} = $rows[0]->[0];
            return 0;
        }
    } else {
        $self->{report}{id} = $self->{CPANSTATS}->id_query($SQL{INSERT}{CPANSTATS},@fields);
    }

    # update the sqlite database
    @rows = $self->{LITESTATS}->get_query('array',$SQL{SELECT}{LITESTATS},$fields[0]);
    if(@rows) {
        if($self->{reparse}) {
            my ($guid,@update) = @fields;
            $self->{LITESTATS}->do_query($SQL{UPDATE}{LITESTATS},@update,$guid);
        }
    } else {
        $self->{LITESTATS}->do_query($SQL{INSERT}{LITESTATS},$self->{report}{id},@fields);
    }

    # only valid reports
    if($self->{report}{type} == 2) {
        unshift @fields, $self->{report}{id};

        # push page requests
        # - note we only update the author if this is the *latest* version of the distribution
        my $author = $self->{report}{pauseid} || $self->_get_author($fields[5],$fields[6]);
        $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('author',?,1)",$author)  if($author);
        $self->{CPANSTATS}->do_query("INSERT INTO page_requests (type,name,weight) VALUES ('distro',?,1)",$fields[5]);

        my @rows = $self->{CPANSTATS}->get_query('array',$SQL{SELECT}{RELEASE},$fields[1]);
        #print STDERR "# select release $SQL{SELECT}{RELEASE},$fields[1]\n";
        if(@rows) {
            if($self->{reparse}) {
                $self->{CPANSTATS}->do_query($SQL{UPDATE}{RELEASE},
                    $fields[0],             # id,
                    $fields[5],$fields[6],  # dist, version

                    $self->_oncpan($fields[5],$fields[6])   ? 1 : 2,

                    $fields[6] =~ /_/                       ? 2 : 1,
                    $fields[8] =~ /^5.(7|9|[1-9][13579])/   ? 2 : 1,    # odd numbers now mark development releases
                    $fields[8] =~ /(RC\d+|patch)/           ? 2 : 1,

                    $fields[2] eq 'pass'    ? 1 : 0,
                    $fields[2] eq 'fail'    ? 1 : 0,
                    $fields[2] eq 'na'      ? 1 : 0,
                    $fields[2] eq 'unknown' ? 1 : 0,

                    $fields[1]);    # guid
            }
        } else {
        #print STDERR "# insert release $SQL{INSERT}{RELEASE},$fields[0],$fields[1]\n";
            $self->{CPANSTATS}->do_query($SQL{INSERT}{RELEASE},
                $fields[0],$fields[1],  # id, guid
                $fields[5],$fields[6],  # dist, version

                $self->_oncpan($fields[5],$fields[6])   ? 1 : 2,

                $fields[6] =~ /_/                       ? 2 : 1,
                $fields[8] =~ /^5.(7|9|[1-9][13579])/   ? 2 : 1,    # odd numbers now mark development releases
                $fields[8] =~ /(RC\d+|patch)/           ? 2 : 1,

                $fields[2] eq 'pass'    ? 1 : 0,
                $fields[2] eq 'fail'    ? 1 : 0,
                $fields[2] eq 'na'      ? 1 : 0,
                $fields[2] eq 'unknown' ? 1 : 0);
        }
    }

    if((++$self->{stat_count} % 500) == 0) {
        $self->commit;
    }

    return 1;
}

sub cache_report {
    my $self = shift;
    return  unless($self->{report}{guid} && $self->{report}{metabase});

    $self->{'METABASE'}->do_query('INSERT IGNORE INTO metabase (guid,id,updated,report) VALUES (?,?,?,?)',
        $self->{report}{guid},$self->{report}{id},$self->{report}{updated},encode_json($self->{report}{metabase}));

    if((++$self->{meta_count} % 500) == 0) {
        $self->{CPANSTATS}->do_commit;
    }

    return 1;
}

sub cache_update {
    my $self = shift;
    return  unless($self->{report}{guid} && $self->{report}{id});

    $self->{'METABASE'}->do_query('UPDATE metabase SET id=? WHERE guid=?',$self->{report}{id},$self->{report}{guid});

    if((++$self->{meta_count} % 500) == 0) {
        $self->{CPANSTATS}->do_commit;
    }

    return 1;
}

#----------------------------------------------------------------------------
# Private Functions

sub _get_createdate {
    my ($self,$guid,$date) = @_;

    return  unless($guid || $date);
    if($guid) {
        my @rows = $self->{METABASE}->get_query('hash','SELECT updated FROM metabase WHERE guid=?',$guid);
        $date = $rows[0]->{updated}  if(@rows);
    }

    return $date    if($date =~ /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
    return;        
}

sub _get_tester {
    my ($self,$creator) = @_;
    return $testers{$creator}   if($testers{$creator});

    my $profile  = Metabase::Resource->new( $creator );
    return $creator unless($profile);

    my $user;
    eval { $user = $self->{librarian}->extract( $profile->guid ) };
    return $creator unless($user);

    my ($name,@emails);
    for my $fact ($user->facts()) {
        if(ref $fact eq 'Metabase::User::EmailAddress') {
            push @emails, $fact->{content};
        } elsif(ref $fact eq 'Metabase::User::FullName') {
            $name = $fact->{content};
        }
    }

    $name ||= 'NONAME'; # shouldn't happen, but allows for checks later

    for my $em (@emails) {
        $self->{METABASE}->do_query('INSERT INTO testers_email (resource,fullname,email) VALUES (?,?,?)',$creator,$name,$em);
    }

    $testers{$creator} = @emails ? $emails[0] : $creator;
    $testers{$creator} =~ s/\'/''/g if($testers{$creator});
    return $testers{$creator};
}

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

    my @rows = $self->{METABASE}->get_query('array',"SELECT max(id) FROM metabase");
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

sub _send_email {
    my $self = shift;
    my $t = localtime;
    my $DATE = $t->strftime("%a, %d %b %Y %H:%M:%S +0000");
    $DATE =~ s/\s+$//;
    my $INVALID = join("\n",@{$self->{invalid}});
    $self->_log("INVALID:\n$INVALID\n");

    for my $admin (@{$self->{admins}}) {
        my $cmd = qq!| $HOW $admin!;

        my $body = $HEAD . $BODY;
        $body =~ s/FROM/$FROM/g;
        $body =~ s/EMAIL/$admin/g;
        $body =~ s/DATE/$DATE/g;
        $body =~ s/INVALID/$INVALID/g;

        if(my $fh = IO::File->new($cmd)) {
            print $fh $body;
            $fh->close;
            $self->_log(".. MAIL SEND - SUCCESS - $admin\n");
        } else {
            $self->_log(".. MAIL SEND - FAILED - $admin\n");
        }
    }
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
  | date     | TEXT                |
  | guid     | TEXT                |
  | type     | INTEGER             |
  +----------+---------------------+

It should be noted that 'postdate' refers to the YYYYMM formatted date, whereas
the 'date' field refers to the YYYYMMDDhhmm formatted date and time.

The metabase database schema is again very straightforward, and consists of one
table, as below:

  +--------------------------------+
  | metabase                       |
  +----------+---------------------+
  | guid     | TEXT PRIMARY KEY    |
  | report   | TEXT                |
  +----------+---------------------+

The report field is JSON encoded, and is a cached version of the one extracted
from Metabase::Librarian.

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

=head2 v1.00 CHANGES

Moved to Metabase API. The change to a definite major version number hopefully
indicates that this is a major interface change. All previous NNTP access has
been dropped and is no longer relavent. All report updates are now fed from
the Metabase API.

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

=back

=head2 Public Methods

=over

=item * generate

Starting from the last cached report, retrieves all the more recent reports
from the Metabase Report Submission server, parsing each and recording each
report in both the cpanstats databases (MySQL & SQLite) and the metabase cache
database.

=item * regenerate

For a given date range, retrieves all the reports from the Metabase Report 
Submission server, parsing each and recording each report in both the cpanstats
databases (MySQL & SQLite) and the metabase cache database.

Note that as only 2500 can be returned at any one time due to Amazon SimpleDB
restrictions, this method will only process the guids returned from a given
start data, up to a maxiumu of 2500 guids.

This methog will return the guid of the last report processed.

=item * rebuild

In the event that the cpanstats database needs regenerating, either in part or
for the whole database, this method allow you to do so. You may supply
parameters as to the 'start' and 'end' values (inclusive), where all records
are assumed by default. Records are rebuilt using the local metabase cache
database.

=item * reparse

Rather than a complete rebuild the option to selective reparse selected entries
is useful if there are reports which were previously unable to correctly supply
a particular field, which now has supporting parsing code within the codebase.

In addition there is the option to exclude fields from parsing checks, where
they may be corrupted, and can be later amended using the 'cpanstats-update'
tool.

=back

=head2 Private Methods

=over

=item * commit

To speed up the transaction process, a commit is performed every 500 inserts.
This method is used as part of the clean up process to ensure all transactions
are completed.

=item * get_next_guids

Get the list of GUIDs for the reports that have been submitted since the last
cached report.

=item * get_fact

Get a specific report factfor a given GUID.

=item * parse_report

Parses a report extracting the metadata required for the cpanstats database.

=item * reparse_report

Parses a report (from a local metabase cache) extracting the metadata required
for the stats database.

=item * retrieve_report

Given a guid will attempt to return the report metadata from the cpanstats 
database.

=item * store_report

Inserts the components of a parsed report into the cpanstats database.

=item * cache_report

Inserts a serialised report into a local metabase cache database.

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

L<CPAN::Testers::Report>,
L<Metabase>,
L<Metabase::Fact>,
L<CPAN::Testers::Fact::LegacyReport>,
L<CPAN::Testers::Fact::TestSummary>,
L<CPAN::Testers::Metabase::AWS>

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
