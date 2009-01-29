package CPAN::Testers::Data::Generator::Article;

use warnings;
use strict;
use vars qw($VERSION);

$VERSION = '0.35';

#----------------------------------------------------------------------------
# Library Modules

use CPAN::DistnameInfo;
use Email::Simple;
use MIME::Base64;
use MIME::QuotedPrint;
use Time::Local;

use base qw( Class::Accessor::Fast );

#----------------------------------------------------------------------------
# Variables

my %month = (
	Jan => 1, Feb => 2, Mar => 3, Apr => 4,  May => 5,  Jun => 6,
	Jul => 7, Aug => 8, Sep => 9, Oct => 10, Nov => 11, Dec => 12,
);

my %regexes = (
    # with time
    1 => { re => qr/(?:\w+,)?\s+(\d+)\s+(\w+)\s+(\d+)\s+(\d+):(\d+)/,   f => [qw(day month year hour min)] },     # Wed, 13 September 2004 06:29
    2 => { re => qr/(\d+)\s+(\w+)\s+(\d+)\s+(\d+):(\d+)/,               f => [qw(day month year hour min)] },     # 13 September 2004 06:29
    3 => { re => qr/(\w+)?\s+(\d+),?\s+(\d+)\s+(\d+):(\d+)/,            f => [qw(month day year hour min)] },     # September 22, 1999 06:29

    # just the date
    4 => { re => qr/(?:\w+,)?\s+(\d+)\s+(\w+)\s+(\d+)/, f => [qw(day month year)] },  # Wed, 13 September 2004
    5 => { re => qr/(\d+)\s+(\w+)\s+(\d+)/,             f => [qw(day month year)] },  # 13 September 2004
    6 => { re => qr/(\w+)?\s+(\d+),?\s+(\d+)/,          f => [qw(month day year)] },  # September 22, 1999 06:29
);

my $OSNAMES = qr/(cygwin|freebsd|netbsd|openbsd|darwin|linux|cygwin|darwin|MSWin32|dragonfly|solaris|MacOS|irix|mirbsd|gnu|bsdos|aix|sco|os2)/i;
my %OSNAMES = (
    'MacPPC'    => 'macos',
    'osf'       => 'dec_osf',
    'pa-risc'   => 'hpux',
    's390'      => 'os390',
    'VMS_'      => 'vms',
    'ARCHREV_0' => 'hpux',
);

#----------------------------------------------------------------------------
# The Application Programming Interface

__PACKAGE__->mk_accessors(qw(
                    postdate date epoch status from distribution version
                    perl osname osvers archname subject author filename));

sub new {
    my($class, $article) = @_;
    my $self = {};
    bless $self, $class;

    $article = decode_qp($article)	if($article =~ /=3D/);

    my $mail = Email::Simple->new($article);
    return unless $mail;
    return if $mail->header("In-Reply-To");

    my $from    = $mail->header("From");
    my $subject = $mail->header("Subject");
    return unless $subject;
    return if $subject =~ /::/; # it's supposed to be a distribution

    $self->{mail}    = $mail;
    $self->{from}    = $from;
    $self->{subject} = $subject;

    ($self->{postdate},$self->{date},$self->{epoch}) = _parse_date($mail);

    return $self;
}

sub _parse_date {
    my $mail = shift;
    my ($date1,$date2,$date3) = _extract_date($mail->header("Date"));
    my @received  = $mail->header("Received");

    for my $hdr (@received) {
        next    unless($hdr =~ /.*;\s+(.*)\s*$/);
        my ($dt1,$dt2,$dt3) = _extract_date($1);
        if($dt2 > $date2 + 1200) {
            $date1 = $dt1;
            $date2 = $dt2;
            $date3 = $dt3;
        }
    }

#print STDERR "        ... X.[Date: ".($date||'')."]\n";
    return($date1,$date2,$date3);
}

sub _extract_date {
    my $date = shift;
    my (%fields,@fields,$index);

#print STDERR "#        ... 0.[Date: ".($date||'')."]\n";

    for my $inx (sort {$a <=> $b} keys %regexes) {
        (@fields) = ($date =~ $regexes{$inx}->{re});
        if(@fields) {
            $index = $inx;
            last;
        }
    }

    return('000000','000000000000',0) unless(@fields && $index);

    @fields{@{$regexes{$index}->{f}}} = @fields;

    $fields{month} = substr($fields{month},0,3);
    $fields{mon}   = $month{$fields{month}};
    return('000000','000000000000',0) unless($fields{mon} && $fields{year} > 1998);

    $fields{$_} ||= 0          for(qw(sec min hour day mon year));
    my @date = map { $fields{$_} } qw(sec min hour day mon year);

#print STDERR "#        ... 1.[$_][$fields{$_}]\n"   for(qw(year month day hour min));
    my $short = sprintf "%04d%02d",             $fields{year}, $fields{mon};
    my $long  = sprintf "%04d%02d%02d%02d%02d", $fields{year}, $fields{mon}, $fields{day}, $fields{hour}, $fields{min};
    $date[4]--;
    my $epoch = timelocal(@date);

    return($short,$long,$epoch);
}

sub parse_upload {
    my $self = shift;
    my $mail = $self->{mail};
    my $subject = $self->{subject};

    return 0	unless($subject =~ /CPAN Upload:\s+([-\w\/\.\+]+)/i);
    my $distvers = $1;

    # only record supported archives
    return 0    if($distvers !~ /\.(?:(?:tar\.|t)(?:gz|bz2)|zip)$/);

    # CPAN::DistnameInfo doesn't support .tar.bz2 files ... yet
    $distvers =~ s/\.(?:tar\.|t)bz2$//i;
    $distvers .= '.tar.gz' unless $distvers =~ /\.(?:(?:tar\.|t)gz|zip)$/i;

    # CPAN::DistnameInfo doesn't support old form of uploads
    my @parts = split("/",$distvers);
    if(@parts == 2) {
        my ($first,$second,$rest) = split(//,$distvers,3);
        $distvers = "$first/$first$second/$first$second$rest";
    }

    my $d = CPAN::DistnameInfo->new($distvers);
    $self->distribution($d->dist);
    $self->version($d->version);
    $self->author($d->cpanid);
    $self->filename($d->filename);

    return 1;
}

sub parse_report {
    my $self = shift;
    my $mail = $self->{mail};
    my $from = $self->{from};
    my $subject = $self->{subject};

    my ($status, $distversion, $platform, $osver) = split /\s+/, $subject;
    return 0  unless $status =~ /^(PASS|FAIL|UNKNOWN|NA)$/i;

    $platform ||= "";
    $platform =~ s/[\s&,<].*//;

    $distversion =~ s!/$!!;
    $distversion =~ s/\.tar.*/.tar.gz/;
    $distversion .= '.tar.gz' unless $distversion =~ /\.(tar|tgz|zip)/;

    my $d = CPAN::DistnameInfo->new($distversion);
    my ($dist, $version) = ($d->dist, $d->version);
    return 0 unless defined $dist;
    return 0 unless defined $version;

    my $encoding = $mail->header('Content-Transfer-Encoding');

    my $body = $mail->body;
    $body = decode_base64($body)  if($encoding && $encoding eq 'base64');

    my $perl = $self->_extract_perl_version(\$body);

    my ($osname)   = $body =~ /(?:Summary of my perl5|Platform:).*?osname=([^\s\n,<\']+)/s;
    my ($osvers)   = $body =~ /(?:Summary of my perl5|Platform:).*?osvers=([^\s\n,<\']+)/s;
    my ($archname) = $body =~ /(?:Summary of my perl5|Platform:).*?archname=([^\s\n&,<\']+)/s;
    $archname =~ s/\n.*//	if($archname);

    $self->status($status);
    $self->distribution($dist);
    $self->version($version);
    $self->from($from || "");
    $self->perl($perl);
    $self->filename($d->filename);

    unless($archname || $platform) {
  	    if($osname && $osvers)	{ $platform = "$osname-$osvers" }
	    elsif($osname)		    { $platform = $osname }
    }

    unless($osname) {
        for my $text ($platform, $archname) {
            next    unless($text);
            if($text =~ $OSNAMES) {
                $osname = $1;
            } else {
                for my $rx (keys %OSNAMES) {
                    if($text =~ /$rx/i) {
                        $osname = $OSNAMES{$rx};
                        last;
                    }
                }
            }
            last    if($osname);
        }
    }

    $osvers ||= $osver;

    $self->osname($osname || "");
    $self->osvers($osvers || "");
    $self->archname($archname || $platform);

    return 1;
}

sub passed {
    my $self = shift;
    return $self->status eq 'PASS';
}

sub failed {
    my $self = shift;
    return $self->status eq 'FAIL';
}

# there are a few old test reports that omitted the perl version number.
# In these instances 0 is assumed. These reports are now so old, that
# worrying about them is not worth the effort.

sub _extract_perl_version {
    my ($self, $body) = @_;

    # Summary of my perl5 (revision 5.0 version 6 subversion 1) configuration:
    # Summary of my perl5 (revision 5 version 10 subversion 0) configuration:
    my ($rev, $ver, $sub, $extra) =
        $$body =~ /Summary of my (?:perl(?:\d+)?)? \((?:revision )?(\d+(?:\.\d+)?) (?:version|patchlevel) (\d+) subversion\s+(\d+) ?(.*?)\) configuration/si;

    if(defined $rev) {
        my $perl = $rev + ($ver / 1000) + ($sub / 1000000);
        $rev = int($perl);
        $ver = int(($perl*1000)%1000);
        $sub = int(($perl*1000000)%1000);

        my $version = sprintf "%d.%d.%d", $rev, $ver, $sub;
        $version .= " $extra" if $extra;
        return $version;
    #   return sprintf "%0.6f", $perl;	# an alternate format
    }

    # the following is experimental and may provide incorrect data

    ($rev, $ver, $sub) =
        $$body =~ m!/(?:(?:site_perl|perl|perl5|\.?cpanplus)/|perl-)(5)\.?([6-9]|1[0-2])\.?(\d+)/!;
    if(defined $rev) {
        my $version = sprintf "%d.%d.%d", $rev, $ver, $sub;
        return $version;
    }


#    warn "Cannot parse perl version for article:\n$body";
    return 0;
}

1;

__END__

=head1 NAME

CPAN::Testers::Data::Generator::Article - Parse a CPAN Testers article

=head1 DESCRIPTION

This is used by CPAN::Testers::Data::Generator.

=head1 INTERFACE

=head2 The Constructor

=over 4

=item * new

The constructor. Pass in a reference to the article.

=back

=head2 Methods

=over 4

=item * parse_upload

Parses an upload article.

=item * parse_report

Parses a report article.

=item * passed

Whether the report was a PASS

=item * failed

Whether the report was a FAIL

=back

=head2 Accessors

All the following are accessors available through via the object, once an
article has been parsed as a report or upload announcement.

=over 4

=item * postdate

'YYYYMM' representation of the date article was posted.

=item * date

'YYYYMMDDhhmm' representation of the date article was posted.

=item * status

For reports this will be the grade, for uploads this will be 'CPAN'.

=item * from

Who posted the article.

=item * distribution

The distribution name.

=item * version

The distribution version.

=item * perl

The perl interpreter version used for testing.

=item * osname

Operating system name.

=item * osvers

Operating system version.

=item * archname

Operating system architecture name. This is usually based on the osname and
osvers, but they are not always the same.

=item * subject

Subject line of the original post.

=back

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
  Current maintainer: Barbie       <barbie@cpan.org>   (C) 2008-2009

=head1 LICENSE

This code is distributed under the same license as Perl.
