use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME'          => 'CPAN::Testers::Data::Generator',
    'VERSION_FROM'  => 'lib/CPAN/Testers/Data/Generator.pm',
    'PREREQ_PM'     => {
        'Getopt::Long'                      => 0,       # only for scripts
        'Getopt::ArgvFile'                  => 0,       # only for scripts

        'Config::IniFiles'                  => 0,
        'CPAN::Testers::Common::Article'    => 0.36,
        'CPAN::Testers::Common::DBUtils'    => 0.03,
        'CPAN::Testers::Fact::LegacyReport' => 0,
        'CPAN::Testers::Fact::TestSummary'  => 0,
        'CPAN::Testers::Metabase::AWS'      => 0,
        'CPAN::Testers::Report'             => 0,
        'Data::Dumper'                      => 0,
        'File::Basename'                    => 0,
        'File::Path'                        => 0,
        'File::Slurp'                       => 0,
        'IO::File'                          => 0,
        'JSON'                              => 0,
        'Time::Local'                       => 0,
        'XML::Feed'                         => 0,
        'Metabase'                          => 0.014,
        'Metabase::Fact'                    => 0,

        'DBI'                               => 0,
        'DBD::mysql'                        => 0,
        'DBD::SQLite'                       => 1.07,

        'Test::More'                        => 0.70,    # only for testing
    },

    'PL_FILES'      => {},
    'INSTALLDIRS'   => 'site',
    'EXE_FILES'     => [ 'bin/cpanstats' ],
    NO_META         => 1,

    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT	=> 'CPAN Testers Database Generator',
       AUTHOR	=> 'Barbie <barbie@cpan.org>') : ()),
);
