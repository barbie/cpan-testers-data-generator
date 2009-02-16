use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME'          => 'CPAN::Testers::Data::Generator',
    'VERSION_FROM'  => 'lib/CPAN/Testers/Data/Generator.pm',
    'PREREQ_PM'     => {
        'Email::Simple'                     => 0,       # only for scripts
        'Getopt::Long'                      => 0,       # only for scripts
        'Getopt::ArgvFile'                  => 0,       # only for scripts

        'Config::IniFiles'                  => 0,
        'CPAN::Testers::Common::Article'    => 0.36,
        'CPAN::Testers::Common::DBUtils'    => 0.03,
        'File::Basename'                    => 0,
        'File::Path'                        => 0,
        'IO::File'                          => 0,
        'Net::NNTP'                         => 0,

        'DBD::mysql'                        => 0,
        'DBD::SQLite'                       => 1.07,

        'Test::More'                        => 0,       # only for testing
    },

    'PL_FILES'      => {},
    'INSTALLDIRS'   => 'site',
    'EXE_FILES'     => [ 'bin/cpanstats' ],
    NO_META         => 1,

    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT	=> 'CPAN Testers Database Generator',
       AUTHOR	=> 'Barbie <barbie@cpan.org>') : ()),
);
