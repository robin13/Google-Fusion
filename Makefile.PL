use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Google::Fusion',
    AUTHOR              => q{Robin Clarke <perl@robinclarke.net>},
    VERSION_FROM        => 'lib/Google/Fusion.pm',
    ABSTRACT_FROM       => 'lib/Google/Fusion.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More'		=> 0,
	'Text::Table'		=> 0,
	'Moose'			=> 0,
	'LWP::UserAgent'	=> 0,
	'URL::Encode'		=> 0,
	'Encode'		=> 0,
	'YAML'			=> 0,
	'Carp'			=> 0,
	'Text::CSV'		=> 0,
	'Net::OAuth2::Client'	=> 0.10,
	'File::HomeDir'		=> 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Google-Fusion-*' },
);
