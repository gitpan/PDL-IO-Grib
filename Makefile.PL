use ExtUtils::MakeMaker;

WriteMakefile(
	NAME => "PDL::IO::Grib",
        VERSION_FROM => 'Grib.pm',
        dist => {COMPRESS => 'gzip', SUFFIX=> 'gz'}
);
