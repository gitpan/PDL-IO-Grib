use ExtUtils::MakeMaker;

WriteMakefile(
				  NAME => 'PDL::IO::Grib',
				  VERSION_FROM => 'Grib.pm',
				  DIR => 'Grib',
				  dist => {COMPRESS => 'gzip', SUFFIX=> 'gz'}
);
