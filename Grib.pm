=head1 NAME

PDL::IO::Grib - Grib file utilities for perl

=head1 SYNOPSIS

       use PDL;
       use PDL::IO::Grib;

       $gh = new PDL::IO::Grib;
       $gh->readgrib("filename" [, $readdata]);
       $gh->getfield("fieldname");
         

=head1 DESCRIPTION

Grib.pm allows the user to read files in the grib FORM FM 92-IX 
Ext. GRIB Edition 1 - it may not read all possible grib format combinations.

=over 4

=item Grib::new 

Grib::new creates a GribHandle, which is a reference to a newly created 
data structure.  If it receives any parameters,
they are passed to Grib::readgrib; if readgrib fails, the GribHandle
object is destroyed.  Otherwise, it is returned to the caller.

=cut

( $VERSION ) = '$Revision: 1.9 $ ' =~ /\$Revision:\s+([^\s]+)/;
package PDL::IO::Grib;

use FileHandle;
use PDL;
use PDL::IO::Misc qw(bswap2);
use strict;

$PDL::IO::Grib::debug=0;
$PDL::IO::Grib::swapbytes=0;


sub new {
    @_ >= 1 or barf 'usage: new Grib [FILENAME]';
    my $class = shift;
    my $gh={};
    bless $gh, $class;
   
    my $uname = `uname`;
    chomp($uname);

    use Config;

    $PDL::IO::Grib::swapbytes=1 if($Config{byteorder} =~ "1234");
    
    if (@_) {
      $gh->readgrib(@_ )
	or return undef;
    }
}

=item Grib::readgrib

Grib::readgrib accepts a GribHandle plus one or two parameters. 
With one parameter, it reads grib header information for all variables in the specified grib file. 
With two parameters, the first parameter is a filename, and the second 
parameter indicates that all of the data in the file should be read and 
decoded.

=cut


sub readgrib {
  my($self,$filename,$readdata) = @_;

  print ">$self<>$filename<>$readdata\n" if($PDL::IO::Grib::debug);

  my $fh = new FileHandle "$filename" 
    or barf "Could not open '$filename' for reading";

  $self->{_FILEHANDLE} = $fh;
  my $offset=0;
  my %fields;


  while(!eof($fh)){
    
#
# Read in the pds
#  
    my ($pa_ref, $trl);
    ($pa_ref,$trl) = readpds($fh);
    last unless($pa_ref);

    my ($ga_ref,$ba_ref);
    if($pa_ref->[5] & 128){
      $ga_ref = readgds($fh) ;
    }else{
      $ga_ref->[0]=0;
    }
    if($pa_ref->[5] & 64){
      $ba_ref = readbms($fh) ;
    }else{
      $ba_ref->[0]=0;
    }
    
    my ($data_ref,$dataloc) = readbds($fh,$pa_ref,$ga_ref,$readdata);

#
# It takes 5 fields to uniquely identify a grib variable we concatinate these
# fields and use that as the hash ref for each variable
#

    my $tnme = "$pa_ref->[6]:$pa_ref->[1]:$pa_ref->[7]:$pa_ref->[8]:$pa_ref->[9]";

    $self->{$tnme} = bless {Total_Record_Length => $trl,
			    Record_Offset => $offset,
			    Data_Offset => $dataloc,
			    PDS => $pa_ref,
			    GDS => $ga_ref,
			    BMS => $ba_ref,
			    BDS => $data_ref}, "Field";


# 
# Why an extra ten bytes - I do not know
# 

    $offset+=$trl+10;
  
  }

  $self->get_grib_names();


  return($self);
}


sub get_grib_names{
  my($self) = @_;

  my %namehash;
  my $namekey;
  my $table;
  if(-e ".gribtables"){
    $table = new FileHandle ".gribtables" ;
  }elsif(-e "$ENV{HOME}/.gribtables"){
    $table = new FileHandle "$ENV{HOME}/.gribtables" ;
  }
  if(defined $table){
    print "Reading table file\n";
    
    foreach($table->getlines){
      chomp;
      next if /^\#/;
      next if /^\s*$/;
      s/ //g;
      s/\#.*$//;
      my($name,@tmp) = split /:/,$_;
      my $namekey = join(':',@tmp);

      $namehash{$namekey}=$name;
    }
    $table->close();

    foreach $namekey (keys %namehash){
      my $rec;
      foreach $rec (keys %$self){
        next if($rec =~ /^[^\d]/);	
#
# Two ways to retreve.
#
#        if($namehash{$namekey} eq "T_2M"){
#	  print "here $rec <> $namekey<\n";
#	}
	if($rec =~ /^$namekey/){
          push(@{$self->{$namehash{$namekey}}},$self->{$rec});
	  $self->{$rec}{name}=$namehash{$namekey};

	}
      }
    }  

  }

  return($self);  
}

# What if we read into a pdl instead then only decode when needed?


sub readpds{
  my($fh) = @_;

  my ($in,$tin,@pdsarray);
#
# Read the header, i seem to have problems knowing exactly where it is
# but the word GRIB should appear followed by another 4 bytes 
#
  my $r = read $fh, $tin, 4;
  while($tin ne "GRIB" && $r>0){
    $r = read $fh, $in, 1;
    $tin = substr($tin,1).$in;
  }
  $r = read $fh, $tin, 4;
  $tin = pack("xa3",substr($tin,0,3));

  print "header: >$tin< $r\n" if($PDL::IO::Grib::debug);
 
  return() if($r<4);

#
# I'm going to return the total record length
#
  my $trl=vec($tin,0,32);
#
# Read in the pds length
# 
  my $rb = read $fh, $tin, 3;

  $in=$tin;

  $tin=pack("xa3",$tin);
  $pdsarray[0] = vec($tin,0,32);

  if($pdsarray[0] <= 0 || $pdsarray[0] > 500){
    print "Error reading pdsarray length: $pdsarray[0]<$in>$rb\n";
  }

  print "pdslength = $pdsarray[0]\n" if($PDL::IO::Grib::debug);


  read $fh, $tin, $pdsarray[0]-3;

  $in.=$tin;
# each of the next 18 elements are stored in 1 byte


  my $I;
  for($I=1;$I<19;$I++){
    $pdsarray[$I] = vec($in,$I+2,8);
  }
# elements 8 and 9 depend on the value of 7
  if($pdsarray[7]==100 || 
     $pdsarray[7]==103 || 
     $pdsarray[7]==105 || 
     $pdsarray[7]==107 || 
     $pdsarray[7]==109 || 
     $pdsarray[7]==111 || 
     $pdsarray[7]==113 || 
     $pdsarray[7]==125 || 
     $pdsarray[7]==160 ){
    $pdsarray[9]+= $pdsarray[8]*256;
    $pdsarray[8]=0;
  }

# element 19 is in two bytes
  $pdsarray[19] = vec($in,21,8)*256+vec($in,22,8);

  for($I=20;$I<23;$I++){
    $pdsarray[$I] = vec($in,$I+3,8);
  }
  
# element 23 is in two bytes
  $pdsarray[23] = vec($in,26,8)*256+vec($in,27,8);

# skip elements 24-37
#
# The following is from the HRM users guide of April 1999
# and may need to be modified for other datasets
#

  $tin = substr($in,42,3);
  $tin=pack("xa3",$tin);
  $pdsarray[38] = vec($tin,0,32);

  for($I=41;$I<46;$I++){
    $pdsarray[$I] = vec($in,$I+6,8);
  }
  
  $pdsarray[46] = vec($in,52,8)*256+vec($in,53,8);

  return(\@pdsarray,$trl);
}



sub readgds{
  my($fh,$pdslength)=@_;
  

  my(@gdsarray,$tin,$in);
#
# Read in the GDS length
#  
  read $fh, $tin, 3;
  $in=$tin;

  $tin=pack("xa3",$tin);
  $gdsarray[0] = vec($tin,0,32);

  read $fh, $tin, $gdsarray[0]-3;
  $in.=$tin;

  $gdsarray[1] = vec($in,3,8);
  $gdsarray[2] = vec($in,4,8);
  $gdsarray[3] = vec($in,5,8);

  $gdsarray[4] = vec($in,6,8)*256+vec($in,7,8);
  $gdsarray[5] = vec($in,8,8)*256+vec($in,9,8);
  
#  print "$gdsarray[0] $gdsarray[1] $gdsarray[2] $gdsarray[3]\n";
#  print "ie=$gdsarray[4] je=$gdsarray[5]\n";
#
# There will be a lot of variation between models in this section
# 
#

  if($gdsarray[3]==10){  # ROTATED LAT/LON GRID


    $tin = substr($in,10,3);
    
    $tin=pack("xa3",$tin);

    $gdsarray[6] = vec($tin,0,32)*1.0e-3;

    if($gdsarray[6]>8388.608){
      $gdsarray[6]=-$gdsarray[6]+8388.608
    }

    $tin = substr($in,13,3);
    $tin=pack("xa3",$tin);
    $gdsarray[7] = vec($tin,0,32)* 1.0e-3;

    if($gdsarray[7]>8388.608){
      $gdsarray[7]=-$gdsarray[7]+8388.608
    }
  
    $gdsarray[8] = vec($in,16,8);

    $tin = substr($in,17,3);
    $tin=pack("xa3",$tin);
    $gdsarray[9] = vec($tin,0,32)* 1.0e-3;

    if($gdsarray[9]>8388.608){
      $gdsarray[9]=-$gdsarray[9]+8388.608
    }
    $tin = substr($in,20,3);
    $tin=pack("xa3",$tin);
    $gdsarray[10] = vec($tin,0,32)* 1.0e-3;

    if($gdsarray[10]>8388.608){
      $gdsarray[10]=-$gdsarray[10]+8388.608
    }

    $gdsarray[11] = vec($in,23,8)*256+vec($in,24,8);
    $gdsarray[12] = vec($in,25,8)*256+vec($in,26,8);

    $gdsarray[13] = vec($in,27,8);
  
    $tin = substr($in,32,3);
    $tin=pack("xa3",$tin);
    $gdsarray[19] = vec($tin,0,32);

    $tin = substr($in,35,3);
    $tin=pack("xa3",$tin);
    $gdsarray[20] = vec($tin,0,32);

    $tin = substr($in,35,4);
    $tin=pack("xa4",$tin);
    $gdsarray[21] = vec($tin,0,32);
  }
  if($gdsarray[3]==192){
    $tin = substr($in,10,3);
    $tin=pack("xa3",$tin);
    $gdsarray[4] = vec($tin,0,32);

    $tin = substr($in,13,3);
    $tin=pack("xa3",$tin);
    $gdsarray[5] = vec($tin,0,32);
  
  }    

  return(\@gdsarray);
}



sub readbms{

  my($fh)=@_;
  
  my(@bmsarray,$tin,$in);

  read $fh, $tin, 3;
  $in=$tin;

  $tin=pack("xa3",$tin);
  $bmsarray[0] = vec($tin,0,32);

  read $fh, $tin, $bmsarray[0]-3;
  $in.=$tin;

  $bmsarray[1] = vec($in,3,8);

  $bmsarray[2] = vec($in,4,8)*256+vec($in,5,8);

  $bmsarray[3] = substr($in,7,$bmsarray[0]-6) if($bmsarray[0]>6);

  return(\@bmsarray);
}

sub readbds{

  my($fh,$pds_ref,$gds_ref,$readdata)=@_;
  
  my(@bdsarray,$tin,$in);

#
# Get the data section length
#
  
  read $fh, $tin, 11;
  $in=$tin;

  $tin=pack("xa3",$tin);
  $bdsarray[0] = vec($tin,0,32);

  $bdsarray[1] = vec($in,3,8);

  my $sparebits = $bdsarray[1]&15;

  my $pm;
  if (vec($in,4,8)&0x80) {$pm=-1} else {$pm=1};
  $bdsarray[2]=2**($pm*( (vec($in,4,8)&0x7F)*256+vec($in,5,8) ));

  $bdsarray[3] = get_ref_val(substr($in,6,4));
  $bdsarray[3] = 0 if(abs($bdsarray[3])<1.0e-18);

  $bdsarray[4] = vec($in,10,8);

  print "datasize=$bdsarray[0]  $bdsarray[1] scale factor=$bdsarray[2] 
         ref_val=$bdsarray[3] bitsperval=$bdsarray[4] $sparebits\n" if($PDL::IO::Grib::debug);

#
# get the data location for later use
#
  my $dataloc=$fh->getpos;

  if($readdata){  
    $bdsarray[5] = read_data($fh,$$gds_ref[4],$$gds_ref[5],\@bdsarray);
  }else{
    seek $fh, $bdsarray[0]-10, 1;
  }
#
# read the end of record info
#


#
# We actually read two bytes past the end of record?
#

  my $eor; my $cnt=0;

  $cnt = read $fh, $eor, 6-int($sparebits/8);

  return(\@bdsarray,$dataloc);
}

sub read_data{
  my($fh, $xdim, $ydim, $bds_ref,$dataloc)=@_;

  print "read_data: $fh, $xdim, $ydim, $bds_ref,$dataloc\n" if($PDL::IO::Grib::debug);
  $fh->setpos($dataloc) if($dataloc);

  my $dataarray = PDL->zeroes((new PDL::Type(ushort)),$xdim,$ydim);

  if($bds_ref->[0]-12 != $xdim*$ydim*2){
    print "Error in Grib::read_data $bds_ref->[0] $xdim $ydim\n";
  }
  read $fh, ${$dataarray->get_dataref}  ,$bds_ref->[0]-12;

  bswap2($dataarray) if($PDL::IO::Grib::swapbytes);

  $dataarray = float($dataarray);
  unless(($bds_ref->[2] == 1) && ($bds_ref->[3] == 0)){
    $dataarray=$dataarray*$bds_ref->[2]+ $bds_ref->[3];
  }
  return($dataarray);
}

# Subroutine to unpack the reference values (and ref-value like
# objects) from 4 bytes
sub get_ref_val {

  my ($in)=@_;

  my $ref_frac=vec($in,1,8)*256*256+vec($in,2,8)*256+vec($in,3,8);
  my $ref_mant=vec($in,0,8);
  my $ref_sign;
  if ($ref_mant & 128) {$ref_sign=-1} else {$ref_sign=1};
  $ref_mant=($ref_mant & 127) - 64;
  $ref_sign*$ref_frac*2**(-24)*16**($ref_mant);

}

sub idsort{

  my(@a) = split(/:/,$a);
  my(@b) = split(/:/,$b);

#
# avoid errors due to sorting on nonnumeric fields
#

  $a =~ /^[^\d]/ 
    or
  $b =~ /^[^\d]/
    or  
  $a[0] <=> $b[0]
    or
  $a[1] <=> $b[1]
    or
  $a[2] <=> $b[2]
    or
  $a[3] <=> $b[3]
    or
  $a[4] <=> $b[4]
    or
  $a cmp $b;
  
}

=item Grib::getfield 

Grib::getfield accepts one parameter which is either the 5 field
identifier for grib variables (pds[6]:pds[1]:pds[7]:pds[8]:pds[9] as
defined in the grib format definition) or a variable name associated
with that identifier as defined in the file .gribtables and returns
the name and data field for that variable.  Grib::getfield will check
to see if the data has already been read into memory and will only
read the file when this is not the case.  The data field is returned
as a PDL piddle or an array of PDL piddles where the identifier matchs
more than one field.

=cut

sub getfield{
  my($gh,$field) = @_;
  
  my ($he,$key);
  my ($level);
  if($field=~/^\d.*/){
    push(@$he, $gh->{$field}) if(exists $gh->{$field});
  }else{
    if($field=~/^([^:]+):(\d+:*\d*)$/){
       $field = $1;
       $level = $2;
     }
    print "looking for >$field<$level<\n" if($PDL::IO::Grib::debug);
    
    my @keys = reverse sort idsort keys %$gh;

    foreach $key (@keys){
      next if($key =~ /^[^\d]/); # special keys
      next unless(defined $gh->{$key}{name});


      print "lev: $level\n$key\n" if($PDL::IO::Grib::debug);
      if($gh->{$key}{name} eq $field){
        if(defined $level){
          next unless($key=~/:$level$/);
        }
	push(@$he,$gh->{$key});
      }
    }
  }

  unless(defined $he){
    $field.=":$level" if(defined $level);
    barf "Could not find match for $field in grib file";
    return;
  }
  my ($href,$retval,@pdllist,$name);
  foreach $href (@$he){
    print "href = $href\n" if($PDL::IO::Grib::debug==1);
    $name=$href->{name} ;
    $name.=":$level" if(defined $level);
   
#
# If the data has already been read in don't read it again
#

    if(defined $href->{BDS}[5]){ 
      push(@pdllist,$href->{BDS}[5]);
    }else{
      print "reading data \n" if($PDL::IO::Grib::debug);

      $href->{BDS}[5] = read_data($gh->{_FILEHANDLE},$href->{GDS}[4],
                      $href->{GDS}[5],$href->{BDS},$href->{Data_Offset});
   
      push(@pdllist,$href->{BDS}[5]);

    }
  }


  if($#pdllist>0){
    return(\@pdllist);
  }else{
    return($pdllist[0]);
  }
 
}


sub getallfields{
  my($gh) = @_;
  my $fieldlist;

  foreach(sort idsort keys %$gh){
    next if(/^[^\d]/); # ignore special keys
    push(@$fieldlist,getfield($gh,$_));
  }
  $fieldlist;
}

#
# Create a single 3d piddle from a list of 2d piddles
#

sub stack2d{
  my(@pdls)=@_;
  
  my $cube = PDL->zeroes($pdls[0]->type,$pdls[0]->dims,$#pdls+1); # make 3D piddle

  for (0.. $#pdls){
    (my $tmp = $cube->slice(":,:,($_)")) .= $pdls[$_];
  }
  return($cube);
}

sub stack{
  my(@pdls) = @_;

  my $ndims = $pdls[0]->getndims;

  my $cube = PDL->zeroes($pdls[0]->type,$pdls[0]->dims,$#pdls+1); # make $ndims+1 piddle

  my $slice_str;
  for(1..$ndims){
    $slice_str.=":,";
  }
  
  for (0.. $#pdls){
    (my $tmp = $cube->slice("$slice_str($_)")) .= $pdls[$_];
    
  }
  return($cube);
}
  


=item Grib::showinventory 

Grib::showinventory prints a list of variables found in the open file and names
associated with them from the .gribtables file.

=cut

sub showinventory{
  my($gh) = @_;

  foreach(sort idsort keys %$gh){
    next if(/^[^\d]/); # ignore special keys
    if(defined $gh->{$_}{name}){
        print "$_ $gh->{$_}{name}\n";
      }else{
        print "$_\n"; 
      }
  }
}

sub pds_attribute{
  my $gh=shift;
  my $attnum=shift;
  

  my $ref = ref($gh);
  
#  print "ref = $ref\n";

  if($ref eq "Grib"){
#    print "This is a Grib ref\n";
    my $prevval=-999;
    foreach(sort idsort keys %$gh){
      next if(/^[^\d]/); # ignore special keys
      my $val = $gh->{$_}{PDS}[$attnum];
#      print "$val\n" unless($val==$prevval);
      $prevval=$val;
    } 
    $prevval;
  }elsif($ref eq "Field"){
    @_ ? $gh->{PDS}[$attnum] = shift :
      $gh->{PDS}[$attnum];
  }

}

sub gds_attribute{
  my $gh=shift;
  my $attnum=shift;
  

  my $ref = ref($gh);
  
#  print "ref = $ref\n";

  if($ref eq "Grib"){
#    print "This is a Grib ref\n";
    my $prevval=-999;
    foreach(sort idsort keys %$gh){
      next if(/^[^\d]/); # ignore special keys
      my $val = $gh->{$_}{GDS}[$attnum];
#      print "$val\n" unless($val==$prevval);
      $prevval=$val;
    } 
    $prevval;
  }elsif($ref eq "Field"){
    @_ ? ${$$gh{GDS}}[$attnum] = shift :
      ${$$gh{GDS}}[$attnum];
  }
  
}

=item get_grib1_date()

Returns the initialization date from a grib file in the form
yyyymmddhh.  Takes either the file name or a valid Grib handle as
an input argument.

=cut

sub get_grib1_date{
  my $gh = shift;
  my $date;
  my $passed_file_name;
  unless(ref($gh) eq "Grib"){
    $gh = new Grib($gh);
    $passed_file_name=1;
  }

  $date = ($gh->pds_attribute(10)+1900)*1000000+ $gh->pds_attribute(11)*10000
    +$gh->pds_attribute(12)*100+$gh->pds_attribute(13);

  $gh->close if($passed_file_name==1);

  $date;

}

sub close {
  my($gh) = @_;
  $gh->{_FILEHANDLE}->close;
}



1;

=item .gribtables

The .gribtables file is searched for first in the working directory then 
in the user's home directory.  The format is rather simple - anything following a 
B<#> sign is a comment otherwise a 

name:pds[6]:pds[1]:pds[7]:pds[8]:pds[9]  
 
is expected where name can be anything as long as it begins with an alpha character and does not containb{:} 
and the pds[#] refers to the element number in the pds
sector of the grib file.  Fields 8 and 9 are optional in the file and if
not found all of the records which match fields 6 1 7 will be combined into a 
single 3d dataset by getfield, unless the name passed in get field is modified 
by :pds[8]:pds[9] (or simply :pds[9]) which can be used to get a slice of 
what would otherwise be a 3d variable.

So suppose the file .gribtables contains the entry

T_PG:11:2:100

to specify 3d temperature on a pressure grid, then to get the 500mb
pressure you would do

$t500 = $gh->getfield ("T_PG:500");

=back 

=head1 Author

Jim Edwards <jedwards@inmet.gov.br> 

=cut
