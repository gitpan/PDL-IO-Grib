=head1 NAME

  PDL::IO::Grib::Field - Field tools for Grib files

=head1 SYNOPSIS

       use PDL;
       use PDL::IO::Grib;

       $gh = new PDL::IO::Grib;
       $gh->readgrib("filename");
       $gh->getfield("fieldname");
       $f = $gh->anyfield();
       print $f->pds_dump();

=head1  DESCRIPTION

Field.pm gives you access to the individual fields of a Grib
file. Here is the code that attempts to decipher each of the sections
associated with the format.  All of the data is read into PDLS (one
for each section) and it only deciphers those pdls on demand.  It only
knows how to decipher a small subset of the possibilities at this
time, but should allow the developer to write and test new or local
decipher methods easily.  Of course if you write a new method and can
cleanly merge it with whats working please feel free to send it back
to me.

=head1 FUNCTIONS

=head2 new

=for ref

  PDL::IO::Grib::Field new creates a new Field structure hash and if passed an
  open file handle reads the next grib record into that structure.  It reads 
  each of the sections into a PDL.  

=cut

package PDL::IO::Grib::Field;
use strict;
#use warnings;
use FileHandle;
use PDL;
use PDL::IO::FlexRaw;
use PDL::IO::Grib::Wgrib;
use fields qw/FileHandle PDS_pos PDS_len GDS_len BMS_len BDS_len
              PDS GDS BMS BDS BDS_pos DATA name/;

sub new {
  my($class,$fh) = @_;


  my $field={};
  bless $field, $class;
  my $id;
  if(defined $fh){
    $field->{FileHandle} = $fh ;
    $field->initialize();
	 return if($fh->eof);
  }
  return ($field);
}

=head2 name

=for ref 

$f->name('fred') assigns the name fred to field, 
$name = $f->name(); retreves the name of $f if it has one.  Initial names
are read from Gribtables.pm

=cut

sub name{
  my($field,$name) = @_;
  if(defined $name){
	 $field->{name} = $name;
  }
  return $field->{name};
} 


=head2 id

=for ref

  It takes 5 fields to uniquely identify a grib variable we concatinate these
  fields return them, Grib.pm uses this to identify each variable

=cut

sub id{
  my($field) = @_;
  # should be an error
  return unless defined $field->{PDS};

  my $id;
  if($field->pds_has_octet12()){
	  $id = sprintf("%u:%u:%u:%u:%u",
							$field->pds_attribute(9),
							$field->pds_attribute(4),
							$field->pds_attribute(10),
							$field->pds_attribute(11),
							$field->pds_attribute(12));
  }else{
	  $id = sprintf("%u:%u:%u:%u",
							$field->pds_attribute(9),
							$field->pds_attribute(4),
							$field->pds_attribute(10),
							$field->pds_attribute(11));


  }
  print "id = $id\n" if($PDL::IO::Grib::debug);
  return $id;
}

=head2 initialize

=for ref

Should only be called from new - reads a Grib record from a file 

=cut

sub initialize{
  my($field) = @_;

#
# Read the length of each section.  I seem to have problems finding 
# the header exactly
# but the word GRIB should appear followed by another 4 bytes 
#
  my $fh = $field->{FileHandle};
    
  my $tin;

  my $r = $fh->read( $tin, 4);
  until($tin =~ /GRIB$/ || $r==0){
	 my $in;
    $r = $fh->read( $in, 1);
    $tin .= $in;
  }
  
  return unless($r>=1);  
  
  my $a =  PDL->zeroes((new PDL::Type(byte)),4);
  $r = PDL::IO::FlexRaw::readchunk($fh,$a,4);
  my $trl = PDL::unpackint($a,3);

  print "$r Total record length: $trl\n" if($PDL::IO::Grib::debug);
   
  $field->{PDS_pos}=$fh->tell;

  print "Read PDS\n"  if($PDL::IO::Grib::debug);

  ($field->{PDS_len},$field->{PDS}) = $field->read_section($fh);

  my $gdsbms = $field->pds_attribute(8); 


  if($gdsbms & 128){
	 print "Read GDS\n"  if($PDL::IO::Grib::debug);
    ($field->{GDS_len},$field->{GDS}) = $field->read_section($fh);
  }

  if($gdsbms & 64){
	 print "Read BMS\n"  if($PDL::IO::Grib::debug);
    ($field->{BMS_len},$field->{BMS}) = $field->read_section($fh);
  }
  $field->{BDS_pos} = $fh->tell();

  print "Read BDS\n"  if($PDL::IO::Grib::debug);
  ($field->{BDS_len},$field->{BDS}) = $field->read_section($fh,1);

  
}

=head2 read_section

=for ref

read_section is an internal subroutine to read a record section from a 
grib file it expects a file handle and an optional flag to tell it that the
bds section is being read.  We only read the first 16 bytes of the bds. 

=cut

sub read_section{
  my($field,$fh,$bds) = @_;
#
# first unpack the section length then go back and read the entire section 
# into an appropriatly sized PDL
#
  my $a =  PDL->zeroes((new PDL::Type(byte)),3);
  PDL::IO::FlexRaw::readchunk($fh,$a,3);

  my $len = PDL::unpackint($a,3);

  $fh->seek(-3,1);
  my $section;
#
# Usually we read everything but we do not read the actual data in the 
# BDS section
#
  if($bds){
	 #
	 # We need at least the first 16 octets
	 #
	 $section = PDL->zeroes((new PDL::Type(byte)),16);
    PDL::IO::FlexRaw::readchunk($fh,$section,16);
    $fh->seek($len-16,1);
  }else{
	 $section = PDL->zeroes((new PDL::Type(byte)),$len);
    PDL::IO::FlexRaw::readchunk($fh,$section,$len);
  }

  print "read_section  $len ", ushort($section)," \n" if($PDL::IO::Grib::debug);
  return($len,$section);
}

=head2 pds_has_octet12

=for ref

Returns true if pds octet 10 indicates that octet 11 is one byte and 
octet 12 is a seperate field, returns 0 otherwise

=cut

sub pds_has_octet12{
  my ($f) = @_;

  my $o10 = $f->pds_attribute(10);
  if($o10 == 100 || 
	  $o10 == 103 || 
	  $o10 == 105 || 
	  $o10 == 107 || 
	  $o10 == 109 || 
	  $o10 == 111 || 
	  $o10 == 113 || 
	  $o10 == 125 || 
	  $o10 >= 160 ){
	 return(0);
  }
  return(1) ;
}

=head2 pds_attribute

=for ref

$f->pds_attribute($num) returns the value of the PDS field which
begins at octet $num.  You really need to have the Grib document
(http://www.wmo.ch/web/www/reports/Guide-binary-2.html) in hand to use
these things.  If this function recognizes that a value beginning at
octet $num spans several octets and really takes another form, it
decodes and returns that value.  The default is to return the unsigned
integer value of the byte at that octet.  In a few cases it recognizes
that no value exists for a given octet and returns undef.

=cut

sub pds_attribute{
  my($f, $num, $val) = @_;

  my $offset = $num-1;
  if($num==1){
    if(defined $val){
     
    }else{
      return $f->{PDS_len};
    }
  }elsif($num<4 or $num > $f->{PDS_len}){
    barf("$num is not a valid PDS octet identifier\n");
  }elsif($num == 11){
	 if($f->pds_has_octet12()){
      return $f->{PDS}->slice("($offset)");
	 }else{
		return $f->{PDS}->slice("$offset:$num")->unpackint(2);
	 }
  }elsif($num == 12 && !$f->pds_has_octet12()){
	 return undef;
  }elsif($num == 22){
	 if(defined $val){
		print "WARNING - this needs testing\n";
		(my $t0 = $f->{PDS}->slice("($offset)")) = int($val/256);
		(my $t1 = $f->{PDS}->slice("($num)")) = $val%256;
	 }else{
      return  $f->{PDS}->slice("$offset:$num")->unpackint(2);
    }
  }elsif($num == 27){
    my $tval =  $f->{PDS}->slice("$offset:$num")->unpackint(2);
    if($tval>32767){
      $tval =  -($tval & 32767);
    }
    return $tval;
  }
  else{
    if(defined $val){
      print "WARNING - this needs testing\n";
      (my $t = $f->{PDS}->slice("($offset)")) = $val;
    }else{
      return $f->{PDS}->slice("($offset)");
    }
  }
}

=head2 gds_attribute

=for ref

see the pds_attribute description

=cut

sub gds_attribute{
  my($f, $num, $val) = @_;
  my $offset = $num-1;

  if($num==1){
    if(defined $val){
     
    }else{
      return $f->{GDS_len};
    }
  }elsif($num<4 or $num > $f->{GDS_len}){
    barf("$num is not a valid GDS octet identifier\n");
  }elsif($num == 7 || $num == 9){
    return $f->{GDS}->slice("$offset:$num")->unpackint(2);
  }elsif($num == 11 || $num == 14){
    my $ep=$offset+3;
    my $tval =  $f->{GDS}->slice("$offset:$ep")->unpackint(3);
    if($tval>8388607){
      $tval =  -($tval & 8388607);
    }
    return $tval/1000;
  }elsif($num == 18 || $num==21){
    my $ep=$offset+3;
    my $tval =  $f->{GDS}->slice("$offset:$ep")->unpackint(3);
    if($tval>8388607){
      $tval =  -($tval & 8388607);
    }
    return $tval/1000;
    

#
# note that we can't call $f->gds_attribute(5) here, we need to 
# decode it directly to avoid recursion
#
  }elsif($num < 255 && $num ==  $f->{GDS}->slice("(4)")){ 
	 my $end =  $offset+4*$f->gds_attribute(4)-1;
    print  $f->{GDS}->nelem," Getting PV $offset:$end\n" if($PDL::IO::Grib::debug);
	 return(PDL::IO::Grib::Wgrib::get_ref_val($f->{GDS}->slice("$offset:$end")));
  }
  else{
    if(defined $val){
      print "WARNING - this needs testing\n";
      (my $t = $f->{GDS}->slice("($offset)")) = $val;
    }else{
      return $f->{GDS}->slice("($offset)");
    }
  }
  
}

=head2 bds_attribute

=for ref

see the pds_attribute description

=cut

sub bds_attribute{
  my($f, $num, $val) = @_;
  my $offset = $num-1;

#  print $f->{BDS},"\n";

  if($num == 4){
    # return an array reference 
    my $v = $f->{BDS}->slice("($offset)");
    return ($v & 128, $v & 64, $v & 32, $v & 16, $v & 15);
  }elsif($num == 5){
	 my $pm;
    if( $f->{BDS}->slice("($offset)") & 128) {$pm=-1} else {$pm=1};
	 return 2**($pm* (256* ($f->{BDS}->slice("($offset)") & 127)+
						  $f->{BDS}->slice("($num)")));

  }elsif($num == 7){
	 my $lof = $offset+3;
	 return PDL::IO::Grib::Wgrib::get_ref_val($f->{BDS}->slice("$offset:$lof"));
  }
  else{
    if(defined $val){
      print "WARNING - this needs testing\n";
      (my $t = $f->{BDS}->slice("($offset)")) = $val;
    }else{
      return $f->{BDS}->slice("($offset)");
    }
  }

}  

=head2 section_dump

=for ref

section_dump returns a string which includes the entire contents of the 
specified section, except in the case of the BDS where it only includes the
first 16 bytes.  It is a little braindead - if I know that octet 42 is really 
a list of 21 float values I return a pdl of 21 values from octet 42 but I also 
return octets 43 - 126 as well.  

=cut

sub section_dump{
  my($f,$section) = @_;
  my $function;

  $section = uc $section;
  if($section eq 'PDS'){
    $function = \&PDL::IO::Grib::Field::pds_attribute;
  }elsif($section eq 'GDS'){
    $function = \&PDL::IO::Grib::Field::gds_attribute;
  }elsif($section eq 'BMS'){
    $function = \&PDL::IO::Grib::Field::bms_attribute;
  }elsif($section eq 'BDS'){
    $function = \&PDL::IO::Grib::Field::bds_attribute;
  }else{
    barf("invalid section name $section\n");
  }
  my $out = "$section octet  value\n";
    

  $out.="     1   ". &{$function}($f,1)."\n";
  for(4 .. $f->{$section."_len"}-1){
    $out.="     $_   ".&{$function}($f,$_)."\n";
  }
  return $out;
}

=head2 PDL::unpackint

=for ref

Given a byte pdl and a cnt this function computes an integer representation
for that number of bytes.  There are several 3 byte integers in Grib

=cut

# private 
sub PDL::unpackint{
  my($slice,$cnt,$val) = @_;

  $cnt--;
  
  if($cnt > 0){
	 $val = (256**$cnt)*$slice->at(0)+$slice->slice("1:-1")->unpackint($cnt);
  }else{
#	 $val =  $slice->slice("(0)");
	 $val =  $slice->at(0);
  }
  return($val);
}


=head2 gds_vertical_parameters

=for ref

Function to decode the vertical parameter field if present.
Convenience function, could be done with gds_attribute directly.

=cut

sub gds_vertical_parameters{
  my($f) = @_;
  return $f->gds_attribute( $f->gds_attribute(5) );
}

use PDL::IO::Misc qw(bswap2 bswap4);

sub read_data{
  my($f)=@_;

  my $fh = $f->{FileHandle};


  my @flags = $f->bds_attribute(4);

#  print "flags = @flags\n";
  my $dataarray;
  

  if($flags[0]+$flags[1] == 0){
	 # easiest case - grid point simple packing
	 my $xdim = $f->gds_attribute(7);
	 my $ydim = $f->gds_attribute(9);
	 $fh->seek($f->{BDS_pos}+11,0);
	 my $len = $f->{BDS_len}-12;
    if($len<=0){
		print "Warning: data length reported as 0\n";
		$f->{DATA} = $f->bds_attribute(7);
		return( $f->{DATA} );
	 }
    print "dimensions $xdim $ydim $len \n"  if($PDL::IO::Grib::debug);
    my $decoded;
	 my $bitsperval = $f->bds_attribute(11);
	 print "Reading $bitsperval bits per value ",ref($bitsperval),"\n"  if($PDL::IO::Grib::debug);
	 if($bitsperval == 8){
		$dataarray = PDL->zeroes((new PDL::Type(byte)),$xdim,$ydim);
		PDL::IO::FlexRaw::readchunk($fh,$dataarray,$len);
	 }elsif($bitsperval ==  16){
		$dataarray = PDL->zeroes((new PDL::Type(ushort)),$xdim,$ydim);
		PDL::IO::FlexRaw::readchunk($fh,$dataarray,$len);
		bswap2($dataarray) if($PDL::IO::Grib::swapbytes);
	 }elsif($bitsperval ==  32){
		$dataarray = PDL->zeroes((new PDL::Type(long)),$xdim,$ydim);
		PDL::IO::FlexRaw::readchunk($fh,$dataarray,$len);
		bswap4($dataarray) if($PDL::IO::Grib::swapbytes);
	 }else{
		my $bytearray = PDL->zeroes((new PDL::Type(byte)),$len);
		$dataarray = PDL->zeroes((new PDL::Type(float)),$xdim*$ydim);
		PDL::IO::FlexRaw::readchunk($fh,$bytearray,$len);
		my $bitmap;

		if(defined $f->{BMS}){
		  $bitmap = $f->{BMS}->slice("6:-1");
		}else{
		  $bitmap = 128+PDL->zeroes((new PDL::Type(byte)),$xdim*$ydim/8+1); 
		}
      
      my $val = $bytearray->slice("0:1");
#      print "enter: ",ushort($val)," ",$f->bds_attribute(5)," ",$f->bds_attribute(7),"\n";

      		
		PDL::IO::Grib::Wgrib::BDSunpack($bytearray,$bitmap, $bitsperval, 
					 $f->bds_attribute(7),$f->bds_attribute(5), $dataarray);

#      print "exit: ",$dataarray->at(0),"\n";

		$decoded=1;
		#		barf "Do not know how to handle $bitsperval bits per value";
    }
	 unless($decoded || ($f->bds_attribute(5) == 1) && ($f->bds_attribute(7) == 0)){
		$dataarray=float $dataarray*$f->bds_attribute(5)+ $f->bds_attribute(7);
	 }
  }else{
	 barf "Do not know how to handle bds octet 4 = @flags yet";
  }

 
  $f->{DATA} = $dataarray;
  
  return($dataarray);
}

1;
