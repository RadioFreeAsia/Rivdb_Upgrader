#!/usr/bin/perl  
#
#    rivdb_upgrader.pl
#    The Rivendell 3.0 DB  Upgrade Utility
#
#   (C) Copyright 08-23-2016 Todd Baker <bakert@rfa.org>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License version 2 as
#    published by the Free Software Foundation.
#   
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#    
#   You should have received a copy of the GNU General Public
#   License along with this program; if not, write to the Free Software
#   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
#
#
#
####################################################################
#   This is the database utility which is used to convert 
#   a Rivendell 2.x Database to a Rivendell 3.0 Database.  
#
#   It is strongly Recommended that you backup your database
#   BEFORE running this utility!
#
#   Assumptions the program makes are the following:
#
#   1)  The file /etc/rd.conf exists, is readable by this
#       program, and it uses the user information stored 
#       there to access the Rivendell Database.
#
#   2)  The Rivendell 2.0 Database Schema must be 259.
#       Any other schema will cause the program to abort.
#
#   3)  The program will remove any non-Ascii characters
#       from input Rivendell Database Columns, specfically
#       any ASCII value less than a "Space" (CR/LF ignored)
#	and any ASCII greater than "~" (value 126). It will 
#	only change text/character columns - and will not
#	change any KEYS (Primary or otherwise).
#
#   4)  An empty Mysql database named "Rivendell3" must exist
#       on the host defined in rd.conf. This is where it will place
#	the converted the Rivendell datasbase.  All the new Rivendell
#	tables will be created as InnoDB tables using UTF8 
#	Character set. All Rivendell 2.x tables will be 
#	copied. The rd.conf user must have read/write permissions.
#
#   5)  The PERL DBI Module must be installed on the running 
#       system before this program will work!
#
####################################################################

use strict;
use DBI;

my $host = "";
my $user = "";
my $database="";
my $password = "";
my $dsnriv2;
my $dbhriv2;
my $mysqlriv2;
my $rcriv2;
my $dsnriv3;
my $dbhriv3;
my $mysqlriv3;
my $rcriv3;
my $dsninfo1;
my $dbhinfo1;
my $dsninfo2;
my $dbhinfo2;
my $mysqlinfo1;
my $rcinfo1;
my $mysqlinfo2;
my $rcinfo2;
my $error = 0;
my $Max_char = "~";
my $Min_char = " ";
my $Replace_char="";


#load Database Values from INI
open (INIFILE,"/etc/rd.conf") 
   or die " No rd.conf File Found - Aborting!";

while (my $row = <INIFILE>) {
    my $pos = 0;
    my $var = "";
    my $val;
    chomp $row;
    if ((substr($row,0,1) ne "#") || 
	(substr($row,0,1) ne ";") )     #Ignore Comments
    {
        $pos = index($row,"=");
        if ($pos > 0)
        {
            $var = substr($row,0,$pos);
            $val = substr($row,$pos + 1, (length($row) - $pos));
            if ($var eq "Hostname")
            {
                $host = $val;
            }
            if ($var eq "Loginname")
            {
                $user = $val;
            }
            if ($var eq "Password")
            {
                $password = $val;
            }
            if ($var eq "Database")
            {
                $database = $val;
            }
        }
    }
}

if ($host eq "")
{
   print "Error: Invalid Initilization Host Undefined - check INI file!: /etc/rd.conf\n";
   exit(2);
}
if ($user eq "")
{
   print "Error Invalid Initilization User Undefined- check INI file!: /etc/rd.conf'\n";
   exit(2);
}
if ($database eq "")
{
   print "Error Invalid Initilization Database Undefined- check INI file!: /etc/rd.conf'\n";
   exit(2);
}

# Start database Connections

$dsnriv2 = "dbi:mysql:dbname=Rivendell:host_name=$host";
$dbhriv2 = DBI->connect($dsnriv2,$user,$password)
   or die "Cant connect to database: DBI$::errstr";

$mysqlriv2 = $dbhriv2->prepare( "select DB from VERSION")
   or die "Cant prepare check version statement: $DBI::errstr";

$rcriv2 = $mysqlriv2->execute()
   or die "Error executing version check select statement : $DBI::errstr";

my $Riv_version = $mysqlriv2->fetchrow();

print "The Riv version = $Riv_version\n";

if ($Riv_version != 259)
{
   print "Schema Version Incorrect - should be 259 - Got Version $Riv_version !   Aborting\n";
    exit(3);
}
$mysqlriv2->finish();

$dsninfo1 = "dbi:mysql:dbname=INFORMATION_SCHEMA:host_name=$host";
$dbhinfo1 = DBI->connect($dsninfo1,$user,$password)
   or die "Cant connect to database: DBI$::errstr";

$mysqlinfo1 = $dbhinfo1->prepare ( 
   "select TABLE_NAME,COLUMN_NAME from COLUMNS where TABLE_SCHEMA=\"Rivendell\" and COLUMN_KEY = \"PRI\"" )
    or die "Error preparing select Table and Column names: $DBI::errstr";

$rcinfo1 = $mysqlinfo1->execute()
   or die "Error executing Table Name,Column Name Select: $DBI::errstr";

while (my ($table_name,$table_key) = $mysqlinfo1->fetchrow()) 
{
    $dsninfo2 = "dbi:mysql:dbname=INFORMATION_SCHEMA:host_name=$host";
    $dbhinfo2 = DBI->connect($dsninfo2,$user,$password)
       or die "Cant connect to database: DBI$::errstr";
    $mysqlinfo2 = $dbhinfo2->prepare( 
	"select COLUMN_NAME from COLUMNS where TABLE_NAME = \"$table_name\" and
            TABLE_SCHEMA = \"Rivendell\" and
	    ( (DATA_TYPE like \"%char%\") or (DATA_TYPE = \"text\") ) and
	    COLUMN_KEY !=\"UNI\" and
	    COLUMN_KEY !=\"MUL\" and
	    COLUMN_NAME != \"$table_key\"" )
        or die "Error executing Column Name Prepare : $DBI::errstr";

    $rcinfo2 = $mysqlinfo2->execute()
       or die "Error executing Column Name Select INFO: $DBI::errstr";
    
    while (my($table_column) = $mysqlinfo2->fetchrow())
    {
        Chk_Field($table_name,$table_column,$table_key);    
    }
}
$mysqlinfo2->finish();
$mysqlinfo1->finish();
##  Finished checking and fixing the Database Tables!
#   If we got here we can proceed to make Rivendell3 database tables.
#
#
print " Finished validity checking the Rivendell 2.0 database \n";
print "\n  *** Attempting to Copy Rivendell 2.0 tables to Rivendell 3.0 Database ***\n\n";
$mysqlinfo1 = $dbhinfo1->prepare (
   "select SCHEMA_NAME from SCHEMATA where SCHEMA_NAME=\"Rivendell3\"")
   or die "Error preparing select SCHEMA Rivendell3: DBI$::errstr";

$rcinfo1 = $mysqlinfo1->execute()
   or die "Error executing Select Rivendell3 from SCEMATA: $DBI::errstr";

my $found = $mysqlinfo1->fetchrow();

if (!$found)
{
    die "No Rivendell3 Database was found - Please Create A Rivendell3 database with \n the appropriate User Permissions for your User(s)!\n";
}
$mysqlinfo1->finish();

$dsnriv3 = "dbi:mysql:dbname=Rivendell3:host_name=$host";
$dbhriv3 = DBI->connect($dsnriv3,$user,$password)
   or die "Cant connect to Rivendell3 database - Check Permissions!: DBI$::errstr";

$mysqlinfo1 = $dbhinfo1->prepare (
   "select TABLE_NAME from COLUMNS where TABLE_SCHEMA=\"Rivendell3\"" )
    or die "Error preparing select Table and Column names: $DBI::errstr";

$rcinfo1 = $mysqlinfo1->execute()
   or die "Error executing Table Name From Rivendell3: $DBI::errstr";

$found = $mysqlinfo1->fetchrow();
if ($found)
{
    die " *** Rivendell3 Database TABLES were found - Aborting - must be an Empty Database! *** \n";
}

$mysqlinfo1->finish();

#  Get all the Rivendell 2.0 Table Names

$mysqlinfo1 = $dbhinfo1->prepare (
   "select TABLE_NAME from TABLES where TABLE_SCHEMA=\"Rivendell\"" )
    or die "Error preparing select Table and Column names: $DBI::errstr";

$rcinfo1 = $mysqlinfo1->execute()
   or die "Error executing Select Table Names From Rivendell2.0: $DBI::errstr";

my @All_Table_Names;
while (my $tablename = $mysqlinfo1->fetchrow())
{
    my $newtable="";
    push (@All_Table_Names,$tablename);
    my $stmt = "SHOW CREATE TABLE `$tablename` ";
    $mysqlriv2 = $dbhriv2->prepare( $stmt )
       or die "Cant SHOW CREATE TABLE $tablename: $DBI::errstr";
    $rcriv2 = $mysqlriv2->execute()
       or die "Error executing SHOW CREATE TABLE $tablename : $DBI::errstr";
    while (my $row = $mysqlriv2->fetchrow())
    {
        my $end = index($row,"ENGINE=");  #Find Engine - to ignore the rest
        if ($end != -1)
        {
	    my $newrow = substr($row,0,$end);
            $newrow .= "ENGINE=InnoDB DEFAULT CHARSET=utf8";
            $mysqlriv3 = $dbhriv3->prepare ("$newrow")
                or Abort_Create(\@All_Table_Names,$tablename,"Prepare Create Error");
            $rcriv3 = $mysqlriv3->execute()
                or Abort_Create(\@All_Table_Names,$tablename,"Execute Create Error");
            print "New Rivendell 3. $tablename Created...\n";
            $mysqlriv3->finish();
            $mysqlriv3 = $dbhriv3->prepare("INSERT INTO `$tablename` SELECT * from Rivendell.`$tablename`")
                or Abort_Create(\@All_Table_Names,$tablename, "Error preparing Insert Into $tablename");
	    $rcriv3 = $mysqlriv3->execute()
                or Abort_Create(\@All_Table_Names,$tablename, "Error executing Insert Into $tablename");

        }
    }
}

$mysqlriv3->finish();
# Update VERSION to 300
$mysqlriv3 = $dbhriv3->prepare("UPDATE VERSION SET DB=1000")
  or Abort_Create(\@All_Table_Names,"VERSIONUPDATEFAIL", "Error preparing Update VERSION");
$rcriv3 = $mysqlriv3->execute()
  or Abort_Create(\@All_Table_Names,"VERSIONUPDATEFAIL", "Error executing Update VERSION");
$mysqlriv3->finish();
$mysqlriv2->finish();
$mysqlinfo1->finish();

exit();
####################Subroutines
 
#####################################################################################
#  Chk_Field Subroutine - will check for and remove invalid characters in Fields
#
#   Parameters are :  Table Name , Column Name, Table Key Column
#####################################################################################
sub Chk_Field 
{

my $Table = shift;
my $Field = shift;
my $Key = shift;
my $change_count=0;

if (!defined($Table)) {return;}
if (!defined($Field)) {return;}
if (!defined($Key)) {return;}
#print "Checking $Field column in Table:  $Table The Key Column is : $Key\n";

my $found = "false";

$dsnriv2 = "dbi:mysql:dbname=Rivendell:host_name=$host";
$dbhriv2 = DBI->connect($dsnriv2,$user,$password)
   or die "Cant connect to database: DBI$::errstr";

$mysqlriv2 = $dbhriv2->prepare (
    " Select $Key, $Field from \`$Table\`  where $Field is NOT NULL and $Field != \" \" ")
  or die "Cant prepare statement: $DBI::errstr";

$rcriv2 = $mysqlriv2->execute()
   or die "Error executing statement : $DBI::errstr";

while (my ($col_key,$thefield) = $mysqlriv2->fetchrow()) {
    
        chomp($col_key);
        chomp($thefield);
        my @chars = map substr( $thefield, $_, 1), 0 .. length($thefield) - 1;
        my $new_field;
        my $a_char;
        foreach $a_char(@chars)
        {
            if ( ( (ord($a_char) > ord($Max_char)) ||
		 (ord($a_char) < ord($Min_char))) &&
		 (ord($a_char) != 13) &&         #ignore Carriage Returns
		 (ord($a_char) != 10) )          #ignore Line Feed
	    {
                $found = "true";
                $new_field = $new_field . $Replace_char;
                $change_count++;
		#print ("Illegal Character ! \n");
	    }
	    else
	    {
                $new_field = $new_field . $a_char;
            }
        }
        if ($found eq "true")
        {
            #print "Field was: $thefield\n";
            #print("=> $Field <=  Field will be changed to:\n   $new_field \n");
            #$mysqlriv2 = $dbhriv2->prepare (
               #"UPDATE $Table SET $Field = \"$new_field\" WHERE $Key = '$col_key'")
            #or die  "Error executing statement : $DBI::errstr";

            #my $rcriv2 = $mysqlriv2->execute()
              #or die "Error executing UPDATE Field statement : $DBI::errstr";
        }
}
if ($found eq "true")  
{
    print " In Rivendell 2.0 Tables $change_count instances were fixed \n";
}
return;
}
#####################################################################################
#  Abort_Create - will Back out all Successful Table Creations 
#
#   Parameters are :  Table Name Array
#####################################################################################
sub Abort_Create
{
    my ($table_array_ref,$last_table,$msg) = @_;

    foreach my $table (@$table_array_ref) 
    {
        if ($table ne $last_table)
	{
            $mysqlriv3 = $dbhriv3->prepare ("DROP TABLE `$table`")
                or die "Failed Prepare - DROP Table $table $DBI::errstr";
 
            $rcriv3 = $mysqlriv3->execute()
            or die "Error Executing DROP Table $table \n$DBI::errstr";
            print " Rivendell 3 Table - $table DELETED!\n";
        }
    }
die " Error Detected: $msg on TABLE $last_table\n$DBI::errstr";
}
