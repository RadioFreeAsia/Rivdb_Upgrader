# Rivdb_Upgrader
Rivendell Database Upgrade to Rivendell 3.0 Script

This is a script which performs the following functions to a Rivendell Database.

     -    Checks that it is Schema 259. It will only update that schema.
     -    Removes any non valid Ascii characters from Non-Key DB Columns.
     -    Copies the entire Rivendell 2.x DB to an already existing  empty Rivendell 3.0. DB. The new tables
          are all InnoDB tables and character Set is UTF8.
