====
    Copyright (C) 2011 Akiban Technologies Inc.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see http://www.gnu.org/licenses.
====


FindFile is a small Swing application that demonstrates use of Persistit's
KeyFilter class, the Exchange pool, and use of Persistit with Swing.

To use FindFile, first build a text file containing a (large) list of file
names. To to this, use one of these commands:

	Windows:	dir /b /s C:\ >> FileNames.txt
	Unix/Linux:	ls -1 -p -R > FileNames.txt

After launching FindFile, specify this file as the source file to load and 
click the "Load" button.

Or you can specify the full path name of a directory, press the "Load" button
and crawl the file system directory.

This demo loads the file names from a text file rather than 
enumerating all the files on the disk through Java file operations
for simplicity, and also because we want you to see that Persistit 
is capable of loading and indexing a lot of information in just a few 
seconds.

To build FindFile:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar FindFile.java

To run FindFile:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. FindFile

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property 
of this configuration file.
