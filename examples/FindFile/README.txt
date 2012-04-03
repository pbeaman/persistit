====
    END USER LICENSE AGREEMENT (“EULA”)

    READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
    http://www.akiban.com/licensing/20110913

    BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
    ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
    AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.

    IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
    THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
    NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
    YOUR INITIAL PURCHASE.

    IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
    CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
    FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
    LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
    BY SUCH AUTHORIZED PERSONNEL.

    IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
    USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
    PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
