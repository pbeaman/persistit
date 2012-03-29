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

Copyright (c) 2004 Persistit Corporation. All Rights Reserved.

SimpleDemo is a very simple example to demonstrate basic operations of
Persistit. This class performs the following operations:

-  Initializes com.persistit.Persistit using a Properties file.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a com.persistit.Tree.
-  Reads some names from the console and stores records in the <tt>Tree</tt>
   indexed by those names.
-  Dispays the names in alphabetical order.
-  Closes Persistit to ensure all records have been written to disk.

To build SimpleDemo:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../lib/persistit_jsa110.jar SimpleDemo.java

To run SimpleDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../lib/persistit_jsa110.jar;. SimpleDemo

To run SimpleDemo without building it first:

	java -classpath ../../lib/persistit_jsa110.jar;../../lib/examples.jar SimpleDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
