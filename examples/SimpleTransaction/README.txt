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

SimpleTransaction performs the following steps:

-  Initializes com.persistit.Persistit using a Properties file.
-  Launches multiple threads that concurrently perform simple transactions. 
-  Each transaction randomly transfers "money" between "accounts"
   such that the sum of all "accounts" remains unchanged.
-  Closes Persistit to ensure all records have been written to disk.

To build SimpleTransaction:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleTransaction.java

To run SimpleTransaction:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleTransaction

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.

To verify that transactional integrity is preserved across failures:

	Interrupt SimpleTransaction as it runs by stopping the JVM, shutting down
	the operating system or powering off the computer.
	Restart SimpleTransaction to get a recalculated "balance".  The 
	result should be zero.
	
