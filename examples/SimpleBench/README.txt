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

SimpleBench is a simple demonstration of Persistit's insert and retrieval
and performance. This class performs the following cycle:

-  Initializes com.persistit.Persistit using built-in default properties.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a tree.
-  Removes any data formerly stored in the tree.
-  Inserts numerous String values, indexed by  small integers.
-  Traverses all keys in forward-sequential order.
-  Traverses all keys in reverse-sequential order.
-  Updates numerous String values with equal length using random keys.
-  Lengthens all String values in forward-seqential key order.
-  Fetches numerous records, using random keys.
-  Flushes all pending updates to disk

To build SimpleBench:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleBench.java

To run SimpleBench:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleBench

By default, SimpleBench will create its volume, prewrite journal and log files
in your current working directory. You can change the location of these files
by specifying system properties, for example:

	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;.
		-Dcom.persistit.datapath=/myDataPath
		-Dcom.persistit.logpath=/myLogPath
		 SimpleBench
