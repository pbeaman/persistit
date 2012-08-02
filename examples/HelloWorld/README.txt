====
    Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
    
    This program and the accompanying materials are made available
    under the terms of the Eclipse Public License v1.0 which
    accompanies this distribution, and is available at
    http://www.eclipse.org/legal/epl-v10.html
    
    This program may also be available under different license terms.
    For more information, see www.akiban.com or contact licensing@akiban.com.
    
    Contributors:
    Akiban Technologies, Inc.
====


HelloWorld performs the following steps:

-  Initializes com.persistit.Persistit using a Properties file.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a com.persistit.Tree.
-  Stores a key/value pair.
-  Traverses the tree to find and display the key and value.
-  Closes Persistit to ensure all records have been written to disk.

To build HelloWorld:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../core.target/akiban-persistit-xx.yy.zz-SNAPSHOT-jar-with-dependencies-and-tests.jar HelloWorld.java

To run HelloWorld:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../core/target/akiban-persistit-xx.yy.zz-SNAPSHOT-jar-with-dependencies-and-tests.jar;. HelloWorld

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
