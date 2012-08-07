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
