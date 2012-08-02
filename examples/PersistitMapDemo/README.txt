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


PersistitMapDemo uses a com.persistit.PersistitMap, which
implements java.util.SortedMap, to store a copy of the system
properties. Each time you run this program it will compare the current
system properties to those that were stored previously and will
display any differences.

To build PersistitMapDemo:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../core/target/akiban-persistit-xx.yy.zz-SNAPSHOT-jar-with-dependencies-and-tests.jar PersistitMapDemo.java

To run PersistitMapDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../core/target/akiban-persistit-xx.yy.zz-SNAPSHOT-jar-with-dependencies-and-tests.jar;. PersistitMapDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
