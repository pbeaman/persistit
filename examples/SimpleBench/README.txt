====
    Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, version 3 (only) of the
    License.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    This program may also be available under different license terms. For more
    information, see www.akiban.com or contact licensing@akiban.com.
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
