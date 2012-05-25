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
