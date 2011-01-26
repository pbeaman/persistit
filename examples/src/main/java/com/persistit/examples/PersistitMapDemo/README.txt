PersistitMapDemo uses a com.persistit.PersistitMap, which
implements java.util.SortedMap, to store a copy of the system
properties. Each time you run this program it will compare the current
system properties to those that were stored previously and will
display any differences.

To build PersistitMapDemo:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar PersistitMapDemo.java

To run PersistitMapDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. PersistitMapDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
