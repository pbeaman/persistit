************************************
Akiban Persistit
************************************

Overview
========
We have worked hard to make Akiban Persistit™ exceptionally fast, reliable, simple and lightweight. We hope you will enjoy learning more about it and using it.

Akiban Persistit is a key/value data storage library written in Java™. Key features include:

- Support for highly concurrent transaction processing with multi-version concurrency control
- Optimized serialization and deserialization mechanism for Java primitives and objects
- Multi-segment (compound) keys to enable a natural logical key hierarchy
- Support for long records (megabytes)
- Implementation of a persistent SortedMap
- Extensive management capability including command-line and GUI tools

See http://www.akiban.com/akiban-persistit for a more detailed summary of features and benefits, licensing information and how to get support.

Documentation
=============
Users Guide: http://www.akiban.com/ak-docs/admin/persistit

JavaDoc: http://www.akiban.com/sites/all/libraries/persistit-api/index.html

Building Akiban Persistit From Source
=====================================
Use Maven (http://maven.apache.org) to build Persistit.

To build::

  mvn install

The resulting jar files are in the ``target`` directory. To build the Javadoc::

  mvn javadoc:javadoc

The resulting Javadoc HTML files are in ``target/site/apidocs``.

Building and Running the Examples
---------------------------------

Small examples are located in the ``examples`` directory. Each has a short README file describing the example, and an Ant build script (http://ant.apache.org). After building the main akiban-persistit jar file using Maven, you may run::

  ant run

in each of the examples subdirectories to build and run the examples.


Download and Install Akiban Persistit From Binaries
---------------------------------------------------

Pre-built jars can be downloaded directly from http://akiban.com/akiban-persistit.

Unpack the distribution kit into a convenient directory using the appropriate utility (e.g. unzip or tar).

Review the ``LICENSE.txt`` file located in the root of the installation directory. Persistit is licensed under the Eclipse Public License or a free-use community license, see our `licensing options <http://www.akiban.com/akiban-licensing-options>`_ for more details. By installing, copying or otherwise using the Software contained in the distribution kit, you agree to be bound by the terms of the license agreement. If you do not agree to these terms, remove and destroy all copies of the software in your possession immediately.

Working With Persistit
----------------------

Add the jar file (e.g. ``akiban-persistit-3.1.8.jar``), found in the root directory of the distribution kit, to your project's classpath. For example, copy it to ``jre/lib/ext`` in your Java Runtime Environment, or add it to your classpath environment variable. 

That's it. You are ready to work with Persistit.
