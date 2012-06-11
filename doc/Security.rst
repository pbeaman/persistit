.. _Security:

Security Notes
==============

Akiban Persistit provides no built-in data access control because it is intended for embedded use in applications that provide their own logical access control. Security-conscious applications must also prevent unauthorized access to Persistit's physical files and to the Persistit API. The following resources must be protected from unauthorized access:

- volume and journal files
- configuration properties file, if one is used
- access by unauthorized code to the API exposed by the Persistit library
- the port opened and exposed by Persistit when either the ``com.persistit.rmiport`` or ``com.persistit.rmihost`` property is set. If you are using Persistit's remote administration feature, be sure to block unauthorized access to the RMI port.
- access to MXBeans via JMX
- the CLI server (see :ref:`cliserver`) if instantiated

In addition to these general deployment considerations, Persistit requires certain permissions in an environment controlled by a security manager.

Java programs run from the command line typically do not install a security manager, and therefore implicitly grant all permissions. However, when Persistit is used within an Applet, or within any framework that installs a security manager, it is important to understand what permissions are required.

Security Domains
----------------

This section assumes a basic understanding of the Java security model. See `New Protection Mechanisms - Overview of Basic Concepts <http://docs.oracle.com/javase/1.5.0/docs/guide/security/spec/security-spec.doc2.html>`_ for further information.

Note that when Java is started from the command line, as is often the case in server applications, all security privileges are granted by default. The information in this section is intended for cases where security privileges need to be controlled.

Akiban Persistit performs various security-sensitive operations: it reads and writes files, it reads system properties, it optionally opens a TCP/IP socket, and it performs various security-sensitive operations related to reflection and serialization. These operations are divided into two categories: those required only by the Persistit library itself, and those required by the application that is using Persistit. For example, the application code must have permission to read and write files, but it does not require permission to access private fields through reflection; only the Persistit library itself needs this permission. The latter operation is called a “privileged” operation because Persistit invokes the access controller's doPrivileged method to establish its permission to perform the privileged operation.

At a practical level, this means you can create two separate security domains for applications embedding Persistit.  One domain is specific to the Persistit library itself, and grants all the permissions required by Persistit, including the privileged permissions. The other domain includes the application, and grants only the non-privileged permissions.

Using the default java.lang.SecurityManager implementation, you define domains and the permissions available to them in a policy file stored in the user's home directory.  For those already familiar with the policy file format, here is a security policy file that illustrates these concepts:

.. code-block:: java

  grant codeBase "file:/appdir/myapplication.jar" {
    permission java.io.FilePermission "e:\\data\\*", "read, write, delete";
    permission java.io.FilePermission "e:\\logs\\*", "read, write, delete";
    permission java.net.SocketPermission "localhost",
           	  "accept, connect, listen, resolve";
  };

  grant codeBase "file:/lib/akiban-persistit.jar" {
    permission java.io.FilePermission "<<ALL FILES>>", "read, write, delete";
    permission java.net.SocketPermission "*:1099-",
         	  "accept, connect, listen, resolve";
    permission java.lang.reflect.ReflectPermission "suppressAccessChecks";
    permission java.io.SerializablePermission "enableSubclassImplementation";
    permission java.util.PropertyPermission "com.persistit.*", "read";
    permission java.lang.RuntimePermission "accessDeclaredMembers";
  };

This policy file sets up two security domains. One covers the application code in ``myapplication.jar`` and grants just the restricted set of permissions needed by the application, while the other grants additional permissions to the Persistit library.

Although the file and socket permissions granted by the privileged domain are less restrictive than those granted to ``myapplication.jar``, the actual permission granted to running code is the intersection of these two and is therefore restricted to just the set of files and sockets granted to myapplication.jar.

Permissions Required for DefaultValueCoder
------------------------------------------

DefaultValueCoder performs three security-sensitive operations:

- It enumerates the declared fields of the class being serialized,
- It reads and writes data from and to those fields using reflection even if they are private, and
- It overrides the default implementations of java.io.ObjectInputStream and java.io.ObjectOutputStream.

If a SecurityManager is installed then three permissions must be granted to enable this new mechanism::

  java.lang.RuntimePermission "accessDeclaredMembers";
  java.lang.reflect.ReflectPermission("suppressAccessChecks")
  java.io.SerializablePermission("enableSubclassImplementation")

Persistit acquires these permissions through privileged operations, meaning that only the Persistit library domain needs to have them – they do not need to be and should not be granted to the application domain.

Permission Required for Reading System Properties
-------------------------------------------------

Persistit attempts to read system properties whose names begin with “com.persistit.” Specific permission to do this is granted by the line::

   permission java.util.PropertyPermission "com.persistit.*", "read";

Again, only the Persistit library domain needs to have this permission. If this permission is not granted, Persistit ignores all system properties.

Permissions Required for File and Socket I/O
--------------------------------------------

Persistit needs permission to read and write its volume and journal files, to read a configuration properties file and (optionally) write to a log file.  File I/O permissions also apply to the source and destination files specified for Import and Export tasks available within the AdminUI utility. In addition, if you specify either the rmihost or rmiport property to enable remote administration, Persistit needs permission to create RMI connections.

These are not privileged operations, meaning that if the policy establishes separate domains for the application and the Persistit library, both domains must grant permission for all I/O operations. (If they were privileged operations, an untrusted application code could use the Persistit library as a proxy to perform malicious file I/O.) As is defined by the Java security mechanism, when Persistit attempts to open a file, permissions of both the application domain and the Persistit library domains are checked; if the operation is denied by either domain then the attempt fails with a java.security.AccessControlException.

As specified in the sample policy file above, the Persistit library domain has been granted permissions on `<<ALL FILES>>`. This means that the application domain controls what subset of the file system is accessible.  In the example, files may only be read and written to the e:\data and e:\logs directories on a Windows box.  (See the Java documentation on Permissions for details on how to construct File and Socket permissions.)

Deploying Persistit as an Installed Optional Package
----------------------------------------------------

A convenient way to grant Persistit the permissions required to perform its privileged operations is to install it as an optional package. The Sun Java Runtime Environment treats JAR files located in the ``<jre-home>/lib/ext`` directory as optional Java extension classes, and by default grants them the same privileges as Java system classes. If the Persistit library is loaded from this directory then only the application File and Socket privileges need to be granted explicitly through a security policy. To deploy Persistit in this manner simply copy the Persistit library jar file to the appropriate ``*jre-home*/lib/ext`` directory.

