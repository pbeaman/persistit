/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;

import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PropertiesNotFoundException;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.util.Util;

/**
 * <p>
 * Manages and stores a Persistit configuration. Fields of this class contain
 * the configuration data used when Persistit starts up. These elements can be
 * populated from a Properties object or set through the accessor methods.
 * </p>
 * <p>
 * A <code>Config</code> is a lightweight object containing no references to the
 * main Persistit instance.
 * </p>
 * 
 * @version 1.1
 */
public class Configuration {
    /**
     * Prefix used to form the a system property name. For example, the property
     * named <code>journalpath=xyz</code> can also be specified as a system
     * property on the command line with the option
     * -Dcom.persistit.journalpath=xyz.
     */
    public final static String SYSTEM_PROPERTY_PREFIX = "com.persistit.";

    /**
     * Default suffix for properties file name
     */
    public final static String DEFAULT_PROPERTIES_FILE_SUFFIX = ".properties";

    /**
     * Default properties file name
     */
    public final static String DEFAULT_CONFIG_FILE = "persistit.properties";
    /**
     * Default maximum time (in milliseconds) to allow for successful completion
     * of an operation.
     */
    static final long DEFAULT_TIMEOUT_VALUE = 30000; // Thirty seconds
    /**
     * Upper bound maximum time (in milliseconds) to allow for successful
     * completion of an operation. This is the maximum timeout value you can
     * specify for individual <code>Exchange</code>.
     */
    static final long MAXIMUM_TIMEOUT_VALUE = 86400000; // One day
    /**
     * Property name by which name of properties file can be supplied.
     */
    public final static String CONFIG_FILE_PROPERTY_NAME = "properties";
    /**
     * Property name prefix for specifying buffer size and count. The full
     * property name should be one of "1024", "2048", "4096", "8192" or "16384"
     * appended to this string, e.g., "buffer.count.8192".
     */
    public final static String BUFFERS_PROPERTY_NAME = "buffer.count.";
    /**
     * Property name prefix for specifying buffer memory allocation. The full
     * property name should be one of "1024", "2048", "4096", "8192" or "16384"
     * appended to this string, e.g., "buffer.memory.8192". This property is an
     * alternative to "buffer.count.nnnn", and only one of these may be used in
     * a configuration per buffer size. With the buffer.memory property
     * Persistit computes a buffer count that will consume approximately the
     * specified memory allocation, including overhead for FastIndex elements.
     */
    public final static String BUFFER_MEM_PROPERTY_NAME = "buffer.memory.";
    /**
     * Property name prefix for specifying Volumes. The full property name
     * should be a unique ordinal number appended to this string, e.g.,
     * "volume.1", "volume.2", etc.
     */
    public final static String VOLUME_PROPERTY_PREFIX = "volume.";
    /**
     * Property name for specifying the file specification for the journal.
     */
    public final static String JOURNAL_PATH_PROPERTY_NAME = "journalpath";

    /**
     * Property name for specifying the size of each journal file, e.g.,
     * "journalsize=400000000".
     */
    public final static String JOURNAL_BLOCKSIZE_PROPERTY_NAME = "journalsize";

    /**
     * Default path name for the journal. Note, sequence suffix in the form
     * .nnnnnnnnnnnnnnnn (16 digits, zero-filled) will be appended.
     */
    public final static String DEFAULT_JOURNAL_PATH = "persistit_journal";

    /**
     * Default System Volume Name
     */
    public final static String DEFAULT_SYSTEM_VOLUME_NAME = "_system";

    /**
     * Property name for specifying the system volume name
     */
    public final static String SYSTEM_VOLUME_PROPERTY_NAME = "sysvolume";

    /**
     * Property name for specifying default temporary volume page size
     */
    public final static String TEMPORARY_VOLUME_PAGE_SIZE_PROPERTY_NAME = "tmpvolpagesize";

    /**
     * Property name for specifying default temporary volume directory
     */
    public final static String TEMPORARY_VOLUME_DIR_PROPERTY_NAME = "tmpvoldir";

    /**
     * Property name for specifying upper bound on temporary volume size
     */
    public final static String TEMPORARY_VOLUME_MAX_SIZE_PROPERTY_NAME = "tmpvolmaxsize";
    public final static long MINIMUM_TEMP_VOL_MAX_SIZE = 16384 * 4;
    public final static long MAXIMUM_TEMP_VOL_MAX_SIZE = Long.MAX_VALUE;
    /**
     * Property name for specifying the default {@link Transaction.CommitPolicy}
     * ("soft", "hard" or "group")
     */
    public final static String COMMIT_POLICY_PROPERTY_NAME = "txnpolicy";

    /**
     * Property name for specifying whether Persistit should display diagnostic
     * messages. Property value must be "true" or "false".
     */
    public final static String VERBOSE_PROPERTY = "verbose";
    /**
     * Property name for specifying whether Persistit should retry read
     * operations that fail due to IOExceptions.
     */
    public final static String READ_RETRY_PROPERTY_NAME = "readretry";
    /**
     * Property name for maximum length of time a Persistit operation will wait
     * for I/O completion before throwing a TimeoutException.
     */
    public final static String TIMEOUT_PROPERTY = "timeout";

    /**
     * Property name to specify package and/or class names of classes that must
     * be serialized using standard Java serialization.
     */
    public final static String SERIAL_OVERRIDE_PROPERTY_NAME = "serialOverride";

    /**
     * Property name to specify whether DefaultValueCoder should use a declared
     * no-arg contructor within each class being deserialized. Unless specified
     * as <code>true</code>, Serializable classes will be instantiated through
     * platform-specific API calls.
     */
    public final static String CONSTRUCTOR_OVERRIDE_PROPERTY_NAME = "constructorOverride";

    /**
     * Property name for specifying whether Persistit should attempt to launch a
     * diagnostic utility for viewing internal state.
     */
    public final static String SHOW_GUI_PROPERTY_NAME = "showgui";

    /**
     * Property name for log switches
     */
    public final static String LOGGING_PROPERTIES_NAME = "logging";

    /**
     * Property name for log file name
     */
    public final static String LOGFILE_PROPERTY_NAME = "logfile";
    /**
     * Property name for the optional RMI registry host name
     */
    public final static String RMI_REGISTRY_HOST_PROPERTY_NAME = "rmihost";
    /**
     * Property name for the optional RMI registry port
     */
    public final static String RMI_REGISTRY_PORT_PROPERTY_NAME = "rmiport";

    /**
     * Name of port on which RMI server accepts connects. If zero or unassigned,
     * RMI picks a random port. Specifying a port can be helpful when using SSH
     * to tunnel RMI to a server.
     */
    public final static String RMI_SERVER_PORT_PROPERTY_NAME = "rmiserverport";

    /**
     * Property name for enabling Persistit Open MBean for JMX
     */
    public final static String ENABLE_JMX_PROPERTY_NAME = "jmx";

    /**
     * Property name for pseudo-property "timestamp";
     */
    public final static String TIMESTAMP_PROPERTY = "timestamp";

    /**
     * Property name for the "append only" property.
     */
    public final static String APPEND_ONLY_PROPERTY = "appendonly";

    /**
     * Property name to specify the default {@link SplitPolicy}.
     */
    public final static String SPLIT_POLICY_PROPERTY_NAME = "splitpolicy";

    /**
     * Property name to specify the default {@link JoinPolicy}.
     */
    public final static String JOIN_POLICY_PROPERTY_NAME = "joinpolicy";

    private final static SplitPolicy DEFAULT_SPLIT_POLICY = SplitPolicy.PACK_BIAS;
    private final static JoinPolicy DEFAULT_JOIN_POLICY = JoinPolicy.EVEN_BIAS;
    private final static CommitPolicy DEFAULT_TRANSACTION_COMMIT_POLICY = CommitPolicy.SOFT;

    private final static long KILO = 1024;
    private final static long MEGA = KILO * KILO;
    private final static long GIGA = MEGA * KILO;
    private final static long TERA = GIGA * KILO;

    private final static int[] BUFFER_SIZES = { 1024, 2048, 4096, 8192, 16384 };

    public static class BufferMemorySpecification {

        private int bufferSize = -1;
        private int minimumCount = 0;
        private int maximumCount = Integer.MAX_VALUE;
        private long minimumMemory = 0;
        private long maximumMemory = Long.MAX_VALUE;
        private long reservedMemory = 0;
        private float fraction = 1.0f;

        /**
         * @return the bufferSize
         */
        public int getBufferSize() {
            return bufferSize;
        }

        /**
         * @param bufferSize
         *            the bufferSize to set
         */
        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        /**
         * @return the minimumCount
         */
        public int getMinimumCount() {
            return minimumCount;
        }

        /**
         * @param minimumCount
         *            the minimumCount to set
         */
        public void setMinimumCount(int minimumCount) {
            this.minimumCount = minimumCount;
        }

        /**
         * @return the maximumCount
         */
        public int getMaximumCount() {
            return maximumCount;
        }

        /**
         * @param maximumCount
         *            the maximumCount to set
         */
        public void setMaximumCount(int maximumCount) {
            this.maximumCount = maximumCount;
        }

        /**
         * @return the minimumMemory
         */
        public long getMinimumMemory() {
            return minimumMemory;
        }

        /**
         * @param minimumMemory
         *            the minimumMemory to set
         */
        public void setMinimumMemory(long minimumMemory) {
            this.minimumMemory = minimumMemory;
        }

        /**
         * @return the maximumMemory
         */
        public long getMaximumMemory() {
            return maximumMemory;
        }

        /**
         * @param maximumMemory
         *            the maximumMemory to set
         */
        public void setMaximumMemory(long maximumMemory) {
            this.maximumMemory = maximumMemory;
        }

        /**
         * @return the reservedMemory
         */
        public long getReservedMemory() {
            return reservedMemory;
        }

        /**
         * @param reservedMemory
         *            the reservedMemory to set
         */
        public void setReservedMemory(long reservedMemory) {
            this.reservedMemory = reservedMemory;
        }

        /**
         * @return the fraction
         */
        public float getFraction() {
            return fraction;
        }

        /**
         * @param fraction
         *            the fraction to set
         */
        public void setFraction(float fraction) {
            this.fraction = fraction;
        }

        public int computeBufferCount(final long availableHeapMemory) {
            if (bufferSize == -1) {
                return 0;
            }
            long maximumAvailable = (long) ((availableHeapMemory - reservedMemory) * fraction);
            long allocation = Math.min(maximumAvailable, maximumMemory);
            int bufferSizeWithOverhead = Buffer.bufferSizeWithOverhead(bufferSize);
            int buffers = Math.max(minimumCount, Math.min(maximumCount, (int) (allocation / bufferSizeWithOverhead)));
            if (buffers < BufferPool.MINIMUM_POOL_COUNT || buffers > BufferPool.MAXIMUM_POOL_COUNT
                    || buffers * bufferSizeWithOverhead > maximumAvailable) {
                throw new IllegalArgumentException(String.format(
                        "Invalid buffer pool configuration: %,d buffers in %sb of maximum available memory", buffers,
                        Persistit.displayableLongValue(maximumAvailable)));
            }
            return buffers;
        }
        
        public String toString() {
            StringBuilder sb = new StringBuilder("BufferMemorySpecification(");
            sb.append(String.format("size=%d", bufferSize));
            if (minimumCount == maximumCount) {
                sb.append(String.format(",count=%,d", minimumCount));
            } else  if ( minimumCount != 0 || maximumCount != Integer.MAX_VALUE) {
                sb.append(String.format(",minCount=%,d,maxCount=%,d", minimumCount, maximumCount));
            }
            if (minimumMemory != 0 || maximumMemory != Long.MAX_VALUE || reservedMemory != 0 || fraction != 1.0f) {
                sb.append(String.format(",minMem=%s,maxMem=%s,reserved=%s,fraction=%f",
                        Persistit.displayableLongValue(minimumMemory),
                        Persistit.displayableLongValue(maximumMemory),
                        Persistit.displayableLongValue(reservedMemory),
                        fraction));
            }
            sb.append(')');
            return sb.toString();
        }
    }

    private final Properties _properties = new Properties();

    private long timeoutValue = DEFAULT_TIMEOUT_VALUE;
    private BufferMemorySpecification[] buffers = new BufferMemorySpecification[BUFFER_SIZES.length];
    private List<VolumeSpecification> volumeSpecifications = new ArrayList<VolumeSpecification>();
    private String journalPath = DEFAULT_JOURNAL_PATH;
    private long journalSize;
    private String sysVolume = DEFAULT_SYSTEM_VOLUME_NAME;
    private CommitPolicy commitPolicy = DEFAULT_TRANSACTION_COMMIT_POLICY;
    private JoinPolicy joinPolicy = DEFAULT_JOIN_POLICY;
    private SplitPolicy splitPolicy = DEFAULT_SPLIT_POLICY;
    private boolean verbose;
    private boolean readRetry;
    private String serialOverride;
    private boolean constructorOverride;
    private boolean showGUI;
    private String logging;
    private String logFile;
    private String rmiHost;
    private int rmiPort;
    private int rmiServerPort;
    private boolean jmx;
    private boolean appendOnly;
    private String tmpVolDir;
    private int tmpVolPageSize;
    private long tmpVolMaxSize;

    public Configuration() {
        for (int index = 0; index < BUFFER_SIZES.length; index++) {
            buffers[index] = new BufferMemorySpecification();
        }
    }

    void readPropertiesFile() throws PersistitException {
        readPropertiesFile(getProperty(CONFIG_FILE_PROPERTY_NAME, DEFAULT_CONFIG_FILE));
    }

    void readPropertiesFile(final String propertiesFileName) throws PersistitException {
        Properties properties = new Properties();
        try {
            if (propertiesFileName.contains(DEFAULT_PROPERTIES_FILE_SUFFIX)
                    || propertiesFileName.contains(File.separator)) {
                properties.load(new FileInputStream(propertiesFileName));
            } else {
                ResourceBundle bundle = ResourceBundle.getBundle(propertiesFileName);
                for (Enumeration<String> e = bundle.getKeys(); e.hasMoreElements();) {
                    final String key = e.nextElement();
                    properties.put(key, bundle.getString(key));
                }
            }
        } catch (FileNotFoundException fnfe) {
            // A friendlier exception when the properties file is not found.
            throw new PropertiesNotFoundException(fnfe.getMessage());
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
        merge(properties);
    }

    final static int bufferSizeIndex(final int bufferSize) {
        for (int index = 0; index < BUFFER_SIZES.length; index++) {
            if (BUFFER_SIZES[index] == bufferSize) {
                return index;
            }
        }
        return -1;
    }

    void merge(final Properties properties) {
        for (final Enumeration<? extends Object> e = properties.propertyNames(); e.hasMoreElements();) {
            final String propertyName = (String) e.nextElement();
            _properties.put(propertyName.toLowerCase(), properties.getProperty(propertyName));
        }
    }

    void loadProperties() throws PersistitException {
        setAppendOnly(getBooleanProperty(APPEND_ONLY_PROPERTY, false));
        setCommitPolicy(getProperty(COMMIT_POLICY_PROPERTY_NAME));
        setConstructorOverride(getBooleanProperty(CONSTRUCTOR_OVERRIDE_PROPERTY_NAME, false));
        setJmxEnabled(getBooleanProperty(ENABLE_JMX_PROPERTY_NAME, false));
        setJoinPolicy(getProperty(JOIN_POLICY_PROPERTY_NAME));
        setJournalPath(getProperty(JOURNAL_PATH_PROPERTY_NAME, DEFAULT_JOURNAL_PATH));
        setJournalSize(getLongProperty(JOURNAL_BLOCKSIZE_PROPERTY_NAME, JournalManager.DEFAULT_BLOCK_SIZE));
        setLogFile(getProperty(LOGFILE_PROPERTY_NAME));
        setLogging(getProperty(LOGGING_PROPERTIES_NAME));
        setTimeoutValue(getLongProperty(TIMEOUT_PROPERTY, DEFAULT_TIMEOUT_VALUE));
        setTmpVolDir(getProperty(TEMPORARY_VOLUME_DIR_PROPERTY_NAME));
        setTmpVolPageSize(getIntegerProperty(TEMPORARY_VOLUME_PAGE_SIZE_PROPERTY_NAME, 0));
        setTmpVolMaxSize(getLongProperty(TEMPORARY_VOLUME_MAX_SIZE_PROPERTY_NAME, MAXIMUM_TEMP_VOL_MAX_SIZE));
        setReadRetry(getBooleanProperty(READ_RETRY_PROPERTY_NAME, false));
        setRmiHost(getProperty(RMI_REGISTRY_HOST_PROPERTY_NAME));
        setRmiPort((int) getLongProperty(RMI_REGISTRY_PORT_PROPERTY_NAME, 0));
        setRmiServerPort((int) getLongProperty(RMI_SERVER_PORT_PROPERTY_NAME, 0));
        setSerialOverride(getProperty(SERIAL_OVERRIDE_PROPERTY_NAME));
        setShowGUI(getBooleanProperty(SHOW_GUI_PROPERTY_NAME, false));
        setSplitPolicy(getProperty(SPLIT_POLICY_PROPERTY_NAME));
        setSysVolume(getProperty(SYSTEM_VOLUME_PROPERTY_NAME, DEFAULT_SYSTEM_VOLUME_NAME));

        loadPropertiesBufferSpecifications();
        loadPropertiesVolumeSpecifications();
    }

    void loadPropertiesBufferSpecifications() {
        for (int index = 0; index < BUFFER_SIZES.length; index++) {
            int size = BUFFER_SIZES[index];

            final String countPropertyName = BUFFERS_PROPERTY_NAME + size;
            final String memPropertyName = BUFFER_MEM_PROPERTY_NAME + size;

            String countSpec = getProperty(countPropertyName);
            String memSpec = getProperty(memPropertyName);
            int count = 0;

            if (countSpec != null) {
                parseBufferMemorySpecification(countPropertyName, countSpec);
                count++;
            }
            if (memSpec != null) {
                parseBufferMemorySpecification(memPropertyName, memSpec);
                count++;
            }

            if (count > 1) {
                throw new IllegalArgumentException("Only one of " + countPropertyName + " and " + memPropertyName
                        + " may be specified");
            }
        }
    }

    void loadPropertiesVolumeSpecifications() throws InvalidVolumeSpecificationException {
        for (Enumeration<?> enumeration = _properties.propertyNames(); enumeration.hasMoreElements();) {
            String key = (String) enumeration.nextElement();
            if (key.startsWith(VOLUME_PROPERTY_PREFIX)) {
                boolean isOne = true;
                try {
                    Integer.parseInt(key.substring(VOLUME_PROPERTY_PREFIX.length()));
                } catch (NumberFormatException nfe) {
                    isOne = false;
                }
                if (isOne) {
                    final String vstring = getProperty(key);
                    volumeSpecifications.add(volumeSpecification(vstring));
                }
            }
        }
    }

    final static int bufferSizeFromPropertyName(final String propertyName) {
        if (propertyName.startsWith(BUFFERS_PROPERTY_NAME) || propertyName.startsWith(BUFFER_MEM_PROPERTY_NAME)) {
            String[] s = propertyName.split("\\.");
            try {
                int size = Integer.parseInt(s[2]);
                if (bufferSizeIndex(size) != -1) {
                    return size;
                }
            } catch (Exception e) {
                // default to -1
            }
        }
        return -1;
    }

    /**
     * Replaces substitution variables in a supplied string with values taken
     * from the properties available to Persistit (see {@link #getProperties()}
     * ).
     * 
     * @param text
     *            String in in which to make substitutions
     * @param properties
     *            Properties containing substitution values
     * @return text with substituted property values
     */
    public String substituteProperties(String text, Properties properties) {
        return substituteProperties(text, properties, 0);
    }

    /**
     * Replaces substitution variables in a supplied string with values taken
     * from the properties available to Persistit (see {@link getProperty}).
     * 
     * @param text
     *            String in in which to make substitutions
     * @param properties
     *            Properties containing substitution values
     * @param depth
     *            Count of recursive calls - maximum depth is 20. Generall
     * @return
     */
    String substituteProperties(String text, Properties properties, int depth) {
        int p = text.indexOf("${");
        while (p >= 0 && p < text.length()) {
            p += 2;
            int q = text.indexOf("}", p);
            if (q > 0) {
                String propertyName = text.substring(p, q);
                if (Util.isValidName(propertyName)) {
                    // sanity check to prevent stack overflow
                    // due to infinite loop
                    if (depth > 20)
                        throw new IllegalArgumentException("property " + propertyName
                                + " substitution cycle is too deep");
                    String propertyValue = getProperty(propertyName, depth + 1, properties);
                    if (propertyValue == null)
                        propertyValue = "";
                    text = text.substring(0, p - 2) + propertyValue + text.substring(q + 1);
                } else
                    break;
            } else {
                break;
            }
            p = text.indexOf("${");
        }
        return text;
    }

    public Properties getProperties() {
        return _properties;
    }

    /**
     * <p>
     * Returns a property value, or <code>null</code> if there is no such
     * property. The property is taken from one of the following sources:
     * <ol>
     * <li>A system property having a prefix of "com.persistit.". For example,
     * the property named "journalpath" can be supplied as the system property
     * named com.persistit.journalpath. (Note: if the security context does not
     * permit access to system properties, then system properties are ignored.)</li>
     * <li>The supplied Properties object, which was either passed to the
     * {@link #initialize(Properties)} method, or was loaded from the file named
     * in the {@link #initialize(String)} method.</li>
     * <li>The pseudo-property name <code>timestamp</code>. The value is the
     * current time formated by <code>SimpleDateFormat</code> using the pattern
     * yyyyMMddHHmm. (This pseudo-property makes it easy to specify a unique log
     * file name each time Persistit is initialized.</li>
     * </ol>
     * </p>
     * If a property value contains a substitution variable in the form
     * <code>${<i>pppp</i>}</code>, then this method attempts perform a
     * substitution. To do so it recursively gets the value of a property named
     * <code><i>pppp</i></code>, replaces the substring delimited by
     * <code>${</code> and <code>}</code>, and then scans the resulting string
     * for further substitution variables.
     * 
     * @param propertyName
     *            The property name
     * @return The resulting string
     */
    public String getProperty(String propertyName) {
        return getProperty(propertyName, null);
    }

    /**
     * <p>
     * Returns a property value, or a default value if there is no such
     * property. The property is taken from one of the following sources:
     * <ol>
     * <li>A system property having a prefix of "com.persistit.". For example,
     * the property named "journalpath" can be supplied as the system property
     * named com.persistit.journalpath. (Note: if the security context does not
     * permit access to system properties, then system properties are ignored.)</li>
     * <li>The supplied Properties object, which was either passed to the
     * {@link #initialize(Properties)} method, or was loaded from the file named
     * in the {@link #initialize(String)} method.</li>
     * <li>The pseudo-property name <code>timestamp</code>. The value is the
     * current time formated by <code>SimpleDateFormat</code> using the pattern
     * yyyyMMddHHmm. (This pseudo-property makes it easy to specify a unique log
     * file name each time Persistit is initialized.</li>
     * </ol>
     * </p>
     * If a property value contains a substitution variable in the form
     * <code>${<i>pppp</i>}</code>, then this method attempts perform a
     * substitution. To do so it recursively gets the value of a property named
     * <code><i>pppp</i></code>, replaces the substring delimited by
     * <code>${</code> and <code>}</code>, and then scans the resulting string
     * for further substitution variables. </p>
     * <p>
     * For all properties, the value "-" (a single hyphen) explicitly specifies
     * the <i>default</i> value.
     * </p>
     * 
     * @param propertyName
     *            The property name
     * @param defaultValue
     *            The default value
     * @return The resulting string
     */
    public String getProperty(String propertyName, String defaultValue) {
        String value = getProperty(propertyName, 0, _properties);
        return value == null ? defaultValue : value;
    }

    private String getProperty(String propertyName, int depth, Properties properties) {
        String value = null;

        value = getSystemProperty(SYSTEM_PROPERTY_PREFIX + propertyName);

        if (value == null && properties != null) {
            value = properties.getProperty(propertyName);
        }
        if ("-".equals(value)) {
            value = null;
        }
        if (value == null && TIMESTAMP_PROPERTY.equals(propertyName)) {
            value = (new SimpleDateFormat("yyyyMMddHHmm")).format(new Date());
        }
        if (value != null)
            value = substituteProperties(value, properties, depth);

        return value;
    }

    /**
     * Sets a property value in the Persistit Properties map. If the specified
     * value is null then an existing property of the specified name is removed.
     * 
     * @param propertyName
     *            The property name
     * @param value
     *            Value to set, or <code>null</code> to remove an existing
     *            property
     */
    public void setProperty(final String propertyName, final String value) {
        if (value == null) {
            _properties.remove(propertyName);
        } else {
            _properties.setProperty(propertyName, value);
        }
    }

    private String getSystemProperty(final String propertyName) {
        return (String) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return System.getProperty(propertyName);
            }
        });
    }

    int getIntegerProperty(String propName, int dflt) {
        long v = getLongProperty(propName, dflt);
        if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
            return (int) v;
        }
        throw new IllegalArgumentException("Value out of range: " + v);
    }

    /**
     * Interpret a property value as a long integer. Permits suffix values of
     * "K" for Kilo- and "M" for Mega-, "G" for Giga- and "T" for Tera-. For
     * example, the supplied value of "100K" yields a parsed result of 102400.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation
     */
    long getLongProperty(String propName, long dflt) {
        String str = getProperty(propName);
        if (str == null) {
            return dflt;
        }
        return parseLongProperty(propName, str);
    }

    /**
     * Parse a string as a long integer. Permits suffix values of "K" for Kilo-
     * and "M" for Mega-, "G" for Giga- and "T" for Tera-. For example, the
     * supplied value of "100K" yields a parsed result of 102400.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param str
     *            The string representation, e.g., "100K".
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation,
     *             or is outside the supplied bounds.
     */
    static long parseLongProperty(String propName, String str) {
        long result = Long.MIN_VALUE;
        long multiplier = 1;
        if (str.length() > 1) {
            switch (str.charAt(str.length() - 1)) {
            case 't':
            case 'T':
                multiplier = TERA;
                break;
            case 'g':
            case 'G':
                multiplier = GIGA;
                break;
            case 'm':
            case 'M':
                multiplier = MEGA;
                break;
            case 'k':
            case 'K':
                multiplier = KILO;
                break;
            }
        }
        String sstr = str;
        if (multiplier > 1) {
            sstr = str.substring(0, str.length() - 1);
        }

        try {
            result = Long.parseLong(sstr) * multiplier;
        }

        catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Invalid number '" + str + "' for property " + propName);
        }
        return result;
    }

    /**
     * Parse a string as a float value
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param str
     *            The string representation, e.g., "100K".
     * @param min
     *            Minimum permissible value
     * @param max
     *            Maximum permissible value
     * @return The numeric value of the supplied String, as a floag.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid floating point
     *             representation, or is outside the supplied bounds.
     */

    static float parseFloatProperty(String propName, String str) {
        try {
            return Float.parseFloat(str);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number '" + str + "' for property " + propName);
        }
    }

    void parseBufferMemorySpecification(final String propertyName, final String propertyValue) {
        int bufferSize = bufferSizeFromPropertyName(propertyName);
        int index = bufferSizeIndex(bufferSize);
        BufferMemorySpecification spec = buffers[index];
        if (propertyName.startsWith(BUFFERS_PROPERTY_NAME)) {
            int count = (int) parseLongProperty(propertyName, propertyValue);
            spec.setMaximumCount(count);
            spec.setMinimumCount(count);
        } else if (propertyName.startsWith(BUFFER_MEM_PROPERTY_NAME)) {
            long minimum = 0;
            long maximum = Long.MAX_VALUE;
            long reserved = 0;
            float fraction = 1.0f;

            final String[] terms = propertyValue.split(",", 4);

            if (terms.length > 0 && !terms[0].isEmpty()) {
                minimum = parseLongProperty(propertyName, terms[0]);
                maximum = minimum;
            }
            if (terms.length > 1 && !terms[1].isEmpty()) {
                maximum = parseLongProperty(propertyName, terms[1]);
            }
            if (terms.length > 2 && !terms[2].isEmpty()) {
                reserved = parseLongProperty(propertyName, terms[2]);
            }
            if (terms.length > 3 && !terms[3].isEmpty()) {
                fraction = parseFloatProperty(propertyName, terms[3]);
            }
            spec.setMinimumMemory(minimum);
            spec.setMaximumMemory(maximum);
            spec.setReservedMemory(reserved);
            spec.setFraction(fraction);
        } else {
            throw new IllegalArgumentException("Invalid property name " + propertyName);
        }
        spec.setBufferSize(bufferSize);
    }

    /**
     * Parse a String as a boolean value. The suppled value <code>str</code>
     * must be either <code>true</code> or <code>false</code>. The comparison is
     * case-insensitive.
     * 
     * @param propName
     * @param str
     * @return the boolean value
     */
    static boolean parseBooleanValue(String propName, String str) {
        if ("true".equalsIgnoreCase(str))
            return true;
        if ("false".equalsIgnoreCase(str))
            return false;
        throw new IllegalArgumentException("Value '" + str + "' of property " + propName + " must be "
                + " either \"true\" or \"false\"");

    }

    /**
     * Provide a displayable version of a long value, preferable using one of
     * the suffixes 'K', 'M', 'G', or 'T' to abbreviate values that are integral
     * multiples of powers of 1,024.
     * 
     * @param value
     *            to convert
     * @return Readable format of long value
     */
    static String displayableLongValue(final long value) {
        if (value <= 0) {
            return String.format("%d", value);
        }
        long v = value;
        int scale = 0;
        while ((v / 1024) * 1024 == v && scale < 3) {
            scale++;
            v /= 1024;
        }
        return String.format("%d%s", v, " KMGT".substring(scale, scale + 1));
    }

    /**
     * Parses a string value as either <i>true</i> or <i>false</i>.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param dflt
     *            The default value
     * @return <i>true</i> or <i>false</i>
     */
    public boolean getBooleanProperty(String propName, boolean dflt) {
        String str = getProperty(propName);
        if (str == null)
            return dflt;
        str = str.toLowerCase();
        if ("true".equals(str))
            return true;
        if ("false".equals(str))
            return false;
        throw new IllegalArgumentException("Value '" + str + "' of property " + propName + " must be "
                + " either \"true\" or \"false\"");
    }

    public VolumeSpecification volumeSpecification(final String vstring) throws InvalidVolumeSpecificationException {
        final VolumeSpecification volumeSpec = new VolumeSpecification(substituteProperties(vstring, _properties, 0));
        return volumeSpec;
    }

    // -------------------------------------------

    /**
     * @return the timeoutValue
     */
    public long getTimeoutValue() {
        return timeoutValue;
    }

    /**
     * @param timeoutValue
     *            the timeoutValue to set
     */
    public void setTimeoutValue(long timeoutValue) {
        this.timeoutValue = timeoutValue;
    }

    /**
     * @return the buffers
     */
    public BufferMemorySpecification[] getBuffers() {
        return buffers;
    }

    /**
     * @return the volumeSpecifications
     */
    public List<VolumeSpecification> getVolumeSpecifications() {
        return volumeSpecifications;
    }

    /**
     * @return the journalPath
     */
    public String getJournalPath() {
        return journalPath;
    }

    /**
     * @param journalPath
     *            the journalPath to set
     */
    public void setJournalPath(String journalPath) {
        this.journalPath = journalPath;
    }

    /**
     * @return the journalSize
     */
    public long getJournalSize() {
        return journalSize;
    }

    /**
     * @param journalSize
     *            the journalSize to set
     */
    public void setJournalSize(long journalSize) {
        Util.rangeCheck(journalSize, JournalManager.MINIMUM_BLOCK_SIZE, JournalManager.MAXIMUM_BLOCK_SIZE);
        this.journalSize = journalSize;
    }

    /**
     * @return the sysVolume
     */
    public String getSysVolume() {
        return sysVolume;
    }

    /**
     * @param sysVolume
     *            the sysVolume to set
     */
    public void setSysVolume(String sysVolume) {
        this.sysVolume = sysVolume;
    }

    /**
     * @return the tmpVolDir
     */
    public String getTmpVolDir() {
        return tmpVolDir;
    }

    /**
     * @param tmpVolDir
     *            the tmpVolDir to set
     */
    public void setTmpVolDir(String tmpVolDir) {
        this.tmpVolDir = tmpVolDir;
    }

    /**
     * @return the tmpVolPageSize
     */
    public int getTmpVolPageSize() {
        return tmpVolPageSize;
    }

    /**
     * @param tmpVolPageSize
     *            the tmpVolPageSize to set
     */
    public void setTmpVolPageSize(int tmpVolPageSize) {
        this.tmpVolPageSize = tmpVolPageSize;
    }

    /**
     * @return the tmpVolMaxSize
     */
    public long getTmpVolMaxSize() {
        return tmpVolMaxSize;
    }

    /**
     * @param tmpVolMaxSize
     *            the tmpVolMaxSize to set
     */
    public void setTmpVolMaxSize(long tmpVolMaxSize) {
        Util.rangeCheck(tmpVolMaxSize, MINIMUM_TEMP_VOL_MAX_SIZE, MAXIMUM_TEMP_VOL_MAX_SIZE);
        this.tmpVolMaxSize = tmpVolMaxSize;
    }

    /**
     * @return the commitPolicy
     */
    public CommitPolicy getCommitPolicy() {
        return commitPolicy;
    }

    /**
     * @param policyName
     *            Name of the commitPolicy the commitPolicy to set
     */
    public void setCommitPolicy(String policyName) {
        if (policyName != null) {
            setCommitPolicy(CommitPolicy.valueOf(policyName));
        }
    }

    /**
     * @param commitPolicy
     *            the commitPolicy to set
     */
    public void setCommitPolicy(CommitPolicy commitPolicy) {
        this.commitPolicy = commitPolicy;
    }

    /**
     * @return the joinPolicy
     */
    public JoinPolicy getJoinPolicy() {
        return joinPolicy;
    }

    /**
     * 
     * @param policyName
     *            Name of the JoinPoliy to set
     */
    public void setJoinPolicy(String policyName) {
        if (policyName != null) {
            setJoinPolicy(JoinPolicy.forName(policyName));
        }
    }

    /**
     * @param joinPolicy
     *            the joinPolicy to set
     */
    public void setJoinPolicy(JoinPolicy joinPolicy) {
        this.joinPolicy = joinPolicy;
    }

    /**
     * @return the splitPolicy
     */
    public SplitPolicy getSplitPolicy() {
        return splitPolicy;
    }

    /**
     * @param policyName
     *            Name of the splitPolicy to set
     */
    public void setSplitPolicy(String policyName) {
        if (policyName != null) {
            setSplitPolicy(SplitPolicy.forName(policyName));
        }
    }

    /**
     * @param splitPolicy
     *            the splitPolicy to set
     */
    public void setSplitPolicy(SplitPolicy splitPolicy) {
        this.splitPolicy = splitPolicy;
    }

    /**
     * @return the verbose
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @param verbose
     *            the verbose to set
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * @return the readRetry
     */
    public boolean isReadRetry() {
        return readRetry;
    }

    /**
     * @param readRetry
     *            the readRetry to set
     */
    public void setReadRetry(boolean readRetry) {
        this.readRetry = readRetry;
    }

    /**
     * @return the serialOverride
     */
    public String getSerialOverride() {
        return serialOverride;
    }

    /**
     * @param serialOverride
     *            the serialOverride to set
     */
    public void setSerialOverride(String serialOverride) {
        this.serialOverride = serialOverride;
    }

    /**
     * @return the constructorOverride
     */
    public boolean isConstructorOverride() {
        return constructorOverride;
    }

    /**
     * @param constructorOverride
     *            the constructorOverride to set
     */
    public void setConstructorOverride(boolean constructorOverride) {
        this.constructorOverride = constructorOverride;
    }

    /**
     * @return the showGUI
     */
    public boolean isShowGUI() {
        return showGUI;
    }

    /**
     * @param showGUI
     *            the showGUI to set
     */
    public void setShowGUI(boolean showGUI) {
        this.showGUI = showGUI;
    }

    /**
     * @return the logging
     */
    public String getLogging() {
        return logging;
    }

    /**
     * @param logging
     *            the logging to set
     */
    public void setLogging(String logging) {
        this.logging = logging;
    }

    /**
     * @return the logFile
     */
    public String getLogFile() {
        return logFile;
    }

    /**
     * @param logFile
     *            the logFile to set
     */
    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    /**
     * @return the rmiHost
     */
    public String getRmiHost() {
        return rmiHost;
    }

    /**
     * @param rmiHost
     *            the rmiHost to set
     */
    public void setRmiHost(String rmiHost) {
        this.rmiHost = rmiHost;
    }

    /**
     * @return the rmiPort
     */
    public int getRmiPort() {
        return rmiPort;
    }

    /**
     * @param rmiPort
     *            the rmiPort to set
     */
    public void setRmiPort(int rmiPort) {
        this.rmiPort = rmiPort;
    }

    /**
     * @return the rmiServerPort
     */
    public int getRmiServerPort() {
        return rmiServerPort;
    }

    /**
     * @param rmiServerPort
     *            the rmiServerPort to set
     */
    public void setRmiServerPort(int rmiServerPort) {
        this.rmiServerPort = rmiServerPort;
    }

    /**
     * @return the jmx
     */
    public boolean isJmxEnabled() {
        return jmx;
    }

    /**
     * @param jmx
     *            the jmx to set
     */
    public void setJmxEnabled(boolean jmx) {
        this.jmx = jmx;
    }

    /**
     * @return the appendOnly
     */
    public boolean isAppendOnly() {
        return appendOnly;
    }

    /**
     * @param appendOnly
     *            the appendOnly to set
     */
    public void setAppendOnly(boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

}
