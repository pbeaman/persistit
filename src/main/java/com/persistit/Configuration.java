/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import static com.persistit.mxbeans.CheckpointManagerMXBean.DEFAULT_CHECKPOINT_INTERVAL_S;
import static com.persistit.mxbeans.CheckpointManagerMXBean.MAXIMUM_CHECKPOINT_INTERVAL_S;
import static com.persistit.mxbeans.CheckpointManagerMXBean.MINIMUM_CHECKPOINT_INTERVAL_S;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PropertiesNotFoundException;
import com.persistit.logging.DefaultPersistitLogger;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.util.Util;

/**
 * <p>
 * Configuration parameters used to determine locations of files, sizes of
 * buffer pool and journal allocation, policies and other parameters required
 * during Persistit initialization.
 * </p>
 * <p>
 * An application can construct and set up a <code>Configuration</code> using
 * <code>setXXX</code> methods directly. Alternatively, the configuration can be
 * specified in a set of properties that are read and interpreted by the
 * <code>Configuration</code>. This object is used directly or indirectly by the
 * following methods:
 * <ul>
 * <li>{@link Persistit#setConfiguration(Configuration)} - assigns a supplied
 * <code>Configuration</code> directly.</li>
 * <li>{@link Persistit#setProperties(Properties)} - creates and loads a
 * <code>Configuration</code> from the supplied <code>Properties</code>.</li>
 * <li>{@link Persistit#setPropertiesFromFile(String)} - loads a properties file
 * from the specified file name, and then constructs a
 * <code>Configuration</code> from the loaded <code>Properties</code>.
 * <li>{@link Persistit#initialize()} - loads a properties file from a default
 * name and then constructs a <code>Configuration</code> from the loaded
 * <code>Properties</code>.
 * </ul>
 * </p>
 * <p>
 * When parsing <code>Properties</code> values this class implements a simple
 * substitution mechanism so that, for example, a common directory path may be
 * referenced by multiple properties as shown here: <code><pre>
 * datapath = /var/lib/persistit
 * journalpath = ${datapath}/akiban_journal
 * volume.1=${datapath}/hwdemo,create,pageSize:16K,\
 *              initialSize:10M,extensionSize:10M,maximumSize:1G 
 * </pre></code>
 * 
 * In this example the property named <code>datapath</code> has no special
 * meaning other than as a substitution parameter.
 * </p>
 * 
 * @author peter
 * 
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
     * Property name for checkpoint interval in seconds
     */
    public final static String CHECKPOINT_INTERVAL_PROPERTY_NAME = "checkpointinterval";

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
    public final static String APPEND_ONLY_PROPERTY_NAME = "appendonly";

    /**
     * Property name for the "ignore missing volumes" property.
     */
    public final static String IGNORE_MISSING_VOLUMES_PROPERTY = "ignoremissingvolumes";

    /**
     * Property name to enable writing backward-compatible IV records
     */
    public final static String USE_OLD_VSPEC = "useoldvspec";

    /**
     * Property name to specify the default {@link SplitPolicy}.
     */
    public final static String SPLIT_POLICY_PROPERTY_NAME = "splitpolicy";

    /**
     * Property name to specify whether buffer preloading is enabled.
     */
    public final static String BUFFER_PRELOAD_PROPERTY_NAME = "bufferpreload";

    /**
     * Property name to specify whether buffer inventory is enabled.
     */
    public final static String BUFFER_INVENTORY_PROPERTY_NAME = "bufferinventory";

    /**
     * Property name to specify the default {@link JoinPolicy}.
     */
    public final static String JOIN_POLICY_PROPERTY_NAME = "joinpolicy";

    private final static SplitPolicy DEFAULT_SPLIT_POLICY = SplitPolicy.PACK_BIAS;
    private final static JoinPolicy DEFAULT_JOIN_POLICY = JoinPolicy.EVEN_BIAS;
    private final static CommitPolicy DEFAULT_TRANSACTION_COMMIT_POLICY = CommitPolicy.SOFT;

    /**
     * 1,024
     */
    public final static long KILO = 1024;
    /**
     * 1,024 * {@value #KILO}
     */
    public final static long MEGA = KILO * KILO;
    /**
     * 1,024 * {@value #MEGA}
     */
    public final static long GIGA = MEGA * KILO;
    /**
     * 1,024 * {@value #GIGA}
     */
    public final static long TERA = GIGA * KILO;

    private final static int[] BUFFER_SIZES = validBufferSizes();
    private final static int MAX_RECURSION_COUNT = 20;

    /**
     * <p>
     * Configuration constraints that determine the number of
     * <code>Buffer</code>s in a {@link BufferPool}. There is one permanent
     * <code>BufferPoolConfiguration</code> instance for each valid buffer size:
     * 1024, 2048, 4096, 8192 and 16384. The {@link #getBufferPoolMap()} method
     * provides access to these, where the map key is the buffer size.
     * </p>
     * <p>
     * Each <code>BufferPoolConfiguration</code> specifies minimum and maximum
     * buffer count values, and four parameters used to allocate a buffers
     * according to available memory. Values of <code>minimumCount</code> and
     * <code>maximumCount</code> are absolute bounds; if the
     * <code>maximumCount</code> is zero then no buffers of the specified buffer
     * size will be allocated.
     * </p>
     * <p>
     * The memory-based parameters are used as follows:
     * <ul>
     * <li>Determine the maximum heap size by accessing the platform
     * MemoryMXBean, which in turn supplies the value given by the +-Xmx+ JVM
     * property.</li>
     * <li>Subtract the value of <code>reservedMemory</code> size from the
     * maximum heap size.</li>
     * <li>Multiply the result by <code>fraction</code>.</li>
     * <li>Adjust the result to fall between the boundaries specified by
     * <code>minimumMemory</code> and <code>maximumMemory</code>.</li>
     * <li>Determine the buffer count by dividing the result by the memory
     * consumption per buffer</li>
     * <li>Adjust the buffer count to fall between the boundaries specified by
     * <code>minimumCount</code> and <code>maximumCount</code>.</li>
     * </ul>
     * </p>
     * <p>
     * These parameters may be set through the <code>set</code> and
     * <code>get</code> methods, or by parsing a string property value using
     * {@link #parseBufferCount(int, String, String)} or
     * {@link #parseBufferMemory(int, String, String)}
     */
    public static class BufferPoolConfiguration {

        private final int bufferSize;
        private int minimumCount;
        private int maximumCount;
        private long minimumMemory;
        private long maximumMemory;
        private long reservedMemory;
        private float fraction;

        private void reset() {
            minimumCount = 0;
            maximumCount = 0;
            minimumMemory = 0;
            maximumMemory = Long.MAX_VALUE;
            reservedMemory = 0;
            fraction = 1.0f;
        }

        private BufferPoolConfiguration(final int size) {
            bufferSize = size;
            reset();
        }

        /**
         * @return the bufferSize
         */
        public int getBufferSize() {
            return bufferSize;
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
        public void setMinimumCount(final int minimumCount) {
            Util.rangeCheck(minimumCount, 0, BufferPool.MAXIMUM_POOL_COUNT);
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
        public void setMaximumCount(final int maximumCount) {
            Util.rangeCheck(maximumCount, BufferPool.MINIMUM_POOL_COUNT, Integer.MAX_VALUE);
            this.maximumCount = maximumCount;
        }

        /**
         * Set the minimum and maximum buffer count.
         * 
         * @param count
         */
        public void setCount(final int count) {
            setMinimumCount(count);
            setMaximumCount(count);
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
        public void setMinimumMemory(final long minimumMemory) {
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
        public void setMaximumMemory(final long maximumMemory) {
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
        public void setReservedMemory(final long reservedMemory) {
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
        public void setFraction(final float fraction) {
            Util.rangeCheck(fraction, 0.0f, 1.0f);
            this.fraction = fraction;
        }

        private final static String SIMPLE_COUNT_FORMAT = "count=%d";
        private final static String MIN_MAX_COUNT_FORMAT = "minCount=%d,maxCount=%d";
        private final static String MIN_MAX_MEMORY_FORMAT = "minMem=%s,maxMem=%s,reserved=%s,fraction=%s";
        private final static Pattern SIMPLE_COUNT_PATTERN = Pattern.compile("count=([0-9]+[KMGT]?)",
                Pattern.CASE_INSENSITIVE);
        private final static Pattern MIN_MAX_COUNT_PATTERN = Pattern.compile(
                "minCount=([0-9KMGT]+),maxCount=([0-9KMGT]+)", Pattern.CASE_INSENSITIVE);
        private final static Pattern MIN_MAX_MEMORY_PATTERN = Pattern.compile(
                "minMem=([0-9]+[KMGT]?),maxMem=([0-9]+[KMGT]?),reserved=([0-9]+[KMGT]?),fraction=([0-9\\.]+)",
                Pattern.CASE_INSENSITIVE);

        /**
         * Compute the buffer count determined by the constraints of this
         * <code>BufferPoolConfiguration</code> given the supplied
         * availableHeapMemory.
         * 
         * @param availableHeapMemory
         *            available memory, in bytes
         * @return number of <code>Buffers</code> to allocate
         * @throws IllegalArgumentException
         *             if the allocation is infeasible
         */
        public int computeBufferCount(final long availableHeapMemory) {
            if (maximumCount == 0) {
                return 0;
            }
            final long maximumAvailable = (long) ((availableHeapMemory - reservedMemory) * fraction);
            final long allocation = Math.min(maximumAvailable, maximumMemory);
            final int bufferSizeWithOverhead = Buffer.bufferSizeWithOverhead(bufferSize);
            final int buffers = Math.max(minimumCount,
                    Math.min(maximumCount, (int) (allocation / bufferSizeWithOverhead)));
            if (buffers < BufferPool.MINIMUM_POOL_COUNT || buffers > BufferPool.MAXIMUM_POOL_COUNT
                    || (long) buffers * (long) bufferSizeWithOverhead > maximumAvailable
                    || (long) (buffers + 1) * (long) bufferSizeWithOverhead < minimumMemory) {
                throw new IllegalArgumentException(String.format(
                        "Invalid buffer pool configuration: %,d buffers in %sb of maximum available memory", buffers,
                        displayableLongValue(maximumAvailable)));
            }
            return buffers;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(bufferSize).append(",");
            if (minimumCount == maximumCount) {
                sb.append(String.format(SIMPLE_COUNT_FORMAT, minimumCount));
            } else if (minimumCount != 0 || maximumCount != Integer.MAX_VALUE) {
                sb.append(String.format(MIN_MAX_COUNT_FORMAT, minimumCount, maximumCount));
            }
            if (minimumMemory != 0 || maximumMemory != Long.MAX_VALUE || reservedMemory != 0 || fraction != 1.0f) {
                sb.append(String.format(MIN_MAX_MEMORY_FORMAT, displayableLongValue(minimumMemory),
                        displayableLongValue(maximumMemory), displayableLongValue(reservedMemory), fraction));
            }
            return sb.toString();
        }

        /**
         * Parse the String description generated by {@link #toString()}. The
         * string format is
         * 
         * <code><pre>
         * bufferSize,propertyName=value,...
         * </pre><code>
         * 
         * where propertyNames are <code>count</code>, <code>minCount</code>,
         * <code>maxCount</code>, <code>minMemory</code>, <code>maxMemory</code>
         * , <code>reserved</code> or <code>fraction</code>
         */
        public void parse(final String string) {
            final String[] terms = string.split(",", 2);
            if (terms.length > 1) {
                checkBufferSize((int) parseLongProperty(string, terms[0]), string);
                Matcher matcher;
                matcher = SIMPLE_COUNT_PATTERN.matcher(terms[1]);
                if (matcher.matches()) {
                    setCount((int) parseLongProperty(string, matcher.group(1)));
                    return;
                }
                matcher = MIN_MAX_COUNT_PATTERN.matcher(terms[1]);
                if (matcher.matches()) {
                    setMinimumCount((int) parseLongProperty(string, matcher.group(1)));
                    setMaximumCount((int) parseLongProperty(string, matcher.group(2)));
                    return;
                }
                matcher = MIN_MAX_MEMORY_PATTERN.matcher(terms[1]);
                if (matcher.matches()) {
                    setMemoryConstraints(string, parseLongProperty(string, matcher.group(1)),
                            parseLongProperty(string, matcher.group(2)), parseLongProperty(string, matcher.group(3)),
                            parseFloatProperty(string, matcher.group(4)));
                    return;
                }
            }
            throw new IllegalArgumentException("Invalid BufferPool memory specification: " + string);
        }

        /**
         * Parse the supplied property value as an integer-valued buffer count.
         * Both <code>minimumCount</code> and <code>maximumCount</code> are set
         * to this value. The supplied property value must be a valid integer or
         * an integer followed by "K", "M" or "G" for
         * {@value com.persistit.Configuration#KILO},
         * {@value com.persistit.Configuration#MEGA}, or
         * {@value com.persistit.Configuration#GIGA} as a multiplier.
         * 
         * @param bufferSize
         * @param propertyName
         * @param propertyValue
         * @throws IllegalArgumentException
         *             if the propertyValue is not in the form of a valid
         *             Integer
         */
        public void parseBufferCount(final int bufferSize, final String propertyName, final String propertyValue) {
            checkBufferSize(bufferSize, propertyName);
            reset();
            setCount((int) parseLongProperty(propertyName, propertyValue));
        }

        /**
         * <p>
         * Parse the supplied property value as a sequence of values to populate
         * the <code>minimumMemory</code> <code>maximumMemory</code>,
         * <code>reservedMemory</code> and <code>fraction</code> fields. These
         * values are separated by commas, and the first three may be specified
         * as blank for the default value, an integer, or an integer followed by
         * by "K", "M" or "G" for {@value com.persistit.Configuration#KILO},
         * {@value com.persistit.Configuration#MEGA}, or
         * {@value com.persistit.Configuration#GIGA} as a multiplier.
         * </p>
         * For example, the property value <code><pre>
         *  64M,8G,128M,.75
         * </pre></code> reserves 128M from available memory and then allocates
         * 75% of the remainder up to 8Gb, but not less than 64Mb.
         * <p>
         * </p>
         * 
         * @param bufferSize
         * @param propertyName
         * @param propertyValue
         */
        public void parseBufferMemory(final int bufferSize, final String propertyName, final String propertyValue) {
            checkBufferSize(bufferSize, propertyName);
            reset();
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
            setMemoryConstraints(propertyValue, minimum, maximum, reserved, fraction);
        }

        private void setMemoryConstraints(final String propertyValue, final long minimum, final long maximum,
                final long reserved, final float fraction) {
            if (minimum >= 0 && minimum <= maximum && maximum - minimum >= reserved && reserved >= 0) {
                Util.rangeCheck(fraction, 0.0f, 1.0f);
                setMinimumMemory(minimum);
                setMaximumMemory(maximum);
                setReservedMemory(reserved);
                setFraction(fraction);
                setMinimumCount(0);
                setMaximumCount(Integer.MAX_VALUE);
            } else {
                throw new IllegalArgumentException("Invalid BufferPool memory specification: " + propertyValue);
            }
        }

        private void checkBufferSize(final int size, final String s) {
            if (size != this.bufferSize) {
                throw new IllegalArgumentException("Buffer size " + size + " does not match " + s);
            }
        }

    }

    private final Properties _properties = new Properties();

    private final Map<Integer, BufferPoolConfiguration> bufferPoolMap;
    private final List<VolumeSpecification> volumeSpecifications = new ArrayList<VolumeSpecification>();
    private String journalPath = DEFAULT_JOURNAL_PATH;
    private long journalSize = JournalManager.DEFAULT_BLOCK_SIZE;
    private long checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL_S;
    private String sysVolume = DEFAULT_SYSTEM_VOLUME_NAME;
    private CommitPolicy commitPolicy = DEFAULT_TRANSACTION_COMMIT_POLICY;
    private JoinPolicy joinPolicy = DEFAULT_JOIN_POLICY;
    private SplitPolicy splitPolicy = DEFAULT_SPLIT_POLICY;
    private String serialOverride;
    private boolean constructorOverride;
    private boolean showGUI;
    private String logging;
    private String logFile;
    private String rmiHost;
    private int rmiPort;
    private int rmiServerPort;
    private boolean jmx = true;
    private boolean appendOnly;
    private boolean bufferInventoryEnabled;
    private boolean bufferPreloadEnabled;
    private boolean ignoreMissingVolumes;
    private String tmpVolDir;
    private int tmpVolPageSize;
    private long tmpVolMaxSize;
    private boolean useOldVSpec;

    /**
     * Construct a <code>Configuration</code> instance. This object may be
     * passed to the {@link Persistit#initialize(Configuration)} method.
     */
    public Configuration() {
        final Map<Integer, BufferPoolConfiguration> map = new TreeMap<Integer, BufferPoolConfiguration>();
        for (final int bufferSize : BUFFER_SIZES) {
            map.put(bufferSize, new BufferPoolConfiguration(bufferSize));
        }
        bufferPoolMap = Collections.unmodifiableMap(map);
    }

    /**
     * Construct a <code>Configuration</code> instance and merge the supplied
     * <code>Properties</code>. This object may be passed to the
     * {@link Persistit#initialize(Configuration)} method.
     * 
     * @param properties
     *            Properties from which configuration elements are parsed and
     *            assigned. Property names unknown to Persistit are ignored.
     * @throws IllegalArgumentException
     *             of a property contains an invalid value
     */
    public Configuration(final Properties properties) throws InvalidVolumeSpecificationException {
        this();
        merge(properties);
        loadProperties();
    }

    void readPropertiesFile() throws PersistitException {
        readPropertiesFile(getProperty(CONFIG_FILE_PROPERTY_NAME, DEFAULT_CONFIG_FILE));
    }

    void readPropertiesFile(final String propertiesFileName) throws PersistitException {
        final Properties properties = new Properties();
        try {
            if (propertiesFileName.contains(DEFAULT_PROPERTIES_FILE_SUFFIX)
                    || propertiesFileName.contains(File.separator)) {
                properties.load(new FileInputStream(propertiesFileName));
            } else {
                final ResourceBundle bundle = ResourceBundle.getBundle(propertiesFileName);
                for (final Enumeration<String> e = bundle.getKeys(); e.hasMoreElements();) {
                    final String key = e.nextElement();
                    properties.put(key, bundle.getString(key));
                }
            }
        } catch (final FileNotFoundException fnfe) {
            // A friendlier exception when the properties file is not found.
            throw new PropertiesNotFoundException(fnfe.getMessage());
        } catch (final IOException ioe) {
            throw new PersistitIOException(ioe);
        }
        merge(properties);
        loadProperties();
    }

    final static void checkBufferSize(final int bufferSize) {
        for (final int size : BUFFER_SIZES) {
            if (size == bufferSize) {
                return;
            }
        }
        throw new IllegalArgumentException("Invalid buffer size: " + bufferSize);
    }

    void merge(final Properties properties) {
        for (final Enumeration<? extends Object> e = properties.propertyNames(); e.hasMoreElements();) {
            final String propertyName = (String) e.nextElement();
            _properties.put(propertyName, properties.getProperty(propertyName));
        }
    }

    void loadProperties() throws InvalidVolumeSpecificationException {
        setAppendOnly(getBooleanProperty(APPEND_ONLY_PROPERTY_NAME, false));
        setCommitPolicy(getProperty(COMMIT_POLICY_PROPERTY_NAME));
        setConstructorOverride(getBooleanProperty(CONSTRUCTOR_OVERRIDE_PROPERTY_NAME, false));
        setIgnoreMissingVolumes(getBooleanProperty(IGNORE_MISSING_VOLUMES_PROPERTY, false));
        setJmxEnabled(getBooleanProperty(ENABLE_JMX_PROPERTY_NAME, true));
        setJoinPolicy(getProperty(JOIN_POLICY_PROPERTY_NAME));
        setJournalPath(getProperty(JOURNAL_PATH_PROPERTY_NAME, DEFAULT_JOURNAL_PATH));
        setJournalSize(getLongProperty(JOURNAL_BLOCKSIZE_PROPERTY_NAME, JournalManager.DEFAULT_BLOCK_SIZE));
        setLogFile(getProperty(LOGFILE_PROPERTY_NAME));
        setLogging(getProperty(LOGGING_PROPERTIES_NAME));
        setTmpVolDir(getProperty(TEMPORARY_VOLUME_DIR_PROPERTY_NAME));
        setTmpVolPageSize(getIntegerProperty(TEMPORARY_VOLUME_PAGE_SIZE_PROPERTY_NAME, 0));
        setTmpVolMaxSize(getLongProperty(TEMPORARY_VOLUME_MAX_SIZE_PROPERTY_NAME, MAXIMUM_TEMP_VOL_MAX_SIZE));
        setRmiHost(getProperty(RMI_REGISTRY_HOST_PROPERTY_NAME));
        setRmiPort((int) getLongProperty(RMI_REGISTRY_PORT_PROPERTY_NAME, 0));
        setRmiServerPort((int) getLongProperty(RMI_SERVER_PORT_PROPERTY_NAME, 0));
        setSerialOverride(getProperty(SERIAL_OVERRIDE_PROPERTY_NAME));
        setShowGUI(getBooleanProperty(SHOW_GUI_PROPERTY_NAME, false));
        setSplitPolicy(getProperty(SPLIT_POLICY_PROPERTY_NAME));
        setSysVolume(getProperty(SYSTEM_VOLUME_PROPERTY_NAME, DEFAULT_SYSTEM_VOLUME_NAME));
        setBufferInventoryEnabled(getBooleanProperty(BUFFER_INVENTORY_PROPERTY_NAME, false));
        setBufferPreloadEnabled(getBooleanProperty(BUFFER_PRELOAD_PROPERTY_NAME, false));
        setUseOldVSpec(getBooleanProperty(USE_OLD_VSPEC, false));

        loadPropertiesBufferSpecifications();
        loadPropertiesVolumeSpecifications();
    }

    void loadPropertiesBufferSpecifications() {
        for (int index = 0; index < BUFFER_SIZES.length; index++) {
            final int size = BUFFER_SIZES[index];

            final String countPropertyName = BUFFERS_PROPERTY_NAME + size;
            final String memPropertyName = BUFFER_MEM_PROPERTY_NAME + size;

            final String countSpec = getProperty(countPropertyName);
            final String memSpec = getProperty(memPropertyName);
            int count = 0;
            final BufferPoolConfiguration bpc = bufferPoolMap.get(size);

            if (countSpec != null) {
                bpc.parseBufferCount(size, countPropertyName, countSpec);
                count++;
            }
            if (memSpec != null) {
                bpc.parseBufferMemory(size, countPropertyName, memSpec);
                count++;
            }

            if (count > 1) {
                throw new IllegalArgumentException("Only one of " + countPropertyName + " and " + memPropertyName
                        + " may be specified");
            } else if (count == 0) {
                bpc.reset();
            }
        }
    }

    void loadPropertiesVolumeSpecifications() throws InvalidVolumeSpecificationException {
        for (final Enumeration<?> enumeration = _properties.propertyNames(); enumeration.hasMoreElements();) {
            final String key = (String) enumeration.nextElement();
            if (key.startsWith(VOLUME_PROPERTY_PREFIX)) {
                boolean isOne = true;
                try {
                    Integer.parseInt(key.substring(VOLUME_PROPERTY_PREFIX.length()));
                } catch (final NumberFormatException nfe) {
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
            final String[] s = propertyName.split("\\.");
            try {
                final int size = Integer.parseInt(s[2]);
                checkBufferSize(size);
                return size;
            } catch (final Exception e) {
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
    public String substituteProperties(final String text, final Properties properties) {
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
     *            Count of recursive calls - maximum depth is 20.
     * @return String value resulting from substitutions
     */
    String substituteProperties(String text, final Properties properties, final int depth) {
        int p = text.indexOf("${");
        while (p >= 0 && p < text.length()) {
            p += 2;
            final int q = text.indexOf("}", p);
            if (q > 0) {
                final String propertyName = text.substring(p, q);
                if (Util.isValidName(propertyName)) {
                    // sanity check to prevent stack overflow
                    // due to infinite loop
                    if (depth > MAX_RECURSION_COUNT)
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

    /**
     * Return the <code>Properties</code> from which this
     * <code>Configuration</code> was loaded. In a newly constructed instance,
     * this <code>Properties</code> instance is empty.
     * 
     * @return the <code>Properties</code> from which this
     *         <code>Configuration</code> was loaded
     */
    public Properties getProperties() {
        return _properties;
    }

    /**
     * <p>
     * Return a property value for the supplied <code>propertyName</code>, or
     * <code>null</code> if there is no such property. The property is taken
     * from one of the following sources:
     * <ol>
     * <li>A system property having a prefix of "com.persistit.". For example,
     * the property named <code>journalpath</code> can be supplied as the system
     * property named <code>com.persistit.journalpath</code>. (Note: if the
     * security context does not permit access to system properties then system
     * properties are ignored.)</li>
     * <li>The <code>Properties</code> object which was either passed to the
     * {@link #Configuration(Properties)} or supplied by the
     * {@link Persistit#initialize(Properties)} method.</li>
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
    public String getProperty(final String propertyName) {
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
     * {@link Persistit#initialize(Properties)} method, or was loaded from the
     * file named in the {@link Persistit#initialize(String)} method.</li>
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
    public String getProperty(final String propertyName, final String defaultValue) {
        final String value = getProperty(propertyName, 0, _properties);
        return value == null ? defaultValue : value;
    }

    private String getProperty(final String propertyName, final int depth, final Properties properties) {
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
     * Set a property value in the configuration <code>Properties</code> map. If
     * the specified value is null then an existing property of the specified
     * name is removed.
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
            @Override
            public Object run() {
                return System.getProperty(propertyName);
            }
        });
    }

    int getIntegerProperty(final String propName, final int dflt) {
        final long v = getLongProperty(propName, dflt);
        if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
            return (int) v;
        }
        throw new IllegalArgumentException("Value out of range: " + v);
    }

    /**
     * Interpret a property value as a long integer. Permits suffix values of
     * "K" for Kilo- and "M" for Mega-, "G" for Giga- and "T" for Tera-. For
     * example, the property value "100K" yields a parsed result of 102400.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation
     */
    long getLongProperty(final String propName, final long dflt) {
        final String str = getProperty(propName);
        if (str == null) {
            return dflt;
        }
        return parseLongProperty(propName, str);
    }

    /**
     * Parse a string as a long integer. Permits suffix values of "K" for Kilo-
     * and "M" for Mega-, "G" for Giga- and "T" for Tera-. For example, the
     * supplied <code>str</code> value of "100K" yields a parsed result of
     * 102400.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param str
     *            The string representation, e.g., "100K".
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation.
     */
    public static long parseLongProperty(final String propName, final String str) {
        if (str != null) {
            try {
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
                return Long.parseLong(sstr) * multiplier;
            } catch (final NumberFormatException nfe) {
            }
        }
        throw new IllegalArgumentException("Invalid number '" + str + "' for property " + propName);
    }

    /**
     * Parse a string as a float value
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param str
     *            The string representation, e.g., "100K".
     * @return The numeric value of the supplied String, as a floag.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid floating point
     *             representation, or is outside the supplied bounds.
     */
    public static float parseFloatProperty(final String propName, final String str) {
        if (str != null) {
            try {
                return Float.parseFloat(str);
            } catch (final NumberFormatException e) {

            }
        }
        throw new IllegalArgumentException("Invalid number '" + str + "' for property " + propName);
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
    public static boolean parseBooleanValue(final String propName, final String str) {
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
        if (scale == 0) {
            return String.format("%d", v);
        } else {
            return String.format("%d%s", v, "KMGT".substring(scale - 1, scale));
        }
    }

    /**
     * Return an array containing all valid {@link Buffer} sizes.
     * 
     * @return valid {@link Buffer} sizes
     */
    public static int[] validBufferSizes() {
        return new int[] { 1024, 2048, 4096, 8192, 16384 };
    }

    /**
     * Parse and return a string value as either <i>true</i> or <i>false</i>.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param dflt
     *            The default value
     * @return <i>true</i> or <i>false</i> as specified by the property
     */
    public boolean getBooleanProperty(final String propName, final boolean dflt) {
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

    /**
     * Parse the supplied String as a {@link VolumeSpecification} after
     * performing any available property value substitutions.
     * 
     * @param vstring
     *            a specification string as defined in
     *            {@link VolumeSpecification#VolumeSpecification(String)}.
     * @return a <code>VolumeSpecification</code>
     * @throws InvalidVolumeSpecificationException
     * @see #getProperty(String)
     */
    public VolumeSpecification volumeSpecification(final String vstring) throws InvalidVolumeSpecificationException {
        final VolumeSpecification volumeSpec = new VolumeSpecification(substituteProperties(vstring, _properties, 0));
        return volumeSpec;
    }

    // -------------------------------------------

    /**
     * Return an an unmodifiable Map containing the
     * <code>BufferPoolConfiguration</code> instances for each possible buffer
     * size.
     * 
     * @return the map
     */
    public Map<Integer, BufferPoolConfiguration> getBufferPoolMap() {
        return bufferPoolMap;
    }

    /**
     * Return a summary of the buffer configuration of all buffer pool sizes as
     * a string. For example, a value such as
     * 
     * <code><pre>
     * 4096,count=1000;16384,minMemory=0,maxMemory=512G,reserved=1G,fraction=0.6
     * </pre></code>
     * 
     * @return describes two buffer pools, on with 4K buffers and the other with
     *         16K buffers. There the configuration specifies the smaller pool
     *         by count, and the larger buffers by memory.
     */
    public String getBufferPoolConfiguration() {
        final StringBuilder sb = new StringBuilder();
        for (final BufferPoolConfiguration bpc : bufferPoolMap.values()) {
            if (bpc.getMaximumCount() > 0) {
                if (sb.length() > 0) {
                    sb.append(";");
                }
                sb.append(bpc);
            }
        }
        return sb.toString();
    }

    /**
     * Parses and sets up the configuration of all buffer pools from the
     * supplied string.
     * 
     * @param string
     */
    public void setBufferPoolConfiguration(final String string) {
        for (final BufferPoolConfiguration bpc : bufferPoolMap.values()) {
            bpc.reset();
        }
        for (final String s : string.split(";")) {
            try {
                final int bufferSize = Integer.parseInt(s.split(",")[0]);
                bufferPoolMap.get(bufferSize).parse(s);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("Invalid BufferPool memory specification: " + string);
            } catch (final ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Invalid BufferPool memory specification: " + string);
            }
        }
    }

    /**
     * Return a List of <code>VolumeSpecification</code> instances for
     * {@link Volume}s that Persistit should load or create during
     * initialization. An application can add <code>VolumeSpecification</code>
     * elements to this list before calling
     * {@link Persistit#initialize(Configuration)}.
     * 
     * @return a List of <code>VolumeSpecification</code>s
     */
    public List<VolumeSpecification> getVolumeList() {
        return volumeSpecifications;
    }

    /**
     * Copies the supplied {@link VolumeSpecification} elements to the volume
     * list after first clearing it. This method allows a list of
     * VolumeSpecifications to be injected as a property.
     * 
     * @param list
     *            List of VolumeSpecifications for volumes to be opened when the
     *            {@link Persistit#initialize()} method is called
     */
    public void setVolumeList(final List<VolumeSpecification> list) {
        volumeSpecifications.clear();
        volumeSpecifications.addAll(list);
    }

    /**
     * Return the value defined by {@link #setJournalPath}
     * 
     * @return the journalPath
     */
    public String getJournalPath() {
        return journalPath;
    }

    /**
     * <p>
     * Set the path name on which journal files will be created. Each journal
     * file name will be created by adding a period followed by a twelve-digit
     * number to this value. A typical value would be
     * 
     * <code><pre>
     * ${datapath}/journal
     * </pre></code>
     * 
     * Where the <code>datapath</code> property specifies a directory containing
     * Persistit data files.
     * </p>
     * Default value is {@value #DEFAULT_JOURNAL_PATH} <br />
     * Property name is {@value #JOURNAL_PATH_PROPERTY_NAME}
     * 
     * @param journalPath
     *            the path to set
     * 
     */
    public void setJournalPath(final String journalPath) {
        this.journalPath = journalPath;
    }

    /**
     * Return the value defined by {@link #setJournalSize}
     * 
     * @return the journalSize
     */
    public long getJournalSize() {
        return journalSize;
    }

    /**
     * <p>
     * Set the maximum size of each journal file. When adding a record to a
     * journal file would result in a file larger than this size, Persistit
     * instead create a new journal file with a larger numeric suffix. The
     * default size of 1,000,000,000 has been tested extensively on current
     * server-class configurations and is recommended.
     * </p>
     * Default size is
     * {@value com.persistit.mxbeans.JournalManagerMXBean#DEFAULT_BLOCK_SIZE} <br/>
     * Property name is {@value #JOURNAL_BLOCKSIZE_PROPERTY_NAME}
     * 
     * @param journalSize
     *            the journalSize to set
     */
    public void setJournalSize(final long journalSize) {
        Util.rangeCheck(journalSize, JournalManager.MINIMUM_BLOCK_SIZE, JournalManager.MAXIMUM_BLOCK_SIZE);
        this.journalSize = journalSize;
    }

    /**
     * Return the value defined by {@link #setCheckpointInterval(long)}
     * 
     * @return the checkpoint interval, in seconds
     */
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * <p>
     * Set the checkpoint interval, in seconds. This setting controls the
     * elapsed time between attempts to write a checkpoint to the journal. A
     * longer interval allows more updates to accumulate in buffers before they
     * are required to be written to disk, but also potentially causes recovery
     * from an abrupt termination (crash) to take more time.
     * </p>
     * Default size is
     * {@value com.persistit.mxbeans.CheckpointManagerMXBean#DEFAULT_CHECKPOINT_INTERVAL_S}
     * <br/>
     * Property name is {@value #CHECKPOINT_INTERVAL_PROPERTY_NAME}
     */
    public void setCheckpointInterval(final long checkpointInterval) {
        this.checkpointInterval = Util.rangeCheck(checkpointInterval, MINIMUM_CHECKPOINT_INTERVAL_S,
                MAXIMUM_CHECKPOINT_INTERVAL_S);
    }

    /**
     * Return the value defined by {@link #setSysVolume}
     * 
     * @return the system volume name
     */
    public String getSysVolume() {
        return sysVolume;
    }

    /**
     * <p>
     * Set the system volume name attribute. The system volume is used by
     * {@link DefaultCoderManager} and others to store metadata about objects
     * encoded in Persistit. The value should specify a valid volume name or
     * alias. If a configuration contains only one volume then by default it is
     * also selected as the system volume.
     * </p>
     * <p>
     * Default value is determined by volume specifications</br /> Property name
     * is {@value #SYSTEM_VOLUME_PROPERTY_NAME}
     * </p>
     * 
     * @param sysVolume
     *            the sysVolume to set
     */
    public void setSysVolume(final String sysVolume) {
        this.sysVolume = sysVolume;
    }

    /**
     * Return the value defined by {@link #setTmpVolDir}
     * 
     * @return the temporary volume directory
     */
    public String getTmpVolDir() {
        return tmpVolDir;
    }

    /**
     * <p>
     * Set the name of a directory where backing files to hold temporary volumes
     * are created when needed. By default such files are created as
     * system-defined temporary files.
     * </p>
     * Default value is <code>null</code> meaning the system-assigned temporary
     * file path is used<br />
     * Property name is {@value #TEMPORARY_VOLUME_DIR_PROPERTY_NAME}
     * 
     * @param tmpVolDir
     *            the temporary volume directory to set
     */
    public void setTmpVolDir(final String tmpVolDir) {
        this.tmpVolDir = tmpVolDir;
    }

    /**
     * Return the value defined by {@link #setTmpVolPageSize}
     * 
     * @return the default temporary volume page size
     */
    public int getTmpVolPageSize() {
        return tmpVolPageSize;
    }

    /**
     * <p>
     * Set the default page size for newly created temporary volumes. In the
     * normal case where Persistit has only one {@link BufferPool} it is
     * unnecessary to set this value because Persistit implicitly selects the
     * buffer size for that pool. However, in a configuration with multiple
     * buffer pools, this attribute selects which one to use.
     * </p>
     * <p>
     * Default value is determined by buffer pool configuration <br />
     * Property name is {@value #TEMPORARY_VOLUME_PAGE_SIZE_PROPERTY_NAME}
     * 
     * @param tmpVolPageSize
     *            the default temporary volume page size to set
     */
    public void setTmpVolPageSize(final int tmpVolPageSize) {
        this.tmpVolPageSize = tmpVolPageSize;
    }

    /**
     * Return the value defined by {@link #setTmpVolMaxSize};
     * 
     * @return the maximum size in bytes to which a temporary volume file may
     *         grow
     */
    public long getTmpVolMaxSize() {
        return tmpVolMaxSize;
    }

    /**
     * <p>
     * Set the maximum temporary volume file size in bytes. This method
     * specifies a constraint on file growth to avoid runaway disk utilization.
     * </p>
     * <p>
     * Default value is @link {@link Long#MAX_VALUE} <br />
     * Property name is {@value #TEMPORARY_VOLUME_MAX_SIZE_PROPERTY_NAME}
     * </p>
     * 
     * @param tmpVolMaxSize
     *            the maximum size in bytes to which a temporary volume file may
     *            grow to set
     */
    public void setTmpVolMaxSize(final long tmpVolMaxSize) {
        Util.rangeCheck(tmpVolMaxSize, MINIMUM_TEMP_VOL_MAX_SIZE, MAXIMUM_TEMP_VOL_MAX_SIZE);
        this.tmpVolMaxSize = tmpVolMaxSize;
    }

    /**
     * Return the value defined by {@link #setCommitPolicy}
     * 
     * @return the commitPolicy
     */
    public CommitPolicy getCommitPolicy() {
        return commitPolicy;
    }

    /**
     * <p>
     * Set the default {@link com.persistit.Transaction.CommitPolicy}. The
     * string value must be one of "HARD", "GROUP" or "SOFT" (case insensitive).
     * </p>
     * <p>
     * Default value is SOFT <br />
     * Property name is {@value #COMMIT_POLICY_PROPERTY_NAME}
     * </p>
     * 
     * @param policyName
     *            Name of the commitPolicy the commitPolicy to set
     * @see com.persistit.Transaction#commit()
     */
    public void setCommitPolicy(final String policyName) {
        if (policyName != null) {
            setCommitPolicy(CommitPolicy.forName(policyName));
        }
    }

    /**
     * <p>
     * Set the default {@link Transaction.CommitPolicy} used by the
     * {@link Transaction#commit()} method.
     * </p>
     * <p>
     * Default value is SOFT
     * </p>
     * 
     * @param commitPolicy
     *            the commitPolicy to set
     * 
     * 
     */
    public void setCommitPolicy(final CommitPolicy commitPolicy) {
        this.commitPolicy = commitPolicy;
    }

    /**
     * Return the value defined by {@link #setJoinPolicy}
     * 
     * @return the joinPolicy
     */
    public JoinPolicy getJoinPolicy() {
        return joinPolicy;
    }

    /**
     * <p>
     * Set the default policy for balancing content between two pages when keys
     * are removed. The {@link Exchange#setJoinPolicy(JoinPolicy)} may be used
     * to override this behavior in a particular <code>Exchange</code>.
     * </p>
     * <p>
     * Default value is EVEN<br />
     * Property name is {@value #JOIN_POLICY_PROPERTY_NAME}
     * </p>
     * 
     * @param policyName
     *            Name of the <code>JoinPolicy</code> to set, one of "LEFT",
     *            "RIGHT" or "EVEN" (case insensitive)
     */
    public void setJoinPolicy(final String policyName) {
        if (policyName != null) {
            setJoinPolicy(JoinPolicy.forName(policyName));
        }
    }

    /**
     * <p>
     * Set the default policy for balancing content between two pages when keys
     * are removed. The {@link Exchange#setJoinPolicy(JoinPolicy)} may be used
     * to override this behavior in a particular <code>Exchange</code>.
     * </p>
     * <p>
     * Default value is EVEN
     * </p>
     * 
     * @param joinPolicy
     *            the <code>JoinPolicy</code> to set
     */
    public void setJoinPolicy(final JoinPolicy joinPolicy) {
        this.joinPolicy = joinPolicy;
    }

    /**
     * Return the value defined by {@link #setSplitPolicy}.
     * 
     * @return the splitPolicy
     */
    public SplitPolicy getSplitPolicy() {
        return splitPolicy;
    }

    /**
     * <p>
     * Set the default policy for balancing content between two pages when a
     * page is split. The {@link Exchange#setSplitPolicy(SplitPolicy)} may be
     * used to override this behavior in a particular <code>Exchange</code>.
     * </p>
     * <p>
     * Default value is PACK<br />
     * Property name is {@value #SPLIT_POLICY_PROPERTY_NAME}
     * </p>
     * 
     * @param policyName
     *            Name of the <code>SplitPolicy</code> to set.
     * 
     * @see SplitPolicy
     */
    public void setSplitPolicy(final String policyName) {
        if (policyName != null) {
            setSplitPolicy(SplitPolicy.forName(policyName));
        }
    }

    /**
     * <p>
     * Set the default policy for balancing content between two pages when a
     * page is split. The {@link Exchange#setSplitPolicy(SplitPolicy)} may be
     * used to override this behavior in a particular <code>Exchange</code>.
     * </p>
     * <p>
     * Default value is PACK
     * </p>
     * 
     * @param splitPolicy
     *            the <code>SplitPolicy</code> to set.
     * 
     * @see SplitPolicy
     */
    public void setSplitPolicy(final SplitPolicy splitPolicy) {
        this.splitPolicy = splitPolicy;
    }

    /**
     * Return the value defined by {@link #setSerialOverride}.
     * 
     * @return the serial override pattern
     */
    public String getSerialOverride() {
        return serialOverride;
    }

    /**
     * <p>
     * Set a pattern that identifies classes to be serialized using standard
     * Java serialization rather than Persistit's default serialization.
     * </p>
     * <p>
     * Default value is <code>null</code><br />
     * Property name is {@value #SERIAL_OVERRIDE_PROPERTY_NAME}
     * </p>
     * 
     * @param serialOverride
     *            the serial override pattern to set
     * @see DefaultCoderManager
     * @see <a
     *      href="http://www.akiban.com/ak-docs/admin/persistit/Serialization.html">Serialization</a>
     */
    public void setSerialOverride(final String serialOverride) {
        this.serialOverride = serialOverride;
    }

    /**
     * Return the value defined by {@link #setConstructorOverride}.
     * 
     * @return the constructorOverride
     */
    public boolean isConstructorOverride() {
        return constructorOverride;
    }

    /**
     * <p>
     * Control Persistit should require and use every serialized object to have
     * a public no-argument constructor. If so, then that constructor is used
     * when deserializing in the {@link DefaultObjectCoder}; if not then
     * Persistit uses private methods within the JDK to emulate standard Java
     * serialization logic.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #CONSTRUCTOR_OVERRIDE_PROPERTY_NAME}
     * </p>
     * 
     * @param constructorOverride
     *            the constructorOverride to set
     * @see <a
     *      href="http://www.akiban.com/ak-docs/admin/persistit/Serialization.html">Serialization</a>
     */
    public void setConstructorOverride(final boolean constructorOverride) {
        this.constructorOverride = constructorOverride;
    }

    /**
     * Return the value defined by {@link #setShowGUI}
     * 
     * @return the showGUI
     */
    public boolean isShowGUI() {
        return showGUI;
    }

    /**
     * <p>
     * Control whether Persistit starts up an attached instance of the AdminUI
     * tool during initialization.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #SHOW_GUI_PROPERTY_NAME}
     * </p>
     * 
     * @param showGUI
     *            <code>true</code> to start the AdminUI
     */
    public void setShowGUI(final boolean showGUI) {
        this.showGUI = showGUI;
    }

    /**
     * Return the value defined by {@link #setLogging}
     * 
     * @return the log detail level
     */
    public String getLogging() {
        return logging;
    }

    /**
     * <p>
     * Set the logging detail level for {@link DefaultPersistitLogger}. This
     * parameter has effect only if <code>DefaultPersistitLogger</code> is in
     * use; it has no effect on any of the logging adapters.
     * </p>
     * <p>
     * Default value is INFO<br />
     * Property name is {@value #LOGGING_PROPERTIES_NAME}
     * </p>
     * 
     * @param logging
     *            log detail level, one of "NONE", "TRACE", "DEBUG", "INFO",
     *            "WARNING", "ERROR".
     * @see Persistit#setPersistitLogger(com.persistit.logging.PersistitLogger)
     */
    public void setLogging(final String logging) {
        this.logging = logging;
    }

    /**
     * Return the value defined by {@link #setLogFile}
     * 
     * @return the logFile
     */
    public String getLogFile() {
        return logFile;
    }

    /**
     * <p>
     * Set the file name to which {@link DefaultPersistitLogger} writes log
     * entries. This parameter has effect only if
     * <code>DefaultPersistitLogger</code> is in use; it has no effect on any of
     * the logging adapters.
     * </p>
     * <p>
     * Default value is <code>null</code> - no log file is created</br />
     * Property value is {@value #LOGFILE_PROPERTY_NAME}
     * </p>
     * 
     * @param logFile
     *            the logFile to set
     */
    public void setLogFile(final String logFile) {
        this.logFile = logFile;
    }

    /**
     * Return the value defined by {@link #setRmiHost}
     * 
     * @return the rmiHost
     */
    public String getRmiHost() {
        return rmiHost;
    }

    /**
     * <p>
     * Set the URL of an Remote Method Invocation registry. If non-null,
     * Persistit registers its a server for its {@link Management} interface at
     * the specified external registry.
     * </p>
     * <p>
     * Default value is <code>null</code></br /> Property value is
     * {@value #RMI_REGISTRY_HOST_PROPERTY_NAME}
     * </p>
     * 
     * @param rmiHost
     *            the rmiHost to set
     */
    public void setRmiHost(final String rmiHost) {
        this.rmiHost = rmiHost;
    }

    /**
     * Return the value defined by {@link #setRmiPort}.
     * 
     * @return the rmiPort
     */
    public int getRmiPort() {
        return rmiPort;
    }

    /**
     * <p>
     * Set a port number on which Persistit will create a temporary Remote
     * Method Invocation registry. If non-zero Persistit creates a registry and
     * registers a {@link Management} server on it. This allows remote access to
     * management facilities within Persistit and permits the AdminUI utility to
     * attach to and manage a Persistit instance running on a headless server.
     * Specifying a non-zero value of this attribute is incompatible with
     * setting a non-null value in {@link #setRmiHost(String)}.
     * </p>
     * <p>
     * Default value is 0<br />
     * Property name is {@value #RMI_REGISTRY_PORT_PROPERTY_NAME}
     * </p>
     * 
     * @param rmiPort
     *            the rmiPort to set
     */
    public void setRmiPort(final int rmiPort) {
        this.rmiPort = rmiPort;
    }

    /**
     * Return the value defined by {@link #setRmiServerPort(int)}.
     * 
     * @return the rmiServerPort
     */
    public int getRmiServerPort() {
        return rmiServerPort;
    }

    /**
     * <p>
     * Define a port to be used with an external RMI registry defined by
     * {@link #setRmiHost}. If zero, the registry uses an anonymous port.
     * </p>
     * <p>
     * Default value is 0 <br />
     * Property name is {@value #RMI_SERVER_PORT_PROPERTY_NAME}
     * </p>
     * 
     * @param rmiServerPort
     *            the rmiServerPort to set
     */
    public void setRmiServerPort(final int rmiServerPort) {
        this.rmiServerPort = rmiServerPort;
    }

    /**
     * Return the value defined by {@link #setJmxEnabled(boolean)}.
     * 
     * @return whether JMX will be enabled during initialization
     */
    public boolean isJmxEnabled() {
        return jmx;
    }

    /**
     * <p>
     * Control whether Persistit registers JMX MXBeans with the platform MBean
     * server during initialization. Enables access to performance and other
     * data by JConsole, VisualVM and JMX clients.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@link #ENABLE_JMX_PROPERTY_NAME}
     * </p>
     * 
     * @param jmx
     *            the jmx to set
     */
    public void setJmxEnabled(final boolean jmx) {
        this.jmx = jmx;
    }

    /**
     * Return the value defined by {@link #setAppendOnly}
     * 
     * @return <true>true</code> if append-only mode is enabled at startup
     */
    public boolean isAppendOnly() {
        return appendOnly;
    }

    /**
     * <p>
     * Control whether Persistit starts in <i>append-only</i> mode. In this mode
     * Persistit accumulates database updates in the journal without copying
     * changes back into the volume files. This method changes only the initial
     * state; use {@link Management#setAppendOnly(boolean)} method to change
     * this behavior dynamically while the system is running.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #APPEND_ONLY_PROPERTY_NAME}
     * </p>
     * 
     * @param appendOnly
     *            <code>true</code> to start Persistit in append-only mode
     */
    public void setAppendOnly(final boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    /**
     * Return the value defined by {@link #setBufferInventoryEnabled}
     * 
     * @return <code>true</code> if periodic buffer pool inventory recording is
     *         enabled
     */
    public boolean isBufferInventoryEnabled() {
        return bufferInventoryEnabled;
    }

    /**
     * <p>
     * Control whether Persistit periodically records an inventory of its buffer
     * pools to enable buffer pool preloading on a subsequent startup.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #BUFFER_INVENTORY_PROPERTY_NAME}
     * 
     * @param bufferInventoryEnabled
     *            <code>true</code> to enable periodic buffer inventory behavior
     */
    public void setBufferInventoryEnabled(final boolean bufferInventoryEnabled) {
        this.bufferInventoryEnabled = bufferInventoryEnabled;
    }

    /**
     * Return the value defined by {@link #setBufferPreloadEnabled}
     * 
     * @return <code>true</code> if the option to preload buffer pools is
     *         enabled
     */
    public boolean isBufferPreloadEnabled() {
        return bufferPreloadEnabled;
    }

    /**
     * <p>
     * Control whether Persistit attempts to preload (warm up) the buffer pools
     * by preloading pages recorded in a previously generated inventory.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #BUFFER_INVENTORY_PROPERTY_NAME}
     * 
     * @param bufferPreloadEnabled
     *            <code>true</code> to enable buffer pool preloading during
     *            startup
     */
    public void setBufferPreloadEnabled(final boolean bufferPreloadEnabled) {
        this.bufferPreloadEnabled = bufferPreloadEnabled;
    }

    /**
     * Return the value defined by {@link #setIgnoreMissingVolumes(boolean)}
     * 
     * @return <code>true</code> indicates that <i>ignore-missing-volumes</i>
     *         mode is enabled
     */
    public boolean isIgnoreMissingVolumes() {
        return ignoreMissingVolumes;
    }

    /**
     * <p>
     * Control whether Persistit starts in <i>ignore-missing-volumes</i> mode.
     * In this mode references in the journal to unknown volumes are ignored
     * rather than noted as {@link CorruptJournalException}s. Almost always this
     * mode should be disabled; the setting is available to enable recovery of a
     * journal into a subset of formerly existing volumes and should be used
     * only with care.
     * </p>
     * <p>
     * Default value is <code>false</code><br />
     * Property name is {@value #IGNORE_MISSING_VOLUMES_PROPERTY}
     * </p>
     * 
     * @param ignoreMissingVolumes
     *            <code>true</code> to ignore missing volumes
     */
    public void setIgnoreMissingVolumes(final boolean ignoreMissingVolumes) {
        this.ignoreMissingVolumes = ignoreMissingVolumes;
    }

    /**
     * Return the value defined by {@link #setUseOldVSpec(boolean)}
     * 
     * @return <code>true</code> indicates that <i>use-old-vspec</i> mode is
     *         enabled
     */
    @Deprecated
    public boolean isUseOldVSpec() {
        return useOldVSpec;
    }

    /**
     * <p>
     * Control whether Persistit writes old-format volume identifiers into the
     * Journal. By default as of version 3.1.8, Persistit writes a complete
     * {@link VolumeSpecification} into the journal IV (Identify Volume) record
     * rather than just a volume name. This is preferable in almost all cases.
     * However, journals written in this new format cannot be read by earlier
     * versions of Persistit. To retain the ability to drop back to an earlier
     * version of Persistit, enabling <i>use-old-vspec</i> mode causes Persistit
     * to write only the volume name rather than the entire VolumeSpecification.
     * Journal files created by version 3.1.8 with this mode enabled can be used
     * by earlier versions of Persistit. However, doing so prevents volumes
     * created dynamically using
     * {@link Persistit#loadVolume(com.persistit.VolumeSpecification)} from
     * being recovered properly. (See
     * https://bugs.launchpad.net/akiban-persistit/+bug/1045971)
     * </p>
     * <p>
     * This method and configuration parameter is deprecated and will be removed
     * after existing sites have been upgraded and are likely not to revert to
     * any version of Persistit earlier than 3.1.8
     * </p>
     * 
     * @param useOldVSpec
     *            <code>true</code> to write only the volume name into the
     *            journal.
     */
    @Deprecated
    public void setUseOldVSpec(final boolean useOldVSpec) {
        this.useOldVSpec = useOldVSpec;
    }

}
