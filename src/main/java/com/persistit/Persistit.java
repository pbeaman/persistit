/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.persistit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.WaitingThreadManager.WaitingThread;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.LogInitializationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PropertiesNotFoundException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.logging.AbstractPersistitLogger;
import com.persistit.logging.DefaultPersistitLogger;

/**
 * <p>
 * Creates and manages the the runtime environment for a Persistit&trade;
 * database. To use <tt>Persistit</tt>, an application invokes one of the static
 * {@link #initialize} methods to load a set of properties that govern
 * Persistit's behavior, and to initialize its memory structures. When
 * terminating, the application should invoke the static {@link #close} method
 * to complete all database writes, close all files and relinquish all buffer
 * memory.
 * </p>
 * <p>
 * Once initialized, there is a single <tt>Persistit</tt> instance available
 * from the {@link #getInstance} method. Various non-static methods are
 * available on this instance. The {@link #close} method releases the
 * <tt>Persistit</tt> instance, allowing its memory to be released.
 * </p>
 * <p>
 * An application interacts with Persistit by creating {@link Exchange} objects
 * and invoking their methods.
 * </p>
 * <p>
 * During initialization this class optionally creates a small Swing UI
 * containing various useful diagnostic views of internal state. To request this
 * utility, include the command-line parameter
 * <code>-Dcom.persistit.showgui=true</code>, or specify the property
 * </code>showgui=true</code> in the properties file supplied to the
 * {@link #initialize} method.
 * </p>
 * 
 * @version 1.1
 */
public class Persistit implements BuildConstants {
    /**
     * This version of Persistit
     */
    public final static String VERSION = "Persistit JSA 2.2-20101115"
            + (Debug.ENABLED ? "-DEBUG" : "");
    /**
     * The internal version number
     */
    public final static int BUILD_ID = 22000;
    /**
     * The copyright notice
     */
    public final static String COPYRIGHT = "Copyright (c) 2004-2010 Persistit Corporation. All Rights Reserved.";

    /**
     * Determines whether multi-byte integers will be written in little- or
     * big-endian format. This constant is <tt>true</tt> in all current builds.
     */
    public final static boolean BIG_ENDIAN = true;
    /**
     * Prefix used to form the a system property name. For example, the property
     * named <tt>pwjpath=xyz</tt> can also be specified as a system property on
     * the command line with the option -Dcom.persistit.pwjpath=xyz.
     */
    public final static String SYSTEM_PROPERTY_PREFIX = "com.persistit.";
    /**
     * Name of utility GUI class.
     */
    private final static String PERSISTIT_GUI_CLASS_NAME = SYSTEM_PROPERTY_PREFIX
            + "ui.AdminUI";

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
     * specify for individual <tt>Exchange</tt>.
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
     * Property name for specifying the size of each prewrite journal, e.g.,
     * "pwjsize=512K".
     */
    public final static String JOURNAL_SIZE_PROPERTY_NAME = "journalsize";

    /**
     * Property name for specifying whether Persistit should write fetch and
     * traverse operations to the journal file.
     */
    public final static String JOURNAL_FETCHES = "journalfetches";

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
    public final static String SYSTEM_VOLUME_PROPERTY = "sysvolume";

    /**
     * Default Transactions Volume Name
     */
    public final static String DEFAULT_TXN_VOLUME_NAME = "_txn";

    /**
     * Property name for specifying the transaction volume name
     */
    public final static String TXN_VOLUME_PROPERTY = "txnvolume";

    /**
     * Property name for specifing whether Persistit should display diagnostic
     * messages. Property value must be "true" or "false".
     */
    public final static String VERBOSE_PROPERTY = "verbose";
    /**
     * Property name for specifying whether Persistit should retry read
     * operations that fail due to IOExceptions.
     */
    public final static String READ_RETRY_PROPERTY = "readretry";
    /**
     * Property name for maximum length of time a Persistit operation will wait
     * for I/O completion before throwing a TimeoutException.
     */
    public final static String TIMEOUT_PROPERTY = "timeout";

    /**
     * Property name to specify package and/or class names of classes that must
     * be serialized using standard Java serialization.
     */
    public final static String SERIAL_OVERRIDE_PROPERTY = "serialOverride";

    /**
     * Property name to specify whether DefaultValueCoder should use a declared
     * no-arg contructor within each class being deserialized. Unless specified
     * as <tt>true</tt>, Serializable classes will be instantiated through
     * platform-specific API calls.
     */
    public final static String CONSTRUCTOR_OVERRIDE_PROPERTY = "constructorOverride";

    /**
     * Property name for specifying whether Persistit should attempt to launch a
     * diagnostic utility for viewing internal state.
     */
    public final static String SHOW_GUI_PROPERTY = "showgui";

    /**
     * Property name for log switches
     */
    public final static String LOGGING_PROPERTIES = "logging";

    /**
     * Property name for log file name
     */
    public final static String LOGFILE_PROPERTY = "logfile";
    /**
     * Property name for the optional RMI registry host name
     */
    public final static String RMI_REGISTRY_HOST_PROPERTY = "rmihost";
    /**
     * Property name for the optional RMI registry port
     */
    public final static String RMI_REGISTRY_PORT = "rmiport";
    /**
     * Property name for enabling Persistit Open MBean for JMX
     */
    public final static String JMX_PARAMS = "jmx";

    /**
     * Object name (as String) for stock MXBean
     */
    public final static String MXBEAN_OBJECT_NAME = "com.persistit.PersistitOpenMBean:type=Persistit";
    /**
     * Property name for pseudo-property "timestamp";
     */
    public final static String TIMESTAMP_PROPERTY = "timestamp";

    /**
     * Property name for the "append only" property.
     */
    public final static String APPEND_ONLY_PROPERTY = "appendonly";

    /**
     * Maximum number of Exchanges that will be held in an internal pool.
     */
    public final static int MAX_POOLED_EXCHANGES = 100;

    /**
     * Default environment name
     */
    public final static String DEFAULT_ENVIRONMENT = "default";
    /**
     * New-Line character sequence
     */
    public final static String NEW_LINE = System.getProperty("line.separator");
    /**
     * Minimal command line documentation
     */
    private final static String[] USAGE = {
            "java.persistit.Persistit [options] [property_file_name]", "",
            " where flags are", "  -g           to show the Admin UI",
            "  -i           to perform integrity checks on all volumes",
            "  -w           to wait for the Admin UI to connect",
            "  -? or -help  to show this help message", };

    private final static long KILO = 1024;
    private final static long MEGA = KILO * KILO;
    private final static long GIGA = MEGA * KILO;
    private final static long TERA = GIGA * KILO;

    private final static long SHORT_DELAY = 500;

    private final static long CLOSE_LOG_INTERVAL = 30000;
    /**
     * If a thread waits longer than this, apply throttle to slow down other
     * threads.
     */
    private static final long THROTTLE_THRESHOLD = 5000;

    private AbstractPersistitLogger _logger;

    /**
     * Start time
     */
    private final long _startTime = System.currentTimeMillis();
    private final HashMap<Integer, BufferPool> _bufferPoolTable = new HashMap<Integer, BufferPool>();
    private final ArrayList<Volume> _volumes = new ArrayList<Volume>();
    private final HashMap<Long, Volume> _volumesById = new HashMap<Long, Volume>();
    private Properties _properties = new Properties();

    private AtomicBoolean _initialized = new AtomicBoolean();
    private AtomicBoolean _closed = new AtomicBoolean();

    private long _beginCloseTime;
    private long _nextCloseTime;

    private final LogBase _logBase = new LogBase(this);

    private AtomicBoolean _suspendShutdown = new AtomicBoolean(false);
    private AtomicBoolean _suspendUpdates = new AtomicBoolean(false);

    private UtilControl _localGUI;

    private CoderManager _coderManager;
    private ClassIndex _classIndex = new ClassIndex(this);
    private ThreadLocal<Transaction> _transactionThreadLocal = new ThreadLocal<Transaction>();
    private ThreadLocal<WaitingThread> _waitingThreadLocal = new ThreadLocal<WaitingThread>();
    private ThreadLocal<Throttle> _throttleThreadLocal = new ThreadLocal<Throttle>();

    private ManagementImpl _management;

    private final RecoveryManager _recoveryManager = new RecoveryManager(this);

    private final JournalManager _journalManager = new JournalManager(this);

    private final TimestampAllocator _timestampAllocator = new TimestampAllocator();

    private final IOMeter _ioMeter = new IOMeter();

    private Stack<Exchange> _exchangePool = new Stack<Exchange>();

    private boolean _readRetryEnabled;

    private long _defaultTimeout;

    private final SharedResource _transactionResourceA = new SharedResource(
            this);

    private final SharedResource _transactionResourceB = new SharedResource(
            this);

    private final LockManager _lockManager = new LockManager();

    private static volatile long _globalThrottleCount;

    private long _nextThrottleBumpTime;

    private long _localThrottleCount;

    private final List<Checkpoint> _outstandingCheckpoints = new ArrayList<Checkpoint>();

    private Checkpoint _currentCheckpoint = new Checkpoint(0, 0);

    /**
     * <p>
     * Initialize Persistit using properties supplied by the default properties
     * file. The name of this file is supplied by the system property
     * <tt>com.persistit.properties</tt>. If that property is not specified, the
     * default file path is <tt>./persistit.properties</tt> in the current
     * working directory. If Persistit has already been initialized, this method
     * does nothing. This method is threadsafe; if multiple threads concurrently
     * attempt to invoke this method, one of the threads will actually perform
     * the initialization and the other threads will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * 
     * @throws PersistitException
     * @throws IOException
     * @throws Exception
     */
    public void initialize() throws PersistitException {
        initialize(getProperty(CONFIG_FILE_PROPERTY_NAME, DEFAULT_CONFIG_FILE));
    }

    /**
     * <p>
     * Initialize Persistit using the supplied properties file path. If
     * Persistit has already been initialized, this method does nothing. This
     * method is threadsafe; if multiple threads concurrently attempt to invoke
     * this method, one of the threads will actually perform the initialization
     * and the other threads will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * 
     * @param propertiesFileName
     *            The path to the properties file.
     * @throws PersistitException
     * @throws IOException
     */
    public void initialize(String propertiesFileName) throws PersistitException {
        initialize(parseProperties(propertiesFileName));
    }

    /**
     * <p>
     * Initialize Persistit using the supplied <code>java.util.Properties</code>
     * instance. Applications can use this method to supply computed Properties
     * rather than reading them from a file. If Persistit has already been
     * initialized, this method does nothing. This method is threadsafe; if
     * multiple threads concurrently attempt to invoke this method, one of the
     * threads will actually perform the initialization and the other threads
     * will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * 
     * @param properties
     *            The Properties object from which to initialize Persistit
     * @throws PersistitException
     * @throws IOException
     */
    public void initialize(Properties properties) throws PersistitException {
        getTransaction().assignTimestamp();
        initializeProperties(properties);
        initializeManagement();
        initializeOther();
        initializeLogging();
        initializeRecovery();
        initializeJournal();
        initializeBufferPools();
        initializeVolumes();
        startBufferPools();
        finishRecovery();
        startJournal();
        flush();

        _initialized.set(true);
        _closed.set(false);
    }

    Properties parseProperties(final String propertiesFileName)
            throws PersistitException {
        Properties properties = new Properties();
        try {
            if (propertiesFileName.contains(DEFAULT_PROPERTIES_FILE_SUFFIX)
                    || propertiesFileName.contains(File.separator)) {
                properties.load(new FileInputStream(propertiesFileName));
            } else {
                ResourceBundle bundle = ResourceBundle
                        .getBundle(propertiesFileName);
                for (Enumeration<String> e = bundle.getKeys(); e
                        .hasMoreElements();) {
                    final String key = (String) e.nextElement();
                    properties.put(key, bundle.getString(key));
                }
            }
        } catch (FileNotFoundException fnfe) {
            // A friendlier exception when the properties file is not found.
            throw new PropertiesNotFoundException(fnfe.getMessage());
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
        return properties;
    }

    void initializeProperties(final Properties properties) {
        properties.putAll(_properties);
        _properties = properties;

        _readRetryEnabled = getBooleanProperty(READ_RETRY_PROPERTY, true);
        _defaultTimeout = getLongProperty(TIMEOUT_PROPERTY,
                DEFAULT_TIMEOUT_VALUE, 0, MAXIMUM_TIMEOUT_VALUE);

    }

    void initializeLogging() throws PersistitException {
        try {
            _logBase.logstart();
        } catch (Exception e) {
            System.err.println("Persistit(tm) Logging is disabled due to " + e);
            if (e.getMessage() != null && e.getMessage().length() > 0) {
                System.err.println(e.getMessage());
            }
            e.printStackTrace();
        }

        String logSpecification = getProperty(LOGGING_PROPERTIES);
        if (logSpecification != null) {
            try {
                _logBase.setLogEnabled(logSpecification,
                        AbstractPersistitLogger.INFO);
            } catch (Exception e) {
                throw new LogInitializationException(e);
            }
        }
    }

    void initializeRecovery() throws PersistitException {
        String journalPath = getProperty(JOURNAL_PATH_PROPERTY_NAME,
                DEFAULT_JOURNAL_PATH);
        _recoveryManager.init(journalPath);
        _recoveryManager.buildRecoveryPlan();
    }

    void initializeJournal() throws PersistitException {
        String journalPath = getProperty(JOURNAL_PATH_PROPERTY_NAME,
                DEFAULT_JOURNAL_PATH);
        int journalSize = (int) getLongProperty(JOURNAL_SIZE_PROPERTY_NAME,
                JournalManager.DEFAULT_BLOCK_SIZE,
                JournalManager.MINIMUM_BLOCK_SIZE,
                JournalManager.MAXIMUM_BLOCK_SIZE);

        _journalManager.init(_recoveryManager, journalPath, journalSize);
        if (getBooleanProperty(APPEND_ONLY_PROPERTY, false)) {
            _journalManager.setAppendOnly(true);
        }
    }

    void initializeBufferPools() {
        StringBuilder sb = new StringBuilder();
        int bufferSize = Buffer.MIN_BUFFER_SIZE;
        while (bufferSize <= Buffer.MAX_BUFFER_SIZE) {
            sb.setLength(0);
            sb.append(BUFFERS_PROPERTY_NAME);
            sb.append(bufferSize);
            String propertyName = sb.toString();

            int count = (int) getLongProperty(propertyName, -1,
                    BufferPool.MINIMUM_POOL_COUNT,
                    BufferPool.MAXIMUM_POOL_COUNT);

            if (count != -1) {
                if (_logBase.isLoggable(LogBase.LOG_INIT_ALLOCATE_BUFFERS)) {
                    _logBase.log(LogBase.LOG_INIT_ALLOCATE_BUFFERS, count,
                            bufferSize);
                }
                BufferPool pool = new BufferPool(count, bufferSize, this);
                _bufferPoolTable.put(new Integer(bufferSize), pool);
            }
            bufferSize <<= 1;
        }
    }

    void initializeVolumes() throws PersistitException {
        for (Enumeration<?> enumeration = _properties.propertyNames(); enumeration
                .hasMoreElements();) {
            String key = (String) enumeration.nextElement();
            if (key.startsWith(VOLUME_PROPERTY_PREFIX)) {
                boolean isOne = true;
                try {
                    Integer.parseInt(key.substring(VOLUME_PROPERTY_PREFIX
                            .length()));
                } catch (NumberFormatException nfe) {
                    isOne = false;
                }
                if (isOne) {
                    VolumeSpecification volumeSpecification = new VolumeSpecification(
                            getProperty(key));

                    if (_logBase.isLoggable(LogBase.LOG_INIT_OPEN_VOLUME)) {
                        _logBase.log(LogBase.LOG_INIT_OPEN_VOLUME,
                                volumeSpecification.describe());
                    }
                    Volume.loadVolume(this, volumeSpecification);
                }
            }
        }
    }

    void initializeManagement() {
        String rmiHost = getProperty(RMI_REGISTRY_HOST_PROPERTY);
        String rmiPort = getProperty(RMI_REGISTRY_PORT);
        boolean enableJmx = getBooleanProperty(JMX_PARAMS, true);

        if (rmiHost != null || rmiPort != null) {
            ManagementImpl management = (ManagementImpl) getManagement();
            management.register(rmiHost, rmiPort);
        }
        if (enableJmx) {
            registerMXBean();
        }

    }

    void initializeOther() {
        // Set up the parent CoderManager for this instance.
        String serialOverridePatterns = getProperty(Persistit.SERIAL_OVERRIDE_PROPERTY);
        DefaultCoderManager cm = new DefaultCoderManager(this,
                serialOverridePatterns);
        _coderManager = cm;

        if (getBooleanProperty(SHOW_GUI_PROPERTY, false)) {
            try {
                setupGUI(true);
            } catch (Exception e) {
                // If we can't open the utility gui, well, tough.
            }
        }
    }

    void startBufferPools() throws PersistitException {
        for (final BufferPool pool : _bufferPoolTable.values()) {
            pool.startThreads();
        }
    }

    void startJournal() throws PersistitException {
        _journalManager.startThreads();
    }

    void finishRecovery() throws PersistitException {
        _recoveryManager.applyAllCommittedTransactions(_recoveryManager
                .getDefaultRecoveredTransactionActor());
        _recoveryManager.close();
        flush();
        checkpoint();
        if (_logBase.isLoggable(LogBase.LOG_RECOVERY_DONE)) {
            _logBase.log(LogBase.LOG_RECOVERY_DONE,
                    _journalManager.getPageMapSize(),
                    _recoveryManager.getAppliedTransactionCount(),
                    _recoveryManager.getErrorCount());
        }

    }

    /**
     * Reflectively attempts to load and execute the PersistitOpenMBean setup
     * method. This will work only if the persistit_jsaXXX_jmx.jar is on the
     * classpath. By default, PersistitOpenMBean uses the platform JMX server,
     * so this also required Java 5.0+.
     * 
     * @param params
     *            "true" to enable the PersistitOpenMBean, else "false".
     */
    private void registerMXBean() {
        MBeanServer server = java.lang.management.ManagementFactory
                .getPlatformMBeanServer();
        try {
            ObjectName objectName = new ObjectName(MXBEAN_OBJECT_NAME);
            server.registerMBean((ManagementMXBean) getManagement(), objectName);
        } catch (Exception exception) {
            if (_logBase.isLoggable(LogBase.LOG_MBEAN_EXCEPTION)) {
                _logBase.log(LogBase.LOG_MBEAN_EXCEPTION, exception);
            }
        }
    }

    private void unregisterMXBean() {
        MBeanServer server = java.lang.management.ManagementFactory
                .getPlatformMBeanServer();
        try {
            ObjectName objectName = new ObjectName(MXBEAN_OBJECT_NAME);
            server.unregisterMBean(objectName);
        } catch (InstanceNotFoundException exception) {
            // ignore
        } catch (Exception exception) {
            if (_logBase.isLoggable(LogBase.LOG_MBEAN_EXCEPTION)) {
                _logBase.log(LogBase.LOG_MBEAN_EXCEPTION, exception);
            }
        }
    }

    synchronized void addVolume(Volume volume)
            throws VolumeAlreadyExistsException {
        Long idKey = new Long(volume.getId());
        Volume otherVolume = _volumesById.get(idKey);
        if (otherVolume != null) {
            throw new VolumeAlreadyExistsException("Volume " + otherVolume
                    + " has same ID");
        }
        otherVolume = getVolume(volume.getPath());
        if (otherVolume != null
                && volume.getPath().equals(otherVolume.getPath())) {
            throw new VolumeAlreadyExistsException("Volume " + otherVolume
                    + " has same path");
        }
        _volumes.add(volume);
        _volumesById.put(idKey, volume);
    }

    synchronized void removeVolume(Volume volume, boolean delete) {
        Long idKey = new Long(volume.getId());
        _volumesById.remove(idKey);
        _volumes.remove(volume);
        // volume.getPool().invalidate(volume);
        if (delete) {
            volume.getPool().delete(volume);
        }
    }

    /**
     * Replaces substitution variables in a supplied string with values taken
     * from the properties available to Persistit (see {@link getProperty}).
     * 
     * @param text
     *            String in in which to make substitutions
     * @param properties
     *            Properties containing substitution values
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
                        throw new IllegalArgumentException("property "
                                + propertyName
                                + " substitution cycle is too deep");
                    String propertyValue = getProperty(propertyName, depth + 1,
                            properties);
                    if (propertyValue == null)
                        propertyValue = "";
                    text = text.substring(0, p - 2) + propertyValue
                            + text.substring(q + 1);
                    q += propertyValue.length() - (propertyName.length() + 3);
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
     * Returns a property value, or <tt>null</tt> if there is no such property.
     * The property is taken from one of the following sources:
     * <ol>
     * <li>A system property having a prefix of "com.persistit.". For example,
     * the property named "journalpath" can be supplied as the system property
     * named com.persistit.journalpath. (Note: if the security context does not
     * permit access to system properties, then system properties are ignored.)</li>
     * <li>The supplied Properties object, which was either passed to the
     * {@link #initialize(Properties)} method, or was loaded from the file named
     * in the {@link #initialize(String)} method.</li>
     * <li>The pseudo-property name <tt>timestamp</tt>. The value is the current
     * time formated by <tt>SimpleDateFormat</tt> using the pattern
     * yyyyMMddHHmm. (This pseudo-property makes it easy to specify a unique log
     * file name each time Persistit is initialized.</li>
     * </ol>
     * </p>
     * If a property value contains a substitution variable in the form
     * <tt>${<i>pppp</i>}</tt>, then this method attempts perform a
     * substitution. To do so it recursively gets the value of a property named
     * <tt><i>pppp</i></tt>, replaces the substring delimited by <tt>${</tt> and
     * <tt>}</tt>, and then scans the resulting string for further substitution
     * variables.
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
     * the property named "pwjpath" can be supplied as the system property named
     * com.persistit.pwjpath. (Note: if the security context does not permit
     * access to system properties, then system properties are ignored.)</li>
     * <li>The supplied Properties object, which was either passed to the
     * {@link #initialize(Properties)} method, or was loaded from the file named
     * in the {@link #initialize(String)} method.</li>
     * <li>The pseudo-property name <tt>timestamp</tt>. The value is the current
     * time formated by <tt>SimpleDateFormat</tt> using the pattern
     * yyyyMMddHHmm. (This pseudo-property makes it easy to specify a unique log
     * file name each time Persistit is initialized.</li>
     * </ol>
     * </p>
     * If a property value contains a substitution variable in the form
     * <tt>${<i>pppp</i>}</tt>, then this method attempts perform a
     * substitution. To do so it recursively gets the value of a property named
     * <tt><i>pppp</i></tt>, replaces the substring delimited by <tt>${</tt> and
     * <tt>}</tt>, and then scans the resulting string for further substitution
     * variables.
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

    private String getProperty(String propertyName, int depth,
            Properties properties) {
        String value = null;

        value = getSystemProperty(SYSTEM_PROPERTY_PREFIX + propertyName);

        if (value == null && properties != null) {
            value = properties.getProperty(propertyName);
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
     *            Value to set, or <tt>null<tt> to remove an existing property
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

    /**
     * <p>
     * Returns an <tt>Exchange</tt> for the specified {@link Volume Volume} and
     * the {@link Tree Tree} specified by the supplied name. This method
     * optionally creates a new <tt>Tree</tt>. If the <tt>create</tt> parameter
     * is false and a <tt>Tree</tt> by the specified name does not exist, this
     * constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * This method uses an <tt>Exchange</tt> from an internal pool if one is
     * available; otherwise it creates a new <tt>Exchange</tt>. When the
     * application no longer needs the <tt>Exchange</tt> returned by this
     * method, it should return it to the pool by invoking
     * {@link #releaseExchange} so that it can be reused.
     * </p>
     * 
     * @param volume
     *            The Volume
     * 
     * @param treeName
     *            The tree name
     * 
     * @param create
     *            <tt>true</tt> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange getExchange(Volume volume, String treeName, boolean create)
            throws PersistitException {
        if (volume == null)
            throw new VolumeNotFoundException();
        Exchange exchange = null;
        synchronized (_exchangePool) {
            if (!_exchangePool.isEmpty()) {
                exchange = _exchangePool.pop();
                exchange.setRelinquished(false);
            }
        }
        if (exchange == null) {
            exchange = new Exchange(this, volume, treeName, create);
        } else {
            exchange.removeState();
            exchange.init(volume, treeName, create);
        }
        return exchange;
    }

    /**
     * <p>
     * Returns an <tt>Exchange</tt> for the {@link Tree} specified by treeName
     * within the {@link Volume} specified by <tt>volumeName</tt>. This method
     * optionally creates a new <tt>Tree</tt>. If the <tt>create</tt> parameter
     * is false and a <tt>Tree</tt> by the specified name does not exist, this
     * constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * The <tt>volumeName</tt< you supply must match exactly one open 
     * <tt>Volume</tt>. The name matches if either (a) the <tt>Volume</tt> has
     * an optional alias that is equal to the supplied name, or (b) if the
     * supplied name matches a substring of the <tt>Volume</tt>'s pathname. If
     * there is not unique match for the name you supply, this method throws a
     * {@link com.persistit.exception.VolumeNotFoundException}.
     * </p>
     * <p>
     * This method uses an <tt>Exchange</tt> from an internal pool if one is
     * available; otherwise it creates a new <tt>Exchange</tt>. When the
     * application no longer needs the <tt>Exchange</tt> returned by this
     * method, it should return it to the pool by invoking
     * {@link #releaseExchange} so that it can be reused.
     * </p>
     * 
     * @param volumeName
     *            The volume name that either matches the alias or a partially
     *            matches the pathname of exactly one open <tt>Volume</tt>.
     * 
     * @param treeName
     *            The tree name
     * 
     * @param create
     *            <tt>true</tt> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange getExchange(String volumeName, String treeName,
            boolean create) throws PersistitException {
        Volume volume = getVolume(volumeName);
        if (volume == null)
            throw new VolumeNotFoundException(volumeName);
        return getExchange(volume, treeName, create);
    }

    /**
     * <p>
     * Releases an <tt>Exchange</tt> to the internal pool. A subsequent
     * invocation of {@link #getExchange} will reuse this <tt>Exchange</tt>. An
     * application that gets an <tt>Exchange</tt> through the
     * {@link #getExchange} method <i>should</i> release it through this method.
     * An attempt to release the <tt>Exchange</tt> if it is already in the pool
     * results in an <tt>IllegalStateException</tt>.
     * </p>
     * <p>
     * This method clears all state information in the <tt>Exchange</tt> so that
     * no information residing in the <tt>Exhange</tt> can be obtained by a
     * different, untrusted application.
     * </p>
     * 
     * @param exchange
     *            The <tt>Exchange</tt> to release to the pool. If <tt>null</tt>
     *            , this method returns silently.
     * 
     * @throws IllegalStateException
     */
    public void releaseExchange(Exchange exchange) {
        releaseExchange(exchange, true);
    }

    /**
     * <p>
     * Releases an <tt>Exchange</tt> to the internal pool. A subsequent
     * invocation of {@link #getExchange} will reuse this <tt>Exchange</tt>. An
     * application that gets an <tt>Exchange</tt> through the
     * {@link #getExchange} method <i>should</i> release it through this method.
     * An attempt to release the <tt>Exchange</tt> if it is already in the pool
     * results in an <tt>IllegalStateException</tt>.
     * </p>
     * <p>
     * This method optionally clears all state information in the
     * <tt>Exchange</tt> so that no residual information in the <tt>Exhange</tt>
     * can be obtained by a different, untrusted thread. In a closed
     * configuration in which there is only one application, it is somewhat
     * faster to avoid clearing the byte arrays used in representing the state
     * of this <tt>Exchange</tt> by passing <tt>false</tt> as the value of the
     * <tt>secure</tt> flag.
     * </p>
     * 
     * @param exchange
     *            The <tt>Exchange</tt> to release to the pool. If <tt>null</tt>
     *            , this method returns silently.
     * @param secure
     *            <tt>true</tt> to clear all state information; <tt>false</tt>
     *            to leave the state unchanged.
     * 
     * @throws IllegalStateException
     */
    public void releaseExchange(Exchange exchange, boolean secure) {
        if (exchange != null) {
            synchronized (_exchangePool) {
                exchange.setRelinquished(true);
                exchange.setSecure(secure);

                if (_exchangePool.contains(exchange)) {
                    throw new IllegalStateException(
                            "Exchange is already in the pool");
                }
                if (_exchangePool.size() < MAX_POOLED_EXCHANGES) {
                    _exchangePool.push(exchange);
                }
            }
        }
    }

    /**
     * Get a <code>link java.util.List</code> of all the {@link Volume}s being
     * managed by this Persistit instance. Volumes are specified by the
     * properties used in initializing Persistit.
     * 
     * @return the List
     */
    public Volume[] getVolumes() {
        Volume[] list = new Volume[_volumes.size()];
        for (int index = 0; index < list.length; index++) {
            list[index] = _volumes.get(index);
        }
        return list;
    }

    /**
     * Opens a Volume. The volume must already exist.
     * 
     * @param pathName
     *            The full pathname to the file containing the Volume.
     * @param ro
     *            <tt>true</tt> if the Volume should be opened in read- only
     *            mode so that no updates can be performed against it.
     * @return The Volume.
     * @throws PersistitException
     */
    public Volume openVolume(final String pathName, final boolean ro)
            throws PersistitException {
        return openVolume(pathName, null, 0, ro);
    }

    /**
     * Opens a Volume with a confirming id. If the id value is non-zero, then it
     * must match the id the volume being opened.
     * 
     * @param pathName
     *            The full pathname to the file containing the Volume.
     * 
     * @param alias
     *            A friendly name for this volume that may be used internally by
     *            applications. The alias need not be related to the
     *            <tt>Volume</tt>'s pathname, and typically will denote its
     *            function rather than physical location.
     * 
     * @param id
     *            The internal Volume id value - if non-zero this value must
     *            match the id value stored in the Volume header.
     * 
     * @param ro
     *            <tt>true</tt> if the Volume should be opened in read- only
     *            mode so that no updates can be performed against it.
     * 
     * @return The <tt>Volume</tt>.
     * 
     * @throws PersistitException
     */
    public Volume openVolume(final String pathName, final String alias,
            final long id, final boolean ro) throws PersistitException {
        File file = new File(pathName);
        if (file.exists() && file.isFile()) {
            return Volume.openVolume(this, pathName, alias, id, ro);
        }
        throw new PersistitIOException(new FileNotFoundException(pathName));
    }

    /**
     * Loads and/or creates a volume based on a String-valued specification. The
     * specification has the form: <br />
     * <i>pathname</i>[,<i>options</i>]... <br />
     * where options include: <br />
     * <dl>
     * <dt><tt>alias</tt></dt>
     * <dd>An alias used in looking up the volume by name within Persistit
     * programs (see {@link com.persistit.Persistit#getVolume(String)}). If the
     * alias attribute is not specified, the the Volume's path name is used
     * instead.</dd>
     * <dt><tt>drive<tt></dt>
     * <dd>Name of the drive on which the volume is located. Sepcifying the
     * drive on which each volume is physically located is optional. If
     * supplied, Persistit uses the information to improve I/O throughput in
     * multi-volume configurations by interleaving write operations to different
     * physical drives.</dd>
     * <dt><tt>readOnly</tt></dt>
     * <dd>Open in Read-Only mode. (Incompatible with create mode.)</dd>
     * 
     * <dt><tt>create</tt></dt>
     * <dd>Creates the volume if it does not exist. Requires <tt>bufferSize</tt>, <tt>initialPagesM</tt>, <tt>extensionPages</tt> and
     * <tt>maximumPages</tt> to be specified.</dd>
     * 
     * <dt><tt>createOnly</tt></dt>
     * <dd>Creates the volume, or throw a {@link VolumeAlreadyExistsException}
     * if it already exists.</dd>
     * 
     * <dt><tt>temporary</tt></dt>
     * <dd>Creates the a new, empty volume regardless of whether an existing
     * volume file already exists.</dd>
     * 
     * <dt><tt>id:<i>NNN</i></tt></dt>
     * <dd>Specifies an ID value for the volume. If the volume already exists,
     * this ID value must match the ID that was previously assigned to the
     * volume when it was created. If this volume is being newly created, this
     * becomes its ID number.</dd>
     * 
     * <dt><tt>bufferSize:<i>NNN</i></tt></dt>
     * <dd>Specifies <i>NNN</i> as the volume's buffer size when creating a new
     * volume. <i>NNN</i> must be 1024, 2048, 4096, 8192 or 16384</dd>.
     * 
     * <dt><tt>initialPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the initial number of pages to be allocated when this
     * volume is first created.</dd>
     * 
     * <dt><tt>extensionPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the number of pages by which to extend the volume when
     * more pages are required.</dd>
     * 
     * <dt><tt>maximumPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the maximum number of pages to which this volume can
     * extend.</dd>
     * 
     * </dl>
     * 
     * 
     * @param volumeSpec
     *            Volume specification
     * 
     * @return The <tt>Volume</tt>
     * 
     * @throws PersistitException
     */
    public Volume loadVolume(final String volumeSpec) throws PersistitException {
        return Volume.loadVolume(this, new VolumeSpecification(
                substituteProperties(volumeSpec, _properties, 0)));
    }

    public boolean deleteVolume(final String volumeName)
            throws PersistitException {
        final Volume volume = getVolume(volumeName);
        if (volume == null) {
            return false;
        } else {
            removeVolume(volume, true);
            new File(volume.getPath()).delete();
            return true;
        }
    }

    /**
     * Returns an implementation of the <tt>Management</tt> interface. This
     * implementation is a singleton; the first invocation of this method will
     * create an instance; subsequent invocations will return the same instance.
     * 
     * @return the singleton implementation of a <tt>Management</tt> from which
     *         system management services can be obtained.
     */
    public synchronized Management getManagement() {
        if (_management == null) {
            _management = new ManagementImpl(this);
        }
        return _management;
    }

    /**
     * Returns the copyright notice for this product
     * 
     * @return The copyright notice
     */
    public static String copyright() {
        return COPYRIGHT;
    }

    /**
     * Returns the version identifier for this version of Persistit&trade;
     * 
     * @return The version identifier
     */
    public static String version() {
        return VERSION;
    }

    /**
     * The time at which the log was started.
     * 
     * @return The time in milliseconds
     */
    public long startTime() {
        return _startTime;
    }

    /**
     * The number of milliseconds since the log was opened.
     * 
     * @return The elapsed time interval in milliseconds
     */
    public long elapsedTime() {
        return System.currentTimeMillis() - _startTime;
    }

    /**
     * Looks up a {@link Volume} by id. At creation time, each <tt>Volume</tt>
     * is assigned a unique long ID value.
     * 
     * @param id
     * @return the <tt>Volume</tt>, or <i>null</i> if there is no open
     *         <tt>Volume</tt> having the supplied ID value.
     */
    public Volume getVolume(long id) {
        return _volumesById.get(new Long(id));
    }

    /**
     * <p>
     * Looks up a {@link Volume} by name or path. The supplied name must match
     * only one of the open volumes. If it matches none of the volumes, or if
     * there are multiple volumes with matching names, then this method returns
     * <tt>null</tt>.
     * </p>
     * <p>
     * The supplied name can match a volume in one of two ways:
     * <ul>
     * <li>(a) its name by exact match</li>
     * <li>(b) its path, by matching the canonical forms of the volume's path
     * and the supplied path.</li>
     * </ul>
     * </p>
     * 
     * @param name
     *            Name that identifies a volume by matching either its alias (if
     *            it has one) or a substring of its file name.
     * 
     * @return the <tt>Volume</tt>, or <i>null</i> if there is no unique open
     *         Volume that matches the supplied <tt>partialName</tt>.
     */
    public Volume getVolume(String name) {
        if (name == null) {
            throw new NullPointerException("Null volume name");
        }
        Volume result = null;

        for (int i = 0; i < _volumes.size(); i++) {
            Volume vol = _volumes.get(i);
            if (name.equals(vol.getName())) {
                if (result == null)
                    result = vol;
                else {
                    return null;
                }
            }
        }
        if (result != null) {
            return result;
        }

        final File file = new File(name);
        for (int i = 0; i < _volumes.size(); i++) {
            Volume vol = _volumes.get(i);
            if (file.equals(new File(vol.getPath()))) {
                if (result == null)
                    result = vol;
                else {
                    return null;
                }
            }
        }
        return result;
    }

    /**
     * <p>
     * Returns the designated system volume. The system volume contains the
     * class index and other structural information. It is specified by the
     * <tt>sysvolume</tt> property with a default value of "_system".
     * </p>
     * <p>
     * This method handles a configuration with exactly one volume in a special
     * way. If the <tt>sysvolume</tt> property is unspecified and there is
     * exactly one volume, then this method returns that volume volume as the
     * system volume even if its name does not match the default
     * <tt>sysvolume</tt> property. This eliminates the need to specify a system
     * volume property for configurations having only one volume.
     * </p>
     * 
     * @return the <tt>Volume</tt>
     * @throws VolumeNotFoundException
     *             if the volume was not found
     */
    public Volume getSystemVolume() throws VolumeNotFoundException {
        return getSpecialVolume(SYSTEM_VOLUME_PROPERTY,
                DEFAULT_SYSTEM_VOLUME_NAME);
    }

    /**
     * <p>
     * Returns the designated transaction volume. The transaction volume is used
     * to transiently hold pending updates prior to transaction commit. It is
     * specified by the <tt>txnvolume</tt> property with a default value of
     * "_txn".
     * </p>
     * <p>
     * This method handles a configuration with exactly one volume in a special
     * way. If the <tt>txnvolume</tt> property is unspecified and there is
     * exactly one volume, then this method returns that volume as the
     * transaction volume even if its name does not match the default
     * <tt>txnvolume</tt> property. This eliminates the need to specify a
     * transaction volume property for configurations having only one volume.
     * </p>
     * 
     * @return the <tt>Volume</tt>
     * @throws VolumeNotFoundException
     *             if the volume was not found
     */
    public Volume getTransactionVolume() throws VolumeNotFoundException {
        return getSpecialVolume(TXN_VOLUME_PROPERTY, DEFAULT_TXN_VOLUME_NAME);
    }

    /**
     * Returns the default timeout for operations on an <tt>Exchange</tt>. The
     * application may override this default value for an instance of an
     * <tt>Exchange</tt> through the {@link Exchange#setTimeout(long)} method.
     * The default timeout may be specified through the
     * <tt>com.persistit.defaultTimeout</tt> property.
     * 
     * @return The default timeout value, in milliseconds.
     */
    public long getDefaultTimeout() {
        return _defaultTimeout;
    }

    /**
     * Indicates whether this instance has been initialized.
     * 
     * @return <tt>true</tt> if this Persistit has been initialized.
     */
    public boolean isInitialized() {
        return _initialized.get();
    }

    /**
     * Indicates whether this instance of Persistit has been closed.
     * 
     * @return <tt>true</tt> if Persistit has been closed.
     */
    public boolean isClosed() {
        return _closed.get();
    }

    /**
     * Indicates whether Persistit will retry read any operation that fails due
     * to an IOException. In many cases, an IOException occurs due to transient
     * conditions, such as a file being locked by a backup program. When this
     * property is <tt>true</tt>, Persistit will repeatedly retry the read
     * operation until the timeout value for the current operation expires. By
     * default this property is <tt>true</tt>. Use the com.persistit.readretry
     * property to disable it.
     * 
     * @return <tt>true</tt> to retry a read operation that fails due to an
     *         IOException.
     */
    public boolean isReadRetryEnabled() {
        return _readRetryEnabled;
    }

    public synchronized Checkpoint getCurrentCheckpoint() {
        return _currentCheckpoint;
    }

    /**
     * Force a new Checkpoint and wait for it to be written. If Persistit is
     * closed or not yet initialized, do nothing and return <tt>null</tt>.
     * 
     * @return the Checkpoint allocated by this process.
     */
    public Checkpoint checkpoint() {
        if (_closed.get() || !_initialized.get()) {
            return null;
        }
        final Checkpoint checkpoint = getTimestampAllocator().forceCheckpoint();
        applyCheckpoint(checkpoint);
        while (true) {
            synchronized (this) {
                if (!_outstandingCheckpoints.contains(checkpoint)) {
                    return checkpoint;
                }
            }
            flushCheckpoint();
            try {
                Thread.sleep(SHORT_DELAY);
            } catch (InterruptedException ie) {
                return null;
            }
        }
    }

    void applyCheckpoint() {
        final Checkpoint newCheckpoint = getTimestampAllocator()
                .updateCheckpoint();
        applyCheckpoint(newCheckpoint);

    }

    void applyCheckpoint(final Checkpoint newCheckpoint) {
        synchronized (this) {
            if (newCheckpoint.getTimestamp() > _currentCheckpoint
                    .getTimestamp()) {
                _outstandingCheckpoints.add(newCheckpoint);
                _currentCheckpoint = newCheckpoint;
                if (getLogBase().isLoggable(LogBase.LOG_CHECKPOINT_PROPOSED)) {
                    getLogBase().log(LogBase.LOG_CHECKPOINT_PROPOSED,
                            newCheckpoint);
                }
                // Now attempt to complete a recent Checkpoint -
                // which is not the one just added to the outstanding
                // checkpoints list, but probably the previous one.
                flushCheckpoint();
            }
        }
    }

    Checkpoint flushCheckpoint() {
        final Checkpoint validCheckpoint = findValidCheckpoint(_outstandingCheckpoints);
        if (validCheckpoint != null) {
            try {
                getJournalManager().writeCheckpointToJournal(validCheckpoint);
            } catch (PersistitIOException e) {
                getLogBase().log(LogBase.LOG_EXCEPTION,
                        e + " while writing " + validCheckpoint + ":" + e);
            }
            synchronized (this) {
                _outstandingCheckpoints.remove(validCheckpoint);
            }
        }
        return validCheckpoint;
    }

    /**
     * Propose a new Checkpoint and update the journal to include all pages made
     * dirty before that Checkpoint.
     * 
     * @param newCheckpoint
     * @return the latest valid checkpoint
     */
    public Checkpoint checkpoint(final Checkpoint newCheckpoint) {
        final Checkpoint validCheckpoint;
        synchronized (this) {
            if (newCheckpoint.getTimestamp() > _currentCheckpoint
                    .getTimestamp()) {
                _outstandingCheckpoints.add(newCheckpoint);
                _currentCheckpoint = newCheckpoint;
                if (getLogBase().isLoggable(LogBase.LOG_CHECKPOINT_PROPOSED)) {
                    getLogBase().log(LogBase.LOG_CHECKPOINT_PROPOSED,
                            newCheckpoint);
                }
            }
        }
        validCheckpoint = findValidCheckpoint(_outstandingCheckpoints);
        if (validCheckpoint != null) {
            try {
                getJournalManager().writeCheckpointToJournal(validCheckpoint);
            } catch (PersistitIOException e) {
                getLogBase().log(LogBase.LOG_EXCEPTION,
                        e + " while writing " + validCheckpoint + ":" + e);
                return null;
            }
        }
        return validCheckpoint;
    }

    /**
     * Given List of outstanding Checkpoints, finds the latest one that is safe
     * to write and returns it. If there are none, return <tt>null</tt>
     * 
     * @param List
     *            of outstanding Checkpoint instances
     * @return The latest Checkpoint from the list that can be written, or
     *         <tt>null</tt> if there are none.
     */
    private Checkpoint findValidCheckpoint(
            final List<Checkpoint> outstandingCheckpoints) {
        long earliestDirtyTimestamp = Long.MAX_VALUE;
        for (final BufferPool pool : _bufferPoolTable.values()) {
            earliestDirtyTimestamp = Math.min(earliestDirtyTimestamp,
                    pool.earliestDirtyTimestamp());
        }
        for (int index = outstandingCheckpoints.size(); --index >= 0;) {
            final Checkpoint checkpoint = outstandingCheckpoints.get(index);
            if (checkpoint.getTimestamp() <= earliestDirtyTimestamp) {
                for (int k = index; k >= 0; --k) {
                    outstandingCheckpoints.remove(k);
                }
                return checkpoint;
            }
        }
        return null;
    }

    /**
     * Copy back all pages from the journal to their host Volumes.
     * 
     * @throws Exception
     */
    public void copyBackPages() throws Exception {
        _journalManager.copyBack(Long.MAX_VALUE);
    }

    /**
     * Looks up a volume by name.
     * 
     * @param name
     *            The name
     * @return the Volume
     * @throws VolumeNotFoundException
     *             if the volume was not found
     */
    private Volume getSpecialVolume(String propName, String dflt)
            throws VolumeNotFoundException {
        String volumeName = getProperty(propName, dflt);

        Volume volume = getVolume(volumeName);
        if (volume == null) {
            if ((_volumes.size() == 1) && (volumeName == dflt)) {
                volume = _volumes.get(0);
            } else {
                throw new VolumeNotFoundException(volumeName);
            }
        }
        return volume;
    }

    /**
     * @param size
     *            the desired buffer size
     * @return the <tt>BufferPool</tt> for the specific buffer size
     */
    BufferPool getBufferPool(int size) {
        return _bufferPoolTable.get(new Integer(size));
    }

    /**
     * @return A HashMap containing all the <tt>BufferPool</tt>s keyed by their
     *         size.
     */
    HashMap<Integer, BufferPool> getBufferPoolHashMap() {
        return _bufferPoolTable;
    }

    /**
     * <p>
     * Close the Persistit Log and all {@link Volume}s. This method is
     * equivalent to {@link #close(boolean) close(true)}.
     * 
     * @throws PersistitException
     * @throws IOException
     * @throws PersistitException
     * @throws IOException
     * @return <tt>true</tt> if Persistit was initialized and this invocation
     *         closed it, otherwise false.
     */
    public void close() throws PersistitException {
        close(true, false);
    }

    /**
     * <p>
     * Close the Persistit Log and all {@link Volume}s. This method does nothing
     * and returns <tt>false</tt> if Persistit is currently not in the
     * initialized state. This method is threadsafe; if multiple threads
     * concurrently attempt to close Persistit, only one close operation will
     * actually take effect.
     * </p>
     * <p>
     * The <tt>flush</tt> determines whether this method will pause to flush all
     * pending updates to disk before shutting down the system. If
     * <tt>flush</tt> is <tt>true</tt> and many updated pages need to be
     * written, the shutdown process may take a significant amount of time.
     * However, upon restarting the system, all updates initiated before the
     * call to this method will be reflected in the B-Tree database. This is the
     * normal mode of operation.
     * </p>
     * <p>
     * When <tt>flush</tt> is false this method returns quickly, but without
     * writing remaining dirty pages to disk. The result after restarting
     * Persistit will be valid, internally consistent B-Trees; however, recently
     * applied updates may be missing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until you close Persistit. This is to ensure that all pending
     * updates are written before the JVM exit. Therefore the recommended
     * pattern for initializing, using and then closing Persistit is:
     * <code><pre>
     *   try
     *   {
     *      Persistit.initialize();
     *      ... do work
     *   }
     *   finally
     *   {
     *      Persisit.close();
     *   }
     * </pre></code> This pattern ensures that Persistit is closed properly and
     * all threads terminated even if the application code throws an exception
     * or error.
     * </p>
     * VolumeClosedException.
     * 
     * @param flush
     *            <tt>true</tt> to ensure all dirty pages are written to disk
     *            before shutdown completes; <tt>false</tt> to enable fast (but
     *            incomplete) shutdown.
     * 
     * @throws PersistitException
     * @throws IOException
     * @throws PersistitException
     * @throws IOException
     * @return <tt>true</tt> if Persistit was initialized and this invocation
     *         closed it, otherwise false.
     */
    public void close(final boolean flush) throws PersistitException {
        close(flush, false);
    }

    private void close(final boolean flush, final boolean byHook)
            throws PersistitException {
        if (_closed.get() || !_initialized.get()) {
            return;
        }
        synchronized (this) {
            // Wait for UI to go down.
            while (!byHook && _suspendShutdown.get()) {
                try {
                    wait(SHORT_DELAY);
                } catch (InterruptedException ie) {
                }
            }
        }

        if (byHook) {
            shutdownGUI();
        }

        flush();

        _closed.set(true);

        final List<Volume> volumes;
        synchronized (this) {
            volumes = new ArrayList<Volume>(_volumes);
        }

        for (final Volume volume : volumes) {
            volume.close();
        }

        if (flush) {
            _timestampAllocator.forceCheckpoint();
        }

        for (final BufferPool pool : _bufferPoolTable.values()) {
            pool.close(flush);
        }

        _journalManager.close();

        while (!_volumes.isEmpty()) {
            removeVolume(_volumes.get(0), false);
        }

        for (final BufferPool pool : _bufferPoolTable.values()) {
            int count = pool.countDirty(null);
            if (count > 0) {
                _logBase.log(LogBase.LOG_STRANDED, pool, count);
            }
        }

        _logBase.logend();
        _volumes.clear();
        _volumesById.clear();
        _bufferPoolTable.clear();
        _transactionThreadLocal.set(null);
        _waitingThreadLocal.set(null);

        if (_management != null) {
            unregisterMXBean();
            _management.unregister();
            _management = null;
        }

    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the writer and collector
     * processes. This method should be used only by tests.
     */
    public void crash() {
        final JournalManager journalManager = _journalManager;
        if (journalManager != null) {
            journalManager.crash();
        }
        final Map<Integer, BufferPool> buffers = _bufferPoolTable;
        if (buffers != null) {
            for (final BufferPool pool : buffers.values()) {
                pool.crash();
            }
        }
    }

    /**
     * Write all pending updates to the underlying OS file system. This
     * operation runs asynchronously with other threads performing updates. Upon
     * successful completion, this method ensures that all updates performed
     * prior to calling flush() (except for those performed within as-yet
     * uncommitted transactions) will be written; however, some updates
     * performed by other threads subsequent to calling flush() may also be
     * written.
     * 
     * @return <i>true</i> if any file writes were performed, else <i>false</i>.
     * @throws PersistitException
     * @throws IOException
     */
    public boolean flush() throws PersistitException {
        boolean okay = true;
        if (_closed.get() || !_initialized.get()) {
            return false;
        }

        for (final Volume volume : _volumes) {
            volume.flush();
        }

        for (final BufferPool pool : _bufferPoolTable.values()) {
            if (pool != null) {
                okay &= pool.flush() == 0;
            }
        }

        _journalManager.force();
        return true;
    }

    void waitForIOTaskStop(final IOTaskRunnable task) {
        if (_beginCloseTime == 0) {
            _beginCloseTime = System.currentTimeMillis();
            _nextCloseTime = _beginCloseTime + CLOSE_LOG_INTERVAL;
        }
        task.kick();
        while (!task.isStopped()) {
            synchronized (this) {
                try {
                    wait(SHORT_DELAY);
                } catch (InterruptedException ie) {
                    break;
                }
            }
            final long now = System.currentTimeMillis();
            if (now > _nextCloseTime) {
                _logBase.log(LogBase.LOG_WAIT_FOR_CLOSE,
                        (_nextCloseTime - _beginCloseTime) / 1000);
                _nextCloseTime += CLOSE_LOG_INTERVAL;
            }
        }
    }

    /**
     * Request OS-level file synchronization for all open files managed by
     * Persistit. An application may call this method after {@link #flush} to
     * ensure (within the capabilities of the host operating system) that all
     * database updates have actually been written to disk.
     * 
     * @throws IOException
     */
    public void sync() throws PersistitIOException {
        if (_closed.get() || !_initialized.get()) {
            return;
        }
        final ArrayList<Volume> volumes = _volumes;

        for (int index = 0; index < volumes.size(); index++) {
            Volume volume = volumes.get(index);
            if (!volume.isReadOnly()) {
                try {
                    volume.sync();
                } catch (ReadOnlyVolumeException rove) {
                    // ignore, because it can't happen
                }
            }
        }
        _journalManager.force();
    }

    /**
     * Waits until updates are no longer suspended. The
     * {@link #setUpdateSuspended} method controls whether update operations are
     * currently suspended.
     */
    public void suspend() {
        while (isUpdateSuspended()) {
            try {
                Thread.sleep(SHORT_DELAY);
            } catch (InterruptedException ie) {
                break;
            }
        }
    }

    /**
     * Gets the <tt>Transaction</tt> object for the current thread. The
     * <tt>Transaction</tt> object lasts for the life of the thread. See
     * {@link com.persistit.Transaction} for more information on how to use
     * Persistit's transaction facilities.
     * 
     * @return This thread <tt>Transaction</tt> object.
     */
    public Transaction getTransaction() {
        Transaction txn = _transactionThreadLocal.get();
        if (txn == null) {
            txn = new Transaction(this);
            _transactionThreadLocal.set(txn);
        }
        return txn;
    }

    /**
     * Returns the <code>java.awt.Container</code> object that contains the
     * diagnostic GUI, if it is open. Otherwise this method returns <i>null</i>.
     * The caller must cast the returned Object to Container. Persistit is
     * designed to avoid loading Swing or AWT classes in the event no GUI is
     * desired in order to minimize memory usage and startup time.
     * 
     * @return an Object that can be cast to <code>java.awt.Container</code>, or
     *         <i>null</i> if no diagnostic UI is open.
     */
    public Object getPersistitGuiContainer() {
        return _localGUI;
    }

    /**
     * Sets the {@link com.persistit.encoding.CoderManager} that will supply
     * instances of {@link com.persistit.encoding.ValueCoder} and
     * {@link com.persistit.encoding.KeyCoder}.
     * 
     * @param coderManager
     */
    public synchronized void setCoderManager(CoderManager coderManager) {
        _coderManager = coderManager;
    }

    /**
     * Returns the current CoderManager.
     * 
     * @return The current {@link com.persistit.encoding.CoderManager}.
     */
    public synchronized CoderManager getCoderManager() {
        return _coderManager;
    }

    public LogBase getLogBase() {
        return _logBase;
    }

    ClassIndex getClassIndex() {
        return _classIndex;
    }

    Class classForHandle(int handle) {
        ClassInfo ci = _classIndex.lookupByHandle(handle);
        if (ci == null)
            return null;
        else
            return ci.getDescribedClass();
    }

    KeyCoder lookupKeyCoder(Class cl) {
        if (_coderManager == null)
            return null;
        return _coderManager.lookupKeyCoder(cl);
    }

    ValueCoder lookupValueCoder(Class cl) {
        if (_coderManager == null)
            return null;
        return _coderManager.lookupValueCoder(cl);
    }

    public RecoveryManager getRecoveryManager() {
        return _recoveryManager;
    }

    public JournalManager getJournalManager() {
        return _journalManager;
    }

    TimestampAllocator getTimestampAllocator() {
        return _timestampAllocator;
    }

    IOMeter getIOMeter() {
        return _ioMeter;
    }

    ThreadLocal<WaitingThread> getWaitingThreadThreadLocal() {
        return _waitingThreadLocal;
    }

    ThreadLocal<Throttle> getThrottleThreadLocal() {
        return _throttleThreadLocal;
    }

    SharedResource getTransactionResourceA() {
        return _transactionResourceA;
    }

    SharedResource getTransactionResourceB() {
        return _transactionResourceB;
    }

    LockManager getLockManager() {
        return _lockManager;
    }

    /**
     * Replaces the current logger implementation.
     * 
     * @see com.persistit.logging.AbstractPersistitLogger
     * @see com.persistit.logging.JDK14LoggingAdapter
     * @see com.persistit.logging.Log4JAdapter
     * @param logger
     *            The new logger implementation
     */
    public void setPersistitLogger(AbstractPersistitLogger logger) {
        _logger = logger;
    }

    /**
     * @return The current logger.
     */
    public AbstractPersistitLogger getPersistitLogger() {
        if (_logger == null)
            _logger = new DefaultPersistitLogger(getProperty(LOGFILE_PROPERTY));
        return _logger;
    }

    /**
     * Convenience method that performs an integrity check on all open
     * <tt>Volume</tt>s and reports detailed results to
     * {@link java.lang.System#out}.
     * 
     * @throws PersistitException
     */
    public void checkAllVolumes() throws PersistitException {
        IntegrityCheck icheck = new IntegrityCheck(this);
        for (int index = 0; index < _volumes.size(); index++) {
            Volume volume = _volumes.get(index);
            System.out.println("Checking " + volume + " ");
            try {
                icheck.checkVolume(volume);
            } catch (Exception e) {
                System.out.println(e + " while performing IntegrityCheck on "
                        + volume);
            }
        }
        System.out.println("  " + icheck.toString(true));
    }

    /**
     * Parses a property value as a long integer. Permits suffix values of "K"
     * for Kilo- and "M" for Mega-, "G" for Giga- and "T" for Tera-. For
     * example, the supplied value of "100K" yields a parsed result of 102400.
     * 
     * @param propName
     *            Name of the property, used in formating the Exception if the
     *            value is invalid.
     * @param dflt
     *            The default value.
     * @param min
     *            Minimum permissible value
     * @param max
     *            Maximum permissible value
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation,
     *             or is outside the supplied bounds.
     */
    long getLongProperty(String propName, long dflt, long min, long max) {
        String str = getProperty(propName);
        if (str == null)
            return dflt;
        return parseLongProperty(propName, str, min, max);
    }

    /**
     * Parses a string as a long integer. Permits suffix values of "K" for Kilo-
     * and "M" for Mega-, "G" for Giga- and "T" for Tera-. For example, the
     * supplied value of "100K" yields a parsed result of 102400.
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
     * @return The numeric value of the supplied String, as a long.
     * @throws IllegalArgumentException
     *             if the supplied String is not a valid integer representation,
     *             or is outside the supplied bounds.
     */
    static long parseLongProperty(String propName, String str, long min,
            long max) {
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
        boolean invalid = false;
        if (multiplier > 1) {
            sstr = str.substring(0, str.length() - 1);
        }

        try {
            result = Long.parseLong(sstr) * multiplier;
        }

        catch (NumberFormatException nfe) {
            invalid = true;
        }
        if (result < min || result > max || invalid) {
            throw new IllegalArgumentException("Value '" + str
                    + "' of property " + propName + " is invalid");
        }
        return result;
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
        throw new IllegalArgumentException("Value '" + str + "' of property "
                + propName + " must be " + " either \"true\" or \"false\"");
    }

    /**
     * Attemps to open the diagnostic GUI that displays some useful information
     * about Persistit's internal state. If the UI has already been opened, this
     * method merely sets the shutdown suspend flag.
     * 
     * @param suspendShutdown
     *            If <tt>true</tt>, sets the shutdown suspend flag. Setting this
     *            flag suspends the {@link #close} method to permit continued
     *            use of the diagnostic GUI.
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void setupGUI(boolean suspendShutdown)
            throws IllegalAccessException, InstantiationException,
            ClassNotFoundException, RemoteException {
        if (_localGUI == null) {
            if (_logBase.isLoggable(LogBase.LOG_INIT_CREATE_GUI)) {
                _logBase.log(LogBase.LOG_INIT_CREATE_GUI);
            }
            _localGUI = (UtilControl) (Class.forName(PERSISTIT_GUI_CLASS_NAME))
                    .newInstance();
        }
        _localGUI.setManagement(getManagement());
        _suspendShutdown.set(suspendShutdown);
    }

    /**
     * Closes the diagnostic GUI if it previously has been opened. Otherwise
     * this method does nothing.
     */
    public void shutdownGUI() {
        final UtilControl localGUI;
        synchronized (this) {
            localGUI = _localGUI;
            _suspendShutdown.set(false);
            _localGUI = null;
        }
        if (localGUI != null) {
            localGUI.close();
        }
    }

    /**
     * Indicates whether Persistit will suspend its shutdown activities on
     * invocation of {@link #close}. This flag is intended for use by management
     * tools that need to keep Persistit open even when the application has
     * requested it to close so that the final state of the Persistit
     * environment can be examined.
     * 
     * @return <tt>true</tt> if Persistit will wait when attempting to close;
     *         <tt>false</tt> if the <tt>close</tt> operation will not be
     *         suspended.
     */
    public boolean isShutdownSuspended() {
        return _suspendShutdown.get();
    }

    /**
     * Determines whether Persistit will suspend its shutdown activities on
     * invocation of {@link #close}. This flag is intended for use by management
     * tools that need to keep Persistit open even when the application has
     * requested it to close so that the final state of the Persistit
     * environment can be examined.
     * 
     * @param suspended
     *            <tt>true</tt> to specify that Persistit will wait when
     *            attempting to close; otherwise <tt>false</tt>.
     */
    public void setShutdownSuspended(boolean suspended) {
        _suspendShutdown.set(suspended);
    }

    /**
     * Indicates whether Persistit is suspending all updates. When set, this
     * property will cause all updates to be suspended until the property is
     * cleared. This capability is intended primarily for diagnostic and
     * management support.
     * 
     * @return <tt>true</tt> if all updates are suspended; otherwise
     *         <tt>false</tt>.
     */
    public boolean isUpdateSuspended() {
        return _suspendUpdates.get();
    }

    /**
     * Controls whether Persistit will suspend all Threads that attempt to
     * update any Volume. When set, this property will cause all updates to be
     * suspended until the property is cleared. This capability is intended
     * primarily for diagnostic support and management support.
     * 
     * @param suspended
     *            <tt>true</tt> to suspend all updates; <tt>false</tt> to enable
     *            updates.
     */
    public synchronized void setUpdateSuspended(boolean suspended) {
        _suspendUpdates.set(suspended);
        if (Debug.ENABLED && !suspended)
            Debug.setSuspended(false);
    }

    private static class Throttle {
        private long _throttleCount;
    }

    private Throttle getThrottle() {
        ThreadLocal<Throttle> throttleThreadLocal = getThrottleThreadLocal();
        Throttle throttle = throttleThreadLocal.get();
        if (throttle == null) {
            throttle = new Throttle();
            throttleThreadLocal.set(throttle);
        }
        return throttle;
    }

    /**
     * Initializes Persistit using a property file path supplied as the first
     * argument, or if no arguments are supplied, the default property file
     * name. As a side-effect, this method will apply any uncommitted updates
     * from the prewrite journal. As a side-effect, this method will also open
     * the diagnostic UI if requested by system property or property value in
     * the property file.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        boolean gui = false;
        boolean icheck = false;
        boolean wait = false;

        String propertiesFileName = null;
        for (int index = 0; index < args.length; index++) {
            String s = args[index];
            if (s.startsWith("?") || s.startsWith("-?") || s.startsWith("-h")
                    || s.startsWith("-H")) {
                usage();
                return;
            }
            if (s.equalsIgnoreCase("-g"))
                gui = true;
            else if (s.equalsIgnoreCase("-i"))
                icheck = true;
            else if (s.equalsIgnoreCase("-w"))
                wait = true;

            else if (!s.startsWith("-") && propertiesFileName == null) {
                propertiesFileName = s;
            } else {
                usage();
                return;
            }
        }
        Persistit persistit = new Persistit();
        persistit.initialize(propertiesFileName);
        try {
            if (gui) {
                persistit.setupGUI(wait);
            }
            if (icheck) {
                persistit.checkAllVolumes();
            }
            if (wait) {
                persistit.setShutdownSuspended(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            persistit.setShutdownSuspended(false);
        } finally {
            persistit.close();
        }
    }

    private static void usage() {
        for (int index = 0; index < USAGE.length; index++) {
            System.out.println(USAGE[index]);
        }
        System.out.println();
    }
}
