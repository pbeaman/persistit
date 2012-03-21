/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.rmi.RemoteException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.persistit.Accumulator.AccumulatorRef;
import com.persistit.CheckpointManager.Checkpoint;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.PersistitClosedException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.PropertiesNotFoundException;
import com.persistit.exception.TestException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.logging.DefaultPersistitLogger;
import com.persistit.logging.LogBase;
import com.persistit.logging.PersistitLogger;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.util.ArgParser;
import com.persistit.util.Debug;
import com.persistit.util.Util;
import com.persistit.util.UtilControl;

/**
 * <p>
 * Creates and manages the the runtime environment for a Persistit&trade;
 * database. To use <code>Persistit</code>, an application invokes one of the
 * static {@link #initialize} methods to load a set of properties that govern
 * Persistit's behavior, and to initialize its memory structures. When
 * terminating, the application should invoke the static {@link #close} method
 * to complete all database writes, close all files and relinquish all buffer
 * memory.
 * </p>
 * <p>
 * Once initialized, there is a single <code>Persistit</code> instance available
 * from the {@link #getInstance} method. Various non-static methods are
 * available on this instance. The {@link #close} method releases the
 * <code>Persistit</code> instance, allowing its memory to be released.
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
public class Persistit {
    /**
     * This version of Persistit
     */
    public final static String VERSION = GetVersion.getVersionString() + (Debug.ENABLED ? "-DEBUG" : "");
    /**
     * The copyright notice
     */
    public final static String COPYRIGHT = "Copyright (c) 2011 Akiban Technologies Inc.";

    /**
     * Determines whether multi-byte integers will be written in little- or
     * big-endian format. This constant is <code>true</code> in all current
     * builds.
     */
    public final static boolean BIG_ENDIAN = true;
    /**
     * Prefix used to form the a system property name. For example, the property
     * named <code>journalpath=xyz</code> can also be specified as a system
     * property on the command line with the option
     * -Dcom.persistit.journalpath=xyz.
     */
    public final static String SYSTEM_PROPERTY_PREFIX = "com.persistit.";
    /**
     * Name of utility GUI class.
     */
    private final static String PERSISTIT_GUI_CLASS_NAME = SYSTEM_PROPERTY_PREFIX + "ui.AdminUI";

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
    public final static String SYSTEM_VOLUME_PROPERTY = "sysvolume";

    /**
     * Property name for specifying default temporary volume page size
     */
    public final static String TEMPORARY_VOLUME_PAGE_SIZE_NAME = "tvpagesize";

    /**
     * Property name for specifying default temporary volume directory
     */
    public final static String TEMPORARY_VOLUME_DIR_NAME = "tmpvoldir";

    /**
     * Property name for specifying upper bound on temporary volume size
     */
    public final static String TEMPORARY_VOLUME_MAX_SIZE = "tmpvolmaxsize";

    /**
     * Property name for specifying a transaction volume
     */
    public final static String TXN_VOLUME_NAME_PROPERTY = "txnvolume";

    /**
     * Property name for specifying whether Persistit should display diagnostic
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
     * as <code>true</code>, Serializable classes will be instantiated through
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
     * Name of port on which RMI server accepts connects. If zero or unassigned,
     * RMI picks a random port. Specifying a port can be helpful when using SSH
     * to tunnel RMI to a server.
     */
    public final static String RMI_SERVER_PORT = "rmiserverport";

    /**
     * Property name for enabling Persistit Open MBean for JMX
     */
    public final static String JMX_PARAMS = "jmx";

    /**
     * Property name for pseudo-property "timestamp";
     */
    public final static String TIMESTAMP_PROPERTY = "timestamp";

    /**
     * Property name for the "append only" property.
     */
    public final static String APPEND_ONLY_PROPERTY = "appendonly";

    public final static String SPLIT_POLICY_PROPERTY = "splitpolicy";

    public final static String JOIN_POLICY_PROPERTY = "joinpolicy";

    /**
     * Maximum number of Exchanges that will be held in an internal pool.
     */
    public final static int MAX_POOLED_EXCHANGES = 10000;

    private final static int TRANSACTION_INDEX_SIZE = 512;

    final static long SHORT_DELAY = 500;

    private final static long KILO = 1024;
    private final static long MEGA = KILO * KILO;
    private final static long GIGA = MEGA * KILO;
    private final static long TERA = GIGA * KILO;

    private final static long CLOSE_LOG_INTERVAL = 30000000000L; // 30 sec

    private final static int ACCUMULATOR_CHECKPOINT_THRESHOLD = 256;

    private final static SplitPolicy DEFAULT_SPLIT_POLICY = SplitPolicy.PACK_BIAS;
    private final static JoinPolicy DEFAULT_JOIN_POLICY = JoinPolicy.EVEN_BIAS;

    private final static int MAX_FATAL_ERROR_MESSAGES = 10;

    public static class FatalErrorException extends RuntimeException {

        final String _threadName = Thread.currentThread().getName();
        final long _systemTime = System.currentTimeMillis();

        private FatalErrorException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private final long _availableHeap = availableHeap();

    private PersistitLogger _logger;

    /**
     * Start time
     */
    private final long _startTime = System.currentTimeMillis();
    private final HashMap<Integer, BufferPool> _bufferPoolTable = new HashMap<Integer, BufferPool>();
    private final ArrayList<Volume> _volumes = new ArrayList<Volume>();
    private Properties _properties = new Properties();

    private AtomicBoolean _initialized = new AtomicBoolean();
    private AtomicBoolean _closed = new AtomicBoolean();
    private AtomicBoolean _fatal = new AtomicBoolean();

    private long _beginCloseTime;
    private long _nextCloseTime;

    private final LogBase _logBase = new LogBase();

    private AtomicBoolean _suspendShutdown = new AtomicBoolean(false);
    private AtomicBoolean _suspendUpdates = new AtomicBoolean(false);

    private UtilControl _localGUI;

    private AtomicReference<CoderManager> _coderManager = new AtomicReference<CoderManager>();

    private ClassIndex _classIndex = new ClassIndex(this);

    private final ThreadLocal<SessionId> _sessionIdThreadLocal = new ThreadLocal<SessionId>() {
        @Override
        protected SessionId initialValue() {
            return new SessionId();
        }
    };

    private final Map<SessionId, Transaction> _transactionSessionMap = new HashMap<SessionId, Transaction>();

    private ManagementImpl _management;

    private final RecoveryManager _recoveryManager = new RecoveryManager(this);

    private final JournalManager _journalManager = new JournalManager(this);

    private final TimestampAllocator _timestampAllocator = new TimestampAllocator();

    private final CheckpointManager _checkpointManager = new CheckpointManager(this);

    private final CleanupManager _cleanupManager = new CleanupManager(this);

    private final IOMeter _ioMeter = new IOMeter();

    private TransactionIndex _transactionIndex = new TransactionIndex(_timestampAllocator, TRANSACTION_INDEX_SIZE);

    private Map<SessionId, List<Exchange>> _exchangePoolMap = new WeakHashMap<SessionId, List<Exchange>>();

    private boolean _readRetryEnabled;

    private long _defaultTimeout;

    private final Set<AccumulatorRef> _accumulators = new HashSet<AccumulatorRef>();

    private final WeakHashMap<SessionId, CLI> _cliSessionMap = new WeakHashMap<SessionId, CLI>();

    private SplitPolicy _defaultSplitPolicy = DEFAULT_SPLIT_POLICY;

    private JoinPolicy _defaultJoinPolicy = DEFAULT_JOIN_POLICY;

    private List<FatalErrorException> _fatalErrors = new ArrayList<FatalErrorException>();

    /**
     * <p>
     * Initialize Persistit using properties supplied by the default properties
     * file. The name of this file is supplied by the system property
     * <code>com.persistit.properties</code>. If that property is not specified,
     * the default file path is <code>./persistit.properties</code> in the
     * current working directory. If Persistit has already been initialized,
     * this method does nothing. This method is threadsafe; if multiple threads
     * concurrently attempt to invoke this method, one of the threads will
     * actually perform the initialization and the other threads will do
     * nothing.
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
        boolean done = false;
        try {
            _closed.set(false);

            initializeProperties(properties);
            initializeLogging();
            initializeManagement();
            initializeOther();
            initializeRecovery();
            initializeJournal();
            initializeBufferPools();
            initializeVolumes();
            startJournal();
            startBufferPools();
            finishRecovery();
            startCheckpointManager();
            startTransactionIndexPollTask();
            flush();
            _checkpointManager.checkpoint();
            startCleanupManager();
            _initialized.set(true);
            done = true;
        } finally {
            if (!done) {
                releaseAllResources();
            }
        }
    }

    Properties parseProperties(final String propertiesFileName) throws PersistitException {
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
        return properties;
    }

    void initializeProperties(final Properties properties) {
        properties.putAll(_properties);
        _properties = properties;

        _readRetryEnabled = getBooleanProperty(READ_RETRY_PROPERTY, true);
        _defaultTimeout = getLongProperty(TIMEOUT_PROPERTY, DEFAULT_TIMEOUT_VALUE, 0, MAXIMUM_TIMEOUT_VALUE);
    }

    void initializeLogging() throws PersistitException {
        try {
            getPersistitLogger().open();
            String logLevel = getProperty(LOGGING_PROPERTIES);
            if (logLevel != null && getPersistitLogger() instanceof DefaultPersistitLogger) {
                ((DefaultPersistitLogger) getPersistitLogger()).setLevel(logLevel);
            }
            _logBase.configure(getPersistitLogger());
            _logBase.start.log(_startTime);
            _logBase.copyright.log(copyright());
        } catch (Exception e) {
            System.err.println("Persistit(tm) Logging is disabled due to " + e);
            if (e.getMessage() != null && e.getMessage().length() > 0) {
                System.err.println(e.getMessage());
            }
            e.printStackTrace();
        }

    }

    void initializeRecovery() throws PersistitException {
        String journalPath = getProperty(JOURNAL_PATH_PROPERTY_NAME, DEFAULT_JOURNAL_PATH);
        _recoveryManager.init(journalPath);
        _recoveryManager.buildRecoveryPlan();
    }

    void initializeJournal() throws PersistitException {
        String journalPath = getProperty(JOURNAL_PATH_PROPERTY_NAME, DEFAULT_JOURNAL_PATH);
        int journalSize = (int) getLongProperty(JOURNAL_BLOCKSIZE_PROPERTY_NAME,
                JournalManagerMXBean.DEFAULT_BLOCK_SIZE, JournalManagerMXBean.MINIMUM_BLOCK_SIZE,
                JournalManagerMXBean.MAXIMUM_BLOCK_SIZE);

        _journalManager.init(_recoveryManager, journalPath, journalSize);
        if (getBooleanProperty(APPEND_ONLY_PROPERTY, false)) {
            _journalManager.setAppendOnly(true);
        }
    }

    void initializeBufferPools() {
        int bufferSize = Buffer.MIN_BUFFER_SIZE;
        while (bufferSize <= Buffer.MAX_BUFFER_SIZE) {
            String countPropertyName = BUFFERS_PROPERTY_NAME + bufferSize;
            String memPropertyName = BUFFER_MEM_PROPERTY_NAME + bufferSize;
            int byCount = (int) getLongProperty(countPropertyName, -1, BufferPool.MINIMUM_POOL_COUNT,
                    BufferPool.MAXIMUM_POOL_COUNT);
            int byMemory = computeBufferCountFromMemoryProperty(memPropertyName, getProperty(memPropertyName),
                    bufferSize);

            if (byCount != -1 && byMemory != -1) {
                throw new IllegalArgumentException("Only one of " + countPropertyName + " and " + memPropertyName
                        + " may be specified");
            }

            if (byMemory != -1) {
                byCount = byMemory;
            }

            if (byCount != -1) {
                _logBase.allocateBuffers.log(byCount, bufferSize);
                BufferPool pool = new BufferPool(byCount, bufferSize, this);
                _bufferPoolTable.put(new Integer(bufferSize), pool);
                registerBufferPoolMXBean(bufferSize);
            }
            bufferSize <<= 1;
        }
    }

    void initializeVolumes() throws PersistitException {
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
                    VolumeSpecification volumeSpecification = new VolumeSpecification(getProperty(key));
                    _logBase.openVolume.log(volumeSpecification.getName());
                    final Volume volume = new Volume(volumeSpecification);
                    volume.open(this);
                }
            }
        }
    }

    void initializeManagement() {
        String rmiHost = getProperty(RMI_REGISTRY_HOST_PROPERTY);
        String rmiPort = getProperty(RMI_REGISTRY_PORT);
        String serverPort = getProperty(RMI_SERVER_PORT);
        boolean enableJmx = getBooleanProperty(JMX_PARAMS, true);

        if (rmiHost != null || rmiPort != null) {
            ManagementImpl management = (ManagementImpl) getManagement();
            management.register(rmiHost, rmiPort, serverPort);
        }
        if (enableJmx) {
            registerMXBeans();
        }
    }

    void initializeOther() {
        // Set up the parent CoderManager for this instance.
        String serialOverridePatterns = getProperty(Persistit.SERIAL_OVERRIDE_PROPERTY);
        DefaultCoderManager cm = new DefaultCoderManager(this, serialOverridePatterns);
        _coderManager.set(cm);

        if (getBooleanProperty(SHOW_GUI_PROPERTY, false)) {
            try {
                setupGUI(true);
            } catch (Exception e) {
                _logBase.configurationError.log(e);
            }
        }

        try {
            _defaultSplitPolicy = SplitPolicy.forName(getProperty(SPLIT_POLICY_PROPERTY, DEFAULT_SPLIT_POLICY
                    .toString()));
        } catch (IllegalArgumentException e) {
            _logBase.configurationError.log(e);
        }
        try {
            _defaultJoinPolicy = JoinPolicy.forName(getProperty(JOIN_POLICY_PROPERTY, DEFAULT_JOIN_POLICY.toString()));
        } catch (IllegalArgumentException e) {
            _logBase.configurationError.log(e);
        }
    }

    void startCheckpointManager() {
        _checkpointManager.start();
    }

    void startCleanupManager() {
        _cleanupManager.start();
    }

    void startTransactionIndexPollTask() {
        _transactionIndex.start(this);
    }

    void startBufferPools() throws PersistitException {
        for (final BufferPool pool : _bufferPoolTable.values()) {
            pool.startThreads();
        }
    }

    void startJournal() throws PersistitException {
        _journalManager.startJournal();
    }

    void finishRecovery() throws PersistitException, TestException {
        _recoveryManager.applyAllRecoveredTransactions(_recoveryManager.getDefaultCommitListener(), _recoveryManager
                .getDefaultRollbackListener());
        _recoveryManager.close();
        flush();
        _logBase.recoveryDone.log(_journalManager.getPageMapSize(), _recoveryManager.getAppliedTransactionCount(),
                _recoveryManager.getErrorCount());
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
    private void registerMXBeans() {
        try {
            registerMBean(getManagement(), ManagementMXBean.MXBEAN_NAME);
            registerMBean(_ioMeter, IOMeterMXBean.MXBEAN_NAME);
            registerMBean(_cleanupManager, CleanupManagerMXBean.MXBEAN_NAME);
            registerMBean(_transactionIndex, TransactionIndexMXBean.MXBEAN_NAME);
            registerMBean(_journalManager, JournalManagerMXBean.MXBEAN_NAME);
            registerMBean(_recoveryManager, RecoveryManagerMXBean.MXBEAN_NAME);
        } catch (Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void registerBufferPoolMXBean(final int bufferSize) {
        try {
            BufferPoolMXBean bean = new BufferPoolMXBeanImpl(this, bufferSize);
            registerMBean(bean, BufferPoolMXBeanImpl.mbeanName(bufferSize));
        } catch (Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void registerMBean(final Object mbean, final String name) throws Exception {
        MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        ObjectName on = new ObjectName(name);
        server.registerMBean(mbean, on);
        _logBase.mbeanRegistered.log(on);
    }

    private void unregisterMXBeans() {
        try {
            unregisterMBean(RecoveryManagerMXBean.MXBEAN_NAME);
            unregisterMBean(JournalManagerMXBean.MXBEAN_NAME);
            unregisterMBean(TransactionIndexMXBean.MXBEAN_NAME);
            unregisterMBean(CleanupManagerMXBean.MXBEAN_NAME);
            unregisterMBean(IOMeterMXBean.MXBEAN_NAME);
            unregisterMBean(ManagementMXBean.MXBEAN_NAME);
            for (int size = Buffer.MIN_BUFFER_SIZE; size <= Buffer.MAX_BUFFER_SIZE; size *= 2) {
                if (_bufferPoolTable.get(size) != null) {
                    unregisterBufferPoolMXBean(size);
                }
            }

        } catch (InstanceNotFoundException exception) {
            // ignore
        } catch (Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void unregisterBufferPoolMXBean(final int bufferSize) {
        try {
            unregisterMBean(BufferPoolMXBeanImpl.mbeanName(bufferSize));
        } catch (InstanceNotFoundException exception) {
            // ignore
        } catch (Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void unregisterMBean(final String name) throws Exception {
        MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        ObjectName on = new ObjectName(name);
        server.unregisterMBean(on);
        _logBase.mbeanUnregistered.log(on);

    }

    synchronized void addVolume(Volume volume) throws VolumeAlreadyExistsException {
        Volume otherVolume;
        otherVolume = getVolume(volume.getName());
        if (otherVolume != null) {
            throw new VolumeAlreadyExistsException("Volume " + otherVolume);
        }
        _volumes.add(volume);
    }

    synchronized void removeVolume(Volume volume) throws PersistitInterruptedException {
        _volumes.remove(volume);
    }

    /**
     * Replaces substitution variables in a supplied string with values taken
     * from the properties available to Persistit (see {@link getProperty}).
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
     *            Value to set, or
     *            <code>null<code> to remove an existing property
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
     * Returns an <code>Exchange</code> for the specified {@link Volume Volume}
     * and the {@link Tree Tree} specified by the supplied name. This method
     * optionally creates a new <code>Tree</code>. If the <code>create</code>
     * parameter is false and a <code>Tree</code> by the specified name does not
     * exist, this constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * This method uses an <code>Exchange</code> from an internal pool if one is
     * available; otherwise it creates a new <code>Exchange</code>. When the
     * application no longer needs the <code>Exchange</code> returned by this
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
     *            <code>true</code> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange getExchange(Volume volume, String treeName, boolean create) throws PersistitException {
        if (volume == null)
            throw new VolumeNotFoundException();
        List<Exchange> stack;
        final SessionId sessionId = getSessionId();

        synchronized (_exchangePoolMap) {
            stack = _exchangePoolMap.get(sessionId);
            if (stack == null) {
                stack = new ArrayList<Exchange>();
                _exchangePoolMap.put(sessionId, stack);
            }
        }
        if (stack.isEmpty()) {
            return new Exchange(this, volume, treeName, create);
        } else {
            final Exchange exchange = stack.remove(stack.size() - 1);
            exchange.init(volume, treeName, create);
            return exchange;
        }
    }

    /**
     * <p>
     * Returns an <code>Exchange</code> for the {@link Tree} specified by
     * treeName within the {@link Volume} specified by <code>volumeName</code>.
     * This method optionally creates a new <code>Tree</code>. If the
     * <code>create</code> parameter is false and a <code>Tree</code> by the
     * specified name does not exist, this constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * The <code>volumeName</tt< you supply must match exactly one open 
     * <code>Volume</code>. The name matches if either (a) the
     * <code>Volume</code> has an optional alias that is equal to the supplied
     * name, or (b) if the supplied name matches a substring of the
     * <code>Volume</code>'s pathname. If there is not unique match for the name
     * you supply, this method throws a
     * {@link com.persistit.exception.VolumeNotFoundException}.
     * </p>
     * <p>
     * This method uses an <code>Exchange</code> from an internal pool if one is
     * available; otherwise it creates a new <code>Exchange</code>. When the
     * application no longer needs the <code>Exchange</code> returned by this
     * method, it should return it to the pool by invoking
     * {@link #releaseExchange} so that it can be reused.
     * </p>
     * 
     * @param volumeName
     *            The volume name that either matches the alias or a partially
     *            matches the pathname of exactly one open <code>Volume</code>.
     * 
     * @param treeName
     *            The tree name
     * 
     * @param create
     *            <code>true</code> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange getExchange(String volumeName, String treeName, boolean create) throws PersistitException {
        Volume volume = getVolume(volumeName);
        if (volume == null)
            throw new VolumeNotFoundException(volumeName);
        return getExchange(volume, treeName, create);
    }

    /**
     * <p>
     * Releases an <code>Exchange</code> to the internal pool. A subsequent
     * invocation of {@link #getExchange} may reuse this <code>Exchange</code>.
     * An application that gets an <code>Exchange</code> through the
     * {@link #getExchange} method <i>should</i> release it through this method.
     * An attempt to release the <code>Exchange</code> if it is already in the
     * pool results in an <code>IllegalStateException</code>.
     * </p>
     * <p>
     * This method clears the key and value fields. Use the
     * {@link #releaseExchange(Exchange, boolean)} method to clear all state
     * information if this <code>Exchange</code> may subsequently be used by
     * another untrusted thread.
     * </p>
     * 
     * @param exchange
     *            The <code>Exchange</code> to release to the pool. If
     *            <code>null</code> , this method returns silently.
     * 
     * @throws IllegalStateException
     */
    public void releaseExchange(Exchange exchange) {
        releaseExchange(exchange, false);
    }

    /**
     * <p>
     * Releases an <code>Exchange</code> to the internal pool. A subsequent
     * invocation of {@link #getExchange} may reuse this <code>Exchange</code>.
     * An application that gets an <code>Exchange</code> through the
     * {@link #getExchange} method <i>should</i> release it through this method.
     * An attempt to release the <code>Exchange</code> if it is already in the
     * pool results in an <code>IllegalStateException</code>.
     * </p>
     * <p>
     * This method optionally clears all state information in the
     * <code>Exchange</code> so that no residual information in the
     * <code>Exchange</code> can be obtained by a different, untrusted thread.
     * In a closed configuration in which there is only one application, it is
     * faster to avoid clearing the byte arrays used in representing the state
     * of this <code>Exchange</code> by passing <code>false</code> as the value
     * of the <code>secure</code> flag.
     * </p>
     * 
     * @param exchange
     *            The <code>Exchange</code> to release to the pool. If
     *            <code>null</code> this method returns silently.
     * @param secure
     *            <code>true</code> to clear all state information;
     *            <code>false</code> to leave the state unchanged.
     * 
     * @throws IllegalStateException
     */

    // TODO - why not one pool.
    public void releaseExchange(Exchange exchange, boolean secure) {
        if (exchange == null) {
            return;
        }
        List<Exchange> stack;
        final SessionId sessionId = getSessionId();

        synchronized (_exchangePoolMap) {
            stack = _exchangePoolMap.get(sessionId);
            if (stack == null) {
                throw new IllegalStateException("Release not preceded by get");
            }
        }
        if (stack.size() < MAX_POOLED_EXCHANGES) {
            exchange.removeState(secure);
            stack.add(exchange);
        }
    }

    /**
     * Get a {@link List} of all {@link Volume}s currently being managed by this
     * Persistit instance. Volumes are specified by the properties used in
     * initializing Persistit.
     * 
     * @return the List
     */
    public List<Volume> getVolumes() {
        return new ArrayList<Volume>(_volumes);
    }

    /**
     * Select a {@link List} of {@link Tree}s determined by the supplied
     * {@link TreeSelector}. This method enumerates all Trees in all open
     * Volumes and selects those which satisfy the TreeSelector. If the Volume
     * has a Volume-only selector (no tree pattern was specified), then this
     * method adds the Volume's directory Tree to the list.
     * 
     * @param selector
     * @return the List
     * @throws PersistitException
     */
    public synchronized List<Tree> getSelectedTrees(final TreeSelector selector) throws PersistitException {
        final List<Tree> list = new ArrayList<Tree>();
        for (final Volume volume : _volumes) {
            if (selector.isSelected(volume)) {
                if (selector.isVolumeOnlySelection(volume.getName())) {
                    list.add(volume.getDirectoryTree());
                } else {
                    for (final String treeName : volume.getTreeNames()) {
                        if (selector.isTreeNameSelected(volume.getName(), treeName)) {
                            list.add(volume.getTree(treeName, false));
                        }
                    }
                }
            }
        }
        return list;
    }

    /**
     * Look up, load and/or creates a volume based on a String-valued
     * specification. See {@link VolumeSpecification} for the specification
     * String format.
     * <p>
     * If a Volume has already been loaded having the same ID or name, this
     * method returns that Volume. Otherwise it tries to open or create a volume
     * on disk (depending on the volume specification) and returns that.
     * 
     * @param vstring
     *            Volume specification string
     * 
     * @return The <code>Volume</code>
     * 
     * @throws PersistitException
     */
    public Volume loadVolume(final String vstring) throws PersistitException {
        final VolumeSpecification volumeSpec = new VolumeSpecification(substituteProperties(vstring, _properties, 0));
        return loadVolume(volumeSpec);
    }

    /**
     * Look up, load and/or creates a volume based on a
     * {@link com.persistit.VolumeSpecification}. If a Volume has already been
     * loaded having the same ID or name, this method returns that Volume.
     * Otherwise it tries to open or create a volume on disk (depending on the
     * volume specification) and returns that.
     * 
     * @param volumeSpec
     *            The VolumeSpecification
     * 
     * @return The <code>Volume</code>
     * 
     * @throws PersistitException
     */
    public Volume loadVolume(final VolumeSpecification volumeSpec) throws PersistitException {
        Volume volume = getVolume(volumeSpec.getName());
        if (volume == null) {
            volume = new Volume(volumeSpec);
            volume.open(this);
        }
        return volume;
    }

    /**
     * Create a temporary volume. A temporary volume is not durable; it should
     * be used to hold temporary data such as intermediate sort or aggregation
     * results that can be recreated in the event the system restarts.
     * <p />
     * The temporary volume page size is can be specified by the configuration
     * property <code>tvpagesize</code>. The default value is determined by the
     * {@link BufferPool} having the largest page size.
     * <p />
     * The backing store file for a temporary volume is created in the directory
     * specified by the configuration property <code>tmpvoldir</code>, or if
     * unspecified, the system temporary directory..
     * 
     * @return the temporary <code>Volume</code>.
     * @throws PersistitException
     */
    public Volume createTemporaryVolume() throws PersistitException {
        int pageSize = (int) getLongProperty(TEMPORARY_VOLUME_PAGE_SIZE_NAME, 0, 0, 99999);
        if (pageSize == 0) {
            for (int size : _bufferPoolTable.keySet()) {
                if (size > pageSize) {
                    pageSize = size;
                }
            }
        }
        return createTemporaryVolume(pageSize);
    }

    /**
     * Create a temporary volume. A temporary volume is not durable; it should
     * be used to hold temporary data such as intermediate sort or aggregation
     * results that can be recreated in the event the system restarts.
     * <p />
     * The backing store file for a temporary volume is created in the directory
     * specified by the configuration property <code>tmpvoldir</code>, or if
     * unspecified, the system temporary directory..
     * 
     * @param pageSize
     *            The page size for the volume. Must be one of 1024, 2048, 4096,
     *            8192 or 16384, and the volume will be usable only if there are
     *            buffers of the specified size in the {@link BufferPool}.
     * @return the temporary <code>Volume</code>.
     * @throws PersistitException
     */
    public Volume createTemporaryVolume(final int pageSize) throws PersistitException {
        if (!Volume.isValidPageSize(pageSize)) {
            throw new IllegalArgumentException("Invalid page size " + pageSize);
        }
        Volume volume = new Volume(Thread.currentThread().getName() + "_temporary_volume", 12345);
        volume.openTemporary(this, pageSize);
        return volume;
    }

    /**
     * Delete a volume currently loaded volume and remove it from the list
     * returned by {@link #getVolumes()}.
     * 
     * @param volumeName
     *            the Volume to delete
     * @return <code>true</code> if the volume was previously loaded and has
     *         been successfully deleted.
     * @throws PersistitException
     */

    public boolean deleteVolume(final String volumeName) throws PersistitException {
        final Volume volume = getVolume(volumeName);
        if (volume == null) {
            return false;
        } else {
            volume.closing();
            boolean deleted = volume.delete();
            volume.close();
            return deleted;
        }
    }

    /**
     * Returns an implementation of the <code>Management</code> interface. This
     * implementation is a singleton; the first invocation of this method will
     * create an instance; subsequent invocations will return the same instance.
     * 
     * @return the singleton implementation of a <code>Management</code> from
     *         which system management services can be obtained.
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
     * <p>
     * Looks up a {@link Volume} by name or path. The supplied name must match
     * only one of the open volumes. If it matches none of the volumes, or if
     * there are multiple volumes with matching names, then this method returns
     * <code>null</code>.
     * </p>
     * <p>
     * The supplied name can match a volume in one of two ways:
     * <ul>
     * <li>(a) its name by exact match</li>
     * <li>(b) its path, by matching the absolute forms of the volume's path and
     * the supplied path.</li>
     * </ul>
     * </p>
     * 
     * @param name
     *            Name that identifies a volume by matching either its alias (if
     *            it has one) or a substring of its file name.
     * 
     * @return the <code>Volume</code>, or <i>null</i> if there is no unique
     *         open Volume that matches the supplied <code>partialName</code>.
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

        final File file = new File(name).getAbsoluteFile();
        for (int i = 0; i < _volumes.size(); i++) {
            Volume vol = _volumes.get(i);
            if (file.equals(new File(vol.getPath()).getAbsoluteFile())) {
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
     * <code>sysvolume</code> property with a default value of "_system".
     * </p>
     * <p>
     * This method handles a configuration with exactly one volume in a special
     * way. If the <code>sysvolume</code> property is unspecified and there is
     * exactly one volume, then this method returns that volume volume as the
     * system volume even if its name does not match the default
     * <code>sysvolume</code> property. This eliminates the need to specify a
     * system volume property for configurations having only one volume.
     * </p>
     * 
     * @return the <code>Volume</code>
     * @throws VolumeNotFoundException
     *             if the volume was not found
     */
    public Volume getSystemVolume() throws VolumeNotFoundException {
        return getSpecialVolume(SYSTEM_VOLUME_PROPERTY, DEFAULT_SYSTEM_VOLUME_NAME);
    }

    /**
     * Return the default timeout for operations on an <code>Exchange</code>.
     * The application may override this default value for an instance of an
     * <code>Exchange</code> through the {@link Exchange#setTimeout(long)}
     * method. The default timeout may be specified through the
     * <code>com.persistit.defaultTimeout</code> property.
     * 
     * @return The default timeout value, in milliseconds.
     */
    public long getDefaultTimeout() {
        return _defaultTimeout;
    }

    /**
     * @return The {@link SplitPolicy} that will by applied by default to newly
     *         created or allocated {@link Exchange}s.
     */
    public SplitPolicy getDefaultSplitPolicy() {
        return _defaultSplitPolicy;
    }

    /**
     * @return The {@link JoinPolicy} that will by applied by default to newly
     *         created or allocated {@link Exchange}s.
     */
    public JoinPolicy getDefaultJoinPolicy() {
        return _defaultJoinPolicy;
    }

    /**
     * Replace the current default {@link SplitPolicy}.
     * 
     * @param policy
     *            The {@link JoinPolicy} that will by applied by default to
     *            newly created or allocated {@link Exchange}s.
     */
    public void setDefaultSplitPolicy(final SplitPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("Default SplitPolicy may not be null");
        }
        _defaultSplitPolicy = policy;
    }

    /**
     * Replace the current default {@link SplitPolicy}.
     * 
     * @param policy
     *            The {@link JoinPolicy} that will by applied by default to
     *            newly created or allocated {@link Exchange}s.
     */
    public void setDefaultJoinPolicy(final JoinPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("Default JoinPolicy may not be null");
        }
        _defaultJoinPolicy = policy;
    }

    /**
     * Indicates whether this instance has been initialized.
     * 
     * @return <code>true</code> if this Persistit has been initialized.
     */
    public boolean isInitialized() {
        return _initialized.get();
    }

    /**
     * Indicates whether this instance of Persistit has been closed.
     * 
     * @return <code>true</code> if Persistit has been closed.
     */
    public boolean isClosed() {
        return _closed.get();
    }

    /**
     * Indicates whether Persistit will retry read any operation that fails due
     * to an IOException. In many cases, an IOException occurs due to transient
     * conditions, such as a file being locked by a backup program. When this
     * property is <code>true</code>, Persistit will repeatedly retry the read
     * operation until the timeout value for the current operation expires. By
     * default this property is <code>true</code>. Use the
     * com.persistit.readretry property to disable it.
     * 
     * @return <code>true</code> to retry a read operation that fails due to an
     *         IOException.
     */
    public boolean isReadRetryEnabled() {
        return _readRetryEnabled;
    }

    /**
     * @return The most recently proposed Checkpoint.
     * @throws PersistitInterruptedException
     */
    public Checkpoint getCurrentCheckpoint() throws PersistitInterruptedException {
        return _checkpointManager.getCurrentCheckpoint();
    }

    /**
     * Force a new Checkpoint and wait for it to be written. If Persistit is
     * closed or not yet initialized, do nothing and return <code>null</code>.
     * 
     * @return the Checkpoint allocated by this process.
     * @throws PersistitInterruptedException
     */
    public Checkpoint checkpoint() throws PersistitException {
        if (_closed.get() || !_initialized.get()) {
            return null;
        }
        cleanup();
        return _checkpointManager.checkpoint();
    }

    final long earliestLiveTransaction() {
        return _transactionIndex.getActiveTransactionFloor();
    }

    final long earliestDirtyTimestamp() {
        long earliest = Long.MAX_VALUE;
        for (final BufferPool pool : _bufferPoolTable.values()) {
            earliest = Math.min(earliest, pool.getEarliestDirtyTimestamp());
        }
        return earliest;
    }

    /**
     * Copy back all pages from the journal to their host Volumes.
     * 
     * @throws Exception
     */
    public void copyBackPages() throws Exception {
        if (!_closed.get() && _initialized.get()) {
            _checkpointManager.checkpoint();
            _journalManager.copyBack();
        } else {
            throw new PersistitClosedException();
        }
    }

    /**
     * @return whether a fatal error has occurred
     */
    public boolean isFatal() {
        return _fatal.get();
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
    private Volume getSpecialVolume(String propName, String dflt) throws VolumeNotFoundException {
        String volumeName = getProperty(propName, dflt);

        Volume volume = getVolume(volumeName);
        if (volume == null) {
            if ((_volumes.size() == 1) && (volumeName.equals(dflt))) {
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
     * @return the <code>BufferPool</code> for the specific buffer size
     */
    BufferPool getBufferPool(int size) {
        return _bufferPoolTable.get(new Integer(size));
    }

    /**
     * @return A HashMap containing all the <code>BufferPool</code>s keyed by
     *         their size.
     */
    HashMap<Integer, BufferPool> getBufferPoolHashMap() {
        return _bufferPoolTable;
    }

    public void cleanup() {
        final Set<SessionId> sessionIds;
        synchronized (_transactionSessionMap) {
            sessionIds = new HashSet<SessionId>(_transactionSessionMap.keySet());
        }
        for (final SessionId sessionId : sessionIds) {
            if (!sessionId.isAlive()) {
                Transaction transaction = null;
                synchronized (_transactionSessionMap) {
                    transaction = _transactionSessionMap.remove(sessionId);
                }
                if (transaction != null) {
                    try {
                        transaction.close();
                    } catch (PersistitException e) {
                        _logBase.exception.log(e);
                    }
                }
            }
        }
        final List<Volume> volumes;
        synchronized (this) {
            volumes = new ArrayList<Volume>(_volumes);
        }
        for (final Volume volume : volumes) {
            volume.getStructure().flushStatistics();
        }
    }

    /**
     * Reports status of the <code>max</code> longest-running transactions, in
     * order from oldest to youngest.
     * 
     * @param max
     * @return
     */
    public String transactionReport(final int max) {
        long[] timestamps = _transactionIndex.oldestTransactions(max);
        if (timestamps == null) {
            return "Unstable after 10 retries";
        }
        if (timestamps.length == 0) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        for (int index = 0; index < timestamps.length; index++) {
            boolean found = false;
            for (final Transaction txn : _transactionSessionMap.values()) {
                if (txn.isActive() && txn.getStartTimestamp() == timestamps[index]) {
                    sb.append(txn.toString());
                    found = true;
                }
            }
            if (!found) {
                sb.append(String.format("No active transaction starting at %,d remains active", timestamps[index]));
            }
            sb.append(Util.NEW_LINE);
        }
        return sb.toString();
    }

    /**
     * <p>
     * Close the Persistit Journal and all {@link Volume}s. This method is
     * equivalent to {@link #close(boolean) close(true)}.
     * 
     * @throws PersistitException
     * @throws IOException
     * @throws PersistitException
     * @throws IOException
     * @return <code>true</code> if Persistit was initialized and this
     *         invocation closed it, otherwise false.
     */
    public void close() throws PersistitException {
        close(true);
    }

    /**
     * <p>
     * Close the Persistit Journal and all {@link Volume}s. This method does
     * nothing and returns <code>false</code> if Persistit is currently not in
     * the initialized state. This method is threadsafe; if multiple threads
     * concurrently attempt to close Persistit, only one close operation will
     * actually take effect.
     * </p>
     * <p>
     * The <code>flush</code> determines whether this method will pause to flush
     * all pending updates to disk before shutting down the system. If
     * <code>flush</code> is <code>true</code> and many updated pages need to be
     * written, the shutdown process may take a significant amount of time.
     * However, upon restarting the system, all updates initiated before the
     * call to this method will be reflected in the B-Tree database. This is the
     * normal mode of operation.
     * </p>
     * <p>
     * When <code>flush</code> is false this method returns quickly, but without
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
     *            <code>true</code> to ensure all dirty pages are written to
     *            disk before shutdown completes; <code>false</code> to enable
     *            fast (but incomplete) shutdown.
     * 
     * @throws PersistitException
     * @throws IOException
     * @throws PersistitException
     * @throws IOException
     * @return <code>true</code> if Persistit was initialized and this
     *         invocation closed it, otherwise false.
     */
    public void close(final boolean flush) throws PersistitException {
        if (_initialized.get() && !_closed.get()) {
            synchronized (this) {
                // Wait for UI to go down.
                while (_suspendShutdown.get()) {
                    try {
                        wait(SHORT_DELAY);
                    } catch (InterruptedException ie) {
                        throw new PersistitInterruptedException(ie);
                    }
                }
            }

            /*
             * The copier is responsible for background pruning of aborted
             * transactions. Halt it so Transaction#close() can be called
             * without being concerned about its state changing.
             */
            _journalManager.stopCopier();

            getTransaction().close();
            cleanup();

            final List<Volume> volumes;
            synchronized (this) {
                volumes = new ArrayList<Volume>(_volumes);
            }

            if (flush) {
                for (final Volume volume : volumes) {
                    volume.getStructure().flushStatistics();
                    volume.getStorage().flush();
                }
            }

            _cleanupManager.close(flush);
            waitForIOTaskStop(_cleanupManager);

            _checkpointManager.close(flush);
            waitForIOTaskStop(_checkpointManager);

            _closed.set(true);

            for (final BufferPool pool : _bufferPoolTable.values()) {
                pool.close();
            }

            /*
             * Close (and abort) all remaining transactions.
             */
            Set<Transaction> transactions;
            synchronized (_transactionSessionMap) {
                transactions = new HashSet<Transaction>(_transactionSessionMap.values());
                _transactionSessionMap.clear();
            }
            for (final Transaction txn : transactions) {
                try {
                    txn.close();
                } catch (PersistitException e) {
                    _logBase.exception.log(e);
                }
            }

            _journalManager.close();
            IOTaskRunnable task = _transactionIndex.close();
            waitForIOTaskStop(task);

            for (final Volume volume : volumes) {
                volume.close();
            }

            if (flush) {
                for (final BufferPool pool : _bufferPoolTable.values()) {
                    int count = pool.getDirtyPageCount();
                    if (count > 0) {
                        _logBase.strandedPages.log(pool, count);
                    }
                }
            }
        }
        releaseAllResources();
    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the writer and collector
     * processes. This method should be used only by tests.
     * 
     * @throws CrashException
     */
    public void crash() {
        final JournalManager journalManager = _journalManager;
        if (journalManager != null) {
            try {
                journalManager.crash();
            } catch (IOException e) {
                _logBase.exception.log(e);
            }
        }
        //
        // Even on simulating a crash we need to try to close
        // the volume files - otherwise there will be left over channels
        // and FileLocks that interfere with subsequent tests.
        //
        final List<Volume> volumes = new ArrayList<Volume>(_volumes);
        for (final Volume volume : volumes) {
            try {
                volume.getStorage().close();
            } catch (PersistitException pe) {
                // ignore -
            }
        }
        final Map<Integer, BufferPool> buffers = _bufferPoolTable;
        if (buffers != null) {
            for (final BufferPool pool : buffers.values()) {
                unregisterBufferPoolMXBean(pool.getBufferSize());
                pool.crash();
            }
        }
        _transactionIndex.crash();
        _cleanupManager.crash();
        _checkpointManager.crash();
        _closed.set(true);
        releaseAllResources();
        shutdownGUI();
    }

    /**
     * Record the cause of a fatal Persistit error, such as imminent data
     * corruption, and set Persistit to the closed and fatal state. We expect
     * this method never to be called except by tests.
     * 
     * @param msg
     *            Explanatory message
     * @param cause
     *            Throwable cause of condition
     */
    void fatal(final String msg, final Throwable cause) {
        _fatal.set(true);
        _closed.set(true);
        final FatalErrorException exception = new FatalErrorException(msg, cause);
        synchronized (_fatalErrors) {
            if (_fatalErrors.size() < MAX_FATAL_ERROR_MESSAGES) {
                _fatalErrors.add(exception);
            }
        }
        throw exception;
    }

    private void releaseAllResources() {

        _accumulators.clear();
        _volumes.clear();
        _exchangePoolMap.clear();
        _cleanupManager.clear();
        _transactionSessionMap.clear();
        _cliSessionMap.clear();
        _sessionIdThreadLocal.remove();
        _fatalErrors.clear();

        unregisterMXBeans();

        _bufferPoolTable.clear();

        if (_management != null) {
            _management.unregister();
            _management = null;
        }

        try {
            if (_logger != null) {
                _logBase.end.log(System.currentTimeMillis());
                _logger.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        if (_closed.get() || !_initialized.get()) {
            return false;
        }
        for (final Volume volume : _volumes) {
            volume.getStructure().flushStatistics();
            volume.getStorage().flush();
            volume.getStorage().force();
        }
        flushBuffers(_timestampAllocator.getCurrentTimestamp());
        _journalManager.force();
        return true;
    }

    void flushBuffers(final long timestamp) throws PersistitInterruptedException {
        for (final BufferPool pool : _bufferPoolTable.values()) {
            pool.flush(timestamp);
        }
    }

    void flushTransactions(final long checkpointTimestamp) throws PersistitException {
        final List<Transaction> transactions;
        synchronized (_transactionSessionMap) {
            transactions = new ArrayList<Transaction>(_transactionSessionMap.values());
        }

        for (final Transaction transaction : transactions) {
            transaction.flushOnCheckpoint(checkpointTimestamp);
        }
    }

    void waitForIOTaskStop(final IOTaskRunnable task) {
        if (_beginCloseTime == 0) {
            _beginCloseTime = System.nanoTime();
            _nextCloseTime = _beginCloseTime + CLOSE_LOG_INTERVAL;
        }
        task.kick();
        while (!task.isStopped()) {
            try {
                task.join(SHORT_DELAY);
            } catch (InterruptedException ie) {
                break;
            }
            final long now = System.currentTimeMillis();
            if (now > _nextCloseTime) {
                _logBase.waitForClose.log((_nextCloseTime - _beginCloseTime) / 1000);
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
    public void force() throws PersistitIOException {
        if (_closed.get() || !_initialized.get()) {
            return;
        }
        final ArrayList<Volume> volumes = _volumes;

        for (int index = 0; index < volumes.size(); index++) {
            Volume volume = volumes.get(index);
            if (!volume.getStorage().isReadOnly()) {
                volume.getStorage().force();
            }
        }
        _journalManager.force();
    }

    void checkClosed() throws PersistitClosedException, PersistitInterruptedException {
        if (isClosed()) {
            checkFatal();
            throw new PersistitClosedException();
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new PersistitInterruptedException(new InterruptedException());
        }
    }

    void checkFatal() throws FatalErrorException {
        if (isFatal()) {
            throw _fatalErrors.get(0);
        }
    }

    /**
     * Waits until updates are no longer suspended. The
     * {@link #setUpdateSuspended} method controls whether update operations are
     * currently suspended.
     * 
     * @throws PersistitInterruptedException
     */
    public void checkSuspended() throws PersistitInterruptedException {
        while (isUpdateSuspended()) {
            Util.sleep(SHORT_DELAY);
        }
    }

    /**
     * Return this thread's SessionId. Constructs a new unique SessionId if the
     * thread has not already been bound to one.
     * 
     * @return Thread-private SessionId
     */
    public SessionId getSessionId() {
        return _sessionIdThreadLocal.get();
    }

    /**
     * Modify this thread's SessionId. This method is intended for server
     * applications that may execute multiple requests, possible on different
     * threads, within the scope of one session. Such applications much use
     * extreme care to avoid having two threads with the same SessionId at any
     * time.
     * 
     * @param sessionId
     */
    public void setSessionId(final SessionId sessionId) {
        sessionId.assign();
        _sessionIdThreadLocal.set(sessionId);
    }

    /**
     * Close the session resources associated with the current thread.
     * 
     * @throws PersistitException
     */
    void closeSession() throws PersistitException {
        final SessionId sessionId = _sessionIdThreadLocal.get();
        if (sessionId != null) {
            final Transaction txn;
            synchronized (_transactionSessionMap) {
                txn = _transactionSessionMap.remove(sessionId);
            }
            if (txn != null) {
                txn.close();
            }
        }
        _sessionIdThreadLocal.set(null);
    }

    /**
     * Get the <code>Transaction</code> object for the current thread. The
     * <code>Transaction</code> object lasts for the life of the thread. See
     * {@link com.persistit.Transaction} for more information on how to use
     * Persistit's transaction facilities.
     * 
     * @return This thread <code>Transaction</code> object.
     */
    public Transaction getTransaction() {
        final SessionId sessionId = getSessionId();
        synchronized (_transactionSessionMap) {
            Transaction txn = _transactionSessionMap.get(sessionId);
            if (txn == null) {
                txn = new Transaction(this, sessionId);
                _transactionSessionMap.put(sessionId, txn);
            }
            return txn;
        }
    }

/**
     * Copy the {@link Transaction} context objects belonging to threads that
     * are currently alive to the supplied List. This method is used by
     * JOURNAL_FLUSHER to look for transactions that need to be written to the
     * Journal and by {@link ManagementImpl to get transaction commit and
     * rollback statistics.
     * 
     * @param transactions List of Transaction objects to be populated
     */
    void populateTransactionList(final List<Transaction> transactions) {
        transactions.clear();
        for (final Map.Entry<SessionId, Transaction> entry : _transactionSessionMap.entrySet()) {
            final SessionId session = entry.getKey();
            final Transaction txn = entry.getValue();
            if (session.isAlive()) {
                transactions.add(txn);
            }
        }
    }

    /**
     * @return The current timestamp value
     */
    public long getCurrentTimestamp() {
        return _timestampAllocator.getCurrentTimestamp();
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
    public void setCoderManager(CoderManager coderManager) {
        _coderManager.set(coderManager);
    }

    /**
     * Returns the current CoderManager.
     * 
     * @return The current {@link com.persistit.encoding.CoderManager}.
     */
    public CoderManager getCoderManager() {
        return _coderManager.get();
    }

    public LogBase getLogBase() {
        return _logBase;
    }

    /**
     * Available heap space at the time this Persistit instance was created.
     * Determined by the {@link MemoryUsage#getMax()} method of
     * {@link MemoryMXBean#getHeapMemoryUsage()} at the time this Persistit
     * instance was created, i.e., before allocation of buffer pools and other
     * data structures.
     * 
     * @return maximum available heap memory, in bytes
     */
    public long getAvailableHeap() {
        return _availableHeap;
    }

    ClassIndex getClassIndex() {
        return _classIndex;
    }

    Class<?> classForHandle(int handle) {
        ClassInfo ci = _classIndex.lookupByHandle(handle);
        if (ci == null)
            return null;
        else
            return ci.getDescribedClass();
    }

    KeyCoder lookupKeyCoder(Class<?> cl) {
        CoderManager cm = _coderManager.get();
        if (cm == null) {
            return null;
        }
        return cm.lookupKeyCoder(cl);
    }

    ValueCoder lookupValueCoder(Class<?> cl) {
        CoderManager cm = _coderManager.get();
        if (cm == null) {
            return null;
        }
        return cm.lookupValueCoder(cl);
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

    CheckpointManager getCheckpointManager() {
        return _checkpointManager;
    }

    CleanupManager getCleanupManager() {
        return _cleanupManager;
    }

    IOMeter getIOMeter() {
        return _ioMeter;
    }

    TransactionIndex getTransactionIndex() {
        return _transactionIndex;
    }

    public long getCheckpointIntervalNanos() {
        return _checkpointManager.getCheckpointIntervalNanos();
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
    public void setPersistitLogger(PersistitLogger logger) {
        _logger = logger;
    }

    /**
     * @return The current logger.
     */
    public PersistitLogger getPersistitLogger() {
        if (_logger == null)
            _logger = new DefaultPersistitLogger(getProperty(LOGFILE_PROPERTY));
        return _logger;
    }

    /**
     * Convenience method that performs an integrity check on all open
     * <code>Volume</code>s and reports detailed results to
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
                System.out.println(e + " while performing IntegrityCheck on " + volume);
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
    static long parseLongProperty(String propName, String str, long min, long max) {
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
            throw new IllegalArgumentException("Value '" + str + "' of property " + propName + " is invalid");
        }
        return result;
    }

    /**
     * Parses a string as a float value
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

    static float parseFloatProperty(String propName, String str, float min, float max) {
        float result = Float.MIN_VALUE;
        boolean invalid = false;
        try {
            result = Float.parseFloat(str);
        }

        catch (NumberFormatException nfe) {
            invalid = true;
        }
        if (result < min || result > max || invalid) {
            throw new IllegalArgumentException("Value '" + str + "' of property " + propName + " is invalid");
        }
        return result;
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
     * Parses a String-valued memory allocation specification to produce a
     * buffer count that will consume approximately the specified amount of
     * memory.
     * <p />
     * The propertyValue specifies a memory specification in the form
     * 
     * <pre>
     * [<i>minimum</i>[,<i>maximum</i>[,<i>reserved</i>[,<i>fraction</i>]]]]
     * </pre>
     * 
     * where <i>minimum</i>, <i>maximum</i> and <i>reserved</i> specify
     * quantities of memory, in bytes. The suffixes 'K', 'M', 'G', and 'T' can
     * be used for scaling; see
     * {@link #parseLongProperty(String, String, long, long)}. <i>fraction</i>
     * is a float between 0.0f and 1.0f denoting a fraction of the total
     * available memory to allocate fof buffers.
     * <p />
     * The available memory is determined by the maximum heap size. The amount
     * of memory to be allocated to buffers is determined by the following
     * formula:
     * 
     * <pre>
     * <i>allocated</i> = (<i>available</i> - <i>reserved</i>) * <i>fraction</i>
     * </pre>
     * 
     * and then that result is bounded by the range (<i>minimum</i>,
     * <i>maximum<i>). Finally, the actual buffer count returned from this
     * method is the allocation divided by the bufferSize plus overhead for
     * FastIndex and other structures.
     * 
     * @param propertyName
     *            the property name, e.g., "buffer.memory.16384"
     * @param propertyValue
     *            the memory specification
     * @param bufferSize
     *            the buffer size
     * @return
     */
    int computeBufferCountFromMemoryProperty(final String propertyName, final String propertyValue, final int bufferSize) {
        if (propertyValue == null || propertyValue.isEmpty()) {
            return -1;
        }
        int bufferSizeWithOverhead = Buffer.bufferSizeWithOverhead(bufferSize);
        long absoluteMinimum = (long) BufferPool.MINIMUM_POOL_COUNT * bufferSizeWithOverhead;
        long absoluteMaximum = (long) BufferPool.MAXIMUM_POOL_COUNT * bufferSizeWithOverhead;
        long minimum = absoluteMinimum;
        long maximum = absoluteMaximum;
        long reserved = 0;
        float fraction = 0.0f;

        final String[] terms = propertyValue.split(",", 4);
        if (terms.length > 0 && !terms[0].isEmpty()) {
            minimum = Math.max(absoluteMinimum, parseLongProperty(propertyName, terms[0], 0, absoluteMaximum));
        }
        if (terms.length > 1 && !terms[1].isEmpty()) {
            maximum = Math.max(absoluteMinimum, parseLongProperty(propertyName, terms[1], minimum, maximum));
        }
        if (terms.length > 2 && !terms[2].isEmpty()) {
            reserved = parseLongProperty(propertyName, terms[2], 0, Long.MAX_VALUE);
            fraction = 1.0f;
        }
        if (terms.length > 3 && !terms[3].isEmpty()) {
            fraction = parseFloatProperty(propertyName, terms[3], 0f, 1f);
        }
        long allocation = (long) ((getAvailableHeap() - reserved) * fraction);
        allocation = Math.max(minimum, allocation);
        allocation = Math.min(maximum, allocation);
        if (allocation < absoluteMinimum || allocation > absoluteMaximum || allocation > getAvailableHeap()) {
            throw new IllegalArgumentException(String.format("%s=%s resulted in invalid memory "
                    + "allocation %,d, available memory is %,d", propertyName, propertyValue, allocation,
                    getAvailableHeap()));
        }

        return (int) (allocation / bufferSizeWithOverhead);
    }

    static long availableHeap() {
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long available = mu.getMax();
        if (available == -1) {
            available = mu.getInit();
        }
        return available;
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

    /**
     * Attemps to open the diagnostic GUI that displays some useful information
     * about Persistit's internal state. If the UI has already been opened, this
     * method merely sets the shutdown suspend flag.
     * 
     * @param suspendShutdown
     *            If <code>true</code>, sets the shutdown suspend flag. Setting
     *            this flag suspends the {@link #close} method to permit
     *            continued use of the diagnostic GUI.
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void setupGUI(boolean suspendShutdown) throws IllegalAccessException, InstantiationException,
            ClassNotFoundException, RemoteException {
        if (_localGUI == null) {
            _logBase.startAdminUI.log();
            _localGUI = (UtilControl) (Class.forName(PERSISTIT_GUI_CLASS_NAME)).newInstance();
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
     * @return <code>true</code> if Persistit will wait when attempting to
     *         close; <code>false</code> if the <code>close</code> operation
     *         will not be suspended.
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
     *            <code>true</code> to specify that Persistit will wait when
     *            attempting to close; otherwise <code>false</code>.
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
     * @return <code>true</code> if all updates are suspended; otherwise
     *         <code>false</code>.
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
     *            <code>true</code> to suspend all updates; <code>false</code>
     *            to enable updates.
     */
    public void setUpdateSuspended(boolean suspended) {
        _suspendUpdates.set(suspended);
    }

    void addAccumulator(final Accumulator accumulator) throws PersistitException {
        int checkpointCount = 0;
        synchronized (_accumulators) {
            _accumulators.add(accumulator.getAccumulatorRef());
            /*
             * Count the checkpoint references. When the count is a multiple of
             * ACCUMULATOR_CHECKPOINT_THRESHOLD, then call create a checkpoint
             * which will write a checkpoint transaction and remove the
             * checkpoint references. The threshold value is chosen to be large
             * enough prevent creating too many checkpoints, but small enough
             * that the number of excess Accumulators is kept to a reasonable
             * number.
             */
            for (AccumulatorRef ref : _accumulators) {
                if (ref._checkpointRef != null) {
                    checkpointCount++;
                }
            }
        }
        if ((checkpointCount % ACCUMULATOR_CHECKPOINT_THRESHOLD) == 0) {
            try {
                _checkpointManager.createCheckpoint();
            } catch (PersistitException e) {
                _logBase.exception.log(e);
            }
        }
    }

    List<Accumulator> getCheckpointAccumulators() {
        final List<Accumulator> result = new ArrayList<Accumulator>();
        synchronized (_accumulators) {
            for (final Iterator<AccumulatorRef> refIterator = _accumulators.iterator(); refIterator.hasNext();) {
                final AccumulatorRef ref = refIterator.next();
                if (!ref.isLive()) {
                    refIterator.remove();
                }
                final Accumulator acc = ref.takeCheckpointRef();
                if (acc != null) {
                    result.add(acc);
                }
            }
            Collections.sort(result, Accumulator.SORT_COMPARATOR);
        }
        return result;
    }

    synchronized CLI getSessionCLI() {
        CLI cli = _cliSessionMap.get(getSessionId());
        if (cli == null) {
            cli = new CLI(this);
            _cliSessionMap.put(getSessionId(), cli);
        }
        return cli;
    }

    synchronized void clearSessionCLI() {
        _cliSessionMap.remove(getSessionId());
    }

    private final static String[] ARG_TEMPLATE = { "_flag|g|Start AdminUI",
            "_flag|i|Perform IntegrityCheck on all volumes", "_flag|w|Wait until AdminUI exists",
            "_flag|c|Perform copy-back", "properties|string|Property file name",
            "cliport|int:-1:1024:99999999|Port on which to start a simple command-line interface server",
            "script|string|Pathname of CLI script to execute", };

    /**
     * Perform various utility functions. Specify arguments on the command line
     * conforming to {@value #ARG_TEMPLATE}. Options:
     * <ul>
     * <li>If the cliport=nnnn argument is set, then this method starts a CLI
     * server on the specified port.</li>
     * <li>Otherwise if the properties=filename argument is set, this method
     * initializes a Persistit instance using the specified properties. With an
     * initialized instance the flags -i, -g, and -c take effect to invoke an
     * integrity check, open the AdminUI or copy back all pages from the
     * Journal.</li>
     * </ul>
     * 
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final ArgParser ap = new ArgParser("Persistit", args, ARG_TEMPLATE);
        if (ap.isUsageOnly()) {
            return;
        }

        Persistit persistit = null;
        String propertiesFileName = ap.getStringValue("properties");
        if (!propertiesFileName.isEmpty()) {
            persistit = new Persistit();
            persistit.initialize(propertiesFileName);
        }
        String scriptName = ap.getStringValue("script");

        int cliport = ap.getIntValue("cliport");

        if (cliport > -1 && !propertiesFileName.isEmpty()) {
            throw new IllegalArgumentException("Specify only one: properties or cliport");
        }

        if (cliport > 1) {
            System.out.printf("Starting a Persistit CLI server on port %d\n", cliport);
            Task task = CLI.cliserver(cliport);
            task.runTask();
            task.setPersistit(persistit);
        } else if (!scriptName.isEmpty()) {
            final BufferedReader reader = new BufferedReader(new FileReader(scriptName));
            final PrintWriter writer = new PrintWriter(System.out);
            CLI.runScript(persistit, reader, writer);
        } else {
            if (persistit == null) {
                throw new IllegalArgumentException("Must specify a properties file");
            }
            boolean gui = ap.isFlag('g');
            boolean icheck = ap.isFlag('i');
            boolean wait = ap.isFlag('w');
            boolean copy = ap.isFlag('c');

            try {
                if (gui) {
                    persistit.setupGUI(wait);
                }
                if (icheck) {
                    persistit.checkAllVolumes();
                }
                if (copy) {
                    persistit.copyBackPages();
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
    }

}
