/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import static com.persistit.Configuration.DEFAULT_SYSTEM_VOLUME_NAME;
import static com.persistit.Configuration.SYSTEM_PROPERTY_PREFIX;
import static com.persistit.Configuration.SYSTEM_VOLUME_PROPERTY_NAME;
import static com.persistit.util.Util.NS_PER_S;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.NotificationEmitter;
import javax.management.ObjectName;

import com.persistit.Accumulator.AccumulatorRef;
import com.persistit.CheckpointManager.Checkpoint;
import com.persistit.Configuration.BufferPoolConfiguration;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.PersistitClosedException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.TestException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.logging.DefaultPersistitLogger;
import com.persistit.logging.LogBase;
import com.persistit.logging.PersistitLogger;
import com.persistit.mxbeans.AlertMonitorMXBean;
import com.persistit.mxbeans.BufferPoolMXBean;
import com.persistit.mxbeans.CheckpointManagerMXBean;
import com.persistit.mxbeans.CleanupManagerMXBean;
import com.persistit.mxbeans.IOMeterMXBean;
import com.persistit.mxbeans.JournalManagerMXBean;
import com.persistit.mxbeans.MXBeanWrapper;
import com.persistit.mxbeans.ManagementMXBean;
import com.persistit.mxbeans.RecoveryManagerMXBean;
import com.persistit.mxbeans.TransactionIndexMXBean;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.util.ArgParser;
import com.persistit.util.Debug;
import com.persistit.util.Util;
import com.persistit.util.UtilControl;

/**
 * <p>
 * Create and manage the runtime environment for a Persistit&trade; database. To
 * use Persistit an application
 * <ul>
 * <li>constructs a Persistit instance when it starts up</li>
 * <li>calls one of the {@link #initialize} methods to set up a configuration
 * and initialize the memory structures and background threads</li>
 * <li>uses various method to acquire {@link Exchange} and {@link Transaction}
 * instances to perform work,</li>
 * <li>calls one of the {@link #close()} methods to gracefully release all
 * memory resources and shut down the background threads.</li>
 * </ul>
 * </p>
 * Generally an application will have no more than one Persistit instance,
 * treating it as a singleton. However, the application is responsible for
 * holding a reference to that instance and calling {@link #close()} when
 * finished with it. Persistit's background threads are not daemon threads, and
 * an application that does not call <code>close</code> therefore will not exit
 * normally. </p>
 * <p>
 * Persistit takes a large variety of configuration properties. These are
 * specified through the <code>initalize</code> method.
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
    public final static String COPYRIGHT = "Copyright (c) 2012 Akiban Technologies Inc.";

    /**
     * Determines whether multi-byte integers will be written in little- or
     * big-endian format. This constant is <code>true</code> in all current
     * builds.
     */
    public final static boolean BIG_ENDIAN = true;

    private final static String PERSISTIT_GUI_CLASS_NAME = SYSTEM_PROPERTY_PREFIX + "ui.AdminUI";
    /**
     * Maximum number of Exchanges that will be held in an internal pool.
     */
    public final static int MAX_POOLED_EXCHANGES = 10000;

    private final static int TRANSACTION_INDEX_SIZE = 256;

    final static long SHORT_DELAY = 500;

    private final static long CLOSE_LOG_INTERVAL = 30000000000L; // 30 sec

    private final static int ACCUMULATOR_CHECKPOINT_THRESHOLD = 256;

    private final static SplitPolicy DEFAULT_SPLIT_POLICY = SplitPolicy.PACK_BIAS;
    private final static JoinPolicy DEFAULT_JOIN_POLICY = JoinPolicy.EVEN_BIAS;
    private final static CommitPolicy DEFAULT_TRANSACTION_COMMIT_POLICY = CommitPolicy.SOFT;
    private final static long DEFAULT_COMMIT_LEAD_TIME_MS = 100;
    private final static long DEFAULT_COMMIT_STALL_TIME_MS = 1;
    private final static long MAX_COMMIT_LEAD_TIME_MS = 5000;
    private final static long MAX_COMMIT_STALL_TIME_MS = 5000;
    private final static long LOG_FLUSH_DELAY_INTERVAL_MS = 5000;

    private final static int MAX_FATAL_ERROR_MESSAGES = 10;

    /**
     * An Exception created when Persistit detects a fatal internal error such
     * as database corruption.
     */
    public static class FatalErrorException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final String _threadName = Thread.currentThread().getName();
        final long _systemTime = System.currentTimeMillis();

        private FatalErrorException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }

    /**
     * Background thread that periodically flushes the log file buffers so that
     * we actually have log information in the event of a failure.
     */
    private class LogFlusher extends Thread {
        boolean _stop;

        LogFlusher() {
            setDaemon(true);
            setName("LOG_FLUSHER");
        }

        @Override
        public void run() {
            while (!_stop) {
                try {
                    Util.sleep(LOG_FLUSH_DELAY_INTERVAL_MS);
                } catch (final PersistitInterruptedException ie) {
                    break;
                }
                pollAlertMonitors(false);
                final PersistitLogger logger = _logger;
                if (logger != null) {
                    logger.flush();
                }
            }
        }
    }

    private final long _availableHeap = availableHeap();

    private volatile PersistitLogger _logger;
    private LogFlusher _logFlusher;

    /**
     * Start time
     */
    private final long _startTime = System.currentTimeMillis();
    private volatile Configuration _configuration;

    private final HashMap<Integer, BufferPool> _bufferPoolTable = new HashMap<Integer, BufferPool>();
    private final ArrayList<Volume> _volumes = new ArrayList<Volume>();

    private final AtomicBoolean _initialized = new AtomicBoolean();
    private final AtomicBoolean _closed = new AtomicBoolean();
    private final AtomicBoolean _fatal = new AtomicBoolean();

    private long _beginCloseTime;
    private long _nextCloseTime;

    private final LogBase _logBase = new LogBase();

    private final AtomicBoolean _suspendShutdown = new AtomicBoolean(false);
    private final AtomicBoolean _suspendUpdates = new AtomicBoolean(false);
    private final AtomicBoolean _enableBufferInventory = new AtomicBoolean(false);

    private UtilControl _localGUI;

    private final AtomicReference<CoderManager> _coderManager = new AtomicReference<CoderManager>();
    private final ClassIndex _classIndex = new ClassIndex(this);

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

    private final AlertMonitor _alertMonitor = new AlertMonitor();

    private final TransactionIndex _transactionIndex = new TransactionIndex(_timestampAllocator, TRANSACTION_INDEX_SIZE);

    private final Map<SessionId, List<Exchange>> _exchangePoolMap = new WeakHashMap<SessionId, List<Exchange>>();

    private final Map<ObjectName, Object> _mxbeans = new TreeMap<ObjectName, Object>();

    private final List<AlertMonitorMXBean> _alertMonitors = Collections
            .synchronizedList(new ArrayList<AlertMonitorMXBean>());

    private final Set<AccumulatorRef> _accumulators = new HashSet<AccumulatorRef>();

    private final Set<WeakReference<TimelyResource<?>>> _timelyResourceSet = new HashSet<WeakReference<TimelyResource<?>>>();

    private final WeakHashMap<SessionId, CLI> _cliSessionMap = new WeakHashMap<SessionId, CLI>();

    private boolean _readRetryEnabled;

    private volatile SplitPolicy _defaultSplitPolicy = DEFAULT_SPLIT_POLICY;

    private volatile JoinPolicy _defaultJoinPolicy = DEFAULT_JOIN_POLICY;

    private volatile List<FatalErrorException> _fatalErrors = new ArrayList<FatalErrorException>();

    private volatile CommitPolicy _defaultCommitPolicy = DEFAULT_TRANSACTION_COMMIT_POLICY;

    private volatile long _commitLeadTime = DEFAULT_COMMIT_LEAD_TIME_MS;

    private volatile long _commitStallTime = DEFAULT_COMMIT_STALL_TIME_MS;

    private final ThreadLocal<SoftReference<int[]>> _intArrayThreadLocal = new ThreadLocal<SoftReference<int[]>>();

    private final ThreadLocal<SoftReference<Key>> _keyThreadLocal = new ThreadLocal<SoftReference<Key>>();

    private final ThreadLocal<SoftReference<Value>> _valueThreadLocal = new ThreadLocal<SoftReference<Value>>();

    private final AtomicLong _uniqueCounter = new AtomicLong();

    private volatile Volume _lockVolume;

    /**
     * Construct a hollow Persistit instance. To be useful, the instance must
     * receive a <code>Configuration</code> through one of the methods
     * {@link #setConfiguration(Configuration)},
     * {@link #setProperties(Properties)} or
     * {@link #setPropertiesFromFile(String)}. The instance must then be started
     * by the {@link #initialize()} method.
     */
    public Persistit() {

    }

    /**
     * Construct a Persistit instance with the supplied
     * <code>Configuration</code> and then initialize it. The code <code><pre>
     * Persistit db = new Persistit(configuration);
     * </pre></code> is equivalent to <code><pre>
     * Persistit db = new Persistit();
     * db.setConfiguration(configuration);
     * db.intialize();
     * </pre></code>
     * 
     * @see #setConfiguration(Configuration)
     * @param configuration
     *            the Configuration
     */
    public Persistit(final Configuration configuration) throws PersistitException {
        setConfiguration(configuration);
        initialize();
    }

    /**
     * Construct a Persistit instance with a <code>Configuration</code> derived
     * from the supplied <code>Properties</code> instance. The code <code><pre>
     * Persistit db = new Persistit(properties);
     * </pre></code> is equivalent to <code><pre>
     * Persistit db = new Persistit();
     * db.setProperties(properties);
     * db.intialize();
     * </pre></code>
     * 
     * @see #setProperties(Properties)
     * @param properties
     *            the Properties
     */
    public Persistit(final Properties properties) throws PersistitException {
        setProperties(properties);
        initialize();
    }

    /**
     * Receive the supplied <code>Configuration</code> which will be used to
     * control the creation of a functional <code>Persistit</code> within the
     * {@link #initialize()} method. The configuration may only be set once.
     * 
     * @param configuration
     * @throws IllegalStateException
     *             if the <code>Configuration</code> has already been set
     */
    public synchronized void setConfiguration(final Configuration configuration) {
        if (_configuration != null) {
            throw new IllegalStateException("Configuration has already been set");
        }
        _configuration = configuration;
    }

    /**
     * Sets a <code>Configuration</code> derived from <code>Properties</code>
     * read from the supplied file name.
     * 
     * @see #setConfiguration(Configuration)
     * @param propertiesFileName
     *            path to properties file
     * @throws PersistitException
     *             if the properties file is invalid
     * @throws IllegalStateException
     *             if the <code>Configuration</code> has already been set
     */
    public void setPropertiesFromFile(final String propertiesFileName) throws PersistitException {
        final Configuration configuration = new Configuration();
        configuration.readPropertiesFile(propertiesFileName);
        setConfiguration(configuration);
    }

    /**
     * Sets a <code>Configuration</code> derived from the supplied
     * <code>Properties</code>.
     * 
     * @see #setConfiguration(Configuration)
     * @param properties
     *            the Properties from which to build a
     *            <code>Configuration</code>
     * @throws IllegalStateException
     *             if the <code>Configuration</code> has already been set
     */
    public void setProperties(final Properties properties) {
        setConfiguration(new Configuration(properties));
    }

    /**
     * <p>
     * Initialize Persistit using the <code>Configuration</code> supplied by the
     * {@link #setConfiguration(Configuration)} method or derived from
     * properties supplied by {@link #setProperties(Properties)} or
     * {@link #setPropertiesFromFile(String)}.
     * </p>
     * <p>
     * If no configuration has been set, this method reverts to its legacy
     * behavior which is to load a configuration from a default properties file.
     * The name of this file is supplied by the system property
     * <code>com.persistit.properties</code>. If that property is not specified,
     * the default file path is <code>./persistit.properties</code> in the
     * current working directory. If Persistit has already been initialized,
     * this method does nothing. This method is thread-safe; if multiple threads
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
    public synchronized void initialize() throws PersistitException {
        if (isInitialized()) {
            return;
        }
        if (_configuration == null) {
            final Configuration config = new Configuration();
            config.readPropertiesFile();
            _configuration = config;
        }
        try {
            _closed.set(false);

            initializeLogging();
            initializeManagement();
            initializeOther();
            initializeRecovery();
            initializeJournal();
            initializeBufferPools();
            initializeVolumes();
            startJournal();
            startBufferPools();
            preloadBufferPools();
            initializeClassIndex();
            finishRecovery();
            startTransactionIndexPollTask();
            flush();
            _checkpointManager.checkpoint();
            _journalManager.pruneObsoleteTransactions();
            startCheckpointManager();
            startCleanupManager();
            _initialized.set(true);
        } finally {
            if (!isInitialized()) {
                releaseAllResources();
                _configuration = null;
            }
        }
    }

    /**
     * <p>
     * Initialize Persistit using the supplied properties file path. If
     * Persistit has already been initialized, this method does nothing. This
     * method is thread-safe; if multiple threads concurrently attempt to invoke
     * this method, one of the threads will actually perform the initialization
     * and the other threads will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * <p>
     * This method is deprecated. Applications should instead call
     * 
     * <pre>
     * <code>
     * setPropertiesFromFile(propertiesFileName);
     * initialize();
     * </code>
     * </pre>
     * 
     * </p>
     * 
     * @param propertiesFileName
     *            The path to the properties file.
     * @throws PersistitException
     * @throws IOException
     */
    @Deprecated
    public void initialize(final String propertiesFileName) throws PersistitException {
        if (!isInitialized()) {
            setPropertiesFromFile(propertiesFileName);
            initialize();
        }
    }

    /**
     * <p>
     * Initialize Persistit using the supplied <code>java.util.Properties</code>
     * instance. Applications can use this method to supply computed Properties
     * rather than reading them from a file. If Persistit has already been
     * initialized, this method does nothing. This method is thread-safe; if
     * multiple threads concurrently attempt to invoke this method, one of the
     * threads will actually perform the initialization and the other threads
     * will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * <p>
     * This method is deprecated. Applications should instead call
     * 
     * <pre>
     * <code>
     * setProperties(properties);
     * initialize();
     * </code>
     * </pre>
     * 
     * </p>
     * 
     * @param properties
     *            The <code>Properties</code> instance from which to build the
     *            configuration
     * @throws PersistitException
     * @throws IOException
     */
    @Deprecated
    public void initialize(final Properties properties) throws PersistitException {
        if (!isInitialized()) {
            setProperties(properties);
            initialize();
        }
    }

    /**
     * <p>
     * Initialize Persistit using the supplied {@link Configuration}. If
     * Persistit has already been initialized, this method does nothing. This
     * method is thread-safe; if multiple threads concurrently attempt to invoke
     * this method, one of the threads will actually perform the initialization
     * and the other threads will do nothing.
     * </p>
     * <p>
     * Note that Persistit starts non-daemon threads that will keep a JVM from
     * exiting until {@link #close} is invoked. This is to ensure that all
     * pending updates are written before the JVM exit.
     * </p>
     * <p>
     * This method is deprecated. Applications should instead call
     * 
     * <pre>
     * <code>
     * setConfiguration(configuration);
     * initialize();
     * </code>
     * </pre>
     * 
     * </p>
     * 
     * @param configuration
     *            The <code>Configuration</code> from which to initialize
     *            Persistit
     * @throws PersistitException
     * @throws IOException
     */
    @Deprecated
    public void initialize(final Configuration configuration) throws PersistitException {
        if (!isInitialized()) {
            setConfiguration(configuration);
            initialize();
        }
    }

    private void initializeLogging() throws PersistitException {
        try {
            _logFlusher = new LogFlusher();
            _logFlusher.start();

            getPersistitLogger().open();
            final String logLevel = _configuration.getLogging();
            if (logLevel != null && getPersistitLogger() instanceof DefaultPersistitLogger) {
                ((DefaultPersistitLogger) getPersistitLogger()).setLevel(logLevel);
            }
            _logBase.configure(getPersistitLogger());
            _logBase.start.log(_startTime);
            _logBase.copyright.log(copyright());
        } catch (final Exception e) {
            System.err.println("Persistit(tm) Logging is disabled due to " + e);
            if (e.getMessage() != null && e.getMessage().length() > 0) {
                System.err.println(e.getMessage());
            }
            e.printStackTrace();
        }

    }

    private void initializeRecovery() throws PersistitException {
        final String journalPath = _configuration.getJournalPath();
        _recoveryManager.init(journalPath);
        _recoveryManager.buildRecoveryPlan();
    }

    void initializeJournal() throws PersistitException {
        final String journalPath = _configuration.getJournalPath();
        final long journalSize = _configuration.getJournalSize();

        _journalManager.init(_recoveryManager, journalPath, journalSize);
        _journalManager.setAppendOnly(_configuration.isAppendOnly());
        _journalManager.setIgnoreMissingVolumes(_configuration.isIgnoreMissingVolumes());
    }

    private void initializeBufferPools() {
        for (final BufferPoolConfiguration config : _configuration.getBufferPoolMap().values()) {
            final int poolSize = config.computeBufferCount(getAvailableHeap());
            if (poolSize > 0) {
                final int bufferSize = config.getBufferSize();
                _logBase.allocateBuffers.log(poolSize, bufferSize);
                final BufferPool pool = new BufferPool(poolSize, bufferSize, this);
                _bufferPoolTable.put(bufferSize, pool);
                if (_configuration.isJmxEnabled()) {
                    registerBufferPoolMXBean(bufferSize);
                }
            }
        }
    }

    private void initializeVolumes() throws PersistitException {
        for (final VolumeSpecification volumeSpecification : _configuration.getVolumeList()) {
            Volume volume = _journalManager.getVolumeByName(volumeSpecification.getName());
            if (volume == null) {
                volume = new Volume(volumeSpecification);
            } else {
                volume.overwriteSpecification(volumeSpecification);
            }
            _logBase.openVolume.log(volumeSpecification.getName(), volumeSpecification.getAbsoluteFile());
            volume.open(this);
        }
    }

    private void initializeManagement() {
        final String rmiHost = _configuration.getRmiHost();
        final int rmiPort = _configuration.getRmiPort();
        final int serverPort = _configuration.getRmiServerPort();
        final boolean enableJmx = _configuration.isJmxEnabled();

        if (rmiHost != null || rmiPort > 0) {
            final ManagementImpl management = (ManagementImpl) getManagement();
            management.register(rmiHost, rmiPort, serverPort);
        }
        if (enableJmx) {
            registerMXBeans();
        }
    }

    private void initializeOther() {
        // Set up the parent CoderManager for this instance.
        final DefaultCoderManager cm = new DefaultCoderManager(this, _configuration.getSerialOverride());
        _coderManager.set(cm);
        if (_configuration.isShowGUI()) {
            try {
                setupGUI(true);
            } catch (final Exception e) {
                _logBase.configurationError.log(e);
            }
        }
        _defaultSplitPolicy = _configuration.getSplitPolicy();
        _defaultJoinPolicy = _configuration.getJoinPolicy();
        _defaultCommitPolicy = _configuration.getCommitPolicy();
        _enableBufferInventory.set(_configuration.isBufferInventoryEnabled());
    }

    private void initializeClassIndex() throws PersistitException {
        _classIndex.initialize();
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

    void recordBufferPoolInventory() {
        final long timestamp = _timestampAllocator.getCurrentTimestamp();
        if (_enableBufferInventory.get()) {
            for (final BufferPool pool : _bufferPoolTable.values()) {
                try {
                    pool.recordBufferInventory(timestamp);
                } catch (final PersistitException e) {
                    getLogBase().bufferInventoryException.log(e);
                }
            }
        }
    }

    void preloadBufferPools() throws PersistitException {
        if (_configuration.isBufferPreloadEnabled()) {
            for (final BufferPool pool : _bufferPoolTable.values()) {
                pool.preloadBufferInventory();
            }
        }
    }

    void startJournal() throws PersistitException {
        _journalManager.startJournal();
    }

    void finishRecovery() throws PersistitException, TestException {
        _recoveryManager.applyAllRecoveredTransactions(_recoveryManager.getDefaultCommitListener(),
                _recoveryManager.getDefaultRollbackListener());
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
     */
    private void registerMXBeans() {
        try {
            registerMBean(getManagement(), ManagementMXBean.class, ManagementMXBean.MXBEAN_NAME);
            registerMBean(_ioMeter, IOMeterMXBean.class, IOMeterMXBean.MXBEAN_NAME);
            registerMBean(_checkpointManager, CheckpointManagerMXBean.class, CheckpointManagerMXBean.MXBEAN_NAME);
            registerMBean(_cleanupManager, CleanupManagerMXBean.class, CleanupManagerMXBean.MXBEAN_NAME);
            registerMBean(_transactionIndex, TransactionIndexMXBean.class, TransactionIndexMXBean.MXBEAN_NAME);
            registerMBean(_journalManager, JournalManagerMXBean.class, JournalManagerMXBean.MXBEAN_NAME);
            registerMBean(_recoveryManager, RecoveryManagerMXBean.class, RecoveryManagerMXBean.MXBEAN_NAME);
            registerMBean(_alertMonitor, AlertMonitorMXBean.class, AlertMonitorMXBean.MXBEAN_NAME);
        } catch (final Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void registerBufferPoolMXBean(final int bufferSize) {
        try {
            final BufferPoolMXBean bean = new BufferPoolMXBeanImpl(this, bufferSize);
            registerMBean(bean, BufferPoolMXBean.class, BufferPoolMXBeanImpl.mbeanName(bufferSize));
        } catch (final Exception exception) {
            _logBase.mbeanException.log(exception);
        }
    }

    private void registerMBean(final Object mbean, final Class<?> mbeanInterface, final String name) throws Exception {
        final MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        final ObjectName on = new ObjectName(name);
        NotificationEmitter emitter = null;
        if (mbean instanceof AlertMonitor) {
            final AlertMonitor monitor = (AlertMonitor) mbean;
            monitor.setObjectName(on);
            emitter = monitor;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final MXBeanWrapper wrapper = new MXBeanWrapper(mbean, mbeanInterface, emitter);
        server.registerMBean(wrapper, on);

        _logBase.mbeanRegistered.log(on);
        _mxbeans.put(on, mbean);
        if (mbean instanceof AlertMonitorMXBean) {
            _alertMonitors.add((AlertMonitorMXBean) mbean);
        }
    }

    Map<ObjectName, Object> getMXBeans() {
        return Collections.unmodifiableMap(_mxbeans);
    }

    private void unregisterMXBeans() {
        final MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        for (final ObjectName on : _mxbeans.keySet()) {
            try {
                server.unregisterMBean(on);
                _logBase.mbeanUnregistered.log(on);
            } catch (final InstanceNotFoundException exception) {
                // ignore
            } catch (final Exception exception) {
                _logBase.mbeanException.log(exception);
            }
        }
    }

    void addVolume(final Volume volume) throws VolumeAlreadyExistsException {
        synchronized (_volumes) {
            Volume otherVolume;
            otherVolume = getVolume(volume.getName());
            if (otherVolume != null) {
                throw new VolumeAlreadyExistsException("Volume " + otherVolume);
            }
            _volumes.add(volume);
        }
    }

    void removeVolume(final Volume volume) throws PersistitInterruptedException {
        synchronized (_volumes) {
            _volumes.remove(volume);
        }
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
    public Exchange getExchange(final Volume volume, final String treeName, final boolean create)
            throws PersistitException {
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
    public Exchange getExchange(final String volumeName, final String treeName, final boolean create)
            throws PersistitException {
        final Volume volume = getVolume(volumeName);
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
    public void releaseExchange(final Exchange exchange) {
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
    public void releaseExchange(final Exchange exchange, final boolean secure) {
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
        synchronized (_volumes) {
            return new ArrayList<Volume>(_volumes);
        }
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
    public List<Tree> getSelectedTrees(final TreeSelector selector) throws PersistitException {
        final List<Tree> list = new ArrayList<Tree>();
        final List<Volume> volumes = getVolumes();
        for (final Volume volume : volumes) {
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
        final VolumeSpecification volumeSpec = _configuration.volumeSpecification(vstring);
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
     * property <code>tmpvolpagesize</code>. The default value is determined by
     * the {@link BufferPool} having the largest page size.
     * <p />
     * The backing store file for a temporary volume is created in the directory
     * specified by the configuration property <code>tmpvoldir</code>, or if
     * unspecified, the system temporary directory..
     * 
     * @return the temporary <code>Volume</code>.
     * @throws PersistitException
     */
    public Volume createTemporaryVolume() throws PersistitException {
        return createTemporaryVolume(temporaryVolumePageSize());
    }

    /**
     * Create a temporary volume. A temporary volume is not durable; it should
     * be used to hold temporary data such as intermediate sort or aggregation
     * results that can be recreated in the event the system restarts.
     * <p />
     * The backing store file for a temporary volume is created in the directory
     * specified by the configuration property <code>tmpvoldir</code>, or if
     * unspecified, the system temporary directory.
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
        final String directoryName = getConfiguration().getTmpVolDir();
        final File directory = directoryName == null ? null : new File(directoryName);
        return Volume.createTemporaryVolume(this, pageSize, directory);
    }

    private int temporaryVolumePageSize() {
        int pageSize = _configuration.getTmpVolPageSize();
        if (pageSize == 0) {
            for (final int size : _bufferPoolTable.keySet()) {
                if (size > pageSize) {
                    pageSize = size;
                }
            }
        }
        return pageSize;
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
            final boolean deleted = volume.delete();
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

    public Configuration getConfiguration() {
        return _configuration;
    }

    @Deprecated
    public Properties getProperties() {
        return getConfiguration().getProperties();
    }

    @Deprecated
    public String getProperty(final String key) {
        return getConfiguration().getProperty(key);
    }

    @Deprecated
    public String substituteProperties(final String text, final Properties properties) {
        return getConfiguration().substituteProperties(text, properties);
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
    public Volume getVolume(final String name) {
        if (name == null) {
            throw new NullPointerException("Null volume name");
        }
        final List<Volume> volumes = getVolumes();
        Volume result = null;
        for (int i = 0; i < volumes.size(); i++) {
            final Volume vol = volumes.get(i);
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
        for (int i = 0; i < volumes.size(); i++) {
            final Volume vol = volumes.get(i);
            if (file.equals(vol.getAbsoluteFile())) {
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
        return getSpecialVolume(SYSTEM_VOLUME_PROPERTY_NAME, DEFAULT_SYSTEM_VOLUME_NAME);
    }

    /**
     * @return reserved temporary volume for locks
     * @throws PersistitException
     */
    public synchronized Volume getLockVolume() throws PersistitException {
        checkInitialized();
        checkClosed();
        if (_lockVolume == null) {
            final int pageSize = temporaryVolumePageSize();
            if (!Volume.isValidPageSize(pageSize)) {
                throw new IllegalArgumentException("Invalid page size " + pageSize);
            }
            final String directoryName = getConfiguration().getTmpVolDir();
            final File directory = directoryName == null ? null : new File(directoryName);
            _lockVolume = Volume.createLockVolume(this, pageSize, directory);
            _volumes.add(_lockVolume);
        }
        return _lockVolume;
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
    public Checkpoint getCurrentCheckpoint() {
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
        _journalManager.pruneObsoleteTransactions();
        final Checkpoint result = _checkpointManager.checkpoint();
        _journalManager.pruneObsoleteTransactions();
        return result;
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
     * Copy back all pages from the journal to their host Volumes. This
     * condenses the total number of journals as much as possible given the
     * current activity in the system.
     * 
     * @throws Exception
     */
    public void copyBackPages() throws Exception {
        /*
         * Up to three complete cycles needed on an idle system: 1) Outstanding
         * activity, dirty pages 2) Copy back changes made by first checkpoint
         * (accumulators, etc) 3) Journal completely caught up, rollover if big
         * enough
         */
        for (int i = 0; i < 5; ++i) {
            if (!_closed.get() && _initialized.get()) {
                _transactionIndex.updateActiveTransactionCache();
                _journalManager.pruneObsoleteTransactions();
                _checkpointManager.checkpoint();
                _journalManager.copyBack();
                final int fileCount = _journalManager.getJournalFileCount();
                final long size = _journalManager.getCurrentJournalSize();
                if ((fileCount == 1) && (size < JournalManager.ROLLOVER_THRESHOLD)) {
                    break;
                }
            } else {
                throw new PersistitClosedException();
            }
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
     * @param propName
     *            The name
     * @return the Volume
     * @throws VolumeNotFoundException
     *             if the volume was not found
     */
    private Volume getSpecialVolume(final String propName, final String dflt) throws VolumeNotFoundException {
        final String volumeName = _configuration.getSysVolume();
        synchronized (_volumes) {
            if ((_volumes.size() == 1) && (volumeName.equals(dflt))) {
                return _volumes.get(0);
            }
        }
        final Volume volume = getVolume(volumeName);
        if (volume == null) {
            throw new VolumeNotFoundException(volumeName);
        } else {
            return volume;
        }
    }

    /**
     * @param size
     *            the desired buffer size
     * @return the <code>BufferPool</code> for the specific buffer size
     */
    BufferPool getBufferPool(final int size) {
        return _bufferPoolTable.get(new Integer(size));
    }

    /**
     * @return A HashMap containing all the <code>BufferPool</code>s keyed by
     *         their size.
     */
    HashMap<Integer, BufferPool> getBufferPoolHashMap() {
        return _bufferPoolTable;
    }

    /**
     * Remove any sessions that have expired and close transactions associated
     * with them. Also flush statistics for all known volumes.
     */
    void cleanup() {
        closeZombieTransactions(false);
        _transactionIndex.updateActiveTransactionCache();
        pruneTimelyResources();
    }

    /**
     * Reports status of the <code>max</code> longest-running transactions, in
     * order from oldest to youngest.
     * 
     * @param max
     * @return status of the <code>max</code> longest-running transactions, in
     *         order from oldest to youngest, reported as a String with one line
     *         per transaction.
     */
    public String transactionReport(final int max) {
        final long[] timestamps = _transactionIndex.oldestTransactions(max);
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
     * updates are written before the JVM exits. Therefore the recommended
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
     */
    public void close(final boolean flush) throws PersistitException {
        if (_initialized.get() && !_closed.get()) {
            synchronized (this) {
                // Wait for UI to go down.
                while (_suspendShutdown.get()) {
                    try {
                        wait(SHORT_DELAY);
                    } catch (final InterruptedException ie) {
                        throw new PersistitInterruptedException(ie);
                    }
                }
            }
            recordBufferPoolInventory();
            _cleanupManager.close(flush);
            waitForIOTaskStop(_cleanupManager);

            getTransaction().close();
            cleanup();

            if (_lockVolume != null) {
                _lockVolume.close();
            }

            final List<Volume> volumes;
            synchronized (this) {
                volumes = new ArrayList<Volume>(_volumes);
            }

            if (flush) {
                for (final Volume volume : volumes) {
                    volume.getStorage().flush();
                }
            }

            _checkpointManager.close(flush);
            waitForIOTaskStop(_checkpointManager);

            _closed.set(true);

            for (final BufferPool pool : _bufferPoolTable.values()) {
                pool.close();
            }

            _journalManager.close();
            final IOTaskRunnable task = _transactionIndex.close();
            waitForIOTaskStop(task);

            interruptActiveThreads(SHORT_DELAY);
            closeZombieTransactions(true);

            for (final Volume volume : volumes) {
                volume.close();
            }

            if (flush) {
                for (final BufferPool pool : _bufferPoolTable.values()) {
                    final int count = pool.getDirtyPageCount();
                    if (count > 0) {
                        _logBase.strandedPages.log(pool, count);
                    }
                }
            }
            pollAlertMonitors(true);
        }
        releaseAllResources();
    }

    private void closeZombieTransactions(final boolean removeAllSessions) {
        final Set<SessionId> sessionIds;
        synchronized (_transactionSessionMap) {
            sessionIds = new HashSet<SessionId>(_transactionSessionMap.keySet());
        }
        for (final SessionId sessionId : sessionIds) {
            if (!sessionId.isAlive() || removeAllSessions) {
                Transaction transaction = null;
                synchronized (_transactionSessionMap) {
                    transaction = _transactionSessionMap.remove(sessionId);
                }
                if (transaction != null) {
                    try {
                        transaction.close();
                    } catch (final Exception e) {
                        _logBase.exception.log(e);
                    }
                }
            }
        }
    }

    private void interruptActiveThreads(final long timeout) throws PersistitInterruptedException {
        final long expires = System.currentTimeMillis() + timeout;
        boolean remaining = false;
        do {
            final Map<SessionId, Transaction> copy;
            synchronized (_transactionSessionMap) {
                copy = new HashMap<SessionId, Transaction>(_transactionSessionMap);
            }
            for (final Entry<SessionId, Transaction> entry : copy.entrySet()) {
                final SessionId sessionId = entry.getKey();
                final Transaction txn = entry.getValue();
                if (sessionId.isAlive() && txn.isActive()) {
                    if (sessionId.interrupt()) {
                        _logBase.interruptedAtClose.log(sessionId.ownerName());
                    }
                    remaining = true;
                }
            }
            if (remaining) {
                Util.spinSleep();
            }
        } while (remaining && System.currentTimeMillis() < expires);
    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the writer and collector
     * processes. This method should be used only by tests.
     */
    public void crash() {
        final JournalManager journalManager = _journalManager;
        if (journalManager != null) {
            try {
                journalManager.crash();
            } catch (final IOException e) {
                _logBase.exception.log(e);
            }
        }
        //
        // Even on simulating a crash we need to try to close
        // the volume files - otherwise there will be left over channels
        // and FileLocks that interfere with subsequent tests.
        //
        final List<Volume> volumes = getVolumes();
        for (final Volume volume : volumes) {
            try {
                volume.getStorage().close();
            } catch (final PersistitException pe) {
                // ignore -
            }
        }
        final Map<Integer, BufferPool> buffers = _bufferPoolTable;
        if (buffers != null) {
            for (final BufferPool pool : buffers.values()) {
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
        final FatalErrorException exception = new FatalErrorException(msg, cause);
        synchronized (_fatalErrors) {
            if (_fatalErrors.size() < MAX_FATAL_ERROR_MESSAGES) {
                _fatalErrors.add(exception);
            }
        }
        _fatal.set(true);
        _closed.set(true);
        throw exception;
    }

    private void releaseAllResources() {

        unregisterMXBeans();
        try {
            if (_logger != null) {
                _logBase.end.log(System.currentTimeMillis());
                _logger.close();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
        if (_management != null) {
            _management.unregister();
            _management = null;
        }
        if (_logFlusher != null) {
            _logFlusher.interrupt();
        }
        _logFlusher = null;
        /*
         * The following are synchronized to ensure visibility
         */
        synchronized (_accumulators) {
            _accumulators.clear();
        }
        synchronized (_volumes) {
            _volumes.clear();
        }

        synchronized (this) {
            _alertMonitors.clear();
            _bufferPoolTable.clear();
            _intArrayThreadLocal.set(null);
            _keyThreadLocal.set(null);
            _valueThreadLocal.set(null);
            _initialized.set(false);
            _sessionIdThreadLocal.remove();
            _cleanupManager.clear();
            _configuration = null;
        }
        synchronized (_exchangePoolMap) {
            _exchangePoolMap.clear();
        }
        synchronized (_transactionSessionMap) {
            _transactionSessionMap.clear();
        }
        synchronized (_cliSessionMap) {
            _cliSessionMap.clear();
        }
        synchronized (_fatalErrors) {
            _fatalErrors.clear();
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
        final List<Volume> volumes = getVolumes();
        for (final Volume volume : volumes) {
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

    void flushStatistics() throws PersistitException {
        final List<Volume> volumes = getVolumes();
        for (final Volume volume : volumes) {
            volume.getStructure().flushStatistics();
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
            } catch (final InterruptedException ie) {
                break;
            }
            final long now = System.currentTimeMillis();
            if (now > _nextCloseTime) {
                _logBase.waitForClose.log((_nextCloseTime - _beginCloseTime) / NS_PER_S);
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
    public void force() throws PersistitException {
        if (_closed.get() || !_initialized.get()) {
            return;
        }
        final List<Volume> volumes = getVolumes();

        for (int index = 0; index < volumes.size(); index++) {
            final Volume volume = volumes.get(index);
            if (!volume.getStorage().isReadOnly()) {
                volume.getStorage().force();
            }
        }
        _journalManager.force();
    }

    void checkInitialized() throws PersistitClosedException, PersistitInterruptedException {
        if (!isInitialized()) {
            throw new PersistitClosedException();
        }
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
     * This property can be configured with the configuration property
     * {@value com.persistit.Configuration#COMMIT_POLICY_PROPERTY_NAME}.
     * 
     * @return The default system commit policy.
     */
    public CommitPolicy getDefaultTransactionCommitPolicy() {
        return _defaultCommitPolicy;
    }

    /**
     * Set the current default transaction commit property. This policy is
     * applied to transactions that call {@link Transaction#commit()}. Note that
     * {@link Transaction#commit(CommitPolicy)} permits control on a
     * per-transaction basis. The supplied policy value may not be
     * <code>null</code>.
     * 
     * @param policy
     *            The policy.
     */
    public void setDefaultTransactionCommitPolicy(final CommitPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("CommitPolicy may not be null");
        }
        _defaultCommitPolicy = policy;
    }

    /**
     * Set the current default transaction commit property by name. This policy
     * is applied to transactions that call {@link Transaction#commit()}. Note
     * that {@link Transaction#commit(CommitPolicy)} permits control on a
     * per-transaction basis. The supplied policy value must be one of "HARD",
     * "GROUP" or "SOFT".
     * 
     * @param policyName
     *            The policy name: "SOFT", "HARD" or "GROUP"
     */
    public void setDefaultTransactionCommitPolicy(final String policyName) {
        CommitPolicy policy;
        try {
            policy = CommitPolicy.valueOf(policyName.toUpperCase());
            setDefaultTransactionCommitPolicy(policy);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid CommitPolicy name: " + policyName);
        }
    }

    long getTransactionCommitLeadTime() {
        return _commitLeadTime;
    }

    void setTransactionCommitleadTime(final long time) {
        _commitLeadTime = Util.rangeCheck(time, 0, MAX_COMMIT_LEAD_TIME_MS);
    }

    long getTransactionCommitStallTime() {
        return _commitStallTime;
    }

    void setTransactionCommitStallTime(final long time) {
        _commitStallTime = Util.rangeCheck(time, 0, MAX_COMMIT_STALL_TIME_MS);
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
    public void setCoderManager(final CoderManager coderManager) {
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

    Class<?> classForHandle(final int handle) {
        final ClassInfo ci = _classIndex.lookupByHandle(handle);
        if (ci == null)
            return null;
        else
            return ci.getDescribedClass();
    }

    KeyCoder lookupKeyCoder(final Class<?> cl) {
        final CoderManager cm = _coderManager.get();
        if (cm == null) {
            return null;
        }
        return cm.lookupKeyCoder(cl);
    }

    ValueCoder lookupValueCoder(final Class<?> cl) {
        final CoderManager cm = _coderManager.get();
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

    public AlertMonitor getAlertMonitor() {
        return _alertMonitor;
    }

    TransactionIndex getTransactionIndex() {
        return _transactionIndex;
    }

    /**
     * Replaces the current logger implementation.
     * 
     * @see com.persistit.logging.DefaultPersistitLogger
     * @see com.persistit.logging.JDK14LoggingAdapter
     * @see com.persistit.logging.Log4JAdapter
     * @param logger
     *            The new logger implementation
     */
    public void setPersistitLogger(final PersistitLogger logger) {
        _logger = logger;
    }

    /**
     * @return The current logger.
     */
    public PersistitLogger getPersistitLogger() {
        if (_logger == null) {
            _logger = new DefaultPersistitLogger(_configuration.getLogFile());
        }
        return _logger;
    }

    /**
     * Called periodically by the LogFlusher thread to emit pending
     * {@link AlertMonitorMXBean} messages to the log.
     */
    void pollAlertMonitors(final boolean force) {
        for (final AlertMonitorMXBean monitor : _alertMonitors) {
            try {
                monitor.poll(force);
            } catch (final Exception e) {
                _logBase.exception.log(e);
            }
        }
    }

    /**
     * Convenience method that performs an integrity check on all open
     * <code>Volume</code>s and reports detailed results to
     * {@link java.lang.System#out}.
     * 
     * @throws PersistitException
     */
    public void checkAllVolumes() throws PersistitException {
        final IntegrityCheck icheck = new IntegrityCheck(this);
        final List<Volume> volumes = getVolumes();
        for (int index = 0; index < volumes.size(); index++) {
            final Volume volume = volumes.get(index);
            System.out.println("Checking " + volume + " ");
            try {
                icheck.checkVolume(volume);
            } catch (final Exception e) {
                System.out.println(e + " while performing IntegrityCheck on " + volume);
            }
        }
        System.out.println("  " + icheck.toString(true));
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
     * Attempts to open the diagnostic GUI that displays some useful information
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
    public void setupGUI(final boolean suspendShutdown) throws IllegalAccessException, InstantiationException,
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
    public void setShutdownSuspended(final boolean suspended) {
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
    public void setUpdateSuspended(final boolean suspended) {
        _suspendUpdates.set(suspended);
    }

    void addTimelyResource(final TimelyResource<? extends Version> resource) {
        synchronized (_timelyResourceSet) {
            _timelyResourceSet.add(new WeakReference<TimelyResource<? extends Version>>(resource));
        }
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
            for (final AccumulatorRef ref : _accumulators) {
                if (ref._checkpointRef != null) {
                    checkpointCount++;
                }
            }
        }
        if (checkpointCount > 0 && (checkpointCount % ACCUMULATOR_CHECKPOINT_THRESHOLD) == 0) {
            try {
                _checkpointManager.createCheckpoint();
            } catch (final PersistitException e) {
                _logBase.exception.log(e);
            }
        }
    }

    /**
     * Remove an Accumulator from the active list. This will cause it to not be
     * checkpointed or otherwise known about.
     * 
     * @param accumulator
     *            Accumulator to remove
     */
    void removeAccumulator(final Accumulator accumulator) {
        synchronized (_accumulators) {
            _accumulators.remove(accumulator.getAccumulatorRef());
        }
    }

    List<Accumulator> takeCheckpointAccumulators(final long timestamp) {
        final List<Accumulator> result = new ArrayList<Accumulator>();
        synchronized (_accumulators) {
            for (final Iterator<AccumulatorRef> refIterator = _accumulators.iterator(); refIterator.hasNext();) {
                final AccumulatorRef ref = refIterator.next();
                if (!ref.isLive()) {
                    refIterator.remove();
                }
                final Accumulator acc = ref.takeCheckpointRef(timestamp);
                if (acc != null) {
                    result.add(acc);
                }
            }
            Collections.sort(result, Accumulator.SORT_COMPARATOR);
        }
        return result;
    }

    void pruneTimelyResources() {
        final List<TimelyResource<?>> resourcesToPrune = new ArrayList<TimelyResource<?>>();
        synchronized (_timelyResourceSet) {
            for (final Iterator<WeakReference<TimelyResource<?>>> iter = _timelyResourceSet.iterator(); iter.hasNext();) {
                final WeakReference<TimelyResource<?>> ref = iter.next();
                final TimelyResource<?> resource = ref.get();
                if (resource != null) {
                    resourcesToPrune.add(resource);
                }
            }
        }
        for (final TimelyResource<?> resource : resourcesToPrune) {
            try {
                resource.prune();
            } catch (final PersistitException e) {
                _logBase.timelyResourcePruneException.log(e, resource);
            }
        }
        synchronized (_timelyResourceSet) {
            for (final Iterator<WeakReference<TimelyResource<?>>> iter = _timelyResourceSet.iterator(); iter.hasNext();) {
                final WeakReference<TimelyResource<?>> ref = iter.next();
                if (ref.get() == null) {
                    iter.remove();
                }
            }
        }
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

    int[] getThreadLocalIntArray(final int size) {
        final SoftReference<int[]> ref = _intArrayThreadLocal.get();
        if (ref != null) {
            final int[] ints = ref.get();
            if (ints != null && ints.length >= size) {
                return ints;
            }
        }
        final int[] ints = new int[size];
        _intArrayThreadLocal.set(new SoftReference<int[]>(ints));
        return ints;
    }

    Key getThreadLocalKey() {
        final SoftReference<Key> ref = _keyThreadLocal.get();
        if (ref != null) {
            final Key key = ref.get();
            if (key != null) {
                return key;
            }
        }
        final Key key = new Key(this);
        _keyThreadLocal.set(new SoftReference<Key>(key));
        return key;
    }

    Value getThreadLocalValue() {
        final SoftReference<Value> ref = _valueThreadLocal.get();
        if (ref != null) {
            final Value value = ref.get();
            if (value != null) {
                return value;
            }
        }
        final Value value = new Value(this);
        _valueThreadLocal.set(new SoftReference<Value>(value));
        return value;
    }

    long unique() {
        return _uniqueCounter.incrementAndGet();
    }

    private final static String[] ARG_TEMPLATE = { "_flag|g|Start AdminUI",
            "_flag|i|Perform IntegrityCheck on all volumes", "_flag|w|Wait until AdminUI exists",
            "_flag|c|Perform copy-back", "properties|string|Property file name",
            "cliport|int:-1:1024:99999999|Port on which to start a simple command-line interface server",
            "script|string|Pathname of CLI script to execute", };

    /**
     * Perform various utility functions.
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
    public static void main(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("Persistit", args, ARG_TEMPLATE).strict();
        if (ap.isUsageOnly()) {
            return;
        }

        Persistit persistit = null;
        final String propertiesFileName = ap.getStringValue("properties");
        if (!propertiesFileName.isEmpty()) {
            persistit = new Persistit();
            persistit.setPropertiesFromFile(propertiesFileName);
            persistit.initialize();
        }
        final String scriptName = ap.getStringValue("script");

        final int cliport = ap.getIntValue("cliport");

        if (cliport > -1 && !propertiesFileName.isEmpty()) {
            throw new IllegalArgumentException("Specify only one: properties or cliport");
        }

        if (cliport > 1) {
            System.out.printf("Starting a Persistit CLI server on port %d\n", cliport);
            final Task task = CLI.cliserver(cliport);
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
            final boolean gui = ap.isFlag('g');
            final boolean icheck = ap.isFlag('i');
            final boolean wait = ap.isFlag('w');
            final boolean copy = ap.isFlag('c');

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
            } catch (final Exception e) {
                e.printStackTrace();
                persistit.setShutdownSuspended(false);
            } finally {
                persistit.close();
            }
        }
    }

}
