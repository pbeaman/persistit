package com.persistit;

import static com.persistit.JournalRecord.OVERHEAD;
import static com.persistit.JournalRecord.getLength;
import static com.persistit.JournalRecord.getTimestamp;
import static com.persistit.JournalRecord.getType;
import static com.persistit.JournalRecord.isValidType;
import static com.persistit.Util.println;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.JournalManager.JournalNotClosedException;
import com.persistit.JournalManager.PageNode;
import com.persistit.JournalManager.TransactionStatus;
import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.JournalManager.VolumeDescriptor;
import com.persistit.JournalRecord.CP;
import com.persistit.JournalRecord.DR;
import com.persistit.JournalRecord.DT;
import com.persistit.JournalRecord.IT;
import com.persistit.JournalRecord.IV;
import com.persistit.JournalRecord.JE;
import com.persistit.JournalRecord.JH;
import com.persistit.JournalRecord.PA;
import com.persistit.JournalRecord.PM;
import com.persistit.JournalRecord.SR;
import com.persistit.JournalRecord.TC;
import com.persistit.JournalRecord.TM;
import com.persistit.JournalRecord.TS;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

/**
 * Manages the recovery process during Persistit startup. This method is called
 * every time Persistit starts up, even if the previous shutdown was normal.
 * 
 * Phase 1:
 * 
 * Find the most recent valid journal file. This is the "keystone" journal file
 * because everything will be based on its content. Read its JH (JournalHeader)
 * record. Validate all fields in the JH.
 * 
 * Read remaining records in the keystone journal file. Included are IV, PM and
 * TM records that provide an initial load of the pageMap, liveTransactionMap
 * and volume/handle maps for JournalManager. Included also is a keystone CP
 * (checkpoint) record; the presence of a CP record indicates that the IV, PM
 * and TM records constitute a complete checkpoint of the journal to the
 * specified timestamp. Absence of a CP before the scan terminates indicates
 * that the journal file is not a complete snapshot, and therefore the chosen
 * keystone journal file is not valid. In this event, restart Phase 1 using the
 * immediate predecessor file.
 * 
 * The scan stops when recovery finds a JE "journal end" record, end-of-file or
 * an invalid record. The presence of a valid JE record indicates a clean
 * shutdown.
 * 
 * 
 * Phase 2:
 * 
 * For all journal files from base address to current address, read their JH
 * records and verify contiguity (same creation timestamp).
 * 
 * Phase 3:
 * 
 * Executed after the buffer pools have been loaded and the journal manager has
 * been instantiated: For each transaction in the transaction map, apply it if
 * its TransistionStatus is been comitted.
 * 
 * TRecords hold pointers to Transactions that must be applied during recovery,
 * providing a map of timestamp->FileAddress of transaction records that must be
 * processed to finish recovery.
 * 
 * Transactions are applied in their commit timestamp ordering so that their
 * affect on the recovered database is consistent with their original serial
 * order.
 * 
 * A checkpoint at timestamp T indicates that all pages made dirty prior to T
 * have been written to the journal; therefore any transaction with a commit
 * timestamp before T does not need to be reapplied because its effects are
 * already present in the recovered B-Trees.
 * 
 * This class is not threadsafe; it is intended to be called only during the
 * single-threaded recovery process.
 * 
 * @author peter
 * 
 */
public class RecoveryManager implements VolumeHandleLookup {

    public final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

    /**
     * Number of transactions to apply per progress log message
     */
    private final static int APPLY_TRANSACTION_LOG_COUNT = 1000;

    private final Persistit _persistit;

    //
    // These structures mirror those of JournalManager. However, after recovery
    // is complete, only some of the members of these maps will be donated to
    // JournalManager for ongoing processing.
    //
    private final SortedMap<Long, TransactionStatus> _recoveredTransactionMap = new TreeMap<Long, TransactionStatus>();

    private final Map<PageNode, PageNode> _pageMap = new HashMap<PageNode, PageNode>();

    private final Map<VolumeDescriptor, Integer> _volumeToHandleMap = new HashMap<VolumeDescriptor, Integer>();

    private final Map<Integer, VolumeDescriptor> _handleToVolumeMap = new HashMap<Integer, VolumeDescriptor>();

    private final Map<TreeDescriptor, Integer> _treeToHandleMap = new HashMap<TreeDescriptor, Integer>();

    private final Map<Integer, TreeDescriptor> _handleToTreeMap = new HashMap<Integer, TreeDescriptor>();

    private Checkpoint _lastValidCheckpoint = new Checkpoint(0, 0);

    private long _lastValidCheckpointJournalAddress;

    private final Map<Long, FileChannel> _journalFileChannels = new HashMap<Long, FileChannel>();

    private volatile int _uncommittedTransactionCount;

    private volatile int _appliedTransactionCount;

    private volatile int _errorCount;

    private volatile boolean _recoveryDisabledForTestMode;

    private File _journalFilePath;

    private File _keystoneFile;

    private long _journalCreatedTime;

    private long _keystoneCreatedTime;

    private long _blockSize;

    private long _baseAddress = 0;

    private long _keystoneAddress;

    private ByteBuffer _readBuffer;

    private int _readBufferSize = DEFAULT_BUFFER_SIZE;

    private long _readBufferAddress;

    private long _currentAddress;

    private long _recoveryStatus = Long.MIN_VALUE;

    private long _recoveryEndedAddress;

    private Exception _recoveryEndedException;

    private RecoveredTransactionActor _defaultActor = new DefaultRecoveredTransctionActor();

    // private PrintWriter _logWriter; // TODO

    interface RecoveredTransactionActor {

        void store(final long address, final long timestamp,
                final Exchange exchange) throws PersistitException;

        void removeKeyRange(final long address, final long timestamp,
                final Exchange exchange) throws PersistitException;

        void removeTree(final long address, final long timestamp,
                final Exchange exchange) throws PersistitException;
    }

    static private class DefaultRecoveredTransctionActor implements
            RecoveredTransactionActor {
        @Override
        public void store(final long address, final long timestamp,
                Exchange exchange) throws PersistitException {
            final Transaction txn = exchange.getTransaction();
            exchange.store();
        }

        @Override
        public void removeKeyRange(final long address, final long timestamp,
                Exchange exchange) throws PersistitException {
            final Transaction txn = exchange.getTransaction();
            exchange.removeKeyRange(exchange.getAuxiliaryKey1(),
                    exchange.getAuxiliaryKey2());
        }

        @Override
        public void removeTree(final long address, final long timestamp,
                Exchange exchange) throws PersistitException {
            final Transaction txn = exchange.getTransaction();
            exchange.removeTree();
        }
    }

    static File[] files(final File path) {
        final File directory;
        if (!path.isDirectory()) {
            directory = path.getParentFile() == null ? new File(".") : path
                    .getParentFile();
        } else {
            directory = path;
        }
        final String pathString = path.getPath();
        final File[] files = directory.listFiles(new FileFilter() {

            @Override
            public boolean accept(File candidate) {
                final String candidateString = candidate.getPath();
                return candidateString.startsWith(pathString)
                        && JournalManager.PATH_PATTERN.matcher(candidateString)
                                .matches();
            }
        });

        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files);
        return files;
    }

    static void validate(final long value, final File file, final long address,
            final long expected, final String message)
            throws CorruptJournalException {
        if (value == expected) {
            return;
        }

        throw new CorruptJournalException(String.format(message, file, address,
                value, expected));
    }

    static void validate(final long value, final File file, final long address,
            final long min, final long max, final String message)
            throws CorruptJournalException {
        if (value >= min && value <= max) {
            return;
        }

        throw new CorruptJournalException(String.format(message, file, address,
                value, min, max));
    }

    RecoveryManager(final Persistit persistit) {
        _persistit = persistit;
    }

    public synchronized void populateRecoveryInfo(
            final Management.RecoveryInfo info) {
        info.keystoneJournalAddress = _keystoneAddress;
        info.currentAddress = _currentAddress;
        info.recoveryStatus = _recoveryStatus;
        info.recoveryEndAddress = _recoveryEndedAddress;
        info.recoveryException = _recoveryEndedException == null ? ""
                : _recoveryEndedException.toString();
        if (_keystoneAddress > 0) {
            info.keystoneJournalFile = addressToFile(_keystoneAddress)
                    .getPath();
            if (_lastValidCheckpointJournalAddress != 0)
                info.lastValidCheckpointSystemTime = _lastValidCheckpoint
                        .getSystemTimeMillis();
            info.lastValidCheckpointTimestamp = _lastValidCheckpoint
                    .getTimestamp();
            info.lastValidCheckpointJournalFile = addressToFile(
                    _lastValidCheckpointJournalAddress).getPath();
            info.lastValidCheckpointJournalAddress = _lastValidCheckpointJournalAddress;
        } else {
            info.lastValidCheckpointSystemTime = 0;
            info.lastValidCheckpointTimestamp = 0;
            info.lastValidCheckpointJournalFile = null;
            info.lastValidCheckpointJournalAddress = 0;
        }
        info.blockSize = _blockSize;
        info.pageMapSize = _pageMap.size();
        info.baseAddress = _baseAddress;
        info.appliedTransactions = _appliedTransactionCount;
        info.committedTransactions = getCommittedCount();
        info.uncommittedTransactions = getUncommittedCount();
    }

    public void init(final String path) throws PersistitException {
        _journalFilePath = new File(path).getAbsoluteFile();
        _readBuffer = ByteBuffer.allocate(_readBufferSize);
    }

    public File getJournalFilePath() {
        return _journalFilePath;
    }

    public int getCommittedCount() {
        int count = 0;
        for (final TransactionStatus trecord : _recoveredTransactionMap
                .values()) {
            if (trecord.isCommitted()) {
                count++;
            }
        }
        return count;
    }

    public int getUncommittedCount() {
        int count = 0;
        for (final TransactionStatus trecord : _recoveredTransactionMap
                .values()) {
            if (!trecord.isCommitted()) {
                count++;
            }
        }
        return count;
    }

    public int getAppliedTransactionCount() {
        return _appliedTransactionCount;
    }

    public int getErrorCount() {
        return _errorCount;
    }

    public Checkpoint getLastValidCheckpoint() {
        return _lastValidCheckpoint;
    }

    public long getLastValidCheckpointAddress() {
        return _lastValidCheckpointJournalAddress;
    }

    public Exception getRecoveryEndedException() {
        return _recoveryEndedException;
    }

    public long getRecoveryEndedAddress() {
        return _recoveryEndedAddress;
    }

    public long getKeystoneAddress() {
        return _keystoneAddress;
    }

    public long getBaseAddress() {
        return _baseAddress;
    }

    public long getBlockSize() {
        return _blockSize;
    }

    public long getJournalCreatedTime() {
        return _journalCreatedTime;
    }

    public int getTransactionMapSize() {
        return _recoveredTransactionMap.size();
    }

    public int getPageMapSize() {
        return _pageMap.size();
    }

    public RecoveredTransactionActor getDefaultRecoveredTransactionActor() {
        return _defaultActor;
    }

    @Override
    public synchronized VolumeDescriptor lookupVolumeHandle(final int handle) {
        return _handleToVolumeMap.get(Integer.valueOf(handle));
    }

    File addressToFile(final long address) {
        return JournalManager.generationToFile(_journalFilePath, address
                / _blockSize);
    }

    /**
     * Copy PageNodes from the recovered page. This method distributes PageNodes
     * for updates that happened before the keystone checkpoint to
     * <tt>pageMap</tt> and those that happened after the keystone checkpoint to
     * <tt>branchMap</tt>. Updates in <tt>branchMap</tt> are used only in
     * recovering certain transactions (insertions with LONG_RECORD values).
     * 
     * @param pageMap
     */
    void collectRecoveredPages(final Map<PageNode, PageNode> pageMap,
            final Map<PageNode, PageNode> branchMap) {
        if (_lastValidCheckpoint != null) {
            final long lastValidTimestamp = _lastValidCheckpoint.getTimestamp();

            for (final PageNode lastPageNode : _pageMap.values()) {
                boolean branched = false;
                PageNode previous = null;
                for (PageNode pageNode = lastPageNode; pageNode != null; pageNode = pageNode
                        .getPrevious()) {
                    if (pageNode.getTimestamp() <= lastValidTimestamp
                            && pageNode.getJournalAddress() >= _baseAddress) {
                        pageNode.setPrevious(null);
                        if (branched) {
                            previous.setPrevious(null);
                        }
                        pageMap.put(pageNode, pageNode);
                        break;
                    } else {
                        if (!branched) {
                            branchMap.put(pageNode, pageNode);
                            branched = true;
                        }
                        previous = pageNode;
                    }
                }
            }
        }
    }

    void collectRecoveredVolumeMaps(
            final Map<Integer, VolumeDescriptor> handleToVolumeMap,
            final Map<VolumeDescriptor, Integer> volumeToHandleMap) {
        volumeToHandleMap.putAll(_volumeToHandleMap);
        handleToVolumeMap.putAll(_handleToVolumeMap);
    }

    void collectRecoveredTreeMaps(
            final Map<Integer, TreeDescriptor> handleToTreeMap,
            final Map<TreeDescriptor, Integer> treeToHandleMap) {
        treeToHandleMap.putAll(_treeToHandleMap);
        handleToTreeMap.putAll(_handleToTreeMap);
    }

    void collectRecoveredTransactionMap(final Map<Long, TransactionStatus> map) {
        map.putAll(_recoveredTransactionMap);
    }

    /**
     * Clear the maps created during recovery. This method is called after
     * recovery has been completed and the maps are no longer needed.
     * 
     */
    void close() {
        // _logWriter.close();

        if (_recoveryDisabledForTestMode) {
            return;
        }
        for (final FileChannel channel : _journalFileChannels.values()) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ioe) {
                    // Closing it just to be polite to file handle count
                }
            }
        }
        _recoveredTransactionMap.clear();
        _pageMap.clear();
        _volumeToHandleMap.clear();
        _handleToVolumeMap.clear();
        _treeToHandleMap.clear();
        _handleToTreeMap.clear();
        _readBuffer = null;
        _journalFileChannels.clear();
    }

    /**
     * @return <tt>true</tt> if the {@link #applyAllCommittedTransactions()}
     *         method should do nothing. (Lets unit tests look at the plan
     *         before executing it.)
     */
    boolean isRecoveryDisabledForTestMode() {
        return _recoveryDisabledForTestMode;
    }

    /**
     * @param recoverDisabledForTestMode
     *            Set this to <tt>true</tt> to disable the
     *            {@link #applyAllCommittedTransactions()} method. (Lets unit
     *            tests look at the plan before executing it.)
     */
    void setRecoveryDisabledForTestMode(boolean recoveryDisabledForTestMode) {
        _recoveryDisabledForTestMode = recoveryDisabledForTestMode;
    }

    private synchronized FileChannel getFileChannel(long address)
            throws PersistitIOException {
        final long generation = address / _blockSize;
        FileChannel channel = _journalFileChannels.get(generation);
        if (channel == null) {
            try {
                final RandomAccessFile raf = new RandomAccessFile(
                        addressToFile(address), "r");
                channel = raf.getChannel();
                _journalFileChannels.put(generation, channel);
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }
        return channel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final Iterator<Map.Entry<Long, TransactionStatus>> iterator = _recoveredTransactionMap
                .entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry<Long, TransactionStatus> entry = iterator.next();
            sb.append(entry.getValue());
            sb.append(Util.NEW_LINE);
        }
        return sb.toString();
    }

    private String addressToString(final long address) {
        return String.format("JournalAddress %,d", address);
    }

    private String addressToString(final long address, final long timestamp) {
        return String.format("JournalAddress %,d{%,d}", address, timestamp);
    }

    // ----------------------------Phase 1----------------------------

    private void findAndValidateKeystone() throws PersistitIOException {
        _keystoneAddress = -1;

        final File[] files = files(_journalFilePath);
        if (files.length == 0) {
            return;
        }

        File rejectedPrimordialFile = null;
        CorruptJournalException savedException = null;

        for (int fileIndex = files.length; --fileIndex >= 0;) {
            final File candidate = files[fileIndex];
            _keystoneFile = candidate;
            final long generation = JournalManager.fileToGeneration(candidate);
            final long size;

            try {
                //
                // Attempt to read and validate a journal file as a candidate
                // keystone.
                //
                final RandomAccessFile raf = new RandomAccessFile(candidate,
                        "r");
                final FileChannel readChannel = raf.getChannel();
                size = Math.min(readChannel.size(), DEFAULT_BUFFER_SIZE);
                if (size < JH.OVERHEAD) {
                    // This file cannot be a valid journal file because
                    // it's too short.
                    throw new CorruptJournalException(
                            String.format(
                                    "Invalid Persistit journal file %s - no journal header",
                                    candidate));
                }
                _readBufferAddress = 0;
                _readBuffer.limit(JH.MAX_LENGTH);
                readChannel.read(_readBuffer, 0);
                _readBuffer.flip();
                final int recordSize = JH.getLength(_readBuffer);

                final long version = JH.getVersion(_readBuffer);
                _blockSize = JH.getBlockSize(_readBuffer);
                _baseAddress = JH.getBaseJournalAddress(_readBuffer);
                _journalCreatedTime = JH.getJournalCreatedTime(_readBuffer);
                _keystoneCreatedTime = JH.getFileCreatedTime(_readBuffer);
                _keystoneAddress = JH.getCurrentJournalAddress(_readBuffer);
                _currentAddress = _keystoneAddress + recordSize;

                validate(version, candidate, 0, JournalManager.VERSION,
                        "Unsupported Version %3$d at %1$s:%2$d");

                validate(_blockSize, candidate, 0,
                        JournalManager.MINIMUM_BLOCK_SIZE,
                        JournalManager.MAXIMUM_BLOCK_SIZE,
                        "Journal file size %3$,d not in valid range "
                                + "[%4$,d:%5$,d] at %1$s:%2$,d");

                validate(_keystoneAddress, candidate, 0, generation
                        * _blockSize,
                        "Invalid current address %3$,d at %1$s:%2$,d");

                validate(_baseAddress, candidate, 0, 0, _keystoneAddress,
                        "Base address %3$,d after current address %4$,d:  at %1$s:%2$,d");

                readChannel.close();
                //
                // The JH record is valid. Now read records until a CP record is
                // found.
                //

                // _logWriter.println("Scanning records");

                boolean checkpointFound = false;
                while (true) {
                    try {
                        final int type = scanOneRecord();
                        if (type == CP.TYPE) {
                            checkpointFound = true;
                        } else if (type == JE.TYPE) {
                            break;
                        }
                    } catch (CorruptJournalException cje) {
                        _recoveryEndedException = cje;
                        _recoveryEndedAddress = _currentAddress;
                        if (!checkpointFound) {
                            throw cje;
                        } else {
                            break;
                        }
                    }
                }
                if (checkpointFound) {
                    if (_persistit.getLogBase().isLoggable(
                            LogBase.LOG_RECOVERY_RECORD)) {
                        _persistit.getLogBase().log(
                                LogBase.LOG_RECOVERY_RECORD, "JH",
                                JH.getPath(_readBuffer));
                    }
                    _recoveryEndedAddress = _currentAddress;
                    break;
                }
            } catch (CorruptJournalException je) {
                if (rejectedPrimordialFile == null) {
                    //
                    // Normal case - there was a dirty shutdown, but it's the
                    // primordial stub of a new journal file that didn't get
                    // completed. It's OK to go back to previous file.
                    //
                    rejectedPrimordialFile = candidate;
                    savedException = je;
                    _keystoneAddress = -1;
                    _keystoneFile = null;
                } else {
                    throw savedException;
                }
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }
    }

    private long addressUp(final long address) {
        return ((address / _blockSize) + 1) * _blockSize;
    }

    private void read(final long address, final int size)
            throws PersistitIOException {
        if (_readBufferAddress >= 0 && address >= _readBufferAddress
                && size + address - _readBufferAddress <= _readBuffer.limit()) {
            _readBuffer.position((int) (address - _readBufferAddress));
        } else {
            try {
                final FileChannel fc = getFileChannel(address);
                _readBuffer.clear();
                final int maxSize = Math.min(_readBuffer.capacity(),
                        (int) (addressUp(address) - address));
                _readBuffer.limit(maxSize);
                int offset = 0;
                while (_readBuffer.remaining() > 0) {
                    int readSize = fc.read(_readBuffer, offset + address
                            % _blockSize);
                    if (readSize < 0) {
                        break;
                    }
                    offset += readSize;
                }
                _readBufferAddress = address;
                _readBuffer.flip();
                if (_readBuffer.remaining() < size) {
                    throw new CorruptJournalException("End of file at "
                            + addressToString(address));
                }
            } catch (IOException e) {
                throw new PersistitIOException("Exception while reading from "
                        + addressToString(address), e);
            }
        }
    }

    /**
     * Attempts to read and apply the record at _currentAddress. If it finds
     * valid record contained in the current journal file, it advances the
     * _currentAddress to the start of the next record and returns the type of
     * the record. Otherwise this method does nothing and returns -1;
     * 
     * @return The record type: one of the type values specified in
     *         {@link com.persistit.JournalRecord}), 0 if the journal file has
     *         fewer than 16 bytes remaining or -t where t is an invalid type.
     * @throws CorruptJournalException
     * @throws PersistitException
     * @throws JournalNotClosedException
     */
    private int scanOneRecord() throws PersistitIOException {

        final long from = _currentAddress;
        read(_currentAddress, OVERHEAD);
        final int recordSize = getLength(_readBuffer);
        final int type = getType(_readBuffer);
        final long timestamp = getTimestamp(_readBuffer);

        if (recordSize >= _blockSize || recordSize < OVERHEAD) {
            throw new CorruptJournalException("Bad JournalRecord length "
                    + recordSize + " at position "
                    + addressToString(from, timestamp));
        }

        switch (type) {

        case JE.TYPE:
            processJournalEnd(from, timestamp, recordSize);
            break;

        case JH.TYPE:
        case SR.TYPE:
        case DR.TYPE:
        case DT.TYPE:
            break;

        case IV.TYPE:
            identifyVolume(from, timestamp, recordSize);
            break;

        case IT.TYPE:
            identifyTree(from, timestamp, recordSize);
            break;

        case PA.TYPE:
            loadPage(from, timestamp, recordSize);
            break;

        case PM.TYPE:
            loadPageMap(from, timestamp, recordSize);
            break;

        case TM.TYPE:
            loadTransactionMap(from, timestamp, recordSize);
            break;

        case TS.TYPE:
            startTransaction(from, timestamp, recordSize);
            break;

        case TC.TYPE:
            commitTransaction(from, timestamp, recordSize);
            break;

        case CP.TYPE:
            processCheckpoint(from, timestamp, recordSize);
            break;

        default:
            if (!isValidType(type)) {
                _currentAddress -= OVERHEAD;
                throw new CorruptJournalException("Invalid record type " + type
                        + " at " + addressToString(from));
            }
        }
        _currentAddress = from + recordSize;
        return type;
    }

    /**
     * Process an IV (identify volume) record in the journal. Adds a handle ->
     * volume descriptor pair to the handle maps.
     * 
     * @param address
     * @param timestamp
     * @param recordSize
     * @throws PersistitIOException
     */
    void identifyVolume(final long address, final long timestamp,
            final int recordSize) throws PersistitIOException {
        if (recordSize > IV.MAX_LENGTH) {
            throw new CorruptJournalException("IV JournalRecord too long: "
                    + recordSize + " bytes at position "
                    + addressToString(address, timestamp));
        }
        read(address, recordSize);
        final Integer handle = Integer.valueOf(IV.getHandle(_readBuffer));
        final String name = IV.getVolumeName(_readBuffer);
        final long volumeId = IV.getVolumeId(_readBuffer);
        VolumeDescriptor vd = new VolumeDescriptor(name, volumeId);

        _handleToVolumeMap.put(handle, vd);
        _volumeToHandleMap.put(vd, handle);

        if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_RECORD)) {
            _persistit.getLogBase().log(LogBase.LOG_RECOVERY_RECORD, "IV",
                    addressToString(address, timestamp), name, timestamp);
        }
    }

    /**
     * Processes an IT (identify tree) record in the journal. Adds a handle ->
     * TreeDescriptor entry in the handle maps.
     * 
     * @param address
     * @param timestamp
     * @param recordSize
     * @throws PersistitIOException
     */
    void identifyTree(final long address, final long timestamp,
            final int recordSize) throws PersistitIOException {
        if (recordSize > IT.MAX_LENGTH) {
            throw new CorruptJournalException("IT JournalRecord too long: "
                    + recordSize + " bytes at position "
                    + addressToString(address, timestamp));
        }
        if (_readBuffer.remaining() < recordSize) {
            read(address, recordSize);
        }
        final Integer handle = Integer.valueOf(IT.getHandle(_readBuffer));
        final String treeName = IT.getTreeName(_readBuffer);
        final Integer volumeHandle = Integer.valueOf(IT
                .getVolumeHandle(_readBuffer));
        final TreeDescriptor td = new TreeDescriptor(volumeHandle, treeName);
        _handleToTreeMap.put(handle, td);
        _treeToHandleMap.put(td, handle);
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_RECORD)) {
            _persistit.getLogBase().log(LogBase.LOG_RECOVERY_RECORD, "IT",
                    addressToString(address, timestamp), treeName, timestamp);
        }
    }

    /**
     * Process a PA (page) record in the journal. Adds an entry to the Page Map.
     * 
     * @param address
     * @param timestamp
     * @param recordSize
     * @throws PersistitIOException
     */
    void loadPage(final long address, final long timestamp, final int recordSize)
            throws PersistitIOException {
        if (recordSize > Buffer.MAX_BUFFER_SIZE + PA.OVERHEAD) {
            throw new CorruptJournalException("PA JournalRecord too long: "
                    + recordSize + " bytes at position "
                    + addressToString(address, timestamp));
        }
        //
        // timestamp <= 0 means this is a page from a transient volume
        // and should not be added to the recovery set.
        //
        if (timestamp > 0) {
            read(address, recordSize);
            final int volumeHandle = PA.getVolumeHandle(_readBuffer);
            final long pageAddress = PA.getPageAddress(_readBuffer);

            VolumeDescriptor vd = _handleToVolumeMap.get(volumeHandle);
            if (vd == null) {
                throw new CorruptJournalException(
                        "PA reference to volume "
                                + volumeHandle
                                + " is not preceded by an IV record for that handle at "
                                + addressToString(address, timestamp));
            }

            final PageNode pageNode = new PageNode(volumeHandle, pageAddress,
                    address, timestamp);
            final PageNode oldPageNode = _pageMap.get(pageNode);
            pageNode.setPrevious(oldPageNode);
            _pageMap.put(pageNode, pageNode);

            if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_RECORD)) {
                _persistit.getLogBase().log(LogBase.LOG_RECOVERY_RECORD, "PA",
                        pageNode.toStringJournalAddress(this),
                        pageNode.toStringPageAddress(this), timestamp);
            }
        }
    }

    /*
     * Process a PM (page map) record. There is one PM record near the beginning
     * of each journal file. It provides a copy of the page map that existed at
     * the time the journal file was created, thereby eliminating the need for
     * scanning all of the previous journal files in the journal.
     */
    void loadPageMap(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException {
        read(from, PM.OVERHEAD);
        final int count = PM.getEntryCount(_readBuffer);
        if (count * PM.ENTRY_SIZE + PM.OVERHEAD != recordSize) {
            throw new CorruptJournalException("Invalid record size "
                    + recordSize + " for PM record at "
                    + addressToString(from, timestamp));
        }

        long address = from + PM.OVERHEAD;
        int index = 0;
        int loaded = 0;

        for (int remaining = count; remaining > 0; remaining--) {
            if (index == loaded) {
                final int loadedSize = Math.min(
                        (_readBuffer.capacity() / PM.ENTRY_SIZE), remaining)
                        * PM.ENTRY_SIZE;
                read(address, loadedSize);
                address += loadedSize;
                index = 0;
                loaded = loadedSize / PM.ENTRY_SIZE;
                if (loaded <= 0) {
                    throw new CorruptJournalException(
                            "Could not load PageMap segment in entry "
                                    + (count - remaining + 1) + " at "
                                    + addressToString(from, timestamp));
                }
            }
            final int volumeHandle = PM
                    .getEntryVolumeHandle(_readBuffer, index);
            final VolumeDescriptor vd = _handleToVolumeMap.get(volumeHandle);
            if (vd == null) {
                throw new CorruptJournalException(
                        "Page map refers to undefined volume handle "
                                + volumeHandle + " in entry "
                                + (count - remaining + 1) + " at "
                                + addressToString(from, timestamp));
            }
            final long pageAddress = PM.getEntryPageAddress(_readBuffer, index);
            final long pageTimestamp = PM.getEntryTimestamp(_readBuffer, index);
            final long journalAddress = PM.getEntryJournalAddress(_readBuffer,
                    index);
            final PageNode pageNode = new PageNode(volumeHandle, pageAddress,
                    journalAddress, pageTimestamp);
            final PageNode lastPageNode = _pageMap.get(pageNode);
            if (lastPageNode == null
                    || journalAddress > lastPageNode.getJournalAddress()) {
                pageNode.setPrevious(lastPageNode);
                _pageMap.put(pageNode, pageNode);
            } else {
                for (PageNode pn = lastPageNode; pn != null; pn = pn
                        .getPrevious()) {
                    if (journalAddress == pn.getJournalAddress()) {
                        // TODO - redundant entry
                        break;
                    }
                    if (pn.getPrevious() == null
                            || journalAddress > pn.getPrevious()
                                    .getJournalAddress()) {
                        pageNode.setPrevious(pn.getPrevious());
                        pn.setPrevious(pageNode);
                        break;
                    }

                }
            }
            index++;
        }
    }

    /*
     * Process a TM (transaction map) record. There is one TM record near the
     * beginning of each journal file. It provides a copy of the live
     * transactions that existed at the time the journal file was created,
     * thereby eliminating the need for scanning all of the previous journal
     * files in the journal.
     */
    void loadTransactionMap(final long from, final long timestamp,
            final int recordSize) throws PersistitIOException {
        read(from, TM.OVERHEAD);
        final int count = TM.getEntryCount(_readBuffer);
        if (count * TM.ENTRY_SIZE + TM.OVERHEAD != recordSize) {
            throw new CorruptJournalException("Invalid record size "
                    + recordSize + " for TM record at "
                    + addressToString(from, timestamp));
        }
        long address = from + TM.OVERHEAD;
        int index = 0;
        int loaded = 0;
        for (int remaining = count; remaining > 0; remaining--) {
            if (index == loaded) {
                int loadedSize = Math.min(_readBuffer.capacity()
                        / TM.ENTRY_SIZE, remaining)
                        * TM.ENTRY_SIZE;
                read(address, loadedSize);
                address += loadedSize;
                index = 0;
                loaded = loadedSize / TM.ENTRY_SIZE;
                if (loaded <= 0) {
                    throw new CorruptJournalException(
                            "Could not load TramsactionMap segment in entry "
                                    + (count - remaining + 1) + " at "
                                    + addressToString(from, timestamp));
                }
            }
            final long startTimestamp = TM.getEntryStartTimestamp(_readBuffer,
                    index);
            final long commitTimestamp = TM.getEntryCommitTimestamp(
                    _readBuffer, index);
            final long journalAddress = TM.getEntryJournalAddress(_readBuffer,
                    index);
            final boolean isCommitted = TM
                    .getEntryCommitted(_readBuffer, index);
            TransactionStatus ts = new TransactionStatus(startTimestamp,
                    commitTimestamp, journalAddress);
            ts.setCommitted(isCommitted);
            _recoveredTransactionMap.put(Long.valueOf(commitTimestamp), ts);
            index++;
        }
    }

    void processJournalEnd(final long address, final long timestamp,
            final int recordSize) throws PersistitIOException {
        if (recordSize != JE.OVERHEAD) {
            throw new CorruptJournalException(
                    "JE JournalRecord has incorrect length: " + recordSize
                            + " bytes at position "
                            + addressToString(address, timestamp));
        }
        read(address, JE.OVERHEAD);
        final long currentAddress = JE.getCurrentJournalAddress(_readBuffer);
        final long baseAddress = JE.getBaseAddress(_readBuffer);
        final long journalCreated = JE.getJournalCreatedTime(_readBuffer);

        validate(journalCreated, _keystoneFile, address, _journalCreatedTime,
                "JE wrong record journalCreatedTime "
                        + " %3$,d: expected %4$,d at %1$s:%2$,d");
        validate(currentAddress, _keystoneFile, address, address,
                "JE record currentAddress %3$,d mismatch at %1$s:%2$,d");
        _baseAddress = baseAddress;
    }

    void processCheckpoint(final long address, final long timestamp,
            final int recordSize) throws PersistitIOException {
        if (recordSize != CP.OVERHEAD) {
            throw new CorruptJournalException(
                    "CP JournalRecord has incorrect length: " + recordSize
                            + " bytes at position "
                            + addressToString(address, timestamp));
        }
        read(address, CP.OVERHEAD);
        final long systemTimeMillis = CP.getSystemTimeMillis(_readBuffer);
        final Checkpoint checkpoint = new Checkpoint(timestamp,
                systemTimeMillis);
        final long baseAddress = CP.getBaseAddress(_readBuffer);

        if (baseAddress < _baseAddress || baseAddress > _currentAddress) {
            throw new CorruptJournalException("Invalid base journal address "
                    + baseAddress + " for CP record at "
                    + addressToString(address, timestamp));
        }

        _baseAddress = baseAddress;
        _persistit.getTimestampAllocator().updateTimestamp(timestamp);
        _lastValidCheckpoint = checkpoint;
        _lastValidCheckpointJournalAddress = address;

        for (final Iterator<Map.Entry<Long, TransactionStatus>> iterator = _recoveredTransactionMap
                .entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry<Long, TransactionStatus> entry = iterator.next();
            if (entry.getValue().getCommitTimestamp() < timestamp) {
                iterator.remove();
            } else {
                break;
            }
        }

        if (_persistit.getLogBase()
                .isLoggable(LogBase.LOG_CHECKPOINT_RECOVERED)) {
            _persistit.getLogBase().log(LogBase.LOG_CHECKPOINT_RECOVERED,
                    checkpoint,
                    addressToString(address, checkpoint.getTimestamp()));
        }
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_RECORD)) {
            _persistit.getLogBase().log(LogBase.LOG_RECOVERY_RECORD, "CP",
                    addressToString(address, timestamp),
                    checkpoint + " pageMap.size()=" + _pageMap.size(),
                    timestamp);
        }
    }

    /**
     * Validates non-keystone journal files referenced by the keystone. Not all
     * records are read; these files are needed only to complete committed
     * transactions and to supply pages from the page map.
     * 
     * @param generation
     * @throws PersistitIOException
     */
    private void validateMemberFile(final long generation)
            throws PersistitIOException {
        final File file = JournalManager.generationToFile(_journalFilePath,
                generation);
        if (!file.exists()) {
            throw new CorruptJournalException("Missing journal file " + file);
        }
        final long size;
        read(generation * _blockSize, JH.OVERHEAD);
        int recordSize = getLength(_readBuffer);

        validate(recordSize, file, 0, JH.OVERHEAD, JH.MAX_LENGTH,
                "Journal header record size %3$,d is not in valid range "
                        + "[%4$,d:%5$,d] at %1$s:%2$,d");

        int type = getType(_readBuffer);
        validate(type, file, 0, JH.TYPE,
                "Invalid record type %$3,d at  at %1$s:%2$d");

        final long version = JH.getVersion(_readBuffer);
        final long currentAddress = JH.getCurrentJournalAddress(_readBuffer);
        final long blockSize = JH.getBlockSize(_readBuffer);
        final long baseAddress = JH.getBaseJournalAddress(_readBuffer);
        final long journalCreatedTime = JH.getJournalCreatedTime(_readBuffer);

        validate(version, file, 0, JournalManager.VERSION,
                "Unsupported Version %3$d at %1$s:%2$d");

        validate(blockSize, file, 0, _blockSize,
                "Journal file size %3$,d differs from keystone value "
                        + "%4$,d at %1$s:%2$,d");

        validate(journalCreatedTime, file, 0, _journalCreatedTime,
                "Journal creation time %3$,d differs from keystone value "
                        + "%4$,d at %1$s:%2$,d");

        validate(baseAddress, file, 0, 0, _baseAddress,
                "Journal base address %3$,d not in valid range "
                        + "[%4$,d:%5$,d] at %1$s:%2$,d");

        validate(currentAddress, file, 0, 0, _keystoneAddress,
                "Journal base address %3$,d not in valid range "
                        + "[%4$,d:%5$,d] at %1$s:%2$,d");

        if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_RECORD)) {
            _persistit.getLogBase().log(LogBase.LOG_RECOVERY_RECORD, "JH",
                    JH.getPath(_readBuffer));
        }

        //
        // Now make sure we can read the last PA record required for
        // recovery
        //
        final long startingAddress = generation * _blockSize;
        final long endingAddress = startingAddress + blockSize;
        long lastRequiredJournalAddress = startingAddress;

        PageNode lastRequiredPageNode = null;
        for (final PageNode pageNode : _pageMap.values()) {
            for (PageNode pn = pageNode; pn != null; pn = pn.getPrevious()) {
                if (pn.getJournalAddress() < lastRequiredJournalAddress) {
                    break;
                }
                if (pn.getJournalAddress() < endingAddress) {
                    lastRequiredJournalAddress = pn.getJournalAddress();
                    lastRequiredPageNode = pn;
                }
            }
        }
        if (lastRequiredJournalAddress > startingAddress) {
            read(lastRequiredJournalAddress, PA.OVERHEAD);
            type = getType(_readBuffer);
            validate(type, file, startingAddress, PA.TYPE,
                    "Invalid record type %3$,d at %1$s:%2$d");
            recordSize = getLength(_readBuffer);
            validate(recordSize, file, startingAddress, PA.OVERHEAD
                    + Buffer.HEADER_SIZE, PA.OVERHEAD + Buffer.MAX_BUFFER_SIZE,
                    "PA record size %3$,d not in valid range "
                            + "[%4$,d:%5$,d] at %1$s:%2$,d");
            final long pageAddress = PA.getPageAddress(_readBuffer);
            validate(pageAddress, file, startingAddress,
                    lastRequiredPageNode.getPageAddress(),
                    "Mismatched page address %3$d at %1$s:%2$d");
            // confirm that we can read the data
            read(lastRequiredJournalAddress, recordSize);

        }

    }

    public void buildRecoveryPlan() throws PersistitIOException {
        try {
            //
            // Find the keystone (last) journal file and validate it.
            findAndValidateKeystone();
            if (_keystoneAddress == -1) {
                return;
            }

            //
            // Validate the previous journal files required to complete
            // recovery.
            //
            long fromGeneration = _baseAddress / _blockSize;
            long toGeneration = _keystoneAddress / _blockSize;
            for (long generation = fromGeneration; generation < toGeneration; generation++) {
                validateMemberFile(generation);
            }
            //
            // Remove uncommitted transactions. These are transactions that had
            // started but were not committed at the time the journal ended.
            //
            for (Iterator<TransactionStatus> iterator = _recoveredTransactionMap
                    .values().iterator(); iterator.hasNext();) {
                final TransactionStatus ts = iterator.next();
                if (!ts.isCommitted()) {
                    iterator.remove();
                    _uncommittedTransactionCount++;
                }
            }
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_RECOVERY_PLAN)) {
                _persistit.getLogBase().log(LogBase.LOG_RECOVERY_PLAN,
                        _pageMap.size(), _recoveredTransactionMap.size(),
                        _uncommittedTransactionCount);
            }
        } catch (PersistitIOException pe) {
            if (_persistit.getLogBase()
                    .isLoggable(LogBase.LOG_RECOVERY_FAILURE)) {
                _persistit.getLogBase().log(LogBase.LOG_RECOVERY_FAILURE, pe);
            }
            throw pe;
        }

    }

    /**
     * Called during Phase 2 to record the FileAddress of a Transaction Start
     * record in the journal.
     * 
     * @param ja
     * @throws CorruptJournalException
     */
    void startTransaction(final long address, final long timestamp,
            final int recordSize) throws PersistitIOException {

        if (recordSize != TS.OVERHEAD) {
            throw new CorruptJournalException(
                    "TS JournalRecord has incorrect length: " + recordSize
                            + " bytes at position "
                            + addressToString(address, timestamp));
        }
        read(address, recordSize);
        final Long key = Long.valueOf(timestamp);
        final TransactionStatus previous = _recoveredTransactionMap.get(key);
        if (previous != null) {
            throw new CorruptJournalException(
                    "Duplicate transactions with same timestamp(" + key
                            + "): previous/current="
                            + previous.getStartAddress() + "/"
                            + addressToString(address, timestamp));
        }
        final long startTimestamp = TS.getStartTimestamp(_readBuffer);
        _recoveredTransactionMap.put(key, new TransactionStatus(startTimestamp,
                timestamp, address));
    }

    /**
     * Called during Phase 2 to record the FileAddress of a Transaction Commit
     * record in the journal.
     * 
     * @param ja
     * @throws CorruptJournalException
     */
    void commitTransaction(final long address, final long timestamp,
            final int recordSize) throws CorruptJournalException {
        if (recordSize != TC.OVERHEAD) {
            throw new CorruptJournalException(
                    "TC JournalRecord has incorrect length: " + recordSize
                            + " bytes at position "
                            + addressToString(address, timestamp));
        }

        final Long key = Long.valueOf(timestamp);
        final TransactionStatus previous = _recoveredTransactionMap.get(key);
        if (previous == null) {
            throw new CorruptJournalException(
                    "Missing Transaction Start record for timestamp(" + key
                            + ") at " + addressToString(address, timestamp));
        } else if (previous.isCommitted()) {
            throw new CorruptJournalException(
                    "Redundant Transaction Commit Record for " + previous
                            + " at " + addressToString(address, timestamp));
        }
        previous.setCommitted(true);
    }

    // ---------------------------- Phase 3 ------------------------------------

    public void applyAllCommittedTransactions(
            final RecoveredTransactionActor actor) {

        if (_recoveryDisabledForTestMode) {
            return;
        }

        for (final TransactionStatus ts : _recoveredTransactionMap.values()) {
            try {
                applyTransaction(ts, actor);
                _appliedTransactionCount++;
                if (_appliedTransactionCount % APPLY_TRANSACTION_LOG_COUNT == 0) {
                    if (_persistit.getLogBase().isLoggable(
                            LogBase.LOG_RECOVERY_PROGRESS)) {
                        _persistit.getLogBase().log(
                                LogBase.LOG_RECOVERY_PROGRESS,
                                _appliedTransactionCount,
                                _recoveredTransactionMap.size()
                                        - _appliedTransactionCount);
                    }
                }
            } catch (Exception pe) {
                _persistit.getLogBase().log(LogBase.LOG_RECOVERY_EXCEPTION, pe,
                        ts);
                _errorCount++;
            }
        }
    }

    public void applyTransaction(final TransactionStatus ts,
            final RecoveredTransactionActor actor) throws PersistitException {
        _currentAddress = ts.getStartAddress();
        _persistit.getTimestampAllocator().updateTimestamp(
                ts.getCommitTimestamp());
        final Set<Tree> removedTrees = new HashSet<Tree>();

        while (!applyOneRecord(ts, actor, removedTrees)) {
        }

        for (final Tree tree : removedTrees) {
            tree.getVolume().removeTree(tree);
        }
    }

    private boolean applyOneRecord(final TransactionStatus ts,
            final RecoveredTransactionActor actor, final Set<Tree> removedTrees)
            throws PersistitException {

        final long address = _currentAddress;
        read(address, OVERHEAD);

        final int recordSize = getLength(_readBuffer);
        final int type = getType(_readBuffer);
        final long timestamp = getTimestamp(_readBuffer);

        if (timestamp == ts.getCommitTimestamp()) {

            switch (type) {

            case JE.TYPE:
                _readBuffer = null;
                break;

            case TS.TYPE:
                break;

            case TC.TYPE:
                return true;

            case SR.TYPE: {
                read(address, recordSize);
                final int keySize = SR.getKeySize(_readBuffer);
                final int treeHandle = SR.getTreeHandle(_readBuffer);
                final Exchange exchange = getExchange(treeHandle, address,
                        timestamp);
                exchange.ignoreTransactions();
                final Key key = exchange.getKey();
                final Value value = exchange.getValue();
                System.arraycopy(_readBuffer.array(), _readBuffer.position()
                        + SR.OVERHEAD, key.getEncodedBytes(), 0, keySize);
                key.setEncodedSize(keySize);
                final int valueSize = recordSize - SR.OVERHEAD - keySize;
                value.ensureFit(valueSize);
                System.arraycopy(_readBuffer.array(), _readBuffer.position()
                        + SR.OVERHEAD + keySize, value.getEncodedBytes(), 0,
                        valueSize);
                value.setEncodedSize(valueSize);

                if (value.getEncodedSize() >= Buffer.LONGREC_SIZE
                        && (value.getEncodedBytes()[0] & 0xFF) == Buffer.LONGREC_TYPE) {
                    final TreeDescriptor td = _handleToTreeMap.get(treeHandle);

                    convertToLongRecord(value, td.getVolumeHandle(), address,
                            timestamp);
                }

                actor.store(address, timestamp, exchange);
                // Don't keep exchanges with enlarged value - let them be GC'd
                if (exchange.getValue().getMaximumSize() >= Value.DEFAULT_MAXIMUM_SIZE) {
                    _persistit.releaseExchange(exchange);
                }
                break;
            }

            case DR.TYPE: {
                read(address, recordSize);
                final int key1Size = DR.getKey1Size(_readBuffer);
                final Exchange exchange = getExchange(
                        DR.getTreeHandle(_readBuffer), address, timestamp);
                exchange.ignoreTransactions();
                final Key key1 = exchange.getAuxiliaryKey1();
                final Key key2 = exchange.getAuxiliaryKey2();
                System.arraycopy(_readBuffer.array(), _readBuffer.position()
                        + DR.OVERHEAD, key1.getEncodedBytes(), 0, key1Size);
                key1.setEncodedSize(key1Size);
                final int key2Size = recordSize - DR.OVERHEAD - key1Size;
                System.arraycopy(_readBuffer.array(), _readBuffer.position()
                        + DR.OVERHEAD + key1Size, key2.getEncodedBytes(), 0,
                        key2Size);
                key2.setEncodedSize(key2Size);
                actor.removeKeyRange(address, timestamp, exchange);
                _persistit.releaseExchange(exchange);
                break;
            }

            case DT.TYPE:
                read(address, recordSize);
                final Exchange exchange = getExchange(
                        SR.getTreeHandle(_readBuffer), address, timestamp);
                actor.removeTree(address, timestamp, exchange);
                _persistit.releaseExchange(exchange);
                break;

            default:
                if (!isValidType(type)) {
                    // TODO -
                }

            }
        }
        if (type == JE.TYPE) {
            _currentAddress = addressUp(address);
        } else {
            _currentAddress = address + recordSize;
        }
        return false;
    }

    /**
     * Assembles a long record into the provided Value object. This method
     * relies on finding the PAGE_TYPE_LONG_RECORD pages in the journal.
     * Transaction commit writes those pages before writing the TC record; this
     * ensures they are available in the journal.
     * 
     * @param value
     *            Value object contains a value of type LONG_REC, is converted
     *            by this method to an actual long record.
     * @param timestamp
     *            timestamp of the transaction
     * @param page
     * 
     * @throws PersistitException
     */
    void convertToLongRecord(final Value value, final int volumeHandle,
            final long from, final long timestamp) throws PersistitException {
        long page = Buffer.decodeLongRecordDescriptorPointer(
                value.getEncodedBytes(), 0);
        int size = Buffer.decodeLongRecordDescriptorSize(
                value.getEncodedBytes(), 0);
        if (size < 0 || size > Value.MAXIMUM_SIZE) {
            throw new CorruptJournalException(
                    "Transactional long record specification "
                            + "exceeds maximum size of " + Value.MAXIMUM_SIZE
                            + ":" + size);
        }
        final byte[] rawBytes = value.getEncodedBytes();
        final long startAddress = page;
        value.clear();
        if (size > value.getMaximumSize()) {
            value.setMaximumSize(size);
        }
        value.ensureFit(size);

        int offset = 0; // offset of next segment in the value
        int remainingSize = size;

        Util.arraycopy(rawBytes, Buffer.LONGREC_PREFIX_OFFSET,
                value.getEncodedBytes(), offset, Buffer.LONGREC_PREFIX_SIZE);

        offset += Buffer.LONGREC_PREFIX_SIZE;
        remainingSize -= Buffer.LONGREC_PREFIX_SIZE;

        for (int count = 0; page != 0; count++) {

            if (remainingSize == 0) {
                throw new CorruptJournalException(
                        "Long record chain has more than " + size
                                + " bytes starting at page " + startAddress
                                + " for transaction at "
                                + addressToString(from, timestamp));
            }

            PageNode pn = _pageMap
                    .get(new PageNode(volumeHandle, page, -1, -1));
            while (pn != null) {
                if (pn.getTimestamp() <= timestamp) {
                    break;
                }
                pn = pn.getPrevious();
            }

            if (pn == null) {
                throw new CorruptJournalException(
                        "Long record chain missing page " + page + " at count "
                                + count + " at "
                                + addressToString(from, timestamp));
            }

            _currentAddress = pn.getJournalAddress();
            read(_currentAddress, PA.OVERHEAD);
            final int type = PA.getType(_readBuffer);
            final int recordSize = PA.getLength(_readBuffer);
            final int payloadSize = recordSize - PA.OVERHEAD;
            final int leftSize = PA.getLeftSize(_readBuffer);
            final int bufferSize = PA.getBufferSize(_readBuffer);
            final long pageAddress = PA.getPageAddress(_readBuffer);
            //
            // Verify that this is the valid and appropriate PA record
            //
            if (type != PA.TYPE) {
                throw new CorruptJournalException("Record at "
                        + pn.toStringJournalAddress(this)
                        + " is not a PAGE record");
            }

            if (leftSize < 0 || payloadSize < leftSize
                    || payloadSize > bufferSize) {
                throw new CorruptJournalException("Record at "
                        + pn.toStringJournalAddress(this)
                        + " invalid sizes: recordSize= " + payloadSize
                        + " leftSize=" + leftSize + " bufferSize=" + bufferSize);
            }

            if (pageAddress != pn.getPageAddress()) {
                throw new CorruptJournalException("Record at "
                        + pn.toStringJournalAddress(this)
                        + " mismatched page address: expected/actual="
                        + pn.getPageAddress() + "/" + pageAddress);
            }

            //
            // Verify that this is a PAGE_TYPE_LONG_RECORD
            //
            read(_currentAddress, recordSize);
            final int pageType = JournalRecord.getByte(_readBuffer, PA.OVERHEAD
                    + Buffer.TYPE_OFFSET);

            if (pageType != Buffer.PAGE_TYPE_LONG_RECORD) {
                throw new CorruptJournalException(
                        "Long record chain contains invalid page type "
                                + pageType + " for page " + page + " at "
                                + pn.toStringJournalAddress(this)
                                + " in transaction at "
                                + addressToString(from, timestamp));
            }

            int segmentSize = Math.min(remainingSize, payloadSize
                    - Buffer.HEADER_SIZE);

            System.arraycopy(_readBuffer.array(), _readBuffer.position()
                    + PA.OVERHEAD + Buffer.HEADER_SIZE,
                    value.getEncodedBytes(), offset, segmentSize);
            offset += segmentSize;
            remainingSize -= segmentSize;

            // Next page in chain
            page = JournalRecord.getLong(_readBuffer, PA.OVERHEAD
                    + Buffer.RIGHT_SIBLING_OFFSET);

            if (count > Exchange.MAX_LONG_RECORD_CHAIN) {
                throw new CorruptJournalException(
                        "Long record chain has more than "
                                + Exchange.MAX_LONG_RECORD_CHAIN
                                + " pages in starting at page " + startAddress
                                + " for transaction at "
                                + addressToString(from, timestamp));
            }
        }

        if (remainingSize != 0) {
            throw new CorruptJournalException(
                    "Long record chain has fewer than " + size + " bytes ("
                            + remainingSize
                            + " not recovered) starting at page "
                            + startAddress + " for transaction at "
                            + addressToString(from, timestamp));
        }
        value.setEncodedSize(size);
    }

    private Exchange getExchange(final int treeHandle, final long from,
            final long timestamp) throws PersistitException {
        final TreeDescriptor td = _handleToTreeMap.get(treeHandle);
        if (td == null) {
            throw new CorruptJournalException("Tree handle " + treeHandle
                    + " is undefined at " + addressToString(from, timestamp));
        }
        final VolumeDescriptor vd = _handleToVolumeMap
                .get(td.getVolumeHandle());
        if (vd == null) {
            throw new CorruptJournalException("Volume handle "
                    + td.getVolumeHandle() + " is undefined at "
                    + addressToString(from, timestamp));
        }

        final Volume v1 = _persistit.getVolume(vd.getId());
        final Volume v2 = _persistit.getVolume(vd.getName());

        Volume volume = null;

        if (v1 == null) {
            volume = v2;
        } else if (v2 == null) {
            volume = v1;
        } else if (v1 == v2) {
            volume = v1;
        }

        if (volume == null) {
            throw new CorruptJournalException(
                    "No matching Volume found for journal reference " + vd
                            + " at " + addressToString(from, timestamp));
        }

        return _persistit.getExchange(volume, td.getTreeName(), true);
    }

    boolean analyze() throws Exception {
        findAndValidateKeystone();
        if (getKeystoneAddress() == -1) {
            println("No valid journal at %s", getJournalFilePath());
            return false;
        }
        println("Journal at %s:", getJournalFilePath());
        println("Keystone Address:  %,d", getKeystoneAddress());
        println("Base Address: %,d", getBaseAddress());
        println("Block Size: %,d", getBlockSize());
        println("Journal created: %s",
                new SimpleDateFormat("yyyyMMddHHmm").format(new Date(
                        getJournalCreatedTime())));
        println("Last valid checkpoint: %s", getLastValidCheckpoint());
        println("Last valid checkpoint address: %,d",
                getLastValidCheckpointAddress());
        println("Recovered transaction count committed=%,d uncommitted=%,d",
                getCommittedCount(), getUncommittedCount());
        println("Recovered page count: %,d", getPageMapSize());
        println("Volume handle map--");
        for (final Map.Entry<Integer, VolumeDescriptor> entry : _handleToVolumeMap
                .entrySet()) {
            println(" %5d->%s", entry.getKey(), entry.getValue());
        }
        println("Tree handle map--");
        for (final Map.Entry<Integer, TreeDescriptor> entry : _handleToTreeMap
                .entrySet()) {
            println(" %5d->%s", entry.getKey(), entry.getValue());
        }

        long fromGeneration = getBaseAddress() / getBlockSize();
        long toGeneration = getKeystoneAddress() / getBlockSize();
        boolean okay = true;
        for (long generation = fromGeneration; generation < toGeneration; generation++) {
            final File file = addressToFile(generation * getBlockSize());
            println("Validating file %s", file);
            try {
                validateMemberFile(generation);
            } catch (PersistitIOException ioe) {
                println("   Unrecoverable: %s", ioe);
                okay = false;
            }
        }
        return okay;
    }

    /**
     * Read and display information about a journal. Read-only - does not change
     * any file.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        String[] template = {
                "path||pathname of journal, e.g., /xxx/yyy/zzz_journal "
                        + "for files such as /xxx/yyy/zzz_journal.0000000000000047",
                "_flags|t|emit transaction details" };
        final ArgParser argParser = new ArgParser("RecoveryManager", args,
                template);
        final Persistit persistit = new Persistit();
        persistit.initializeJournal();
        final RecoveryManager rman = new RecoveryManager(persistit);
        rman.init(argParser.getStringValue("path"));
        rman.analyze();
    }

}
