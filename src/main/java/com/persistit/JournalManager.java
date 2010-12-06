package com.persistit;

import static com.persistit.IOMeter.ALMOST_URGENT;
import static com.persistit.IOMeter.HALF_URGENT;
import static com.persistit.IOMeter.URGENT;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

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
 * Manages the disk-based I/O journal. The journal contains both committed
 * transactions and images of updated pages.
 * 
 * @author peter
 * 
 */
public class JournalManager implements JournalManagerMXBean, VolumeHandleLookup {

    private long _journalCreatedTime;

    private long _currentTimestamp = 0;

    private final Map<PageNode, PageNode> _pageMap = new HashMap<PageNode, PageNode>();

    private final Map<PageNode, PageNode> _branchMap = new HashMap<PageNode, PageNode>();

    private final Map<VolumeDescriptor, Integer> _volumeToHandleMap = new ConcurrentHashMap<VolumeDescriptor, Integer>();

    private final Map<Integer, VolumeDescriptor> _handleToVolumeMap = new ConcurrentHashMap<Integer, VolumeDescriptor>();

    private final Map<TreeDescriptor, Integer> _treeToHandleMap = new ConcurrentHashMap<TreeDescriptor, Integer>();

    private final Map<Integer, TreeDescriptor> _handleToTreeMap = new ConcurrentHashMap<Integer, TreeDescriptor>();

    private final Map<Long, TransactionStatus> _liveTransactionMap = new HashMap<Long, TransactionStatus>();

    private final Persistit _persistit;

    private long _blockSize;

    private int _writeBufferSize = DEFAULT_BUFFER_SIZE;

    private ByteBuffer _writeBuffer;

    private long _writeBufferAddress = Long.MAX_VALUE;

    private final JournalFlusher _flusher;

    private final JournalCopier _copier;

    private AtomicBoolean _closed = new AtomicBoolean();

    private AtomicBoolean _copying = new AtomicBoolean();

    private AtomicBoolean _copyFast = new AtomicBoolean();

    private AtomicBoolean _flushing = new AtomicBoolean();

    private AtomicBoolean _appendOnly = new AtomicBoolean();

    private String _journalFilePath;

    /**
     * Address of first available byte in the journal. This is usually the
     * address of the next record to be written, but if that next record
     * requires more space than is available in the current journal file, it
     * will advance to the start of the next journal file.
     */
    private long _currentAddress;

    /**
     * Smallest journal address at which a record still needed is located.
     * Initially zero, increases as journal files are consumed and deleted.
     */
    private long _baseAddress;

    private final Map<Long, FileChannel> _journalFileChannels = new HashMap<Long, FileChannel>();

    /**
     * Counter used to assign internal handle values to Volume and Tree records.
     */
    private int _handleCounter = 0;

    private Checkpoint _lastValidCheckpoint = new Checkpoint(0, 0);

    private long _lastValidCheckpointJournalAddress = 0;

    private long _lastValidCheckpointBaseAddress = 0;

    private long _deleteBoundaryAddress = 0;

    private volatile long _writePageCount = 0;

    private volatile long _copiedPageCount = 0;

    private long _unitTestNeverCloseTransactionTimestamp = 0;

    /**
     * Tunable parameters that determine how vigorously the copyBack thread
     * performs I/O. Hopefully we can set good defaults and not expose these as
     * knobs.
     */
    private volatile long _flushInterval = DEFAULT_FLUSH_INTERVAL;

    private volatile long _copierInterval = DEFAULT_COPIER_INTERVAL;

    private volatile int _copiesPerCycle = DEFAULT_COPIES_PER_CYCLE;

    private volatile int _pageMapSizeBase = DEFAULT_PAGE_MAP_SIZE_BASE;

    private volatile long _copierTimestampLimit = Long.MAX_VALUE;

    private long _logRepeatInterval = DEFAULT_LOG_REPEAT_INTERVAL;

    public JournalManager(final Persistit persistit) {
        _persistit = persistit;
        _flusher = new JournalFlusher();
        _copier = new JournalCopier();
    }

    /**
     * <p>
     * Initialize the new journal. This method takes its information from the
     * supplied RecoveryManager if supplied and valid. Otherwise it starts a new
     * journal at address 0.
     * </p>
     * <p>
     * If a RecoveryManager is supplied and has a valid keystone address, then
     * this method continues the existing journal. A new journal file will be
     * created with a generation number one larger than that of the keystone
     * file, and the new file is given the same journal create date as the
     * recovered journal. New journal files are also required to have the same
     * maximumSize and path name (not including generation suffix) as the
     * existing journal, so in the event <tt>rman</tt> is non-null and contains
     * a valid keystone, the <tt>path</tt> and <tt>maximumSize</tt> parameters
     * are ignored.
     * </p>
     * <p>
     * Otherwise, this method creates a new journal starting at journal address
     * 0 with the specified path and maximum file size. Journal file names are
     * created by appending a period followed by a generation number suffix to
     * the supplied path name. For example if the supplied path is
     * "/xxx/yyy/zzz" then journal file names will be
     * "/xxx/yyy/zzz.000000000000", "/xxx/yyy/zzz.000000000001", and so on. (The
     * suffix contains twelve digits.)
     * </p>
     * 
     * @param rman
     * @param path
     * @param maximumSize
     * @throws PersistitException
     */
    public void init(final RecoveryManager rman, final String path,
            final long maximumSize) throws PersistitException {
        _writeBuffer = ByteBuffer.allocate(_writeBufferSize);
        if (rman != null && rman.getKeystoneAddress() != -1) {
            _journalFilePath = rman.getJournalFilePath();
            _blockSize = rman.getBlockSize();
            _currentAddress = rman.getKeystoneAddress() + _blockSize;
            _baseAddress = rman.getBaseAddress();
            _journalCreatedTime = rman.getJournalCreatedTime();
            _lastValidCheckpoint = rman.getLastValidCheckpoint();
            rman.collectRecoveredPages(_pageMap, _branchMap);
            rman.collectRecoveredVolumeMaps(_handleToVolumeMap,
                    _volumeToHandleMap);
            rman.collectRecoveredTreeMaps(_handleToTreeMap, _treeToHandleMap);
            rman.collectRecoveredTransactionMap(_liveTransactionMap);
        } else {
            _journalFilePath = new File(path).getAbsoluteFile().toString();
            _blockSize = maximumSize;
            _currentAddress = 0;
            _journalCreatedTime = System.currentTimeMillis();
        }
        _closed.set(false);

    }

    public void startThreads() {
        _copier.start();
        _flusher.start();
    }

    /**
     * Copy dynamic variables into a {@linkManagement.JournalInfo} structure.
     * 
     * @param info
     */
    public synchronized void populateJournalInfo(
            final Management.JournalInfo info) {
        info.closed = _closed.get();
        info.copiedPageCount = _copiedPageCount;
        info.copying = _copying.get();
        info.currentGeneration = _currentAddress;
        info.currentJournalAddress = _writeBuffer == null ? 0
                : _writeBufferAddress + _writeBuffer.position();
        info.currentJournalFile = addressToFile(_currentAddress).getPath();
        info.flushing = _flushing.get();
        info.journaledPageCount = _writePageCount;
        if (_lastValidCheckpointJournalAddress != 0) {
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
        info.appendOnly = _appendOnly.get();
        info.fastCopying = _copyFast.get();
    }

    public synchronized int getPageMapSize() {
        return _pageMap.size();
    }

    public synchronized long getBaseAddress() {
        return _baseAddress;
    }

    public synchronized long getCurrentAddress() {
        return _currentAddress;
    }

    public long getBlockSize() {
        return _blockSize;
    }

    public boolean isAppendOnly() {
        return _appendOnly.get();
    }

    public boolean isCopyingFast() {
        return _copyFast.get();
    }

    public void setAppendOnly(boolean appendOnly) {
        _appendOnly.set(appendOnly);
    }

    public void setCopyingFast(boolean fast) {
        _copyFast.set(fast);
    }

    public long getFlushInterval() {
        return _flusher.getPollInterval();
    }

    public void setFlushInterval(long flushInterval) {
        _flusher.setPollInterval(flushInterval);
    }

    public long getCopierInterval() {
        return _copier.getPollInterval();
    }

    public void setCopierInterval(long copierInterval) {
        _copier.setPollInterval(copierInterval);
    }

    public boolean isClosed() {
        return _closed.get();
    }

    public boolean isCopying() {
        return _copying.get();
    }

    public String getJournalFilePath() {
        return _journalFilePath;
    }

    public long getJournaledPageCount() {
        return _writePageCount;
    }

    public long getCopiedPageCount() {
        return _copiedPageCount;
    }

    public long getJournalCreatedTime() {
        return _journalCreatedTime;
    }

    public Checkpoint getLastValidCheckpoint() {
        return _lastValidCheckpoint;
    }
    
    public long getLastValidCheckpointTimestamp() {
        return _lastValidCheckpoint.getTimestamp();
    }

    /**
     * Computes an "urgency" factor that determines how vigorously the copyBack
     * thread should perform I/O. This number is computed on a scale of 0 to 10;
     * larger values are intended make the thread work harder. A value of 10
     * suggests the copier should run flat-out.
     * 
     * @return
     */
    public synchronized int urgency() {
        if (_copyFast.get()) {
            return URGENT;
        }
        int urgency = _pageMap.size() / _pageMapSizeBase;
        int journalFileCount = (int) (_currentAddress / _blockSize - _baseAddress
                / _blockSize);
        if (journalFileCount > 1) {
            urgency += journalFileCount - 1;
        }
        return Math.min(urgency, URGENT);
    }

    public int handleForVolume(final Volume volume) throws PersistitIOException {
        final VolumeDescriptor vd = new VolumeDescriptor(volume);
        return handleForVolume(vd);
    }

    public int handleForVolume(final VolumeDescriptor vd)
            throws PersistitIOException {
        Integer handle = _volumeToHandleMap.get(vd);
        if (handle == null) {
            //
            // Could be a race here: effect would be two handles
            // for the same Volume - benign
            //
            synchronized (this) {
                handle = Integer.valueOf(++_handleCounter);
                writeVolumeHandleToJournal(vd, handle.intValue());
                _volumeToHandleMap.put(vd, handle);
                _handleToVolumeMap.put(handle, vd);
            }
        }
        return handle.intValue();
    }

    int handleForTree(final TreeDescriptor td) throws PersistitIOException {
        if (td.getVolumeHandle() == -1) {
            // Tree in transient volume -- don't journal updates to it
            return -1;
        }
        Integer handle = _treeToHandleMap.get(td);
        if (handle == null) {
            //
            // Could be a race here: effect would be two handles
            // for the same Tree - benign
            //
            synchronized (this) {
                handle = Integer.valueOf(++_handleCounter);
                writeTreeHandleToJournal(td, handle.intValue());
                _treeToHandleMap.put(td, handle);
                _handleToTreeMap.put(handle, td);
            }
        }
        return handle.intValue();
    }

    int handleForTree(final Tree tree) throws PersistitIOException {
        final TreeDescriptor td = new TreeDescriptor(
                handleForVolume(tree.getVolume()), tree.getName());
        return handleForTree(td);
    }

    Tree treeForHandle(final int handle) throws PersistitException {
        final TreeDescriptor td = _handleToTreeMap.get(Integer.valueOf(handle));
        if (td == null) {
            return null;
        }
        final Volume volume = volumeForHandle(td.getVolumeHandle());
        return volume.getTree(td.getTreeName(), true);
    }

    Volume volumeForHandle(final int handle) throws PersistitException {
        final VolumeDescriptor vd = lookupVolumeHandle(handle);
        if (vd == null) {
            return null;
        }
        return _persistit.getVolume(vd.getName());
    }

    @Override
    public synchronized VolumeDescriptor lookupVolumeHandle(final int handle) {
        return _handleToVolumeMap.get(Integer.valueOf(handle));
    }

    private void readFully(final ByteBuffer bb, final long address)
            throws PersistitIOException, CorruptJournalException {
        //
        // If necessary read the bytes out of the _writeBuffer
        // before they have been written out to the file. This code
        // requires the _writeBuffer to be a HeapByteBuffer.
        //
        final int position = bb.position();
        final int length = bb.remaining();
        synchronized (this) {
            if (address >= _writeBufferAddress
                    && address + length <= _currentAddress) {
                final byte[] src = _writeBuffer.array();
                final byte[] dest = bb.array();
                final int srcPosition = (int) (address - _writeBufferAddress);
                final int destPosition = bb.position();
                System.arraycopy(src, srcPosition, dest, destPosition, length);
                bb.position(position);
                return;
            }
        }

        final FileChannel fc = getFileChannel(address);

        long fileAddr = addressToOffset(address);
        while (bb.remaining() > 0) {
            int count;
            try {
                count = fc.read(bb, fileAddr);
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
            if (count < 0) {
                final File file = addressToFile(address);
                throw new CorruptJournalException(String.format(
                        "End of file at %s:%d(%,d)", file, fileAddr, address));
            }
            fileAddr += count;
        }
        bb.limit(bb.position());
        bb.position(position);
    }

    boolean readPageFromJournal(final Buffer buffer)
            throws PersistitIOException {
        final int bufferSize = buffer.getBufferSize();
        final long pageAddress = buffer.getPageAddress();
        final ByteBuffer bb = buffer.getByteBuffer();

        final Volume volume = buffer.getVolume();
        final VolumeDescriptor vd = new VolumeDescriptor(volume);
        final Integer volumeHandle = _volumeToHandleMap.get(vd);
        PageNode pn = null;
        if (volumeHandle != null) {
            synchronized (this) {
                pn = _pageMap.get(new PageNode(volumeHandle, pageAddress, -1,
                        -1));
            }
        }

        if (pn == null) {
            return false;
        }

        long recordPageAddress = readPageBufferFromJournal(pn, bb);
        _persistit.getIOMeter().chargeReadPageFromJournal(volume, pageAddress,
                bufferSize, pn.getJournalAddress());

        if (pageAddress != recordPageAddress) {
            throw new CorruptJournalException("Record at " + pn
                    + " is not volume/page " + buffer.toString());
        }

        if (bb.limit() != bufferSize) {
            throw new CorruptJournalException("Record at " + pn
                    + " is wrong size: expected/actual=" + bufferSize + "/"
                    + bb.limit());
        }
        return true;
    }

    private long readPageBufferFromJournal(final PageNode pn,
            final ByteBuffer bb) throws PersistitIOException,
            CorruptJournalException {

        bb.limit(PA.OVERHEAD).position(0);
        readFully(bb, pn.getJournalAddress());
        if (bb.remaining() < PA.OVERHEAD) {
            throw new CorruptJournalException("Record at "
                    + pn.toStringJournalAddress(this) + " is incomplete");
        }
        final int type = PA.getType(bb);
        final int payloadSize = PA.getLength(bb) - PA.OVERHEAD;
        final int leftSize = PA.getLeftSize(bb);
        final int bufferSize = PA.getBufferSize(bb);
        final long pageAddress = PA.getPageAddress(bb);

        if (type != PA.TYPE) {
            throw new CorruptJournalException("Record at "
                    + pn.toStringJournalAddress(this) + " is not a PAGE record");
        }

        if (leftSize < 0 || payloadSize < leftSize || payloadSize > bufferSize) {
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

        bb.limit(payloadSize).position(0);
        readFully(bb, pn.getJournalAddress() + PA.OVERHEAD);

        if (leftSize > 0) {
            final int rightSize = payloadSize - leftSize;
            System.arraycopy(bb.array(), leftSize, bb.array(), bufferSize
                    - rightSize, rightSize);
            Arrays.fill(bb.array(), leftSize, bufferSize - rightSize, (byte) 0);
        }
        bb.limit(bufferSize).position(0);
        return pageAddress;
    }

    private void advance(final int recordSize) {
        Debug.$assert(recordSize > 0
                && recordSize + _writeBuffer.position() <= _writeBuffer
                        .capacity());
        _currentAddress += recordSize;
        _writeBuffer.position(_writeBuffer.position() + recordSize);
    }

    /**
     * Write a JH (journal header) record. This record must be written to the
     * beginning of the journal file. Note that this method does not call
     * {@link #prepareWriteBuffer(int)} - the write buffer needs to be ready to
     * receive the JH record.
     * 
     * @throws PersistitIOException
     */
    synchronized void writeJournalHeader() throws PersistitIOException {
        JH.putType(_writeBuffer);
        JH.putTimestamp(_writeBuffer, _currentTimestamp);
        JH.putVersion(_writeBuffer, VERSION);
        JH.putBlockSize(_writeBuffer, _blockSize);
        JH.putBaseJournalAddress(_writeBuffer, _baseAddress);
        JH.putCurrentJournalAddress(_writeBuffer, _currentAddress);
        JH.putJournalCreatedTime(_writeBuffer, _journalCreatedTime);
        JH.putFileCreatedTime(_writeBuffer, System.currentTimeMillis());
        JH.putPath(_writeBuffer, addressToFile(_currentAddress).getPath());
        final int recordSize = JH.getLength(_writeBuffer);
        _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                _currentAddress);
        advance(recordSize);
    }

    /**
     * Write the JE (journal end) record. This record must be written to the end
     * of each complete journal file. Note that this method does not call
     * {@link #prepareWriteBuffer(int)} - the write buffer needs to be ready to
     * receive the JE record.
     * 
     * @throws PersistitIOException
     */
    synchronized void writeJournalEnd() throws PersistitIOException {
        if (_writeBufferAddress != Long.MAX_VALUE) {
            //
            // prepareWriteBuffer contract guarantees there's always room in
            // the write buffer for this record.
            //
            JE.putType(_writeBuffer);
            JE.putTimestamp(_writeBuffer, _currentTimestamp);
            JE.putLength(_writeBuffer, JE.OVERHEAD);
            JE.putCurrentJournalAddress(_writeBuffer, _currentAddress);
            JE.putBaseAddress(_writeBuffer, _baseAddress);
            JE.putJournalCreatedTime(_writeBuffer, _journalCreatedTime);
            _persistit.getIOMeter().chargeWriteOtherToJournal(JE.OVERHEAD,
                    _currentAddress);
            advance(JE.OVERHEAD);
        }
    }

    synchronized void writePageMap() throws PersistitIOException {
        int count = 0;
        for (final PageNode lastPageNode : _pageMap.values()) {
            PageNode pageNode = lastPageNode;
            while (pageNode != null) {
                count++;
                pageNode = pageNode.getPrevious();
            }
        }
        for (final PageNode lastPageNode : _branchMap.values()) {
            PageNode pageNode = lastPageNode;
            while (pageNode != null) {
                count++;
                pageNode = pageNode.getPrevious();
            }
        }

        final int recordSize = PM.OVERHEAD + PM.ENTRY_SIZE * count;
        prepareWriteBuffer(recordSize);
        PM.putType(_writeBuffer);
        PM.putLength(_writeBuffer, recordSize);
        PM.putTimestamp(_writeBuffer, _currentTimestamp);
        advance(PM.OVERHEAD);
        int offset = 0;
        for (final PageNode lastPageNode : _pageMap.values()) {
            PageNode pageNode = lastPageNode;
            while (pageNode != null) {
                PM.putEntry(_writeBuffer, offset / PM.ENTRY_SIZE,
                        pageNode.getTimestamp(), pageNode.getJournalAddress(),
                        pageNode.getVolumeHandle(), pageNode.getPageAddress());

                offset += PM.ENTRY_SIZE;
                count--;
                if (count == 0
                        || offset + PM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                    advance(offset);
                    offset = 0;
                }
                if (PM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                    flush();
                }
                pageNode = pageNode.getPrevious();
            }
        }
        for (final PageNode lastPageNode : _branchMap.values()) {
            PageNode pageNode = lastPageNode;
            while (pageNode != null) {
                PM.putEntry(_writeBuffer, offset / PM.ENTRY_SIZE,
                        pageNode.getTimestamp(), pageNode.getJournalAddress(),
                        pageNode.getVolumeHandle(), pageNode.getPageAddress());

                offset += PM.ENTRY_SIZE;
                count--;
                if (count == 0
                        || offset + PM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                    advance(offset);
                    offset = 0;
                }
                if (PM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                    flush();
                }
                pageNode = pageNode.getPrevious();
            }
        }
        Debug.$assert(count == 0);
        _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeTransactionMap() throws PersistitIOException {
        int count = _liveTransactionMap.size();
        final int recordSize = TM.OVERHEAD + TM.ENTRY_SIZE * count;
        prepareWriteBuffer(recordSize);
        TM.putType(_writeBuffer);
        TM.putLength(_writeBuffer, recordSize);
        TM.putTimestamp(_writeBuffer, _currentTimestamp);
        advance(TM.OVERHEAD);
        int offset = 0;
        for (final TransactionStatus ts : _liveTransactionMap.values()) {
            TM.putEntry(_writeBuffer, offset / TM.ENTRY_SIZE,
                    ts.getStartTimestamp(), ts.getCommitTimestamp(),
                    ts.getStartAddress(), ts.isCommitted());
            offset += TM.ENTRY_SIZE;
            count--;
            if (count == 0
                    || offset + TM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                advance(offset);
                offset = 0;
            }
            if (TM.ENTRY_SIZE >= _writeBuffer.remaining()) {
                flush();
            }
        }

        Debug.$assert(count == 0);
        _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeCheckpointToJournal(final Checkpoint checkpoint)
            throws PersistitIOException {
        //
        // Make sure all prior journal entries are committed to disk before
        // writing this record.
        //
        force();
        final int recordSize = CP.OVERHEAD;
        _lastValidCheckpoint = checkpoint;
        _lastValidCheckpointJournalAddress = _currentAddress - CP.OVERHEAD;
        _lastValidCheckpointBaseAddress = _baseAddress;
        //
        // Prepare room for CP.OVERHEAD bytes in the journal. If doing so
        // started a new journal file then there's no need to write another
        // CP record.
        //
        if (!prepareWriteBuffer(recordSize)) {
            final long address = _currentAddress;
            CP.putLength(_writeBuffer, CP.OVERHEAD);
            CP.putType(_writeBuffer);
            CP.putTimestamp(_writeBuffer, checkpoint.getTimestamp());
            CP.putSystemTimeMillis(_writeBuffer,
                    checkpoint.getSystemTimeMillis());
            CP.putBaseAddress(_writeBuffer, _baseAddress);
            _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                    _currentAddress);
            advance(recordSize);
            force();

            checkpointWritten(checkpoint);

            if (_persistit.getLogBase().isLoggable(
                    LogBase.LOG_CHECKPOINT_WRITTEN)) {
                _persistit.getLogBase().log(LogBase.LOG_CHECKPOINT_WRITTEN,
                        checkpoint, addressToFile(address), address);
            }
        }
    }

    void writePageToJournal(final Buffer buffer) throws PersistitIOException {

        final Volume volume;
        final int recordSize;

        synchronized (this) {
            volume = buffer.getVolume();
            final int available = buffer.getAvailableSize();
            recordSize = PA.OVERHEAD + buffer.getBufferSize() - available;
            prepareWriteBuffer(recordSize);
            int handle = handleForVolume(volume);
            final long address = _currentAddress;
            final int position = _writeBuffer.position();

            final int leftSize = available == 0 ? 0 : buffer.getAlloc()
                    - available;
            PA.putLength(_writeBuffer, recordSize);
            PA.putVolumeHandle(_writeBuffer, handle);
            PA.putType(_writeBuffer);
            PA.putTimestamp(_writeBuffer,
                    buffer.isTransient() ? -1 : buffer.getTimestamp());
            PA.putLeftSize(_writeBuffer, leftSize);
            PA.putBufferSize(_writeBuffer, buffer.getBufferSize());
            PA.putPageAddress(_writeBuffer, buffer.getPageAddress());
            advance(PA.OVERHEAD);
            final int payloadSize = recordSize - PA.OVERHEAD;

            if (leftSize > 0) {
                final int rightSize = payloadSize - leftSize;
                _writeBuffer.put(buffer.getBytes(), 0, leftSize);
                _writeBuffer.put(buffer.getBytes(), buffer.getBufferSize()
                        - rightSize, rightSize);
            } else {
                _writeBuffer.put(buffer.getBytes());
            }
            Debug.$assert(_writeBuffer.position() - position == recordSize);
            _currentAddress += recordSize - PA.OVERHEAD;

            final PageNode pageNode = new PageNode(handle,
                    buffer.getPageAddress(), address, buffer.getTimestamp());
            final PageNode oldPageNode = _pageMap.put(pageNode, pageNode);
            pageNode.setPrevious(oldPageNode);
            _writePageCount++;
        }
        _persistit.getIOMeter().chargeWritePageToJournal(volume,
                buffer.getPageAddress(), buffer.getBufferSize(),
                _currentAddress - recordSize, urgency());
    }

    /**
     * package-private for unit tests only.
     * 
     * @param volume
     * @param handle
     * @throws PersistitIOException
     */
    void writeVolumeHandleToJournal(final VolumeDescriptor volume,
            final int handle) throws PersistitIOException {
        prepareWriteBuffer(IV.MAX_LENGTH);
        IV.putType(_writeBuffer);
        IV.putHandle(_writeBuffer, handle);
        IV.putVolumeId(_writeBuffer, volume.getId());
        IV.putTimestamp(_writeBuffer, 0);
        IV.putVolumeName(_writeBuffer, volume.getName());
        final int recordSize = IV.getLength(_writeBuffer);
        _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                _currentAddress);
        advance(recordSize);
    }

    void writeTreeHandleToJournal(final TreeDescriptor td, final int handle)
            throws PersistitIOException {
        prepareWriteBuffer(IT.MAX_LENGTH);
        IT.putType(_writeBuffer);
        IT.putHandle(_writeBuffer, handle);
        IT.putVolumeHandle(_writeBuffer, td.getVolumeHandle());
        IT.putTimestamp(_writeBuffer, 0);
        IT.putTreeName(_writeBuffer, td.getTreeName());
        final int recordSize = IT.getLength(_writeBuffer);
        _persistit.getIOMeter().chargeWriteOtherToJournal(recordSize,
                _currentAddress);
        advance(recordSize);
    }

    synchronized void writeStoreRecordToJournal(final long timestamp,
            final int treeHandle, final Key key, final Value value)
            throws PersistitIOException {
        final int recordSize = SR.OVERHEAD + key.getEncodedSize()
                + value.getEncodedSize();
        prepareWriteBuffer(recordSize);
        SR.putLength(_writeBuffer, recordSize);
        SR.putType(_writeBuffer);
        SR.putTimestamp(_writeBuffer, timestamp);
        SR.putTreeHandle(_writeBuffer, treeHandle);
        SR.putKeySize(_writeBuffer, (short) key.getEncodedSize());
        advance(SR.OVERHEAD);
        _writeBuffer.put(key.getEncodedBytes(), 0, key.getEncodedSize());
        _writeBuffer.put(value.getEncodedBytes(), 0, value.getEncodedSize());
        _currentAddress += recordSize - SR.OVERHEAD;
        _persistit.getIOMeter().chargeWriteSRtoJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeDeleteRecordToJournal(final long timestamp,
            final int treeHandle, final Key key1, final Key key2)
            throws PersistitIOException {
        int recordSize = DR.OVERHEAD + key1.getEncodedSize()
                + key2.getEncodedSize();
        prepareWriteBuffer(recordSize);
        DR.putLength(_writeBuffer, recordSize);
        DR.putType(_writeBuffer);
        DR.putTimestamp(_writeBuffer, timestamp);
        DR.putTreeHandle(_writeBuffer, treeHandle);
        DR.putKey1Size(_writeBuffer, (short) key1.getEncodedSize());
        advance(DR.OVERHEAD);
        _writeBuffer.put(key1.getEncodedBytes(), 0, key1.getEncodedSize());
        _writeBuffer.put(key2.getEncodedBytes(), 0, key2.getEncodedSize());
        _currentAddress += recordSize - DR.OVERHEAD;
        _persistit.getIOMeter().chargeWriteDRtoJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeDeleteTreeToJournal(final long timestamp,
            final int treeHandle) throws PersistitIOException {
        final int recordSize = DT.OVERHEAD;
        prepareWriteBuffer(recordSize);
        DT.putLength(_writeBuffer, recordSize);
        DT.putType(_writeBuffer);
        DT.putTimestamp(_writeBuffer, timestamp);
        DT.putTreeHandle(_writeBuffer, treeHandle);
        advance(recordSize);
        _persistit.getIOMeter().chargeWriteDTtoJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeTransactionStartToJournal(final long startTimestamp,
            final long commitTimestamp) throws PersistitIOException {

        final Long key = Long.valueOf(commitTimestamp);
        TransactionStatus ts = _liveTransactionMap.get(key);
        if (ts != null) {
            throw new CorruptJournalException("TS Transaction timestamp "
                    + commitTimestamp + " already started at "
                    + ts.getStartAddress());
        }
        ts = new TransactionStatus(startTimestamp, commitTimestamp,
                _currentAddress);
        _liveTransactionMap.put(key, ts);

        final int recordSize = TS.OVERHEAD;
        prepareWriteBuffer(recordSize);
        TS.putType(_writeBuffer);
        TS.putTimestamp(_writeBuffer, commitTimestamp);
        TS.putStartTimestamp(_writeBuffer, startTimestamp);
        TS.putLength(_writeBuffer, recordSize);
        advance(recordSize);
        _persistit.getIOMeter().chargeWriteTStoJournal(recordSize,
                _currentAddress - recordSize);
    }

    synchronized void writeTransactionCommitToJournal(final long timestamp)
            throws PersistitIOException {

        final Long key = Long.valueOf(timestamp);
        TransactionStatus ts = _liveTransactionMap.get(key);
        if (ts == null) {
            throw new CorruptJournalException("TC Transaction timestamp "
                    + timestamp + " never started");
        }
        if (timestamp != _unitTestNeverCloseTransactionTimestamp) {
            ts.setCommitted(true);
        }

        final int recordSize = TC.OVERHEAD;
        prepareWriteBuffer(recordSize);
        TC.putType(_writeBuffer);
        TC.putTimestamp(_writeBuffer, timestamp);
        TC.putLength(_writeBuffer, recordSize);
        advance(recordSize);
        _persistit.getIOMeter().chargeWriteTCtoJournal(recordSize,
                _currentAddress - recordSize);
    }

    static long fileToGeneration(final File file) {
        final Matcher matcher = PATH_PATTERN.matcher(file.getName());
        if (matcher.matches()) {
            // TODO - validate range
            return Long.parseLong(matcher.group(1));
        } else {
            return -1;
        }
    }

    static File generationToFile(final String path, final long generation) {
        return new File(String.format(PATH_FORMAT, path, generation));
    }

    File addressToFile(final long address) {
        return generationToFile(_journalFilePath, address / _blockSize);
    }

    long fileToAddress(final File file) {
        long generation = fileToGeneration(file);
        if (generation == -1) {
            return generation;
        } else {
            return generation * _blockSize;
        }
    }

    long addressToOffset(final long address) {
        return address % _blockSize;
    }

    public void close() throws PersistitIOException {

        synchronized (this) {
            _closed.set(true);
        }

        rollover();

        _persistit.waitForIOTaskStop(_copier);
        _persistit.waitForIOTaskStop(_flusher);

        synchronized (this) {
            try {
                for (final FileChannel channel : _journalFileChannels.values()) {
                    if (channel != null) {
                        channel.close();
                    }
                }
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
            _journalFileChannels.clear();
            _handleToTreeMap.clear();
            _handleToVolumeMap.clear();
            _volumeToHandleMap.clear();
            _treeToHandleMap.clear();
            _pageMap.clear();
            _writeBuffer = null;
        }

    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the copier and flusher
     * threads. This method should be used only by tests.
     */
    void crash() {
        IOTaskRunnable.crash(_flusher);
        IOTaskRunnable.crash(_copier);
    }

    /**
     * Flushes the write buffer
     * 
     * @throws PersistitIOException
     */
    synchronized void flush() throws PersistitIOException {
        final long address = _writeBufferAddress;
        if (address != Long.MAX_VALUE) {
            try {
                if (_writeBuffer.position() > 0) {
                    final FileChannel channel = getFileChannel(address);
                    Debug.$assert(channel.size() == addressToOffset(address));
                    _writeBuffer.flip();
                    channel.write(_writeBuffer);
                    _writeBufferAddress += _writeBuffer.position();
                    if (_writeBuffer.capacity() != _writeBufferSize) {
                        _writeBuffer = ByteBuffer.allocate(_writeBufferSize);
                    } else {
                        _writeBuffer.clear();
                    }
                    final long remaining = _blockSize
                            - (_writeBufferAddress % _blockSize);
                    if (remaining < _writeBuffer.limit()) {
                        _writeBuffer.limit((int) remaining);
                    }
                }
            } catch (IOException e) {
                throw new PersistitIOException(
                        "IOException while writing to file "
                                + addressToFile(address), e);
            }
        }
    }

    /**
     * Force all data written to the journal file to disk.
     */
    public void force() throws PersistitIOException {
        final long address;
        synchronized (this) {
            address = _writeBufferAddress;
        }
        if (address != Long.MAX_VALUE) {
            try {
                flush();
                final FileChannel channel = getFileChannel(address);
                channel.force(false);
            } catch (IOException e) {
                throw new PersistitIOException(
                        "IOException while writing to file "
                                + addressToFile(address), e);
            }
        }
    }

    /**
     * Maps a ByteBuffer to a file address, as needed to ensure client methods
     * can write their records. This method modifies the values of _writeBuffer,
     * _writeBufferAddress, and in case a new journal file is prepared (a
     * "roll-over" event), it also modifies _currentAddress to reflect the
     * current address in the new file.
     * 
     * @param size
     *            Size of record to be written
     * @return <tt>true</tt> iff a new journal file was started
     * @throws PersistitIOException
     */
    private synchronized boolean prepareWriteBuffer(final int size)
            throws PersistitIOException {
        boolean newJournalFile = false;
        if (_currentAddress % _blockSize == 0) {
            flush();
            _writeBufferAddress = _currentAddress;
            startJournalFile();
            newJournalFile = true;
        }
        Debug.$assert(_writeBufferAddress + _writeBuffer.position() == _currentAddress);
        //
        // If the current journal file has room for the record, then return.
        //
        if (_writeBuffer.remaining() >= size + JE.OVERHEAD) {
            return newJournalFile;
        }
        //
        // Otherwise, flush the write buffer and try again
        flush();

        if (_writeBuffer.remaining() >= size + JE.OVERHEAD) {
            return newJournalFile;
        }
        //
        // In the special case of a record which may be longer than
        // the capacity of the buffer (e.g., the PageMap), then check whether
        // there is enough room in the file to hold the entire map. In that case
        // then the buffer is prepared because the PM and TM writers know how to
        // fill the buffer multiple times.
        //
        if (_writeBuffer.remaining() == _writeBuffer.capacity()) {
            long remaining = _blockSize - (_currentAddress % _blockSize);
            if (remaining > size + JE.OVERHEAD) {
                return newJournalFile;
            }
        }
        //
        // Finally if there's still not enough room we're committed to
        // rolling the journal.
        //
        rollover();
        startJournalFile();
        return true;
    }

    synchronized void rollover() throws PersistitIOException {
        if (_writeBufferAddress != Long.MAX_VALUE) {
            writeJournalEnd();
            flush();
            try {
                final FileChannel channel = getFileChannel(_currentAddress);
                final long length = _currentAddress % _blockSize;
                final boolean matches = length == (_writeBuffer.position() + _writeBufferAddress)
                        % _blockSize;
                Debug.$assert(matches);
                if (matches) {
                    channel.truncate(length);
                }
                channel.force(true);
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
            _currentAddress = ((_currentAddress / _blockSize) + 1) * _blockSize;
            _writeBuffer.clear();
            _writeBufferAddress = _currentAddress;
        }
    }

    private void startJournalFile() throws PersistitIOException {
        //
        // Write the beginning of a new journal file.
        //
        // The information written here is designed to accelerate recovery.
        // The recovery process can simply read the JournalHeader and
        // subsequent records from the last journal file to load the page
        // map and live transaction map. The journal file is valid for
        // recovery only if the CP (checkpoint) record is present in the
        // recovered file.
        //
        writeJournalHeader();
        //
        // Write IV (identify volume) records for each volume in the handle
        // map
        //
        for (final Map.Entry<Integer, VolumeDescriptor> entry : _handleToVolumeMap
                .entrySet()) {
            writeVolumeHandleToJournal(entry.getValue(), entry.getKey()
                    .intValue());
        }
        //
        // Write IT (identify tree) records for each tree in the handle
        // map
        //
        for (final Map.Entry<Integer, TreeDescriptor> entry : _handleToTreeMap
                .entrySet()) {
            writeTreeHandleToJournal(entry.getValue(), entry.getKey()
                    .intValue());
        }
        //
        // Write the PM (Page Map) record
        //
        writePageMap();
        //
        // Write the TM (Transaction Map) record
        //
        writeTransactionMap();
        //
        // Finally, write the current CP (checkpoint) record.
        //
        writeCheckpointToJournal(_lastValidCheckpoint);
    }

    private synchronized FileChannel getFileChannel(long address)
            throws PersistitIOException {
        if (address < _deleteBoundaryAddress
                || address > _currentAddress + _blockSize) {
            throw new IllegalArgumentException("Invalid journal address "
                    + address + " outside of range (" + _baseAddress + ":"
                    + (_currentAddress + _blockSize) + ")");
        }
        final long generation = address / _blockSize;
        FileChannel channel = _journalFileChannels.get(generation);
        if (channel == null) {
            try {
                final RandomAccessFile raf = new RandomAccessFile(
                        addressToFile(address), "rw");
                channel = raf.getChannel();
                _journalFileChannels.put(generation, channel);
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }
        return channel;
    }

    /**
     * Set the copyFast flag and then wait until all pages have been copied to
     * their respective volumes, allowing the journal files to be deleted.
     * 
     * @param toTimestamp
     * @throws PersistitException
     */
    public void copyBack(final long toTimestamp) throws PersistitException {
        synchronized (this) {
            _copyFast.set(true);
            notifyAll();
            while (_copyFast.get()) {
                try {
                    wait(100);
                } catch (InterruptedException ie) {
                    // ignore;
                }
            }
            Debug.$assert(_pageMap.isEmpty()); // TODO - remove this
        }
    }

    /**
     * Remove transactions and PageNode entries when possible due to completion
     * of a new checkpoint.
     * 
     * @param checkpoint
     */
    private void checkpointWritten(final Checkpoint checkpoint) {

        //
        // Will become the earliest timestamp of any record needed to
        // be retained for recovery. For transactions containing LONG_RECORD
        // pages, those pages may be written to the journal with timestamps
        // earlier than the commitTimestamp of the transaction. The are
        // guaranteed to be written with timestamp valuess later than the
        // transaction's startTimestamp. Therefore we can't cull PageMap entries
        // later than this recoveryTimestamp because the pages they refer to may
        // be needed for recovery.
        //
        long recoveryTimestamp = checkpoint.getTimestamp();

        //
        // Remove any committed transactions that committed before the
        // checkpoint. No need to keep a record of such a transaction since it's
        // updates are now fully written to the journal in modified page images.
        //
        for (final Iterator<TransactionStatus> iterator = _liveTransactionMap
                .values().iterator(); iterator.hasNext();) {
            final TransactionStatus ts = iterator.next();
            if (ts.isCommitted()
                    && ts.getCommitTimestamp() < checkpoint.getTimestamp()) {
                iterator.remove();
            } else if (ts.getStartTimestamp() < recoveryTimestamp) {
                recoveryTimestamp = ts.getStartTimestamp();
            }
        }
        //
        // Remove all but the most recent PageNode version before the
        // checkpoint.
        //
        for (final PageNode pageNode : _pageMap.values()) {
            for (PageNode pn = pageNode; pn != null; pn = pn.getPrevious()) {
                if (pn.getTimestamp() < recoveryTimestamp) {
                    pn.setPrevious(null);
                    break;
                }
            }
        }

        //
        // Remove any PageNode from the branchMap having a timestamp less
        // than the checkpoint. Generally all such entries are removed after
        // the first checkpoint that has been established after recovery.
        //
        for (final Iterator<PageNode> iterator = _branchMap.values().iterator(); iterator
                .hasNext();) {
            final PageNode pageNode = iterator.next();
            if (pageNode.getTimestamp() < recoveryTimestamp) {
                iterator.remove();
            }
        }

    }

    static class VolumeDescriptor {

        private final long _id;

        private final String _name;

        VolumeDescriptor(final String name, final long id) {
            this._name = name;
            this._id = id;
        }

        VolumeDescriptor(final Volume volume) {
            _name = volume.getName();
            _id = volume.getId();
        }

        String getName() {
            return _name;
        }

        long getId() {
            return _id;
        }

        @Override
        public boolean equals(final Object object) {
            final VolumeDescriptor vd = (VolumeDescriptor) object;
            return vd._name.equals(_name) && vd._id == _id;
        }

        @Override
        public int hashCode() {
            return _name.hashCode() ^ (int) _id;
        }

        @Override
        public String toString() {
            return _name;
        }

    }

    static class TreeDescriptor {

        final int _volumeHandle;

        final String _treeName;

        TreeDescriptor(final int volumeHandle, final String treeName) {
            _volumeHandle = volumeHandle;
            _treeName = treeName;
        }

        int getVolumeHandle() {
            return _volumeHandle;
        }

        String getTreeName() {
            return _treeName;
        }

        public boolean equals(final Object obj) {
            final TreeDescriptor td = (TreeDescriptor) obj;
            return td._treeName.equals(_treeName)
                    && td._volumeHandle == _volumeHandle;
        }

        public int hashCode() {
            return _treeName.hashCode() ^ _volumeHandle;
        }

        @Override
        public String toString() {
            return "{" + _volumeHandle + "}" + _treeName;
        }
    }

    /**
     * A PageNode represents the existence of a copy of a page in the journal.
     * It links to previously created PageNode objects which refer to earlier
     * versions of the same page. These earlier instances are truncated whenever
     * a later version of the same page has been checkpointed.
     * 
     * PageNode instances are designed to serve as both Key and Value fields of
     * the _pageNodeMap. The general rubric when adding a page to the journal is
     * to construct a PageNode representing the page image, and then use it to
     * perform a lookup in the _pageNodeMap. If there is no matching PageNode
     * already in the map then simply add the new one. If there is a matching
     * PageNode, link it to the new one then replace the entry in the map.
     * 
     * This class implement Comparable on the page address. This is used in
     * forming a sorted set of PageNodes so that we can copy pages in roughly
     * sequential order to each Volume file.
     */
    static class PageNode implements Comparable<PageNode> {

        final int _volumeHandle;

        final long _pageAddress;

        final long _journalAddress;

        final long _timestamp;

        PageNode _previous;

        PageNode(final int volumeHandle, final long pageAddress,
                final long journalAddress, final long timestamp) {
            this._volumeHandle = volumeHandle;
            this._pageAddress = pageAddress;
            this._journalAddress = journalAddress;
            this._timestamp = timestamp;
        }

        /**
         * @return the previous
         */
        public PageNode getPrevious() {
            return _previous;
        }

        /**
         * @param previous
         *            the previous to set
         */
        public void setPrevious(PageNode previous) {
            this._previous = previous;
        }

        /**
         * @return the volumeHandle
         */
        public int getVolumeHandle() {
            return _volumeHandle;
        }

        /**
         * @return the pageAddress
         */
        public long getPageAddress() {
            return _pageAddress;
        }

        /**
         * @return the journalAddress
         */
        public long getJournalAddress() {
            return _journalAddress;
        }

        /**
         * @return the timestamp
         */
        public long getTimestamp() {
            return _timestamp;
        }

        @Override
        public int hashCode() {
            return _volumeHandle ^ (int) _pageAddress
                    ^ (int) (_pageAddress >>> 32);
        }

        @Override
        public boolean equals(Object obj) {
            final PageNode pn = (PageNode) obj;
            return _pageAddress == pn._pageAddress
                    && _volumeHandle == pn._volumeHandle;
        }

        @Override
        public String toString() {
            return String.format("[%d]%d@%d{%d}%s", _volumeHandle,
                    _pageAddress, _journalAddress, _timestamp,
                    _previous == null ? "" : "+");
        }

        public String toString(final JournalManager jman) {
            final VolumeDescriptor vd = jman._handleToVolumeMap
                    .get(_volumeHandle);
            if (vd == null) {
                return toString();
            }
            return String.format("%s:%d@%d{%d}%s", vd, _pageAddress,
                    _journalAddress, _timestamp, _previous == null ? "" : "+");
        }

        public String toStringPageAddress(final VolumeHandleLookup lvh) {
            final VolumeDescriptor vd = lvh.lookupVolumeHandle(_volumeHandle);
            return String.format("%s:%d",
                    vd == null ? String.valueOf(_volumeHandle) : vd.toString(),
                    _pageAddress);
        }

        public String toStringJournalAddress(final VolumeHandleLookup lvn) {
            return String.format("%d{%d}%s", _journalAddress, _timestamp,
                    _previous == null ? "" : "+");

        }

        @Override
        public int compareTo(PageNode pn) {
            if (_volumeHandle != pn.getVolumeHandle()) {
                return _volumeHandle < pn._volumeHandle ? -1 : 1;
            }
            return _pageAddress < pn.getPageAddress() ? -1 : _pageAddress > pn
                    .getPageAddress() ? 1 : 0;
        }
    }

    static class TransactionStatus {

        private final long _startAddress;

        private final long _startTimestamp;

        private final long _commitTimestamp;

        private boolean _committed;

        TransactionStatus(final long startTimestamp,
                final long commitTimestamp, final long address) {
            _startTimestamp = startTimestamp;
            _commitTimestamp = commitTimestamp;
            _startAddress = address;
        }

        long getStartAddress() {
            return _startAddress;
        }

        long getStartTimestamp() {
            return _startTimestamp;
        }

        long getCommitTimestamp() {
            return _commitTimestamp;
        }

        void setCommitted(final boolean committed) {
            _committed = committed;
        }

        boolean isCommitted() {
            return _committed;
        }

        @Override
        public String toString() {
            return String.format("TStatus %,d{%,d}%s", _startAddress,
                    _commitTimestamp, _committed ? "c" : "u");
        }

    }

    private class JournalCopier extends IOTaskRunnable {

        JournalCopier() {
            super(_persistit);
        }

        void start() {
            start("JOURNAL_COPIER", _copierInterval);
        }

        @Override
        public void runTask() throws Exception {

            if (!_appendOnly.get()) {
                _copying.set(true);
                try {
                    copierCycle();
                } finally {
                    _copying.set(false);
                }
            }
        }

        @Override
        protected boolean shouldStop() {
            return _closed.get();
        }

        @Override
        /**
         * Return a nice interval, in milliseconds, to wait between
         * copierCycle invocations. The interval decreases as interval 
         * goes up, and becomes zero when the urgency is 10.
         */
        public long getPollInterval() {
            int urgency = urgency();
            if (urgency <= HALF_URGENT) {
                return super.getPollInterval();
            }
            if (urgency >= ALMOST_URGENT) {
                return 0;
            }
            return super.getPollInterval() / (urgency - HALF_URGENT);
        }
    }

    private class JournalFlusher extends IOTaskRunnable {

        long _lastLogMessageTime = 0;
        Exception _lastException = null;

        JournalFlusher() {
            super(_persistit);
        }

        void start() {
            start("JOURNAL_FLUSHER", _flushInterval);
        }

        @Override
        protected void runTask() {
            _flushing.set(true);
            try {
                try {
                    force();
                } catch (PersistitIOException e) {
                    final long now = System.nanoTime();
                    if (!e.getClass().equals(_lastException.getClass())
                            || now - _lastLogMessageTime > -_logRepeatInterval) {
                        _lastLogMessageTime = now;
                        _lastException = e;
                        if (_persistit.getLogBase().isLoggable(
                                LogBase.LOG_JOURNAL_WRITE_ERROR)) {
                            _persistit.getLogBase().log(
                                    LogBase.LOG_JOURNAL_WRITE_ERROR, e,
                                    addressToFile(_writeBufferAddress));
                        }
                    }
                }
            } finally {
                _flushing.set(false);
            }
        }

        @Override
        protected boolean shouldStop() {
            return _closed.get();
        }
    }

    private void copierCycle() throws PersistitException {
        final SortedMap<PageNode, PageNode> sortedMap = new TreeMap<PageNode, PageNode>();
        final boolean wasUrgent;

        synchronized (this) {
            final long timeStampUpperBound = Math.min(
                    _lastValidCheckpoint.getTimestamp(), _copierTimestampLimit);

            wasUrgent = _copyFast.get();
            for (long boundary = (_baseAddress / _blockSize) * _blockSize
                    + _blockSize; sortedMap.size() < _copiesPerCycle
                    && boundary < _currentAddress + _blockSize; boundary += _blockSize) {
                for (final PageNode pageNode : _pageMap.values()) {
                    for (PageNode pn = pageNode; pn != null; pn = pn
                            .getPrevious()) {
                        if (pn.getTimestamp() < timeStampUpperBound
                                && (pn.getJournalAddress() < boundary || wasUrgent)) {
                            sortedMap.put(pn, pn);
                            break;
                        }
                    }
                    if (_appendOnly.get()) {
                        return;
                    }
                    if (sortedMap.size() >= _copiesPerCycle) {
                        break;
                    }
                }
            }
        }

        Volume volume = null;
        VolumeDescriptor vd = null;
        int handle = -1;

        final HashSet<Volume> volumes = new HashSet<Volume>();

        final ByteBuffer bb = ByteBuffer.allocate(DEFAULT_READ_BUFFER_SIZE);
        for (final Iterator<PageNode> iterator = sortedMap.keySet().iterator(); iterator
                .hasNext();) {
            if (_closed.get() && !wasUrgent || _appendOnly.get()) {
                return;
            }
            final PageNode pageNode = iterator.next();
            if (pageNode.getVolumeHandle() != handle) {
                handle = -1;
                volume = null;
                vd = null;
                vd = _handleToVolumeMap.get(pageNode.getVolumeHandle());
                if (vd == null) {
                    // TODO
                } else {
                    volume = _persistit.getVolume(vd.getName());
                }

            }
            if (volume == null || volume.isClosed() || volume.isTransient()) {
                // Remove from the sortedMap so that below we won't remove from
                // the pageMap.
                iterator.remove();
                continue;
            }

            if (volume.getId() != vd.getId()) {
                throw new CorruptJournalException(vd
                        + " does not identify a valid Volume at "
                        + pageNode.toStringJournalAddress(this));
            }

            final long pageAddress = readPageBufferFromJournal(pageNode, bb);

            if (bb.limit() != volume.getPageSize()) {
                throw new CorruptJournalException(
                        pageNode.toStringPageAddress(this) + " bufferSize "
                                + bb.limit() + " does not match " + volume
                                + " bufferSize " + volume.getPageSize()
                                + " at "
                                + pageNode.toStringJournalAddress(this));
            }

            if (pageAddress != pageNode.getPageAddress()) {
                throw new CorruptJournalException(
                        pageNode.toStringPageAddress(this)
                                + " does not match page address " + pageAddress
                                + " found at "
                                + pageNode.toStringJournalAddress(this));
            }

            try {
                volume.writePage(bb, pageAddress);
                volumes.add(volume);
                _copiedPageCount++;
                _persistit.getIOMeter().chargeCopyPageToVolume(volume,
                        pageAddress, volume.getPageSize(),
                        pageNode.getJournalAddress(),
                        wasUrgent ? URGENT : urgency());
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }

        for (final Volume vol : volumes) {
            vol.sync();
        }

        //
        // Files and FileChannels no longer needed for recovery.
        //
        final List<FileChannel> obsoleteFileChannels = new ArrayList<FileChannel>();
        final List<File> obsoleteFiles = new ArrayList<File>();

        // Address of the first file needed for recovery
        long deleteBoundary = 0;

        synchronized (this) {
            for (final PageNode copiedPageNode : sortedMap.values()) {
                PageNode pageNode = _pageMap.get(copiedPageNode);
                if (pageNode.getJournalAddress() == copiedPageNode
                        .getJournalAddress()) {
                    _pageMap.remove(pageNode);
                } else {
                    PageNode previous = pageNode.getPrevious();
                    while (previous != null) {
                        if (previous.getJournalAddress() == copiedPageNode
                                .getJournalAddress()) {
                            // No need to keep that previous entry, or any of
                            // its predecessors
                            pageNode.setPrevious(null);
                            break;
                        } else {
                            pageNode = previous;
                            previous = pageNode.getPrevious();
                        }
                    }
                }
            }
            //
            // Will hold the address of the first record containing information
            // not yet copied back into a Volume, and therefore required for
            // recovery.
            //
            long recoveryBoundary = _currentAddress;
            //
            // Detect first journal address holding a mapped page
            // required for recovery
            //
            for (final PageNode pageNode : _pageMap.values()) {
                //
                // If there are multiple versions, we need to keep
                // the most recent one that has been checkpointed.
                //
                for (PageNode pn = pageNode; pn != null; pn = pn.getPrevious()) {
                    if (pn.getJournalAddress() < recoveryBoundary) {
                        recoveryBoundary = pn.getJournalAddress();
                    }
                }
            }
            //
            // Detect first journal address still holding an uncheckpointed
            // Transaction required for recovery.
            //
            for (final Iterator<TransactionStatus> iterator = _liveTransactionMap
                    .values().iterator(); iterator.hasNext();) {
                final TransactionStatus ts = iterator.next();
                if (ts.getStartAddress() < recoveryBoundary) {
                    recoveryBoundary = ts.getStartAddress();
                }
            }

            _baseAddress = recoveryBoundary;
            for (deleteBoundary = _deleteBoundaryAddress; deleteBoundary
                    + _blockSize <= _lastValidCheckpointBaseAddress; deleteBoundary += _blockSize) {
                final long generation = deleteBoundary / _blockSize;
                final FileChannel channel = _journalFileChannels
                        .remove(generation);
                if (channel != null) {
                    obsoleteFileChannels.add(channel);
                }
                obsoleteFiles.add(addressToFile(deleteBoundary));
            }
            //
            // Conditions mean that there is no active content in the
            // journal and the current journal file has more than RT bytes
            // in it where RT is the "rolloverThreshold". When these
            // conditions are met then we force a rollover and cause the
            // current journal file to be deleted. This behavior keeps
            // the journal small when there are no un-checkpointed pages
            // or transactions.
            //
            if (_baseAddress == _currentAddress
                    && _lastValidCheckpointBaseAddress >= _currentAddress
                            - CP.OVERHEAD
                    && (_currentAddress % _blockSize) > rolloverThreshold()) {
                final FileChannel channel = _journalFileChannels
                        .remove(_currentAddress / _blockSize);
                if (channel != null) {
                    obsoleteFileChannels.add(channel);
                }
                obsoleteFiles.add(addressToFile(_currentAddress));
                rollover();
                _baseAddress = _currentAddress;
            }
        }

        for (final FileChannel channel : obsoleteFileChannels) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    // TODO - log this?
                    // Ignored for now - this simply means we can't close
                    // a file we don't need any more.
                }
            }
        }

        boolean deleted = true;
        for (final File file : obsoleteFiles) {
            if (!file.delete()) {
                deleted = false;
                // TODO - log this.
                // Ignored for now - this simply means we can't delete
                // a file we don't need any more.
            }
        }
        if (deleted) {
            _deleteBoundaryAddress = deleteBoundary;
        }
        if (sortedMap.isEmpty() && wasUrgent) {
            _copyFast.set(false);
        }
    }

    private long rolloverThreshold() {
        return _closed.get() ? 0 : ROLLOVER_THRESHOLD;
    }

    /**
     * For use only by unit tests that prove having an open transaction prevents
     * deletion of journal files.
     * 
     * @param id
     */
    void setUnitTestNeverCloseTransactionId(final long id) {
        _unitTestNeverCloseTransactionTimestamp = id;
    }

    /**
     * For use only by unit tests that test page maps, etc.
     * 
     * @param handleToVolumeMap
     */
    void unitTestInjectVolumes(
            final Map<Integer, VolumeDescriptor> handleToVolumeMap) {
        _handleToVolumeMap.putAll(handleToVolumeMap);
    }

    /**
     * For use only by unit tests that test page maps, etc.
     * 
     * @param handleToVolumeMap
     */
    void unitTestInjectPageMap(final Map<PageNode, PageNode> pageMap) {
        _pageMap.putAll(pageMap);
    }

    void unitTestInjectTransactionMap(
            final Map<Long, TransactionStatus> transactionMap) {
        _liveTransactionMap.putAll(transactionMap);
    }
}
