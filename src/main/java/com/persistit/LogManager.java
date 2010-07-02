package com.persistit;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.persistit.exception.CorruptLogException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

/**
 * Manages the disk-based I/O log. The log contains both committed transactions
 * and images of updated pages.
 * 
 * @author peter
 * 
 */
public class LogManager {

    private final static long GIG = 1024 * 1024 * 1024;

    public final static long DEFAULT_LOG_SIZE = GIG;

    public final static long MINIMUM_LOG_SIZE = GIG / 64;

    public final static long MAXIMUM_LOG_SIZE = GIG * 64;

    public final static int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;

    public final static int DEFAULT_READ_BUFFER_SIZE = 64 * 1024;

    public final static long DEFAULT_FLUSH_INTERVAL = 100;

    public final static int MAXIMUM_MAPPED_HANDLES = 4096;

    private final static String PATH_FORMAT = "%s.%016d";

    private final static Pattern PATH_PATTERN = Pattern
            .compile(".+\\.(\\d{16})");

    private final Map<VolumePage, FileAddress> _pageMap = new HashMap<VolumePage, FileAddress>();

    private final Map<File, FileChannel> _readChannelMap = new HashMap<File, FileChannel>();

    private final Map<VolumeDescriptor, Integer> _volumeToHandleMap = new HashMap<VolumeDescriptor, Integer>();

    private final Map<Integer, VolumeDescriptor> _handleToVolumeMap = new HashMap<Integer, VolumeDescriptor>();

    private final Map<Tree, Integer> _treeToHandleMap = new HashMap<Tree, Integer>();

    private final Map<Integer, Tree> _handleToTreeMap = new HashMap<Integer, Tree>();

    private final Persistit _persistit;

    private File _writeChannelFile;

    private FileChannel _writeChannel;

    private long _maximumFileSize;

    private int _writeBufferSize = DEFAULT_BUFFER_SIZE;

    private MappedByteBuffer _writeBuffer;

    private long _writeBufferAddress = 0;

    private long _flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Timer _flushTimer;

    private Thread _copierThread;

    private volatile boolean _closed;

    private File _directory;

    private final byte[] _bytes = new byte[4096];

    /**
     * Log file generation - serves as suffix on file name
     */
    private long _currentGeneration;

    private long _earliestGeneration;

    private int _handleCounter = 0;

    private boolean _recovered;

    private FileAddress _dirtyRecoveryFileAddress = null;

    private static class LogNotClosedException extends Exception {
        final FileAddress _fileAddress;

        public LogNotClosedException(final FileAddress fa) {
            _fileAddress = fa;
        }
    }

    public LogManager(final Persistit persistit) {
        _persistit = persistit;
    }

    public void init(final String path, final long maximumSize) {
        _directory = new File(path).getAbsoluteFile();
        _maximumFileSize = maximumSize;
        _closed = false;
        _flushTimer = new Timer("LOG_FLUSHER", true);
        _flushTimer.schedule(new LogFlusher(), _flushInterval, _flushInterval);
        _copierThread = new Thread(new LogCopier(), "LOG_COPIER");
        _copierThread.start();

    }

    public synchronized int getPageMapSize() {
        return _pageMap.size();
    }

    public synchronized long getCurrentGeneration() {
        return _currentGeneration;
    }

    public synchronized File getCurrentFile() {
        return _writeChannelFile;
    }

    public synchronized boolean readPageFromLog(final Buffer buffer)
            throws PersistitIOException {
        final int bufferSize = buffer.getBufferSize();
        final long pageAddress = buffer.getPageAddress();
        final ByteBuffer bb = buffer.getByteBuffer();

        final Volume volume = buffer.getVolume();
        final VolumePage vp = new VolumePage(new VolumeDescriptor(volume),
                pageAddress, 0);
        final FileAddress fa = _pageMap.get(vp);

        if (fa == null) {
            return false;
        }
        long recordPageAddress = readPageBufferFromLog(vp, fa, bb);

        if (pageAddress != recordPageAddress) {
            throw new CorruptLogException("Record at " + fa
                    + " is not volume/page " + vp);
        }

        if (bb.limit() != bufferSize) {
            throw new CorruptLogException("Record at " + fa
                    + " is wrong size: expected/actual=" + bufferSize + "/"
                    + bb.limit());
        }
        return true;
    }

    private long readPageBufferFromLog(final VolumePage vp,
            final FileAddress fa, final ByteBuffer bb)
            throws PersistitIOException, CorruptLogException {
        final FileChannel fc = getFileChannel(fa.getFile());
        try {
            final byte[] bytes = bb.array();
            bb.limit(LogRecord.PA.OVERHEAD).position(0);
            readFully(bb, fc, fa.getAddress(), fa);
            if (bb.position() < LogRecord.PA.OVERHEAD) {
                throw new CorruptLogException("Record at " + fa
                        + " is incomplete");
            }
            final char type = LogRecord.PA.getType(bytes);
            final int payloadSize = LogRecord.PA.getLength(bytes)
                    - LogRecord.PA.OVERHEAD;
            final int leftSize = LogRecord.PA.getLeftSize(bytes);
            final int bufferSize = LogRecord.PA.getBufferSize(bytes);
            final long pageAddress = LogRecord.PA.getPageAddress(bytes);

            if (type != LogRecord.PA.TYPE) {
                throw new CorruptLogException("Record at " + fa
                        + " is not a PAGE record");
            }

            if (leftSize < 0 || payloadSize < leftSize
                    || payloadSize > bufferSize) {
                throw new CorruptLogException("Record at " + fa
                        + " invalid sizes: recordSize= " + payloadSize
                        + " leftSize=" + leftSize + " bufferSize=" + bufferSize);
            }

            bb.limit(payloadSize).position(0);
            readFully(bb, fc, fa.getAddress() + LogRecord.PA.OVERHEAD, fa);

            if (leftSize > 0) {
                final int rightSize = payloadSize - leftSize;
                System.arraycopy(bytes, leftSize, bytes,
                        bufferSize - rightSize, rightSize);
                Arrays.fill(bytes, leftSize, bufferSize - rightSize, (byte) 0);
            }
            bb.limit(bufferSize).position(0);
            return pageAddress;
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
    }

    private void readFully(final ByteBuffer bb, final FileChannel fc,
            final long address, final FileAddress fa) throws IOException,
            CorruptLogException {
        long a = address;
        while (bb.remaining() > 0) {
            int count = fc.read(bb, a);
            if (count < 0) {
                throw new CorruptLogException("End of file at " + fa + ":"
                        + address);
            }
            a += count;
        }
    }

    public synchronized void writePageToLog(final Buffer buffer)
            throws PersistitIOException {

        final Volume volume = buffer.getVolume();
        final int available = buffer.getAvailableSize();
        final int recordSize = LogRecord.PA.OVERHEAD + buffer.getBufferSize()
                - available;
        long address = -1;
        int handle = handleForVolume(volume);

        if (writeBuffer(recordSize)) {
            handle = handleForVolume(volume);
        }
        address = _writeBufferAddress + _writeBuffer.position();
        final int leftSize = available == 0 ? 0 : buffer.getAlloc() - available;
        LogRecord.PA.putLength(_bytes, recordSize);
        LogRecord.PA.putVolumeHandle(_bytes, handle);
        LogRecord.PA.putType(_bytes);
        LogRecord.PA.putTimestamp(_bytes, buffer.getTimestamp());
        LogRecord.PA.putLeftSize(_bytes, leftSize);
        LogRecord.PA.putBufferSize(_bytes, buffer.getBufferSize());
        LogRecord.PA.putPageAddress(_bytes, buffer.getPageAddress());
        _writeBuffer.put(_bytes, 0, LogRecord.PA.OVERHEAD);
        final int payloadSize = recordSize - LogRecord.PA.OVERHEAD;

        if (leftSize > 0) {
            final int rightSize = payloadSize - leftSize;
            _writeBuffer.put(buffer.getBytes(), 0, leftSize);
            _writeBuffer.put(buffer.getBytes(), buffer.getBufferSize()
                    - rightSize, rightSize);
        } else {
            _writeBuffer.put(buffer.getBytes());
        }

        final VolumePage vp = new VolumePage(new VolumeDescriptor(volume),
                buffer.getPageAddress(), buffer.getTimestamp());
        final FileAddress fa = new FileAddress(_writeChannelFile, address,
                buffer.getTimestamp());
        _pageMap.put(vp, fa);

        Debug.$assert(LogRecord.getLength(_bytes) == recordSize);
        if (buffer.getPageAddress() != 0) {
            Debug.$assert(buffer.getPageAddress() == LogRecord.PA.getPageAddress(_bytes));
        }
    }

    public synchronized int handleForVolume(final Volume volume)
            throws PersistitIOException {
        final VolumeDescriptor vd = new VolumeDescriptor(volume);
        Integer handle = _volumeToHandleMap.get(vd);
        if (handle == null) {
            handle = Integer.valueOf(++_handleCounter);
            LogRecord.IV.putType(_bytes);
            LogRecord.IV.putHandle(_bytes, handle.intValue());
            LogRecord.IV.putVolumeId(_bytes, volume.getId());
            LogRecord.IV.putTimestamp(_bytes, 0); // TODO
            LogRecord.IV.putVolumeName(_bytes, volume.getPath());
            final int recordSize = LogRecord.IV.getLength(_bytes);
            writeBuffer(recordSize);
            _writeBuffer.put(_bytes, 0, recordSize);
            if (_volumeToHandleMap.size() >= MAXIMUM_MAPPED_HANDLES) {
                _volumeToHandleMap.clear();
                _handleToVolumeMap.clear();
            }
            _volumeToHandleMap.put(vd, handle);
            _handleToVolumeMap.put(handle, vd);
        }
        return handle.intValue();
    }

    public synchronized int handleForTree(final Tree tree)
            throws PersistitIOException {
        Integer handle = _treeToHandleMap.get(tree);
        if (handle == null) {
            handle = Integer.valueOf(++_handleCounter);
            LogRecord.IT.putType(_bytes);
            LogRecord.IT.putHandle(_bytes, handle.intValue());
            LogRecord.IT.putTimestamp(_bytes, 0);
            LogRecord.IV.putTimestamp(_bytes, 0); // TODO
            LogRecord.IV.putVolumeName(_bytes, tree.getName());
            final int recordSize = LogRecord.IV.getLength(_bytes);
            writeBuffer(recordSize);
            _writeBuffer.put(_bytes, 0, recordSize);
            if (_treeToHandleMap.size() >= MAXIMUM_MAPPED_HANDLES) {
                _treeToHandleMap.clear();
                _handleToTreeMap.clear();
            }
            _treeToHandleMap.put(tree, handle);
            _handleToTreeMap.put(handle, tree);
        }
        return handle.intValue();
    }

    public synchronized void rollover() throws PersistitIOException {
        _currentGeneration++;
        try {
            closeWriteChannel();
            final File file = new File(String.format(PATH_FORMAT, _directory,
                    _currentGeneration));
            final RandomAccessFile raf = new RandomAccessFile(file, "rw");
            _writeChannel = raf.getChannel();
            _writeChannelFile = file;
        } catch (IOException e) {
            throw new PersistitIOException(e);
        }

        _writeBufferAddress = 0;
        // New copies in every log file
        _handleToTreeMap.clear();
        _handleToVolumeMap.clear();
        _volumeToHandleMap.clear();
        _treeToHandleMap.clear();
    }

    public synchronized void recover() throws PersistitException {
        if (_recovered) {
            throw new IllegalStateException("Recovery already completed");
        }
        _pageMap.clear();
        _handleToTreeMap.clear();
        _treeToHandleMap.clear();
        _handleToVolumeMap.clear();
        _volumeToHandleMap.clear();

        _earliestGeneration = Long.MAX_VALUE;
        _currentGeneration = -1;

        final File[] files = files();
        _dirtyRecoveryFileAddress = null;
        try {
            for (final File file : files) {
                if (_dirtyRecoveryFileAddress != null) {
                    // a log file other than the final log file is corrupt.
                    throw new CorruptLogException("Invalid log record at "
                            + _dirtyRecoveryFileAddress.toString());
                }
                final FileChannel channel = new FileInputStream(file)
                        .getChannel();
                long bufferAddress = 0;
                while (bufferAddress < channel.size()) {
                    final long size = Math.min(channel.size() - bufferAddress,
                            _writeBufferSize);
                    final MappedByteBuffer readBuffer = channel.map(
                            MapMode.READ_ONLY, bufferAddress, size);
                    try {
                        while (recoverOneRecord(file, bufferAddress, readBuffer)) {
                            // nothing to do - work is performed in
                            // recoverOneRecord.
                        }
                    } catch (LogNotClosedException e) {
                        _dirtyRecoveryFileAddress = e._fileAddress;
                    }
                    bufferAddress += readBuffer.position();
                }
                final long generation = fileGeneration(file);
                if (generation >= 0) {
                    _currentGeneration = Math.max(generation,
                            _currentGeneration);
                    _earliestGeneration = Math.min(generation,
                            _earliestGeneration);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace(); // TODO!!
        }
        if (_earliestGeneration == Long.MAX_VALUE) {
            _earliestGeneration = 0;
        }
        if (_currentGeneration == -1) {
            _currentGeneration = 0;
        }
        _recovered = true;
    }

    private long fileGeneration(final File file) {
        final Matcher matcher = PATH_PATTERN.matcher(file.getName());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        } else {
            return -1;
        }
    }

    private boolean recoverOneRecord(final File file, final long bufferAddress,
            final MappedByteBuffer readBuffer) throws PersistitException,
            LogNotClosedException {
        final int from = readBuffer.position();
        if (readBuffer.remaining() < LogRecord.OVERHEAD) {
            return false;
        }
        readBuffer.mark();
        readBuffer.get(_bytes, 0, LogRecord.OVERHEAD);
        final char type = LogRecord.getType(_bytes);
        final long timestamp = LogRecord.getTimestamp(_bytes);
        switch (type) {

        case LogRecord.TYPE_IV: {
            final int recordSize = LogRecord.getLength(_bytes);
            if (recordSize > LogRecord.IV.MAX_LENGTH) {
                throw new CorruptLogException("IV LogRecord too long: "
                        + recordSize
                        + " bytes at position "
                        + new FileAddress(file, bufferAddress
                                + readBuffer.position() - LogRecord.OVERHEAD,
                                timestamp));
            }
            if (recordSize + from > readBuffer.limit()) {
                readBuffer.reset();
                return false;
            }
            readBuffer.get(_bytes, LogRecord.OVERHEAD, recordSize
                    - LogRecord.OVERHEAD);
            final Integer handle = Integer.valueOf(LogRecord.IV
                    .getHandle(_bytes));
            final String path = LogRecord.IV.getVolumeName(_bytes);
            final long volumeId = LogRecord.IV.getVolumeId(_bytes);
            VolumeDescriptor vd = new VolumeDescriptor(path, volumeId);
            _handleToVolumeMap.put(handle, vd);
            _volumeToHandleMap.put(vd, handle);
            break;
        }

        case LogRecord.TYPE_IT: {
            final int recordSize = LogRecord.getLength(_bytes);
            if (recordSize > LogRecord.IT.MAX_LENGTH) {
                throw new CorruptLogException("IT LogRecord too long: "
                        + recordSize
                        + " bytes at position "
                        + new FileAddress(file, bufferAddress
                                + readBuffer.position() - LogRecord.OVERHEAD,
                                timestamp));
            }
            if (recordSize + from > readBuffer.limit()) {
                readBuffer.reset();
                return false;
            }
            readBuffer.get(_bytes, LogRecord.OVERHEAD, recordSize
                    - LogRecord.OVERHEAD);
            final Integer handle = Integer.valueOf(LogRecord.IT
                    .getHandle(_bytes));
            final String path = LogRecord.IT.getTreeName(_bytes);
            final Integer volumeHandle = Integer.valueOf(LogRecord.IT
                    .getVolumeHandle(_bytes));
            final Integer treeHandle = Integer.valueOf(LogRecord.IT
                    .getHandle(_bytes));
            // TODO
            break;
        }

        case LogRecord.TYPE_PA: {
            final int recordSize = LogRecord.getLength(_bytes);
            if (recordSize > Buffer.MAX_BUFFER_SIZE + LogRecord.PA.OVERHEAD) {
                throw new CorruptLogException("PA LogRecord too long: "
                        + recordSize
                        + " bytes at position "
                        + new FileAddress(file, bufferAddress
                                + readBuffer.position() - LogRecord.OVERHEAD,
                                timestamp));
            }
            if (recordSize + from > readBuffer.limit()) {
                readBuffer.reset();
                return false;
            }
            readBuffer.get(_bytes, LogRecord.OVERHEAD, LogRecord.PA.OVERHEAD
                    - LogRecord.OVERHEAD);
            readBuffer.position(from + recordSize);
            final long address = bufferAddress + from;
            final long pageAddress = LogRecord.PA.getPageAddress(_bytes);
            final Integer volumeHandle = Integer.valueOf(LogRecord.PA
                    .getVolumeHandle(_bytes));
            final VolumeDescriptor vd = _handleToVolumeMap.get(volumeHandle);
            if (vd == null) {
                throw new CorruptLogException(
                        "PA reference to volume "
                                + volumeHandle
                                + " is not preceded by an IV record for that handle at "
                                + new FileAddress(file, address, timestamp));
            }
            final VolumePage vp = new VolumePage(vd, pageAddress, timestamp);
            final FileAddress fa = new FileAddress(file, address, timestamp);
            _pageMap.put(vp, fa);
            break;
        }

        case LogRecord.TYPE_RR:
        case LogRecord.TYPE_WR:
        case LogRecord.TYPE_TS:
        case LogRecord.TYPE_TC:
        case LogRecord.TYPE_TJ:
            throw new UnsupportedOperationException(
                    "Can't handle record of type " + (int) type);

        default:
            _persistit.getLogBase().log(LogBase.LOG_INIT_RECOVER_TERMINATE,
                    new FileAddress(file, bufferAddress + from, timestamp));
            throw new LogNotClosedException(new FileAddress(file,
                    bufferAddress, -1));
        }
        return true;
    }

    public void close() throws PersistitIOException {

        synchronized (this) {
            _closed = true;
            notifyAll();
        }

        while (_copierThread.isAlive()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {

            }
        }

        synchronized (this) {
            try {
                closeWriteChannel();
                for (final FileChannel channel : _readChannelMap.values()) {
                    channel.close();
                }
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
            _readChannelMap.clear();
            _handleToTreeMap.clear();
            _handleToVolumeMap.clear();
            _volumeToHandleMap.clear();
            _treeToHandleMap.clear();
            _pageMap.clear();
            _writeChannelFile = null;
            _writeChannel = null;
            _writeBuffer = null;
            _recovered = false;
            _copierThread = null;
            Arrays.fill(_bytes, (byte) 0);
        }
        if (_flushTimer != null) {
            _flushTimer.cancel();
            _flushTimer = null;
        }
    }

    /**
     * Force all data written to the log file to disk.
     */
    public void force() {
        final MappedByteBuffer mbb;
        synchronized (this) {
            mbb = _writeBuffer;
        }

        if (mbb != null) {
            mbb.force();
        }
    }

    private boolean writeBuffer(final int size) throws PersistitIOException {
        boolean rolled = false;
        if (_writeBuffer != null) {
            if (_writeBuffer.remaining() >= size) {
                return false;
            } else {
                _writeBufferAddress += _writeBuffer.position();
                _writeBuffer.force();
                _writeBuffer = null;
            }
        }

        if (_writeChannel == null
                || _writeBufferAddress + _writeBufferSize > _maximumFileSize) {
            rollover();
            rolled = true;
        }

        try {
            _writeBuffer = _writeChannel.map(MapMode.READ_WRITE,
                    _writeBufferAddress, _writeBufferSize);
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
        return rolled;
    }

    private void closeWriteChannel() throws IOException {
        if (_writeBuffer != null) {
            _writeBuffer.force();
            _writeBufferAddress += _writeBuffer.position();
            _writeBuffer = null;
        }
        if (_writeChannel != null) {
            _writeChannel.truncate(_writeBufferAddress);
            _writeChannel.force(true);
            _writeChannel.close();
        }
        _writeBufferAddress = 0;
    }

    private File[] files() {
        File file = _directory;
        if (!file.isDirectory()) {
            file = file.getParentFile();
            if (file == null) {
                file = new File(".");
            }
        }
        final File dir = file;

        final File[] files = dir.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                return pathname.getParent().startsWith(dir.getPath())
                        && PATH_PATTERN.matcher(pathname.getPath()).matches();
            }

        });
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files);
        return files;
    }

    private synchronized FileChannel getFileChannel(final File file)
            throws PersistitIOException {
        try {
            FileChannel fc = _readChannelMap.get(file);
            if (fc == null) {
                final RandomAccessFile raf = new RandomAccessFile(file, "r");
                fc = raf.getChannel();
                _readChannelMap.put(file, fc);
            }
            return fc;
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
    }

    public void copyBack(final long toTimestamp) throws PersistitException {
        synchronized (this) {
            _urgentDemand = true;
            notifyAll();
            while (_urgentDemand) {
                try {
                    wait(1000);
                } catch (InterruptedException ie) {
                    // ignore;
                }
            }
        }
    }

    private static class VolumeDescriptor {

        private final long id;

        private final String path;

        VolumeDescriptor(final String path, final long id) {
            this.path = path;
            this.id = id;
        }

        VolumeDescriptor(final Volume volume) {
            this.path = volume.getPath();
            this.id = volume.getId();
        }

        String getPath() {
            return path;
        }

        long getId() {
            return id;
        }

        @Override
        public boolean equals(final Object object) {
            final VolumeDescriptor vd = (VolumeDescriptor) object;
            return vd.path.equals(path) && vd.id == id;
        }

        @Override
        public int hashCode() {
            return path.hashCode() ^ (int) id;
        }

        @Override
        public String toString() {
            return path;
        }

    }

    private static class VolumePage implements Comparable<VolumePage> {

        private final VolumeDescriptor volumeDescriptor;

        private final long page;

        VolumePage(final VolumeDescriptor vd, final long page,
                final long timestamp) {
            this.volumeDescriptor = vd;
            this.page = page;
        }

        VolumeDescriptor getVolumeDescriptor() {
            return volumeDescriptor;
        }

        long getPage() {
            return page;
        }

        @Override
        public int hashCode() {
            return volumeDescriptor.hashCode() ^ (int) page
                    ^ (int) (page >>> 32);
        }

        @Override
        public boolean equals(Object obj) {
            final VolumePage vp = (VolumePage) obj;
            return page == vp.page && volumeDescriptor.equals(volumeDescriptor);
        }

        @Override
        public int compareTo(VolumePage vp) {
            int result = volumeDescriptor.getPath().compareTo(
                    vp.getVolumeDescriptor().getPath());
            if (result != 0) {
                return result;
            }
            return page > vp.getPage() ? 1 : page < vp.getPage() ? -1 : 0;
        }

        @Override
        public String toString() {
            return volumeDescriptor + ":" + page;
        }
    }

    private static class FileAddress implements Comparable<FileAddress> {

        private final File file;

        private final long address;

        private final long timestamp;

        FileAddress(final File file, final long address, final long timestamp) {
            this.file = file;
            this.address = address;
            this.timestamp = timestamp;
        }

        File getFile() {
            return file;
        }

        long getAddress() {
            return address;
        }

        long getTimestamp() {
            return timestamp;
        }

        @Override
        public int hashCode() {
            return file.hashCode() ^ (int) address ^ (int) (address >>> 32);
        }

        @Override
        public boolean equals(final Object object) {
            final FileAddress fa = (FileAddress) object;
            return file.equals(fa.file) && address == fa.address;
        }

        @Override
        public int compareTo(final FileAddress fa) {
            int result = file.compareTo(fa.getFile());
            if (result != 0) {
                return result;
            }
            if (address != fa.getAddress()) {
                return address > fa.getAddress() ? 1 : -1;
            }
            if (timestamp != fa.getTimestamp()) {
                return timestamp > fa.getTimestamp() ? 1 : -1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return file + ":" + address;
        }
    }

    private class LogFlusher extends TimerTask {
        @Override
        public void run() {
            force();
        }
    }

    private class LogCopier implements Runnable {

        @Override
        public void run() {
            while (!_closed) {
                try {
                    doLogCopierCycle();
                } catch (Throwable pe) {
                    pe.printStackTrace(); // TODO
                }
            }
        }
    }

    /**
     * Tunable parameters that determine how vigorously the copyBack thread
     * performs I/O. Hopefully we can set good defaults and not expose these as
     * knobs.
     */
    private final static int DEFAULT_URGENCY_MAP_SIZE_BASE = 1000;
    private final static int DEFAULT_URGENCY_MAP_SIZE_MULTIPLIER = 3;
    private final static int DEFAULT_URGENCY_LOG_FILE_COUNT_BASE = 1;
    private final static int DEFAULT_URGENCY_FULL_THROTTLE = 10;
    private final static int DEFAULT_URGENCY_SLEEP_TIME_BASE = 10000;
    private final static int DEFAULT_URGENCY_MAX_IO_SLEEP = 100;

    private volatile boolean _urgentDemand = false;

    private volatile int _urgencyMapSizeBase = DEFAULT_URGENCY_MAP_SIZE_BASE;

    private volatile int _urgencyMapSizeMultiplier = DEFAULT_URGENCY_MAP_SIZE_MULTIPLIER;

    private volatile int _urgencyLogFileCountBase = DEFAULT_URGENCY_LOG_FILE_COUNT_BASE;

    private volatile int _urgencyFullThrottle = DEFAULT_URGENCY_FULL_THROTTLE;

    private volatile int _urgencySleepTimeBase = DEFAULT_URGENCY_SLEEP_TIME_BASE;

    private volatile int _urgencyMaxIOSleep = DEFAULT_URGENCY_MAX_IO_SLEEP;

    private volatile long _copyLogTimestampLimit = Long.MAX_VALUE;

    /**
     * Computes an "urgency" factor that determines how vigorously the copyBack
     * thread should perform I/O. This number is computed on a scale of 0 to N;
     * larger values are intended make the thread work harder. A value of 10 or
     * above suggests the copier should run flat-out.
     * 
     * @return
     */
    private synchronized int logCopierUrgency() {
        if (_urgentDemand) {
            return _urgencyFullThrottle;
        }
        int urgency = 0;
        int mapSizeBoundary = _urgencyMapSizeBase;
        while (mapSizeBoundary < _pageMap.size()) {
            urgency++;
            mapSizeBoundary *= _urgencyMapSizeMultiplier;
        }
        int logFileCount = (int) (_currentGeneration - _earliestGeneration);
        if (logFileCount > _urgencyLogFileCountBase) {
            urgency += Math.min(logFileCount - _urgencyLogFileCountBase,
                    _urgencyFullThrottle);
        }
        return urgency;
    }

    private long copierSleepInterval(final int urgency) {
        if (urgency >= _urgencyFullThrottle) {
            return 0;
        }
        return _urgencySleepTimeBase / (1 << urgency);
    }

    private synchronized void copierIOSleep() {
        long time = Math.min(copierSleepInterval(logCopierUrgency()),
                _urgencyMaxIOSleep);
        if (time > 0) {
            try {
                this.wait(time);
            } catch (InterruptedException ie) {
                // ignore
            }
        }
    }

    private synchronized void copierCycleSleep() {
        long time = copierSleepInterval(logCopierUrgency());
        if (time > 0) {
            try {
                this.wait(time);
            } catch (InterruptedException ie) {
                // ignore
            }
        }
    }

    private void doLogCopierCycle() throws PersistitException {
        copierCycleSleep();
        copierCycle();
    }

    private void copierCycle() throws PersistitException {
        FileAddress firstMissed = null;
        final SortedMap<VolumePage, FileAddress> sortedMap = new TreeMap<VolumePage, FileAddress>();
        final boolean wasUrgent;
        final long currentGeneration;
        synchronized (this) {
            if (!_recovered) {
                return;
            }
            wasUrgent = _urgentDemand;
            currentGeneration = _currentGeneration;
            for (final Map.Entry<VolumePage, FileAddress> entry : _pageMap
                    .entrySet()) {
                FileAddress fa = entry.getValue();
                if (fa.getTimestamp() <= _copyLogTimestampLimit) {
                    sortedMap.put(entry.getKey(), entry.getValue());
                } else {
                    if (firstMissed == null || fa.compareTo(firstMissed) < 0) {
                        firstMissed = fa;
                    }
                }
            }
        }

        Volume volume = null;
        VolumeDescriptor descriptor = null;

        final ByteBuffer bb = ByteBuffer.allocate(DEFAULT_READ_BUFFER_SIZE);
        for (final Map.Entry<VolumePage, FileAddress> entry : sortedMap
                .entrySet()) {
            if (_closed) {
                return;
            }
            final VolumePage vp = entry.getKey();
            final FileAddress fa = entry.getValue();
            final long pageAddress = readPageBufferFromLog(vp, fa, bb);
            if (descriptor != vp.getVolumeDescriptor()) {
                volume = _persistit.getVolume(vp.getVolumeDescriptor()
                        .getPath());
            }
            if (volume.getId() != vp.getVolumeDescriptor().getId()) {
                throw new CorruptLogException(vp.getVolumeDescriptor()
                        + " does not identify a valid Volume at " + fa);
            }
            if (bb.limit() != volume.getBufferSize()) {
                throw new CorruptLogException(vp + " bufferSize " + bb.limit()
                        + " does not match " + volume + " bufferSize "
                        + volume.getBufferSize() + " at " + fa);
            }
            if (pageAddress != vp.getPage()) {
                throw new CorruptLogException(vp
                        + " does not match page address " + pageAddress
                        + " found at " + fa);
            }
            try {
                volume.writePage(bb, pageAddress);
                copierIOSleep();

            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }

        synchronized (this) {
            for (final Map.Entry<VolumePage, FileAddress> entry : sortedMap
                    .entrySet()) {
                final VolumePage vp = entry.getKey();
                final FileAddress fa = entry.getValue();
                final FileAddress fa2 = _pageMap.get(vp);
                if (fa.equals(fa2)) {
                    _pageMap.remove(vp);
                }
            }
        }

        final File[] files = files();
        for (final File file : files) {
            if (firstMissed == null
                    || file.compareTo(firstMissed.getFile()) < 0) {
                if (!file.equals(_writeChannelFile)) {
                    file.delete();
                }
            }
        }
        File currentFile = null;
        synchronized (this) {
            if (firstMissed == null && _pageMap.size() == 0
                    && _writeBuffer != null
                    && _writeBufferAddress + _writeBuffer.position() != 0) {
                currentFile = _writeChannelFile;
                rollover();
            }
            if (firstMissed == null) {
                _earliestGeneration = currentGeneration;
            } else {
                _earliestGeneration = fileGeneration(firstMissed.getFile());
            }
        }
        if (currentFile != null) {
            currentFile.delete();
        }
        if (wasUrgent) {
            _urgentDemand = false;
        }
    }
}
