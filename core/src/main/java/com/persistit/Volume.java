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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.exception.BufferSizeUnavailableException;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.RetryException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * Holds a collection of data organized logically into trees and physically into
 * blocks called pages. All pages within a volume are the same size. A page must
 * be 1024, 2048, 8192 or 16384 bytes in length. Page 0 is a special control
 * page for the volume.
 * <p>
 * A Volume can contain any number of {@link Tree}s. Each tree has a unique root
 * page. Each Volume also contains one special directory tree that holds
 * information about all other trees.
 * <p>
 * Currently a Volume is hosted inside of a single file. Future implementations
 * may permit a Volume to be split across multiple files, allowing
 * drive-spanning.
 */
public class Volume extends SharedResource {
    /**
     * Designated Tree name for the special directory "tree of trees".
     */
    public final static String DIRECTORY_TREE_NAME = "_directory";
    /**
     * Key segment name for index by directory tree name.
     */
    private final static String BY_NAME = "byName";

    private VolumeHeader _header;
    private FileChannel _channel;
    private FileLock _fileLock;
    private String _path;
    private String _name;
    private long _id;
    private volatile long _timestamp;
    private long _initialPages;
    private long _maximumPages;
    private long _extensionPages;
    private volatile long _openTime;
    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;
    private volatile long _lastExtensionTime;
    private volatile long _highestPageUsed;
    private volatile long _firstAvailablePage;
    private volatile long _createTime;
    private volatile long _pageCount;
    private volatile long _directoryRootPage;
    private volatile long _garbageRoot;
    private int _bufferSize;
    private AtomicReference<Object> _appCache = new AtomicReference<Object>();

    private AtomicLong _readCounter = new AtomicLong();
    private AtomicLong _writeCounter = new AtomicLong();
    private AtomicLong _getCounter = new AtomicLong();
    private AtomicLong _fetchCounter = new AtomicLong();
    private AtomicLong _traverseCounter = new AtomicLong();
    private AtomicLong _storeCounter = new AtomicLong();
    private AtomicLong _removeCounter = new AtomicLong();
    private AtomicInteger _handle = new AtomicInteger();

    private Buffer _headBuffer;
    private BufferPool _pool;
    private boolean _readOnly;

    private volatile IOException _lastIOException;

    // String name --> Tree tree

    private HashMap<String, Tree> _treeNameHashMap = new HashMap<String, Tree>();

    private volatile boolean _closed;
    private final Tree _directoryTree;

    /**
     * Opens a Volume. The volume must already exist.
     * 
     * @param pathName
     *            The full pathname to the file containing the Volume.
     * @param ro
     *            <code>true</code> if the Volume should be opened in read- only
     *            mode so that no updates can be performed against it.
     * @return The Volume.
     * @throws PersistitException
     */
    static Volume openVolume(final Persistit persistit, final String pathName, final boolean ro)
            throws PersistitException {
        return openVolume(persistit, pathName, null, 0, ro);
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
     *            <code>Volume</code>'s pathname, and typically will denote its
     *            function rather than physical location.
     * 
     * @param id
     *            The internal Volume id value - if non-zero this value must
     *            match the id value stored in the Volume header.
     * 
     * @param ro
     *            <code>true</code> if the Volume should be opened in read- only
     *            mode so that no updates can be performed against it.
     * 
     * @return The <code>Volume</code>.
     * 
     * @throws PersistitException
     */
    static Volume openVolume(final Persistit persistit, final String pathName, final String alias, final long id,
            final boolean ro) throws PersistitException {
        File file = new File(pathName);
        if (file.exists() && file.isFile()) {
            return new Volume(persistit, file.getPath(), alias, id, ro);
        }
        throw new PersistitIOException(new FileNotFoundException(pathName));
    }

    /**
     * Loads and/or creates a volume based on a String-valued specification. The
     * specification has the form: <br />
     * <i>pathname</i>[,<i>options</i>]... <br />
     * where options include: <br />
     * <dl>
     * <dt><code>alias</code></dt>
     * <dd>An alias used in looking up the volume by name within Persistit
     * programs (see {@link com.persistit.Persistit#getVolume(String)}). If the
     * alias attribute is not specified, the the Volume's path name is used
     * instead.</dd>
     * <dt><code>drive<code></dt>
     * <dd>Name of the drive on which the volume is located. Specifying the
     * drive on which each volume is physically located is optional. If
     * supplied, Persistit uses the information to improve I/O throughput in
     * multi-volume configurations by interleaving write operations to different
     * physical drives.</dd>
     * <dt><code>readOnly</code></dt>
     * <dd>Open in Read-Only mode. (Incompatible with create mode.)</dd>
     * 
     * <dt><code>create</code></dt>
     * <dd>Creates the volume if it does not exist. Requires
     * <code>bufferSize</code>, <code>initialPagesM</code>,
     * <code>extensionPages</code> and <code>maximumPages</code> to be
     * specified.</dd>
     * 
     * <dt><code>createOnly</code></dt>
     * <dd>Creates the volume, or throw a {@link VolumeAlreadyExistsException}
     * if it already exists.</dd>
     * 
     * <dt><code>id:<i>NNN</i></code></dt>
     * <dd>Specifies an ID value for the volume. If the volume already exists,
     * this ID value must match the ID that was previously assigned to the
     * volume when it was created. If this volume is being newly created, this
     * becomes its ID number.</dd>
     * 
     * <dt><code>bufferSize:<i>NNN</i></code></dt>
     * <dd>Specifies <i>NNN</i> as the volume's buffer size when creating a new
     * volume. <i>NNN</i> must be 1024, 2048, 4096, 8192 or 16384</dd>.
     * 
     * <dt><code>initialPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the initial number of pages to be allocated when this
     * volume is first created.</dd>
     * 
     * <dt><code>extensionPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the number of pages by which to extend the volume when
     * more pages are required.</dd>
     * 
     * <dt><code>maximumPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the maximum number of pages to which this volume can
     * extend.</dd>
     * 
     * </dl>
     * 
     * 
     * @param volumeSpec
     *            Volume specification
     * 
     * @return The <code>Volume</code>
     * 
     * @throws PersistitException
     */
    static Volume loadVolume(final Persistit persistit, final VolumeSpecification volumeSpec) throws PersistitException {
        if (volumeSpec.isCreate() || volumeSpec.isCreateOnly()) {
            return create(persistit, volumeSpec.getPath(), volumeSpec.getName(), volumeSpec.getId(), volumeSpec
                    .getBufferSize(), volumeSpec.getInitialPages(), volumeSpec.getExtensionPages(), volumeSpec
                    .getMaximumPages(), volumeSpec.isCreateOnly());
        } else {
            return openVolume(persistit, volumeSpec.getPath(), volumeSpec.getName(), volumeSpec.getId(), volumeSpec
                    .isReadOnly());
        }
    }

    /**
     * Creates a new volume
     * 
     * @param persistit
     * @param path
     * @param name
     * @param id
     * @param bufferSize
     * @param initialPages
     * @param extensionPages
     * @param maximumPages
     * @throws PersistitException
     */
    private Volume(final Persistit persistit, final String path, final String name, final long id,
            final int bufferSize, long initialPages, long extensionPages, long maximumPages) throws PersistitException {
        super(persistit);

        boolean sizeOkay = false;
        for (int b = Buffer.MIN_BUFFER_SIZE; !sizeOkay && b <= Buffer.MAX_BUFFER_SIZE; b *= 2) {
            if (bufferSize == b)
                sizeOkay = true;
        }

        if (!sizeOkay) {
            throw new InvalidVolumeSpecificationException("Invalid buffer size: " + bufferSize);
        }

        _pool = _persistit.getBufferPool(bufferSize);
        if (_pool == null) {
            throw new BufferSizeUnavailableException("size: " + bufferSize);
        }

        if (initialPages == 0)
            initialPages = 1;
        if (maximumPages == 0)
            maximumPages = initialPages;

        if (initialPages < 0 || initialPages > Long.MAX_VALUE / bufferSize) {
            throw new InvalidVolumeSpecificationException("Invalid initial page count: " + initialPages);
        }

        if (extensionPages < 0 || extensionPages > Long.MAX_VALUE / bufferSize) {
            throw new InvalidVolumeSpecificationException("Invalid extension page count: " + extensionPages);
        }

        if (maximumPages < initialPages || maximumPages > Long.MAX_VALUE / bufferSize) {
            throw new InvalidVolumeSpecificationException("Invalid maximum page count: " + maximumPages);
        }

        boolean open = false;

        try {
            initializePathAndName(path, name, true);
            _readOnly = false;
            _bufferSize = bufferSize;

            long now = System.currentTimeMillis();
            if (id == 0) {
                _id = (now ^ (((long) path.hashCode()) << 32)) & Long.MAX_VALUE;
            } else
                _id = id;

            _highestPageUsed = 0;
            _createTime = now;
            _lastExtensionTime = 0;
            _lastReadTime = 0;
            _lastWriteTime = 0;
            _firstAvailablePage = 1;
            _garbageRoot = 0;
            _pageCount = 1;

            _persistit.addVolume(this);

            _headBuffer = _pool.get(this, 0, true, false);
            _headBuffer.setFixed();
            _header = new VolumeHeader(_channel);

            _headBuffer.clear();

            _initialPages = initialPages;
            _extensionPages = extensionPages;
            _maximumPages = maximumPages;

            _pageCount = initialPages;
            _firstAvailablePage = 1;
            _highestPageUsed = 0;
            _garbageRoot = 0;
            _directoryRootPage = 0;
            _pool.invalidate(this);

            updateHeaderInfo(_headBuffer.getBytes());

            _channel = new MediatedFileChannel(_path, "rw");
            lockChannel();
            _headBuffer.getByteBuffer().position(0).limit(_headBuffer.getBufferSize());
            _channel.write(_headBuffer.getByteBuffer(), 0);
            _channel.force(true);

            if (initialPages > 1) {
                extend(initialPages);
            }

            _directoryTree = new Tree(_persistit, this, DIRECTORY_TREE_NAME);
            initTree(_directoryTree);
            checkpointMetaData();

            open = true;

        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        } finally {
            if (_headBuffer != null) {
                if (!open) {
                    _headBuffer.clearFixed();
                }
                releaseHeadBuffer();
            }
            if (!open) {
                _persistit.removeVolume(this, false);
            }
        }

    }

    /**
     * Opens an existing Volume. Throws CorruptVolumeException if the volume
     * file does not exist, is too short, or is malformed.
     * 
     * @param pathName
     * @param alias
     * @param id
     * @param readOnly
     * @throws PersistitException
     */
    private Volume(final Persistit persistit, String path, String name, long id, boolean readOnly)
            throws PersistitException {
        super(persistit);
        try {
            initializePathAndName(path, name, false);

            _readOnly = readOnly;
            _channel = new MediatedFileChannel(_path, readOnly ? "r" : "rw");
            lockChannel();
            _header = new VolumeHeader(_channel);
            final ByteBuffer bb = _header.validate();

            //
            // Just to populate _bufferSize. getHeaderInfo will be called
            // again below.
            //
            final byte[] bytes = bb.array();
            getHeaderInfo(bytes);
            _pool = _persistit.getBufferPool(_bufferSize);
            if (_pool == null) {
                throw new BufferSizeUnavailableException("size: " + _bufferSize);
            }
            //
            // Now use the pool to get the page. This may read a
            // recently updated copy from the log.
            //
            _headBuffer = _pool.get(this, 0, true, true);
            getHeaderInfo(_headBuffer.getBytes());

            if (id != 0 && id != _id) {
                throw new CorruptVolumeException("Attempt to open with invalid id " + id + " (!= " + _id + ")");
            }
            // TODO -- synchronize opening of Volumes.
            _persistit.addVolume(this);

            if (_directoryRootPage != 0) {
                _directoryTree = new Tree(_persistit, this, DIRECTORY_TREE_NAME);
                _directoryTree.init(_directoryRootPage);
            } else {
                _directoryTree = new Tree(_persistit, this, DIRECTORY_TREE_NAME);
                initTree(_directoryTree);
                checkpointMetaData();
            }

            _headBuffer.setFixed();
            releaseHeadBuffer();
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
    }

    private void lockChannel() throws InUseException, IOException {
        try {
            _fileLock = _channel.tryLock(0, Long.MAX_VALUE, isReadOnly());
        } catch (OverlappingFileLockException e) {
            // Note: OverlappingFileLockException is a RuntimeException
            throw new InUseException("Volume file " + _path + " is locked by another thread in this JVM");
        }
        if (_fileLock == null) {
            throw new InUseException("Volume file " + _path + " is locked by another process");
        }
    }

    private void initializePathAndName(final String path, final String name, final boolean create) throws IOException,
            PersistitException {
        File file = new File(path);
        if (create) {
            if (file.exists() && !file.isDirectory()) {
                throw new VolumeAlreadyExistsException(file.getPath());
            }
        } else {
            if (!file.exists()) {
                throw new FileNotFoundException(path);
            }
        }
        if (file.exists() && file.isDirectory() && name != null && !name.isEmpty()) {
            file = new File(file, name);
        }
        _path = file.getPath();
        if (name == null || name.isEmpty()) {
            _name = file.getName();
            final int p = _name.lastIndexOf('.');
            if (p > 0) {
                _name = _name.substring(0, p);
            }

        } else {
            _name = name;
        }
    }

    /**
     * Creates a new Volume or open an existing Volume. If a volume having the
     * specified <code>id</code> and <code>pathname</code> already exists, and
     * if the mustCreate parameter is false, then this method opens and returns
     * a previously existing <code>Volume</code>. If <code>mustCreate</code> is
     * true and a file of the specified name already exists, then this method
     * throws a <code>VolumeAlreadyExistsException</code>. Otherwise this method
     * creates a new empty volume.
     * 
     * @param persistit
     *            The Persistit instance in which the Volume will be opened
     * 
     * @param path
     *            The full pathname to the file containing the Volume.
     * 
     * @param name
     *            A friendly name for this volume that may be used internally by
     *            applications. The name need not be related to the
     *            <code>Volume</code>'s pathname, and typically will denote its
     *            function rather than physical location.
     * 
     * @param id
     *            The internal Volume id value or zero to if Persistit should
     *            assign a new id.
     * 
     * @param bufferSize
     *            The buffer size (one of 1024, 2048, 4096, 8192 or 16384).
     * 
     * @param initialPages
     *            Initialize number of pages to allocate.
     * 
     * @param extensionPages
     *            Number of additional pages to allocate when all existing pages
     *            have been used.
     * 
     * @param maximumPages
     *            A hard upper bound on the number of pages that can be created
     *            within this volume.
     * 
     * @param mustCreate
     *            <code>true</code> ensure that there is previously no matching
     *            Volume, and that the Volume returned by this method is newly
     *            created.
     * 
     * @return the Volume
     * @throws PersistitException
     */
    public static Volume create(final Persistit persistit, final String path, final String name, final long id,
            final int bufferSize, final long initialPages, final long extensionPages, final long maximumPages,
            final boolean mustCreate) throws PersistitException {
        File file = new File(path);
        if (file.exists()) {
            if (mustCreate) {
                throw new VolumeAlreadyExistsException(path);
            }
            Volume vol = openVolume(persistit, file.getPath(), name, id, false);
            if (vol._bufferSize != bufferSize) {
                throw new VolumeAlreadyExistsException("Different buffer size expected/actual=" + bufferSize + "/"
                        + vol._bufferSize + ": " + vol);
            }
            //
            // Here we overwrite the former growth parameters
            // with those supplied to the create method.
            //
            vol._initialPages = initialPages;
            vol._extensionPages = extensionPages;
            vol._maximumPages = maximumPages;

            return vol;
        } else {
            return new Volume(persistit, file.getPath(), name, id, bufferSize, initialPages, extensionPages,
                    maximumPages);
        }
    }

    /**
     * Updates head page information.
     * 
     * @throws InvalidPageStructureException
     * @throws PersistitIOException
     * 
     * @throws PMapException
     */
    private void checkpointMetaData() throws PersistitException,
            InvalidPageStructureException {
        final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
        _headBuffer.writePageOnCheckpoint(timestamp);
        if (!_readOnly && updateHeaderInfo(_headBuffer.getBytes())) {
            _headBuffer.setDirtyAtTimestamp(timestamp);
        }
    }

    /**
     * Returns the <code>id</code> of this <code>Volume</code>. The
     * <code>id</code> is a 64-bit long that uniquely identifies this volume.
     * 
     * @return The id value
     */
    public long getId() {
        return _id;
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    public String getPath() {
        return _path;
    }

    /**
     * Returns the name specified for the Volume.
     * 
     * @return The alias, or <code>null</code> if there is none.
     */
    public String getName() {
        return _name;
    }

    /**
     * Returns the buffer size for this volume, one of 1024, 2048, 4096, 8192 or
     * 16384.
     * 
     * @return The buffer size.
     */
    public int getPageSize() {
        return _bufferSize;
    }

    /**
     * Returns the page address of the garbage tree. This method is useful to
     * diagnostic utility programs.
     * 
     * @return The page address
     */
    public long getGarbageRoot() {
        return _garbageRoot;
    }

    private void setGarbageRoot(long garbagePage) throws PersistitException {
        _garbageRoot = garbagePage;
        checkpointMetaData();
    }

    /**
     * Returns the directory <code>Tree</code> for this volume
     * 
     * @return The directory <code>Tree</code>
     */
    public Tree getDirectoryTree() {
        return _directoryTree;
    }

    /**
     * Returns the count of physical disk read requests performed on this
     * <code>Volume</code>.
     * 
     * @return The count
     */
    public long getReadCounter() {
        return _readCounter.get();
    }

    /**
     * Returns the count of physical disk write requests performed on this
     * <code>Volume</code>.
     * 
     * @return The count
     */
    public long getWriteCounter() {
        return _writeCounter.get();
    }

    /**
     * Returns the count of logical buffer fetches performed against this
     * <code>Volume</code>. The ratio of get to read operations indicates how
     * effectively the buffer pool is reducing disk I/O.
     * 
     * @return The count
     */
    public long getGetCounter() {
        return _getCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#fetch} operations. These include
     * {@link Exchange#traverse}, {@link Exchange#fetchAndStore} and
     * {@link Exchange#fetchAndRemove} operations. This count is maintained with
     * the stored Volume and is not reset when Persistit closes. It is provided
     * to assist application performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getFetchCounter() {
        return _fetchCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#traverse} operations. These include
     * {@link Exchange#next} and {@link Exchange#_previous} operations. This
     * count is maintained with the stored Volume and is not reset when
     * Persistit closes. It is provided to assist application performance
     * tuning.
     * 
     * @return The count of key traversal operations performed on this in this
     *         Volume.
     */
    public long getTraverseCounter() {
        return _traverseCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#store} operations, including
     * {@link Exchange#fetchAndStore} and {@link Exchange#incrementValue}
     * operations. This count is maintained with the stored Volume and is not
     * reset when Persistit closes. It is provided to assist application
     * performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getStoreCounter() {
        return _storeCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#remove} operations, including
     * {@link Exchange#fetchAndRemove} operations. This count is maintained with
     * the stored Volume and is not reset when Persistit closes. It is provided
     * to assist application performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getRemoveCounter() {
        return _removeCounter.get();
    }

    /**
     * Returns the time at which this <code>Volume</code> was created.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getCreateTime() {
        return _createTime;
    }

    /**
     * Returns the time at which this <code>Volume</code> was last opened.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getOpenTime() {
        return _openTime;
    }

    /**
     * Returns the time at which the last physical read operation was performed
     * on <code>Volume</code>.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastReadTime() {
        return _lastReadTime;
    }

    /**
     * Returns the time at which the last physical write operation was performed
     * on <code>Volume</code>.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastWriteTime() {
        return _lastWriteTime;
    }

    /**
     * Returns the time at which this <code>Volume</code> was last extended
     * (increased in physical size).
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastExtensionTime() {
        return _lastExtensionTime;
    }

    /**
     * Returns the last <code>IOException</code> that was encountered while
     * reading, writing, extending or closing the underlying volume file.
     * Returns <code>null</code> if there have been no <code>IOException</code>s
     * since the volume was opened. If <code>reset</code> is <code>true</code>,
     * the lastException field is cleared so that a subsequent call to this
     * method will return <code>null</code> unless another
     * <code>IOException</code> has occurred.
     * 
     * @param reset
     *            If <code>true</code> then this method clears the last
     *            exception field
     * 
     * @return The most recently encountered <code>IOException</code>, or
     *         <code>null</code> if there has been none.
     */
    public IOException lastException(boolean reset) {
        IOException ioe = _lastIOException;
        if (reset)
            _lastIOException = null;
        return ioe;
    }

    long getPageCount() {
        return _pageCount;
    }

    long getMaximumPageInUse() {
        return _highestPageUsed;
    }

    long getInitialPages() {
        return _initialPages;
    }

    long getMaximumPages() {
        return _maximumPages;
    }

    long getExtensionPages() {
        return _extensionPages;
    }

    void bumpReadCounter() {
        _readCounter.incrementAndGet();
        _lastReadTime = System.currentTimeMillis();
    }

    void bumpWriteCounter() {
        _writeCounter.incrementAndGet();
        _lastWriteTime = System.currentTimeMillis();
    }

    void bumpGetCounter() {
        _getCounter.incrementAndGet();
    }

    void bumpFetchCounter() {
        _fetchCounter.incrementAndGet();
    }

    void bumpTraverseCounter() {
        _traverseCounter.incrementAndGet();
    }

    void bumpStoreCounter() {
        _storeCounter.incrementAndGet();
    }

    void bumpRemoveCounter() {
        _removeCounter.incrementAndGet();
    }

    private String garbageBufferInfo(Buffer buffer) {
        if (buffer.getPageType() != Buffer.PAGE_TYPE_GARBAGE) {
            return "!!!" + buffer.getPageAddress() + " is not a garbage page!!!";
        }
        return "@<" + buffer.getPageAddress() + ":" + buffer.getAlloc() + ">";
    }

    void deallocateGarbageChain(long left, long right) throws PersistitException {
        Debug.$assert0.t(left > 0);

        claimHeadBuffer();

        Buffer garbageBuffer = null;

        try {
            long garbagePage = getGarbageRoot();
            if (garbagePage != 0) {
                Debug.$assert0.t(left != garbagePage && right != garbagePage);

                garbageBuffer = _pool.get(this, garbagePage, true, true);

                final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                garbageBuffer.writePageOnCheckpoint(timestamp);

                boolean fits = garbageBuffer.addGarbageChain(left, right, -1);

                if (fits) {
                    _persistit.getLogBase().newGarbageChain.log(left, right, garbageBufferInfo(garbageBuffer));
                    garbageBuffer.setDirtyAtTimestamp(timestamp);
                    return;
                } else {
                    _persistit.getLogBase().garbagePageFull.log(left, right, garbageBufferInfo(garbageBuffer));
                    garbageBuffer.releaseTouched();
                    garbageBuffer = null;
                }
            }
            boolean solitaire = (right == -1);
            garbageBuffer = _pool.get(this, left, true, !solitaire);

            final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
            garbageBuffer.writePageOnCheckpoint(timestamp);

            Debug.$assert0.t((garbageBuffer.isDataPage() || garbageBuffer.isIndexPage())
                    || garbageBuffer.isLongRecordPage() || (solitaire && garbageBuffer.isUnallocatedPage()));

            long nextGarbagePage = solitaire ? 0 : garbageBuffer.getRightSibling();

            Debug.$assert0.t(nextGarbagePage > 0 || right == 0 || solitaire);

            harvestLongRecords(garbageBuffer, 0, Integer.MAX_VALUE);

            garbageBuffer.init(Buffer.PAGE_TYPE_GARBAGE);

            _persistit.getLogBase().newGarbageRoot.log(garbageBufferInfo(garbageBuffer));

            if (!solitaire && nextGarbagePage != right) {
                // Will always fit because this is a freshly initialized page
                garbageBuffer.addGarbageChain(nextGarbagePage, right, -1);
                _persistit.getLogBase().newGarbageChain.log(nextGarbagePage, right, garbageBufferInfo(garbageBuffer));
            }
            garbageBuffer.setRightSibling(garbagePage);
            garbageBuffer.setDirtyAtTimestamp(timestamp);
            setGarbageRoot(garbageBuffer.getPageAddress());
        } finally {
            if (garbageBuffer != null) {
                garbageBuffer.releaseTouched();
            }
            releaseHeadBuffer();
        }
    }

    // TODO - no one needs the return value
    boolean harvestLongRecords(Buffer buffer, int start, int end) throws PersistitException {
        boolean anyLongRecords = false;
        if (buffer.isDataPage()) {
            int p1 = buffer.toKeyBlock(start);
            int p2 = buffer.toKeyBlock(end);
            for (int p = p1; p < p2 && p != -1; p = buffer.nextKeyBlock(p)) {
                long pointer = buffer.fetchLongRecordPointer(p);
                if (pointer != 0) {
                    deallocateGarbageChain(pointer, 0);
                    anyLongRecords |= true;
                }
            }
        }
        return anyLongRecords;
    }

    /**
     * Commit all dirty Trees.
     * 
     * @throws RetryException
     * @throws PersistitException
     */
    void commitAllTreeUpdates() throws PersistitException {
        _directoryTree.commit();
        for (final Tree tree : _treeNameHashMap.values()) {
            tree.commit();
        }
    }

    Exchange directoryExchange() {
        Exchange ex = new Exchange(_directoryTree);
        ex.ignoreTransactions();
        return ex;
    }

    /**
     * Looks up by name and returns a <code>Tree</code> within this
     * <code>Volume</code>. If no such tree exists, this method either creates a
     * new tree or returns null depending on whether the
     * <code>createIfNecessary</code> parameter is <code>true</code>.
     * 
     * @param name
     *            The tree name
     * 
     * @param createIfNecessary
     *            Determines whether this method will create a new tree if there
     *            is no tree having the specified name.
     * 
     * @return The <code>Tree</code>, or <code>null</code> if
     *         <code>createIfNecessary</code> is false and there is no such tree
     *         in this <code>Volume</code>.
     * 
     * @throws PersistitException
     */
    public Tree getTree(String name, boolean createIfNecessary) throws PersistitException {
        synchronized (_treeNameHashMap) {
            Tree tree = _treeNameHashMap.get(name);
            if (tree != null) {
                return tree;
            }
            final Exchange ex = directoryExchange();
            ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(name);
            Value value = ex.fetch().getValue();
            tree = new Tree(_persistit, this, name);
            if (value.isDefined()) {
                tree.load(ex.getValue());
            } else if (createIfNecessary) {
                initTree(tree);
                tree.commit();
            } else {
                return null;
            }
            _treeNameHashMap.put(name, tree);
            return tree;
        }
    }

    void updateTree(Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            claimHeadBuffer();
            try {
                _directoryRootPage = tree.getRootPageAddr();
                checkpointMetaData();
            } finally {
                releaseHeadBuffer();
            }
        } else {
            Exchange ex = directoryExchange();
            tree.store(ex.getValue());
            ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(tree.getName()).store();
        }
    }

    /**
     * Removes a <code>Tree</code> and makes all the index and data pages
     * formerly associated with that <code>Tree</code> available for reuse.
     * 
     * @param treeName
     *            The name of the <code>Tree</code> to remove.
     * @return <code>true</code> if a there was a <code>Tree</code> of the
     *         specified name and it was removed, otherwise <code>false</code>.
     * @throws PersistitException
     */
    boolean removeTree(String treeName) throws PersistitException {
        Tree tree = null;
        synchronized (this) {
            tree = _treeNameHashMap.get(treeName);
        }
        if (tree != null)
            return removeTree(tree);
        else
            return false;
    }

    boolean removeTree(Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            throw new IllegalArgumentException("Can't delete the Directory tree");
        }
        _persistit.checkSuspended();

        int depth = -1;
        long page = -1;

        tree.claim(true);

        try {
            final long rootPage = tree.getRootPageAddr();
            tree.changeRootPageAddr(-1, 0);
            page = rootPage;
            depth = tree.getDepth();
            Exchange ex = directoryExchange();

            ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(tree.getName()).remove();

            synchronized (this) {
                _treeNameHashMap.remove(tree.getName());
                tree.bumpGeneration();
                tree.invalidate();
            }
        } finally {
            tree.release();
        }
        // The Tree is now gone. The following deallocates the
        // pages formerly associated with it. If this fails we'll be
        // left with allocated pages that are not available on the garbage
        // chain for reuse.

        while (page != -1) {
            Buffer buffer = null;
            long deallocate = -1;
            try {
                buffer = _pool.get(this, page, false, true);
                if (buffer.getPageType() != depth) {
                    throw new CorruptVolumeException(buffer + " type code=" + buffer.getPageType()
                            + " is not equal to expected value " + depth);
                }
                if (buffer.isIndexPage()) {
                    deallocate = page;
                    int p = buffer.toKeyBlock(0);
                    if (p > 0) {
                        page = buffer.getPointer(p);
                    } else {
                        page = -1;
                    }
                    depth--;
                } else if (buffer.isDataPage()) {
                    deallocate = page;
                    page = -1;
                }
            } finally {
                if (buffer != null)
                    buffer.releaseTouched();
                buffer = null;
            }
            if (deallocate != -1) {
                deallocateGarbageChain(deallocate, 0);
            }
        }

        return true;

    }

    /**
     * Returns an array of all currently defined <code>Tree</code> names.
     * 
     * @return The array
     * 
     * @throws PersistitException
     */
    public String[] getTreeNames() throws PersistitException {
        List<String> list = new ArrayList<String>();
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append("");
        while (ex.next()) {
            String treeName = ex.getKey().indexTo(-1).decodeString();
            list.add(treeName);
        }
        String[] names = list.toArray(new String[list.size()]);
        return names;
    }

    /**
     * Returns the next tree name in alphabetical order within this volume.
     * 
     * @param treeName
     *            The starting tree name
     * 
     * @return The name of the first tree in alphabetical order that is larger
     *         than <code>treeName</code>.
     * 
     * @throws PersistitException
     */
    String nextTreeName(String treeName) throws PersistitException {
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(treeName);
        if (ex.next()) {
            return ex.getKey().indexTo(-1).decodeString();
        }
        return null;
    }

    /**
     * Returns an array of all Trees currently open within this Volume.
     * 
     * @return The array.
     */
    Tree[] getTrees() {
        synchronized (this) {
            int size = _treeNameHashMap.values().size();
            Tree[] trees = new Tree[size];
            int index = 0;
            for (final Tree tree : _treeNameHashMap.values()) {
                trees[index++] = tree;
            }
            return trees;
        }
    }

    /**
     * Return a TreeInfo structure for a tree by the specified name. If there is
     * no such tree, then return <i>null</i>.
     * 
     * @param tree
     *            name
     * @return an information structure for the Management interface.
     */
    Management.TreeInfo getTreeInfo(String name) {
        try {
            final Tree tree = getTree(name, false);
            if (tree != null) {
                return new Management.TreeInfo(tree);
            } else {
                return null;
            }
        } catch (PersistitException pe) {
            return null;
        }
    }

    /**
     * Indicates whether this <code>Volume</code> has been closed.
     * 
     * @return <i>true</i> if this Volume is closed.
     */
    public boolean isClosed() {
        return _closed;
    }

    /**
     * Indicates whether this <code>Volume</code> prohibits updates.
     * 
     * @return <i>true</i> if this Volume prohibits updates.
     */
    public boolean isReadOnly() {
        return _readOnly;
    }

    /**
     * Read all pages used within this <code>Volume</code> to "warm up" the
     * buffer pool. The primary purpose is to support benchmarks.
     * 
     * @throws PersistitException
     */
    public void warm() throws PersistitException {
        for (long page = 1; page < _firstAvailablePage; page++) {
            Buffer buffer = _pool.get(this, page, false, true);
            buffer.releaseTouched();
        }
    }

    /**
     * Create a new tree in this volume. A tree is represented by an index root
     * page and all the index and data pages pointed to by that root page.
     * 
     * @return newly create Tree object
     * @throws PersistitException
     */
    private Tree initTree(final Tree tree) throws PersistitException {
        _persistit.checkSuspended();
        Buffer rootPageBuffer = null;

        rootPageBuffer = allocPage();

        final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
        rootPageBuffer.writePageOnCheckpoint(timestamp);

        long rootPage = rootPageBuffer.getPageAddress();

        try {
            rootPageBuffer.init(Buffer.PAGE_TYPE_DATA);
            rootPageBuffer.putValue(Key.LEFT_GUARD_KEY, Value.EMPTY_VALUE);
            rootPageBuffer.putValue(Key.RIGHT_GUARD_KEY, Value.EMPTY_VALUE);
            rootPageBuffer.setDirtyAtTimestamp(timestamp);
        } finally {
            rootPageBuffer.releaseTouched();
        }

        tree.claim(true);
        tree.init(rootPage);
        tree.release();
        tree.setValid();
        tree.commit();
        return tree;
    }

    /**
     * Returns the <code>BufferPool</code> in which this volume's pages are
     * cached.
     * 
     * @return This volume's </code>BufferPool</code>
     */
    BufferPool getPool() {
        return _pool;
    }

    private void claimHeadBuffer() throws PersistitException {
        if (!_headBuffer.claim(true)) {
            throw new InUseException(this + " head buffer " + _headBuffer + " is unavailable");
        }
    }

    private void releaseHeadBuffer() {
        _headBuffer.releaseTouched();
    }

    void readPage(Buffer buffer, long page) throws PersistitIOException, InvalidPageAddressException,
            VolumeClosedException {
        if (page < 0 || page >= _pageCount) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _pageCount + ")");
        }

        try {
            final ByteBuffer bb = buffer.getByteBuffer();
            bb.position(0).limit(buffer.getBufferSize());
            int read = 0;
            while (read < buffer.getBufferSize()) {
                long position = page * _bufferSize + bb.position();
                int bytesRead = _channel.read(bb, position);
                if (bytesRead <= 0) {
                    throw new PersistitIOException("Unable to read bytes at position " + position + " in " + this);
                }
                read += bytesRead;
            }
            _persistit.getIOMeter().chargeReadPageFromVolume(this, buffer.getPageAddress(), buffer.getBufferSize(),
                    buffer.getIndex());
            bumpReadCounter();
            _persistit.getLogBase().readOk.log(this, page, buffer.getIndex());
        } catch (IOException ioe) {
            _persistit.getLogBase().readException.log(ioe, this, page, buffer.getIndex());
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }
    }

    void writePage(final ByteBuffer bb, final long page) throws IOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException {
        if (page < 0 || page >= _pageCount) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _pageCount + ")");
        }

        if (_readOnly) {
            throw new ReadOnlyVolumeException(this.getPath());
        }

        try {
            _channel.write(bb, page * _bufferSize);
        } catch (IOException ioe) {
            _persistit.getLogBase().writeException.log(ioe, this, page);
            _lastIOException = ioe;
            throw ioe;
        }

    }

    /**
     * Allocates a previously unused page. Returns a Buffer containing that
     * page. Empties all previous content of that page and sets its type to
     * UNUSED.
     * 
     * @return a Buffer containing the newly allocated page. The returned buffer
     *         has a writer claim on it.
     */
    Buffer allocPage() throws PersistitException {
        Buffer buffer = null;
        claimHeadBuffer();
        try {
            long garbageRoot = getGarbageRoot();
            if (garbageRoot != 0) {
                Buffer garbageBuffer = _pool.get(this, garbageRoot, true, true);
                final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                garbageBuffer.writePageOnCheckpoint(timestamp);
                try {
                    Debug.$assert0.t(garbageBuffer.isGarbagePage());
                    Debug.$assert0.t((garbageBuffer.getStatus() & CLAIMED_MASK) == 1);

                    long page = garbageBuffer.getGarbageChainLeftPage();
                    long rightPage = garbageBuffer.getGarbageChainRightPage();

                    Debug.$assert0.t(page != 0);

                    if (page == -1) {
                        long newGarbageRoot = garbageBuffer.getRightSibling();
                        _persistit.getLogBase().garbagePageExhausted.log(garbageRoot, newGarbageRoot,
                                garbageBufferInfo(garbageBuffer));
                        setGarbageRoot(newGarbageRoot);
                        buffer = garbageBuffer;
                        garbageBuffer = null;
                    } else {
                        _persistit.getLogBase().allocateFromGarbageChain.log(page, garbageBufferInfo(garbageBuffer));
                        boolean solitaire = rightPage == -1;
                        buffer = _pool.get(this, page, true, !solitaire);

                        Debug.$assert0.t(buffer.getPageAddress() > 0);

                        long nextGarbagePage = solitaire ? -1 : buffer.getRightSibling();

                        if (nextGarbagePage == rightPage || nextGarbagePage == 0) {
                            _persistit.getLogBase().garbageChainDone.log(garbageBufferInfo(garbageBuffer), rightPage);
                            garbageBuffer.removeGarbageChain();
                        } else {
                            _persistit.getLogBase().garbageChainUpdate.log(garbageBufferInfo(garbageBuffer),
                                    nextGarbagePage, rightPage);
                            Debug.$assert0.t(nextGarbagePage > 0);
                            garbageBuffer.setGarbageLeftPage(nextGarbagePage);
                        }
                        garbageBuffer.setDirtyAtTimestamp(timestamp);
                    }

                    Debug.$assert0
                            .t(buffer != null && buffer.getPageAddress() != 0
                                    && buffer.getPageAddress() != _garbageRoot
                                    && buffer.getPageAddress() != _directoryRootPage);

                    harvestLongRecords(buffer, 0, Integer.MAX_VALUE);

                    buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
                    buffer.clear();
                    return buffer;
                } finally {
                    if (garbageBuffer != null) {
                        garbageBuffer.releaseTouched();
                    }
                }
            } else {
                if (_firstAvailablePage >= _pageCount) {
                    extend();
                }
                long page;
                page = _firstAvailablePage++;

                // No need to read the prior content of the page - we trust
                // it's never been used before.
                buffer = _pool.get(this, page, true, false);
                buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
                // -
                // debug
                if (page > _highestPageUsed) {
                    _highestPageUsed = page;
                }

                checkpointMetaData();

                Debug.$assert0.t(buffer.getPageAddress() != 0);
                return buffer;
            }
        } finally {
            releaseHeadBuffer();
        }
    }

    private void extend() throws PersistitException {

        if (_pageCount >= _maximumPages || _extensionPages <= 0) {
            throw new VolumeFullException(this + " is full: " + _pageCount + " pages");
        }
        // Do not extend past maximum pages
        long pageCount = Math.min(_pageCount + _extensionPages, _maximumPages);
        extend(pageCount);
    }

    private void extend(final long pageCount) throws PersistitException {
        long newSize = pageCount * _bufferSize;
        long currentSize = -1;

        claimHeadBuffer();
        try {
            currentSize = _channel.size();
            if (currentSize > newSize) {
                _persistit.getLogBase().extendLonger.log(this, currentSize, newSize);
            }
            if (currentSize < newSize) {
                final ByteBuffer bb = ByteBuffer.allocate(1);
                bb.position(0).limit(1);
                _channel.write(bb, newSize - 1);
                _channel.force(true);
                _persistit.getLogBase().extendNormal.log(this, currentSize, newSize);
            }

            _pageCount = pageCount;
            _lastExtensionTime = System.currentTimeMillis();
            checkpointMetaData();

        } catch (IOException ioe) {
            _lastIOException = ioe;
            _persistit.getLogBase().extendException.log(ioe, this, currentSize, newSize);
            throw new PersistitIOException(ioe);
        } finally {
            releaseHeadBuffer();
        }
    }

    void close() throws PersistitException {
        _headBuffer.clearFixed();
        _closed = true;

        PersistitException pe = null;
        try {
            final FileLock lock = _fileLock;
            _fileLock = null;
            if (lock != null) {
                lock.release();
            }
        } catch (Exception e) {
            _persistit.getLogBase().exception.log(e);
            pe = new PersistitException(e);
        }
        try {
            final FileChannel channel = _channel;
            _channel = null;
            if (channel != null) {
                channel.close();
            }
        } catch (Exception e) {
            _persistit.getLogBase().exception.log(e);
            // has priority over Exception throw by
            // releasing file lock.
            pe = new PersistitException(e);
        }
        if (pe != null) {
            throw pe;
        }
    }

    void flush() throws PersistitException {
        claimHeadBuffer();
        commitAllTreeUpdates();
        setClean();
        checkpointMetaData();
        releaseHeadBuffer();
    }

    void sync() throws PersistitIOException, ReadOnlyVolumeException {
        if (isReadOnly()) {
            throw new ReadOnlyVolumeException(this.toString());
        }
        try {
            if (_channel != null && _channel.isOpen()) {
                _channel.force(true);
            }
        } catch (IOException ioe) {
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }
    }

    private boolean updateHeaderInfo(byte[] bytes) {
        boolean changed = false;
        changed |= Util.changeBytes(bytes, 0, _header.getSignature());
        changed |= Util.changeInt(bytes, 16, _header.getVersion());
        changed |= Util.changeInt(bytes, 20, _bufferSize);
        changed |= Util.changeLong(bytes, 24, _timestamp);
        changed |= Util.changeLong(bytes, 32, _id);
        changed |= Util.changeLong(bytes, 40, _readCounter.get());
        // Ugly, but the act of closing the system increments this
        // counter, leading to an extra write. So basically we
        // ignore the final write by not setting the changed flag.
        Util.putLong(bytes, 48, _writeCounter.get());
        changed |= Util.changeLong(bytes, 56, _getCounter.get());
        changed |= Util.changeLong(bytes, 64, _openTime);
        changed |= Util.changeLong(bytes, 72, _createTime);
        changed |= Util.changeLong(bytes, 80, _lastReadTime);
        // See comment above for _writeCounter
        Util.putLong(bytes, 88, _lastWriteTime);
        changed |= Util.changeLong(bytes, 96, _lastExtensionTime);
        changed |= Util.changeLong(bytes, 104, _highestPageUsed);
        changed |= Util.changeLong(bytes, 112, _pageCount);
        changed |= Util.changeLong(bytes, 120, _extensionPages);
        changed |= Util.changeLong(bytes, 128, _maximumPages);
        changed |= Util.changeLong(bytes, 136, _firstAvailablePage);
        changed |= Util.changeLong(bytes, 144, _directoryRootPage);
        changed |= Util.changeLong(bytes, 152, _garbageRoot);
        changed |= Util.changeLong(bytes, 160, _fetchCounter.get());
        changed |= Util.changeLong(bytes, 168, _traverseCounter.get());
        changed |= Util.changeLong(bytes, 176, _storeCounter.get());
        changed |= Util.changeLong(bytes, 184, _removeCounter.get());
        changed |= Util.changeLong(bytes, 192, _initialPages);
        return changed;
    }

    private void getHeaderInfo(byte[] bytes) {
        _bufferSize = Util.getInt(bytes, 20);
        _timestamp = Util.getLong(bytes, 24);
        _id = Util.getLong(bytes, 32);
        _readCounter.set(Util.getLong(bytes, 40));
        _writeCounter.set(Util.getLong(bytes, 48));
        _getCounter.set(Util.getLong(bytes, 56));
        _openTime = Util.getLong(bytes, 64);
        _createTime = Util.getLong(bytes, 72);
        _lastReadTime = Util.getLong(bytes, 80);
        _lastWriteTime = Util.getLong(bytes, 88);
        _lastExtensionTime = Util.getLong(bytes, 96);
        _highestPageUsed = Util.getLong(bytes, 104);
        _pageCount = Util.getLong(bytes, 112);
        _extensionPages = Util.getLong(bytes, 120);
        _maximumPages = Util.getLong(bytes, 128);
        _firstAvailablePage = Util.getLong(bytes, 136);
        _directoryRootPage = Util.getLong(bytes, 144);
        _garbageRoot = Util.getLong(bytes, 152);
        _fetchCounter.set(Util.getLong(bytes, 160));
        _traverseCounter.set(Util.getLong(bytes, 168));
        _storeCounter.set(Util.getLong(bytes, 176));
        _removeCounter.set(Util.getLong(bytes, 184));
        _initialPages = Util.getLong(bytes, 192);
    }

    /**
     * Returns a displayable string describing this <code>Volume</code>.
     * 
     * @return The description
     */
    @Override
    public String toString() {
        return getName() + "(" + getPath() + ")";
    }

    /**
     * Store an Object with this Volume for the convenience of an application.
     * 
     * @param the
     *            object to be cached for application convenience.
     */
    public void setAppCache(Object appCache) {
        _appCache.set(appCache);
    }

    /**
     * @return the object cached for application convenience
     */
    public Object getAppCache() {
        return _appCache.get();
    }

    int getHandle() {
        return _handle.get();
    }

    int setHandle(final int handle) {
        _handle.set(handle);
        return handle;
    }

}
