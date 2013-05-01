/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

import static com.persistit.VolumeHeader.changeCreateTime;
import static com.persistit.VolumeHeader.changeDirectoryRoot;
import static com.persistit.VolumeHeader.changeExtendedPageCount;
import static com.persistit.VolumeHeader.changeExtensionPages;
import static com.persistit.VolumeHeader.changeFetchCounter;
import static com.persistit.VolumeHeader.changeGarbageRoot;
import static com.persistit.VolumeHeader.changeGlobalTimestamp;
import static com.persistit.VolumeHeader.changeInitialPages;
import static com.persistit.VolumeHeader.changeLastExtensionTime;
import static com.persistit.VolumeHeader.changeLastReadTime;
import static com.persistit.VolumeHeader.changeLastWriteTime;
import static com.persistit.VolumeHeader.changeMaximumPages;
import static com.persistit.VolumeHeader.changeNextAvailablePage;
import static com.persistit.VolumeHeader.changeReadCounter;
import static com.persistit.VolumeHeader.changeRemoveCounter;
import static com.persistit.VolumeHeader.changeStoreCounter;
import static com.persistit.VolumeHeader.changeTraverseCounter;
import static com.persistit.VolumeHeader.changeWriteCounter;
import static com.persistit.VolumeHeader.getCreateTime;
import static com.persistit.VolumeHeader.getDirectoryRoot;
import static com.persistit.VolumeHeader.getExtensionPages;
import static com.persistit.VolumeHeader.getGarbageRoot;
import static com.persistit.VolumeHeader.getGlobalTimestamp;
import static com.persistit.VolumeHeader.getId;
import static com.persistit.VolumeHeader.getInitialPages;
import static com.persistit.VolumeHeader.getLastExtensionTime;
import static com.persistit.VolumeHeader.getLastReadTime;
import static com.persistit.VolumeHeader.getLastWriteTime;
import static com.persistit.VolumeHeader.getMaximumPages;
import static com.persistit.VolumeHeader.putId;
import static com.persistit.VolumeHeader.putPageSize;
import static com.persistit.VolumeHeader.putSignature;
import static com.persistit.VolumeHeader.putVersion;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import com.persistit.AlertMonitor.AlertLevel;
import com.persistit.AlertMonitor.Event;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;
import com.persistit.exception.VolumeNotFoundException;

/**
 * Manage all details of file I/O for a <code>Volume</code> backing file for
 * Version 2.xx storage formats.
 * 
 * @author peter
 */
class VolumeStorageV2 extends VolumeStorage {

    private volatile FileChannel _channel;
    private volatile FileLock _fileLock;

    private volatile Buffer _headBuffer;

    private volatile long _nextAvailablePage;
    private volatile long _extendedPageCount;
    private volatile boolean _opened;
    private volatile boolean _closed;

    /**
     * Generate a random positive (non-zero) long value to be used as a
     * validation of a Volume's identity.
     * 
     * @return
     */

    VolumeStorageV2(final Persistit persistit, final Volume volume) {
        super(persistit, volume);
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    @Override
    String getPath() {
        return _volume.getSpecification().getPath();
    }

    /**
     * Indicate whether this <code>Volume</code> prohibits updates.
     * 
     * @return <code>true</code> if this Volume prohibits updates.
     */
    @Override
    boolean isReadOnly() {
        return _volume.getSpecification().isReadOnly();
    }

    /**
     * Indicate whether this is a temporary volume
     * 
     * @return <code>true</code> if this volume is temporary
     */
    @Override
    boolean isTemp() {
        return false;
    }

    /**
     * @return the channel used to read and write pages of this volume.
     */
    @Override
    FileChannel getChannel() {
        return _channel;
    }

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link Volume}'s volume specification.
     * 
     * @throws PersistitException
     */
    @Override
    synchronized void create() throws PersistitException {
        if (_opened) {
            throw new IllegalStateException("Volume " + this + " cannot be reopened");
        }
        if (exists()) {
            throw new VolumeAlreadyExistsException(getPath());
        }
        try {
            _channel = new MediatedFileChannel(getPath(), "rw");
            lockChannel();
            truncate();
            _opened = true;
        } catch (final IOException ioe) {
            throw new PersistitIOException(ioe);
        } finally {
            if (!_opened) {
                try {
                    closeChannel();
                } catch (final IOException e) {
                    // Not much to do - we're going to try to delete
                    // the file anyway.
                }
                if (getPath() != null) {
                    delete();
                }
            }
        }
    }

    /**
     * Open an existing <code>Volume</code> backing file.
     * 
     * @throws PersistitException
     */
    @Override
    synchronized void open() throws PersistitException {
        if (_opened) {
            throw new IllegalStateException("Volume " + this + " cannot be reopened");
        }
        if (!exists()) {
            throw new VolumeNotFoundException(getPath());
        }

        final VolumeSpecification spec = _volume.getSpecification();
        final VolumeStatistics stat = _volume.getStatistics();
        final VolumeStructure struc = _volume.getStructure();

        try {
            _channel = new MediatedFileChannel(getPath(), isReadOnly() ? "r" : "rw");
            lockChannel();
            _nextAvailablePage = 1; // correct value installed below
            _volume.setId(spec.getId());

            _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, true);
            _headBuffer.setFixed();
            final byte[] bytes = _headBuffer.getBytes();

            _nextAvailablePage = VolumeHeader.getNextAvailablePage(bytes);
            _extendedPageCount = _channel.size() / _volume.getStructure().getPageSize();

            final long id = getId(bytes);
            _volume.verifyId(id);
            _volume.setId(id);

            final long now = System.currentTimeMillis();

            spec.setInitialPages(getInitialPages(bytes));
            spec.setMaximumPages(getMaximumPages(bytes));
            spec.setExtensionPages(getExtensionPages(bytes));

            stat.setCreateTime(getCreateTime(bytes));
            stat.setNextAvailablePage(VolumeHeader.getNextAvailablePage(bytes));
            stat.setLastExtensionTime(getLastExtensionTime(bytes));
            stat.setLastReadTime(getLastReadTime(bytes));
            stat.setLastWriteTime(getLastWriteTime(bytes));
            stat.setOpenTime(now);

            final long directoryRootPage = getDirectoryRoot(bytes);
            final long garbageRootPage = getGarbageRoot(bytes);
            struc.init(directoryRootPage, garbageRootPage);

            final long globalTimestamp = getGlobalTimestamp(bytes);
            stat.setLastGlobalTimestamp(globalTimestamp);

            flushMetaData();
            _opened = true;

        } catch (final IOException ioe) {
            throw new PersistitIOException(ioe);
        } finally {
            if (_headBuffer != null) {
                if (!_opened) {
                    _headBuffer.clearFixed();
                    _headBuffer.clearValid();
                    releaseHeadBuffer();
                    _headBuffer = null;
                } else {
                    releaseHeadBuffer();
                }
            }
        }
    }

    /**
     * @return <code>true</code> if a backing file exists on the specified path.
     * @throws PersistitException
     */
    @Override
    boolean exists() throws PersistitException {
        final File file = new File(getPath());
        return file.exists() && file.isFile();
    }

    /**
     * Delete the backing file for this Volume if it exists.
     * 
     * @return <code>true</code> if there was a file and it was successfully
     *         deleted
     * @throws PersistitException
     */
    @Override
    boolean delete() throws PersistitException {
        final File file = new File(getPath());
        return file.exists() && file.isFile() && file.delete();
    }

    /**
     * Force all file system buffers to disk.
     * 
     * @throws PersistitIOException
     */
    @Override
    void force() throws PersistitIOException {
        try {
            _channel.force(true);
        } catch (final IOException ioe) {
            throw new PersistitIOException(ioe);
        }
    }

    /**
     * Close the file resources held by this <code>Volume</code>. After this
     * method is called no further file I/O is possible.
     * 
     * @throws PersistitException
     */
    @Override
    void close() throws PersistitException {
        /*
         * Exclusive claim here intended to conflict with readPage and writePage
         */
        if (!claim(true)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            if (_closed) {
                return;
            }
            _closed = true;
            _headBuffer = null;
            PersistitException pe = null;
            try {
                final FileLock lock = _fileLock;
                _fileLock = null;
                if (lock != null) {
                    lock.release();
                }
            } catch (final Exception e) {
                _persistit.getLogBase().exception.log(e);
                pe = new PersistitException(e);
            }
            try {
                closeChannel();
            } catch (final Exception e) {
                _persistit.getLogBase().exception.log(e);
                // has priority over Exception thrown by
                // releasing file lock.
                pe = new PersistitException(e);
            }
            if (pe != null) {
                throw pe;
            }
        } finally {
            release();
        }
    }

    private void closeChannel() throws IOException {
        final FileChannel channel = _channel;
        _channel = null;
        if (channel != null) {
            channel.close();
        }
    }

    /**
     * Truncate an existing volume. This method deletes all existing data in the
     * volume. It is equivalent to deleting the volume and creating a new one
     * using the same VolumeSpecifications; however the file is never actually
     * deleted by this operation.
     * <p />
     * This method assigns a new unique id value to the volume so that journal
     * records pertaining to its state prior to being truncated are not confused
     * with the new empty state. Caution: information in the volume is
     * irrecoverably destroyed by this method.
     */

    @Override
    void truncate() throws PersistitException {
        _volume.setId(0);
        _volume.setId(generateId());

        final VolumeSpecification spec = _volume.getSpecification();
        final VolumeStatistics stat = _volume.getStatistics();
        final VolumeStructure struc = _volume.getStructure();

        resize(1);
        resize(spec.getInitialPages());

        final long now = System.currentTimeMillis();
        stat.setCreateTime(now);
        stat.setOpenTime(now);
        _extendedPageCount = spec.getInitialPages();
        _nextAvailablePage = 1;

        _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, false);
        boolean truncated = false;
        try {
            _headBuffer.init(Buffer.PAGE_TYPE_HEAD);
            _headBuffer.setFixed();

            initMetaData(_headBuffer.getBytes());
            //
            // Lay down the initial version of the header page so that the
            // volume file will be valid on restart
            //
            final ByteBuffer bb = _headBuffer.getByteBuffer();
            bb.limit(_headBuffer.getBufferSize()).position(0);
            writePage(bb, _headBuffer.getPageAddress());
            //
            // Now create directory root page, etc.
            //
            struc.init(0, 0);
            flushMetaData();
            truncated = true;
        } finally {
            if (!truncated) {
                _headBuffer.clearValid();
                _headBuffer.clearFixed();
                releaseHeadBuffer();
                _headBuffer = null;
            } else {
                releaseHeadBuffer();
            }
        }
    }

    @Override
    boolean isOpened() {
        return _opened;
    }

    @Override
    boolean isClosed() {
        return _closed;
    }

    @Override
    long getExtentedPageCount() {
        return _extendedPageCount;
    }

    @Override
    long getNextAvailablePage() {
        return _nextAvailablePage;
    }

    @Override
    void claimHeadBuffer() throws PersistitException {
        if (!_headBuffer.claim(true)) {
            throw new InUseException("Unable to acquire claim on " + _headBuffer);
        }
    }

    @Override
    void releaseHeadBuffer() {
        _headBuffer.release();
    }

    @Override
    void readPage(final Buffer buffer) throws PersistitIOException, InvalidPageAddressException, VolumeClosedException,
            PersistitInterruptedException, InUseException {
        // non-exclusive claim here intended to conflict with exclusive claim in
        // close and truncate
        if (!claim(false)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            final long page = buffer.getPageAddress();
            if (page < 0 || page >= _nextAvailablePage) {
                throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _nextAvailablePage + "]");
            }
            if (_persistit.getJournalManager().readPageFromJournal(buffer)) {
                return;
            }

            try {
                final ByteBuffer bb = buffer.getByteBuffer();
                bb.position(0).limit(buffer.getBufferSize());
                int read = 0;
                while (read < buffer.getBufferSize()) {
                    final long position = page * _volume.getStructure().getPageSize() + bb.position();
                    final int bytesRead = _channel.read(bb, position);
                    if (bytesRead <= 0) {
                        throw new PersistitIOException("Unable to read bytes at position " + position + " in " + this);
                    }
                    read += bytesRead;
                }
                _persistit.getIOMeter().chargeReadPageFromVolume(this._volume, buffer.getPageAddress(),
                        buffer.getBufferSize(), buffer.getIndex());
                _volume.getStatistics().bumpReadCounter();

            } catch (final IOException ioe) {
                _persistit.getAlertMonitor().post(
                        new Event(AlertLevel.ERROR, _persistit.getLogBase().readException, ioe, _volume, page,
                                buffer.getIndex()), AlertMonitor.READ_PAGE_CATEGORY);
                throw new PersistitIOException(ioe);
            }
        } finally {
            release();
        }
    }

    @Override
    void writePage(final Buffer buffer) throws PersistitException {
        /*
         * Non-exclusive claim here intended to conflict with exclusive claim in
         * close and truncate
         */
        if (!claim(false)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            _persistit.getJournalManager().writePageToJournal(buffer);
        } finally {
            release();
        }

    }

    @Override
    void writePage(final ByteBuffer bb, final long page) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException {
        if (page < 0 || page >= _nextAvailablePage) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _nextAvailablePage + "]");
        }

        if (isReadOnly()) {
            throw new ReadOnlyVolumeException(getPath());
        }

        try {
            _channel.write(bb, page * _volume.getStructure().getPageSize());
        } catch (final IOException ioe) {
            _persistit.getAlertMonitor().post(
                    new Event(AlertLevel.ERROR, _persistit.getLogBase().writeException, ioe, _volume, page),
                    AlertMonitor.WRITE_PAGE_CATEGORY);
            throw new PersistitIOException(ioe);
        }
    }

    @Override
    long allocNewPage() throws PersistitException {
        long page = -1;
        claimHeadBuffer();
        try {
            for (;;) {
                if (_nextAvailablePage < _extendedPageCount) {
                    page = _nextAvailablePage++;
                    _volume.getStatistics().setNextAvailablePage(page);
                    break;
                }
                extend();
            }
            flushMetaData();
        } finally {
            releaseHeadBuffer();
        }
        return page;
    }

    @Override
    void flush() throws PersistitException {
        claimHeadBuffer();
        try {
            flushMetaData();
        } finally {
            releaseHeadBuffer();
        }
    }

    @Override
    void flushMetaData() throws PersistitException {
        if (!isReadOnly()) {
            assert _headBuffer.isOwnedAsWriterByMe();
            final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
            _volume.getStatistics().setLastGlobalTimestamp(timestamp);
            _headBuffer.writePageOnCheckpoint(timestamp);
            if (updateMetaData(_headBuffer.getBytes())) {
                _headBuffer.setDirtyAtTimestamp(timestamp);
            }
        }
    }

    @Override
    void extend(final long pageAddr) throws PersistitException {
        if (pageAddr >= _extendedPageCount) {
            extend();
            flush();
        }
    }

    private void lockChannel() throws InUseException, IOException {
        try {
            _fileLock = _channel.tryLock(0, Long.MAX_VALUE, isReadOnly());
        } catch (final OverlappingFileLockException e) {
            // Note: OverlappingFileLockException is a RuntimeException
            throw new InUseException("Volume file " + getPath() + " is locked by another thread in this JVM");
        }
        if (_fileLock == null) {
            throw new InUseException("Volume file " + getPath() + " is locked by another process");
        }
    }

    private void initMetaData(final byte[] bytes) {
        final VolumeStructure struc = _volume.getStructure();
        putSignature(bytes);
        putVersion(bytes);
        putPageSize(bytes, struc.getPageSize());
        putId(bytes, _volume.getId());
        changeNextAvailablePage(bytes, _nextAvailablePage);
        changeExtendedPageCount(bytes, _extendedPageCount);
        changeCreateTime(bytes, _volume.getStatistics().getCreateTime());
        changeInitialPages(bytes, _volume.getSpecification().getInitialPages());
        changeMaximumPages(bytes, _volume.getSpecification().getMaximumPages());
        changeExtensionPages(bytes, _volume.getSpecification().getExtensionPages());
    }

    @Override
    boolean updateMetaData(final byte[] bytes) {
        final VolumeStatistics stat = _volume.getStatistics();
        final VolumeStructure struc = _volume.getStructure();

        boolean changed = false;
        changed |= changeNextAvailablePage(bytes, _nextAvailablePage);
        changed |= changeExtendedPageCount(bytes, _extendedPageCount);
        changed |= changeDirectoryRoot(bytes, struc.getDirectoryRoot());
        changed |= changeGarbageRoot(bytes, struc.getGarbageRoot());
        changed |= changeFetchCounter(bytes, stat.getFetchCounter());
        changed |= changeTraverseCounter(bytes, stat.getTraverseCounter());
        changed |= changeStoreCounter(bytes, stat.getStoreCounter());
        changed |= changeRemoveCounter(bytes, stat.getRemoveCounter());
        changed |= changeReadCounter(bytes, stat.getReadCounter());
        changed |= changeLastExtensionTime(bytes, stat.getLastExtensionTime());
        changed |= changeLastReadTime(bytes, stat.getLastReadTime());

        // Ugly, but the act of closing the system increments this
        // counter, leading to an extra write. So basically we
        // ignore the final write by not setting the changed flag.
        changeWriteCounter(bytes, stat.getWriteCounter());
        changeLastWriteTime(bytes, stat.getLastWriteTime());
        changeGlobalTimestamp(bytes, stat.getLastGlobalTimestamp());

        return changed;
    }

    private void extend() throws PersistitException {
        final long maximumPages = _volume.getSpecification().getMaximumPages();
        final long extensionPages = _volume.getSpecification().getExtensionPages();
        if (_extendedPageCount >= maximumPages || extensionPages <= 0) {
            throw new VolumeFullException(this + " is full: " + _extendedPageCount + " pages");
        }
        // Do not extend past maximum pages
        final long pageCount = Math.min(_extendedPageCount + extensionPages, maximumPages);
        resize(pageCount);
    }

    private void resize(final long pageCount) throws PersistitException {
        final long newSize = pageCount * _volume.getStructure().getPageSize();
        long currentSize = -1;

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

            _volume.getStatistics().setLastExtensionTime(System.currentTimeMillis());
            _extendedPageCount = pageCount;
        } catch (final IOException ioe) {
            _persistit.getAlertMonitor().post(
                    new Event(AlertLevel.ERROR, _persistit.getLogBase().extendException, ioe, _volume.getName(),
                            currentSize, newSize), AlertMonitor.EXTEND_VOLUME_CATEGORY);
            throw new PersistitIOException(ioe);
        }
    }

    @Override
    public String toString() {
        return _volume.toString();
    }

}
