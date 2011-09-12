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

import static com.persistit.VolumeHeader.changeDirectoryRoot;
import static com.persistit.VolumeHeader.changeExtendedPageCount;
import static com.persistit.VolumeHeader.changeFetchCounter;
import static com.persistit.VolumeHeader.changeGarbageRoot;
import static com.persistit.VolumeHeader.changeLastExtensionTime;
import static com.persistit.VolumeHeader.changeLastReadTime;
import static com.persistit.VolumeHeader.changeLastWriteTime;
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Random;

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;
import com.persistit.exception.VolumeNotFoundException;

/**
 * Manage all details of file I/O for a <code>Volume</code> backing file.
 * 
 * @author peter
 */
class VolumeStorage extends SharedResource {

    private final static Random ID_GENERATOR = new Random();
    private Volume _volume;

    private FileChannel _channel;
    private FileLock _fileLock;
    private Buffer _headBuffer;

    private volatile long _nextAvailablePage;
    private volatile long _extendedPageCount;
    private volatile boolean _opened;
    private volatile boolean _closed;
    private volatile IOException _lastIOException;

    /**
     * Generate a random positive (non-zero) long value to be used as a
     * validation of a Volume's identity.
     * 
     * @return
     */
    private static long generateId() {
        return (ID_GENERATOR.nextLong() & 0x0FFFFFFFFFFl) + 1;
    }

    VolumeStorage(final Persistit persistit, final Volume volume) {
        super(persistit);
        _volume = volume;
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    String getPath() {
        return _volume.getSpecification().getPath();
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
    IOException lastException(boolean reset) {
        IOException ioe = _lastIOException;
        if (reset)
            _lastIOException = null;
        return ioe;
    }

    /**
     * Indicates whether this <code>Volume</code> prohibits updates.
     * 
     * @return <i>true</i> if this Volume prohibits updates.
     */
    boolean isReadOnly() {
        return _volume.getSpecification().isReadOnly();
    }

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link Volume}'s volume specification.
     * 
     * @throws PersistitException
     */
    void create() throws PersistitException {
        if (exists()) {
            throw new VolumeAlreadyExistsException(getPath());
        }
        try {
            _channel = new RandomAccessFile(getPath(), "rw").getChannel();
            lockChannel();

            VolumeSpecification spec = _volume.getSpecification();
            VolumeStatistics stat = _volume.getStatistics();
            VolumeStructure struc = _volume.getStructure();

            resize(spec.getInitialPages());
            _volume.setId(generateId());

            long now = System.currentTimeMillis();
            stat.setCreateTime(now);
            stat.setOpenTime(now);
            _extendedPageCount = spec.getInitialPages();
            _nextAvailablePage = 1;

            _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, false);
            _headBuffer.setFixed();

            initMetaData(_headBuffer.getBytes());
            //
            // Lay down the initial version of the header page so that the
            // volume file
            // will be valid on restart
            //
            writePage(_headBuffer.getByteBuffer(), _headBuffer.getPageAddress());
            
            struc.init(0, 0);
            flushMetaData();
            _opened = true;
        } catch (IOException ioe) {
            if (_headBuffer != null) {
                _headBuffer.clearFixed();
                _headBuffer.clearValid();
                _headBuffer.release();
                _headBuffer = null;
            }
            throw new PersistitIOException(ioe);
        } finally {
            if (_headBuffer != null) {
                _headBuffer.release();
            }
            if (!_opened) {
                try {
                    _channel.close();
                    _channel = null;
                } catch (IOException e) {
                    // Not much to do - we're going to try to delete
                    // the file anyway.
                }
                new File(getPath()).delete();
            }
        }

    }

    /**
     * Open an existing <code>Volume</code> backing file.
     * 
     * @throws PersistitException
     */
    void open() throws PersistitException {
        if (!exists()) {
            throw new VolumeNotFoundException(getPath());
        }

        VolumeSpecification spec = _volume.getSpecification();
        VolumeStatistics stat = _volume.getStatistics();
        VolumeStructure struc = _volume.getStructure();

        try {
            _channel = new RandomAccessFile(getPath(), isReadOnly() ? "r" : "rw").getChannel();
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

            long now = System.currentTimeMillis();

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
            
            flushMetaData();
            _opened = true;
            
        } catch (IOException ioe) {
            if (_headBuffer != null) {
                _headBuffer.clearFixed();
                _headBuffer.clearValid();
                _headBuffer.release();
                _headBuffer = null;
            }
            throw new PersistitIOException(ioe);
        } finally {
            if (_headBuffer != null) {
                _headBuffer.release();
            }
        }
    }

    /**
     * @return <code>true</code> if a backing file exists on the specified path.
     * @throws PersistitException
     */
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
    boolean delete() throws PersistitException {
        final File file = new File(getPath());
        return file.exists() && file.isFile() && file.delete();
    }

    /**
     * Force all file system buffers to disk.
     * 
     * @throws PersistitIOException
     */
    void force() throws PersistitIOException {
        try {
            _channel.force(true);
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        }
    }

    /**
     * Close the file resources held by this <code>Volume</code>. After this
     * method is called no further file I/O is possible.
     * 
     * @throws PersistitException
     */
    void close() throws PersistitException {
        if (!_closed) {
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
                // has priority over Exception thrown by
                // releasing file lock.
                pe = new PersistitException(e);
            }
            if (pe != null) {
                throw pe;
            }
            _closed = true;
        }
    }

    boolean isOpened() {
        return _opened;
    }

    boolean isClosed() {
        return _closed;
    }

    long getPageCount() {
        return _extendedPageCount;
    }

    long getNextAvailablePage() {
        return _nextAvailablePage;
    }

    void claimHeadBuffer() throws PersistitException {
        if (!_headBuffer.claim(true)) {
            throw new InUseException(this + " head buffer " + _headBuffer + " is unavailable");
        }
    }

    void releaseHeadBuffer() {
        _headBuffer.releaseTouched();
    }

    private void lockChannel() throws InUseException, IOException {
        try {
            _fileLock = _channel.tryLock(0, Long.MAX_VALUE, isReadOnly());
        } catch (OverlappingFileLockException e) {
            // Note: OverlappingFileLockException is a RuntimeException
            throw new InUseException("Volume file " + getPath() + " is locked by another thread in this JVM");
        }
        if (_fileLock == null) {
            throw new InUseException("Volume file " + getPath() + " is locked by another process");
        }
    }

    void readPage(Buffer buffer) throws PersistitIOException, InvalidPageAddressException,
            VolumeClosedException {
        final long page = buffer.getPageAddress();
        if (page < 0 || page >= _nextAvailablePage) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _nextAvailablePage + "]");
        }
        if (!_volume.isTemporary() && _persistit.getJournalManager().readPageFromJournal(buffer))
        {
            return;
        }

        try {
            final ByteBuffer bb = buffer.getByteBuffer();
            bb.position(0).limit(buffer.getBufferSize());
            int read = 0;
            while (read < buffer.getBufferSize()) {
                long position = page * _volume.getStructure().getPageSize() + bb.position();
                int bytesRead = _channel.read(bb, position);
                if (bytesRead <= 0) {
                    throw new PersistitIOException("Unable to read bytes at position " + position + " in " + this);
                }
                read += bytesRead;
            }
            _persistit.getIOMeter().chargeReadPageFromVolume(this._volume, buffer.getPageAddress(),
                    buffer.getBufferSize(), buffer.getIndex());
            _volume.getStatistics().bumpReadCounter();
            _persistit.getLogBase().readOk.log(this, page, buffer.getIndex());
        } catch (IOException ioe) {
            _persistit.getLogBase().readException.log(ioe, this, page, buffer.getIndex());
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }
    }
    
    void writePage(final Buffer buffer) throws PersistitIOException, InvalidPageAddressException, ReadOnlyVolumeException, VolumeClosedException {
        if (_volume.isTemporary()) {
            writePage(buffer.getByteBuffer(), buffer.getPageAddress());
        } else {
            _persistit.getJournalManager().writePageToJournal(buffer);
        }
    }

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
        } catch (IOException ioe) {
            _persistit.getLogBase().writeException.log(ioe, this, page);
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }

    }

    long allocNewPage() throws PersistitException {
        claim(true);
        try {
            for (;;) {
                if (_nextAvailablePage <= _extendedPageCount) {
                    long page = _nextAvailablePage++;
                    _volume.getStatistics().setNextAvailablePage(page);
                    flushMetaData();
                    return page;
                }
                extend();
            }
        } finally {
            release();
        }
    }

    void flushMetaData() throws PersistitException {
        if (!isReadOnly()) {
            final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
            _headBuffer.writePageOnCheckpoint(timestamp);
            if (updateMetaData(_headBuffer.getBytes())) {
                _headBuffer.setDirtyAtTimestamp(timestamp);
            }
        }
    }

    private void initMetaData(final byte[] bytes) {
        VolumeStructure struc = _volume.getStructure();
        putSignature(bytes);
        putVersion(bytes);
        putPageSize(bytes, struc.getPageSize());
        putId(bytes, _volume.getId());

        changeNextAvailablePage(bytes, _nextAvailablePage);
        changeExtendedPageCount(bytes, _extendedPageCount);
}

    private boolean updateMetaData(final byte[] bytes) {
        VolumeStatistics stat = _volume.getStatistics();
        VolumeStructure struc = _volume.getStructure();

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

        return changed;
    }

    void extend(final long pageAddr) throws PersistitException {
        if (pageAddr >= _extendedPageCount) {
            extend();
        }
    }

    private void extend() throws PersistitException {
        final long maximumPages = _volume.getSpecification().getMaximumPages();
        final long extensionPages = _volume.getSpecification().getExtensionPages();
        if (_extendedPageCount >= maximumPages || extensionPages <= 0) {
            throw new VolumeFullException(this + " is full: " + _extendedPageCount + " pages");
        }
        // Do not extend past maximum pages
        long pageCount = Math.min(_extendedPageCount + extensionPages, maximumPages);
        resize(pageCount);
        flushMetaData();

    }

    private void resize(long pageCount) throws PersistitException {
        long newSize = pageCount * _volume.getStructure().getPageSize();
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
        } catch (IOException ioe) {
            _lastIOException = ioe;
            _persistit.getLogBase().extendException.log(ioe, this, currentSize, newSize);
            throw new PersistitIOException(ioe);
        }
    }
}
