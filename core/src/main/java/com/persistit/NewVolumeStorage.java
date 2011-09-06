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

import static com.persistit.NewVolumeHeader.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import com.persistit.exception.BufferSizeUnavailableException;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.util.Util;

/**
 * Manage all details of file I/O for a <code>Volume</code> backing file.
 * 
 * @author peter
 */
public class NewVolumeStorage extends SharedResource {

    private NewVolume _volume;

    private FileChannel _channel;
    private FileLock _fileLock;

    private String _path;

    private long _initialPages;
    private long _maximumPages;
    private long _extensionPages;

    private volatile long _pageAllocationBoundary;
    private volatile long _pageCount;
    private Buffer _headBuffer;

    private volatile IOException _lastIOException;

    NewVolumeStorage(final Persistit persistit, final NewVolume volume) {
        super(persistit);
        _volume = volume;
        _path = _volume.getSpecification().getPath();
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

    /**
     * Indicates whether this <code>Volume</code> prohibits updates.
     * 
     * @return <i>true</i> if this Volume prohibits updates.
     */
    public boolean isReadOnly() {
        return _volume.getSpecification().isReadOnly();
    }

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link NewVolume}'s volume specification.
     * 
     * @throws PersistitException
     */
    void create() throws PersistitException {
        if (exists()) {
            throw new VolumeAlreadyExistsException(_path);
        }
        try {
            _channel = new RandomAccessFile(_path, "rw").getChannel();
            lockChannel();

            NewVolumeSpecification spec = _volume.getSpecification();
            NewVolumeStatistics stat = _volume.getStatistics();
            NewVolumeStructure struc = _volume.getStructure();
            resize(spec.getInitialPages());

            long now = System.currentTimeMillis();
            stat.setCreateTime(now);
            stat.setOpenTime(now);
            _pageCount = spec.getInitialPages();
            _pageAllocationBoundary = 1;

            _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, false);
            _headBuffer.setFixed();
            
            struc.init(0, 0);
            checkpointMetaData();
            
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
     * Open an existing <code>Volume</code> backing file.
     * 
     * @throws PersistitException
     */
    void open() throws PersistitException {
        if (!exists()) {
            throw new VolumeNotFoundException(_path);
        }
        try {
            _channel = new RandomAccessFile(_path, isReadOnly() ? "r" : "rw").getChannel();
            lockChannel();
            _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, true);
            _headBuffer.setFixed();

            NewVolumeSpecification spec = _volume.getSpecification();
            NewVolumeStatistics stat = _volume.getStatistics();
            NewVolumeStructure struc = _volume.getStructure();

            final byte[] bytes = _headBuffer.getBytes();

            spec.setInitialPages(getInitialPages(bytes));
            spec.setMaximumPages(getMaximumPages(bytes));
            spec.setExtensionPages(getExtensionPages(bytes));

            stat.setCreateTime(getCreateTime(bytes));
            stat.setHighestPageUsed(getHighestPageUsed(bytes));
            stat.setLastExtensionTime(getLastExtensionTime(bytes));
            stat.setLastReadTime(getLastReadTime(bytes));
            stat.setLastWriteTime(getLastWriteTime(bytes));
            stat.setOpenTime(getOpenTime(bytes));

            final long directoryRootPage = getDirectoryRoot(bytes);
            final long garbageRootPage = getGarbageRoot(bytes);
            struc.init(directoryRootPage, garbageRootPage);
            checkpointMetaData();

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
        final File file = new File(_path);
        return file.exists() && file.isFile();
    }

    boolean delete() throws PersistitException {
        final File file = new File(_path);
        return file.exists() && file.isFile() && file.delete();
    }

    void close() throws PersistitException {

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
    }

    long getPageCount() {
        return _pageCount;
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

    void readPage(Buffer buffer, long page) throws PersistitIOException, InvalidPageAddressException,
            VolumeClosedException {
        if (page < 0 || page >= _pageAllocationBoundary) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _pageAllocationBoundary + ")");
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
            // _persistit.getIOMeter().chargeReadPageFromVolume(this,
            // buffer.getPageAddress(), buffer.getBufferSize(),
            // buffer.getIndex());
            _volume.getStatistics().bumpReadCounter();
            _persistit.getLogBase().readOk.log(this, page, buffer.getIndex());
        } catch (IOException ioe) {
            _persistit.getLogBase().readException.log(ioe, this, page, buffer.getIndex());
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }
    }

    void writePage(final ByteBuffer bb, final long page) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException {
        if (page < 0 || page >= _pageAllocationBoundary) {
            throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _pageAllocationBoundary + ")");
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
                if (_pageAllocationBoundary <= _pageCount) {
                    long page = _pageAllocationBoundary ++;
                    _volume.getStatistics().setHighestPageUsed(page);
                    return page;
                }
                extend();
            }
        } finally {
            release();
        }
    }

    void checkpointMetaData() throws PersistitException {
        NewVolumeSpecification spec = _volume.getSpecification();
        NewVolumeStatistics stat = _volume.getStatistics();
        NewVolumeStructure struc = _volume.getStructure();
        
        boolean changed = false;
        final byte[] bytes = _headBuffer.getBytes();
        changed |= changeDirectoryRoot(bytes, struc.getDirectoryRoot());
        changed |= changeGarbageRoot(bytes, struc.getGarbageRoot());
        changed |= changeFetchCounter(bytes, stat.getFetchCounter());
        changed |= changeTraverseCounter(bytes, stat.getTraverseCounter());
        changed |= changeStoreCounter(bytes, stat.getStoreCounter());
        changed |= changeRemoveCounter(bytes, stat.getRemoveCounter());
        changed |= changeReadCounter(bytes, stat.getReadCounter());
        changed |= changeWriteCounter(bytes, stat.getWriteCounter());
        changed |= changeLastExtensionTime(bytes, stat.getLastExtensionTime());
        changed |= changeLastReadTime(bytes, stat.getLastReadTime());
        changed |= changeLastWriteTime(bytes, stat.getLastWriteTime());
        
        if (changed) {
            _headBuffer.setDirty();
        }
        
    }

    private void extend() throws PersistitException {
        if (_pageCount >= _maximumPages || _extensionPages <= 0) {
            throw new VolumeFullException(this + " is full: " + _pageCount + " pages");
        }
        // Do not extend past maximum pages
        long pageCount = Math.min(_pageCount + _extensionPages, _maximumPages);
        resize(pageCount);
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

            _pageCount = pageCount;
            _volume.getStatistics().setLastExtensionTime(System.currentTimeMillis());

        } catch (IOException ioe) {
            _lastIOException = ioe;
            _persistit.getLogBase().extendException.log(ioe, this, currentSize, newSize);
            throw new PersistitIOException(ioe);
        }
    }
}
