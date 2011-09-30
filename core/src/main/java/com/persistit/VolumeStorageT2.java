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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeClosedException;

/**
 * Manage all details of file I/O for a temporary <code>Volume</code> backing
 * file. A temporary volume is intended to store data that does not need to be
 * durable. All trees in a temporary volume are lost when Persistit shuts down,
 * whether abruptly or gracefully. Therefore pages writes are not logged,
 * buffers are not flushed on shutdown and there is no volume header page to
 * hold metadata.
 * 
 * @author peter
 */
class VolumeStorageT2 extends VolumeStorage {

    private final static String TEMP_FILE_PREFIX = "persistit_tempvol_";

    private volatile String _path;
    private volatile FileChannel _channel;

    private volatile long _nextAvailablePage;
    private volatile boolean _opened;
    private volatile boolean _closed;
    private volatile IOException _lastIOException;

    VolumeStorageT2(final Persistit persistit, final Volume volume) {
        super(persistit, volume);
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    String getPath() {
        return _path;
    }

    /**
     * Returns the last <code>IOException</code> that was encountered while
     * reading, writing, extending or closing the underlying volume file.
     * Returns <code>null</code> if there have been no <code>IOException</code>s
     * since the volume was opened. If <code>reset</code> is <code>true</code>,
     * the lastException field is cleared so that a subsequent call to this
     * method will return <code>null</code> until another
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
     * Indicate whether this <code>Volume</code> prohibits updates.
     * 
     * @return <code>true</code> if this Volume prohibits updates.
     */
    boolean isReadOnly() {
        return false;
    }

    /**
     * Indicate whether this is a temporary volume
     * 
     * @return <code>true</code> if this volume is temporary
     */
    boolean isTemp() {
        return true;
    }

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link Volume}'s volume specification.
     * 
     * @throws PersistitException
     */
    void create() throws PersistitException {
        final String directoryName = _persistit.getProperty(Persistit.TEMPORARY_VOLUME_DIR_NAME, null);
        final File directory = directoryName == null ? null : new File(directoryName);
        try {
            final File file = File.createTempFile(TEMP_FILE_PREFIX, null, directory);
            _path = file.getPath();
            _channel = new RandomAccessFile(file, "rw").getChannel();
            truncate();
            _opened = true;
        } catch (IOException ioe) {
            throw new PersistitIOException(ioe);
        } finally {
            if (!_opened) {
                try {
                    closeChannel();
                } catch (IOException e) {
                    // Not much to do - we're going to try to delete
                    // the file anyway.
                }
                if (getPath() != null) {
                    new File(getPath()).delete();
                }
            }
        }
    }

    /**
     * Open an existing <code>Volume</code> backing file.
     * 
     * @throws PersistitException
     */
    void open() throws PersistitException {
        throw new UnsupportedOperationException("Temporary volume can only be created.");
    }

    /**
     * @return <code>true</code> if a backing file exists on the specified path.
     * @throws PersistitException
     */
    boolean exists() throws PersistitException {
        return false;
    }

    /**
     * Delete the backing file for this Volume if it exists.
     * 
     * @return <code>true</code> if there was a file and it was successfully
     *         deleted
     * @throws PersistitException
     */
    boolean delete() throws PersistitException {
        return false;
    }

    /**
     * Force all file system buffers to disk.
     * 
     * @throws PersistitIOException
     */
    void force() throws PersistitIOException {
        // No need to force a temporary Volume
    }

    /**
     * Close the file resources held by this <code>Volume</code>. After this
     * method is called no further file I/O is possible.
     * 
     * @throws PersistitException
     */
    void close() throws PersistitException {
        synchronized (this) {
            if (_closed) {
                return;
            }
            _closed = true;
        }

        PersistitException pe = null;

        try {
            closeChannel();
        } catch (Exception e) {
            _persistit.getLogBase().exception.log(e);
            // has priority over Exception thrown by
            // releasing file lock.
            pe = new PersistitException(e);
        }
        try {
            if (_path != null) {
                final File file = new File(_path);
                file.delete();
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

    private void closeChannel() throws IOException {
        final FileChannel channel = _channel;
        _channel = null;
        if (channel != null) {
            channel.close();
        }
    }

    void truncate() throws PersistitException {
        VolumeStatistics stat = _volume.getStatistics();
        VolumeStructure struc = _volume.getStructure();

        long now = System.currentTimeMillis();
        stat.setCreateTime(now);
        stat.setOpenTime(now);

        _nextAvailablePage = 1;

        struc.init(0, 0);
        flushMetaData();
    }

    boolean isOpened() {
        return _opened;
    }

    boolean isClosed() {
        return _closed;
    }

    long getExtentedPageCount() {
        return _nextAvailablePage;
    }

    long getNextAvailablePage() {
        return _nextAvailablePage;
    }

    void claimHeadBuffer() throws PersistitException {
    }

    void releaseHeadBuffer() {
    }

    void readPage(Buffer buffer) throws PersistitIOException, InvalidPageAddressException, VolumeClosedException,
            InUseException, PersistitInterruptedException {
        // non-exclusive claim here intended to conflict with exclusive claim in
        // close and truncate
        if (!claim(false, 0)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            final long page = buffer.getPageAddress();
            if (page < 1 || page >= _nextAvailablePage) {
                throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _nextAvailablePage + "]");
            }
            try {
                final ByteBuffer bb = buffer.getByteBuffer();
                bb.position(0).limit(buffer.getBufferSize());
                int read = 0;
                while (read < buffer.getBufferSize()) {
                    long position = (page - 1) * _volume.getStructure().getPageSize() + bb.position();
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
        } finally {
            release();
        }
    }

    @Override
    void writePage(final Buffer buffer) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException, InUseException, PersistitInterruptedException {
        writePage(buffer.getByteBuffer(), buffer.getPageAddress());
    }

    @Override
    void writePage(final ByteBuffer bb, final long page) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException, InUseException, PersistitInterruptedException {
        // non-exclusive claim here intended to conflict with exclusive claim in
        // close and truncate
        int pageSize = _volume.getStructure().getPageSize();
        if (!claim(false, 0)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            if (page < 0 || page >= _nextAvailablePage) {
                throw new InvalidPageAddressException("Page " + page + " out of bounds [0-" + _nextAvailablePage + "]");
            }

            try {
                bb.position(0).limit(pageSize);

                _channel.write(bb, (page - 1) * pageSize);

            } catch (IOException ioe) {
                _persistit.getLogBase().writeException.log(ioe, this, page);
                _lastIOException = ioe;
                throw new PersistitIOException(ioe);
            }
        } finally {
            release();
        }
    }

    long allocNewPage() throws PersistitException {
        long page = _nextAvailablePage++;
        _volume.getStatistics().setNextAvailablePage(page);
        return page;
    }

    void flush() throws PersistitException {

    }

    void flushMetaData() throws PersistitException {

    }

    void extend(final long pageAddr) throws PersistitException {

    }

    boolean updateMetaData(final byte[] bytes) {
        // Temporary volume has no head buffer
        return false;
    }

    @Override
    public String toString() {
        return _volume.toString();
    }
}
