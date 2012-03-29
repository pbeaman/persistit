/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.persistit.AbstractAlertMonitor.Event;
import com.persistit.AbstractAlertMonitor.AlertLevel;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;

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
    private final static String TEMP_FILE_UNCREATED_NAME = "temp_volume_file_not_created_yet";
    private long _maxPages;
    private volatile String _path;
    private volatile FileChannel _channel;

    private volatile long _nextAvailablePage;
    private volatile boolean _opened;
    private volatile boolean _closed;

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
     * @return the channel used to read and write pages of this volume.
     */
    synchronized FileChannel getChannel() throws PersistitIOException {
        if (_channel == null) {
            try {
                final String directoryName = _persistit.getProperty(Persistit.TEMPORARY_VOLUME_DIR_NAME, null);
                final File directory = directoryName == null ? null : new File(directoryName);
                final File file = File.createTempFile(TEMP_FILE_PREFIX, null, directory);
                _path = file.getPath();
                _channel = new MediatedFileChannel(_path, "rw");
            } catch (IOException ioe) {
                _persistit.getLogBase().tempVolumeCreateException.log(ioe, _path);
                throw new PersistitIOException(ioe);
            }
        }
        return _channel;
    }

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link Volume}'s volume specification.
     * 
     * @throws PersistitException
     */
    void create() throws PersistitException {
        final long maxSize = _persistit.getLongProperty(Persistit.TEMPORARY_VOLUME_MAX_SIZE, Long.MAX_VALUE,
                4 * Buffer.MAX_BUFFER_SIZE, Long.MAX_VALUE);
        _maxPages = maxSize / _volume.getStructure().getPageSize();
        _path = TEMP_FILE_UNCREATED_NAME;
        _channel = null;
        truncate();
        _opened = true;
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
        if (!claim(true, 0)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            VolumeStatistics stat = _volume.getStatistics();
            VolumeStructure struc = _volume.getStructure();

            long now = System.currentTimeMillis();
            stat.setCreateTime(now);
            stat.setOpenTime(now);

            _nextAvailablePage = 1;

            struc.init(0, 0);
        } finally {
            release();
        }
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
                    int bytesRead = getChannel().read(bb, position);
                    if (bytesRead <= 0) {
                        throw new PersistitIOException("Unable to read bytes at position " + position + " in " + this);
                    }
                    read += bytesRead;
                }
                _persistit.getIOMeter().chargeReadPageFromVolume(this._volume, buffer.getPageAddress(),
                        buffer.getBufferSize(), buffer.getIndex());
                _volume.getStatistics().bumpReadCounter();
            } catch (IOException ioe) {
                _persistit.getIOAlertMonitor().post(
                        new Event(_persistit.getLogBase().readException, ioe, _volume, page, buffer.getIndex()),
                        IOAlertMonitor.READ_PAGE_CATEGORY, AlertLevel.ERROR);
                throw new PersistitIOException(ioe);
            }
        } finally {
            release();
        }
    }

    @Override
    void writePage(final Buffer buffer) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException, InUseException, PersistitInterruptedException {
        int pageSize = _volume.getStructure().getPageSize();
        final ByteBuffer bb = buffer.getByteBuffer();
        bb.position(0).limit(pageSize);
        writePage(bb, buffer.getPageAddress());
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
                getChannel().write(bb, (page - 1) * pageSize);

            } catch (IOException ioe) {
                _persistit.getIOAlertMonitor().post(
                        new Event(_persistit.getLogBase().writeException, ioe, _volume, page),
                        IOAlertMonitor.WRITE_PAGE_CATEGORY, AlertLevel.ERROR);
                throw new PersistitIOException(ioe);
            }
        } finally {
            release();
        }
    }

    long allocNewPage() throws PersistitException {
        if (_nextAvailablePage >= _maxPages) {
            throw new VolumeFullException(_volume.getName());
        }
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
