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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.VolumeClosedException;

/**
 * Manage all details of file I/O for a <code>Volume</code> backing file.
 * 
 * @author peter
 */
abstract class VolumeStorage extends SharedResource {

    private final static Random ID_GENERATOR = new Random();
    protected Volume _volume;

    /**
     * Generate a random positive (non-zero) long value to be used as a
     * validation of a Volume's identity.
     * 
     * @return
     */
    protected static long generateId() {
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
    abstract IOException lastException(boolean reset);

    /**
     * Indicate whether this <code>Volume</code> prohibits updates.
     * 
     * @return <code>true</code> if this Volume prohibits updates.
     */
    boolean isReadOnly() {
        return _volume.getSpecification().isReadOnly();
    }

    /**
     * Indicate whether this is a temporary volume
     * 
     * @return <code>true</code> if this volume is temporary
     */
    abstract boolean isTemp();

    /**
     * @return the channel used to read and write pages of this volume.
     * @throws PersistitIOException 
     */
    abstract FileChannel getChannel() throws PersistitIOException;

    /**
     * Create a new <code>Volume</code> backing file according to the
     * {@link Volume}'s volume specification.
     * 
     * @throws PersistitException
     */
    abstract void create() throws PersistitException;

    /**
     * Open an existing <code>Volume</code> backing file.
     * 
     * @throws PersistitException
     */
    abstract void open() throws PersistitException;

    /**
     * @return <code>true</code> if a backing file exists on the specified path.
     * @throws PersistitException
     */
    abstract boolean exists() throws PersistitException;

    /**
     * Delete the backing file for this Volume if it exists.
     * 
     * @return <code>true</code> if there was a file and it was successfully
     *         deleted
     * @throws PersistitException
     */
    abstract boolean delete() throws PersistitException;

    /**
     * Force all file system buffers to disk.
     * 
     * @throws PersistitIOException
     */
    abstract void force() throws PersistitIOException;

    /**
     * Close the file resources held by this <code>Volume</code>. After this
     * method is called no further file I/O is possible.
     * 
     * @throws PersistitException
     */
    abstract void close() throws PersistitException;

    /**
     * Flush metadata to the volume backing store.
     * 
     * @throws PersistitException
     */
    abstract void flush() throws PersistitException;

    abstract void truncate() throws PersistitException;

    abstract boolean isOpened();

    abstract boolean isClosed();

    abstract long getExtentedPageCount();

    abstract long getNextAvailablePage();

    abstract void claimHeadBuffer() throws PersistitException;

    abstract void releaseHeadBuffer();

    abstract void readPage(Buffer buffer) throws PersistitIOException, InvalidPageAddressException,
            VolumeClosedException, InUseException, PersistitInterruptedException;

    abstract void writePage(final Buffer buffer) throws PersistitIOException, InvalidPageAddressException,
            ReadOnlyVolumeException, VolumeClosedException, InUseException, PersistitInterruptedException;

    abstract void writePage(final ByteBuffer bb, final long page) throws PersistitIOException,
            InvalidPageAddressException, ReadOnlyVolumeException, VolumeClosedException, InUseException,
            PersistitInterruptedException;

    abstract long allocNewPage() throws PersistitException;

    abstract void extend(final long pageAddr) throws PersistitException;

    abstract void flushMetaData() throws PersistitException;

    abstract boolean updateMetaData(final byte[] bytes);

}
