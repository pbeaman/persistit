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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.persistit.MediatedFileChannel.TestChannelInjector;

/**
 * <p>
 * A {@link FileChannel} implementation that simulates IOExceptions under
 * control of a unit test program. This class implements only those methods used
 * by Persistit; many methods of FileChannel throw
 * {@link UnsupportedOperationException}.
 * </p>
 * 
 * @author peter
 * 
 */
class ErrorInjectingFileChannel extends FileChannel implements TestChannelInjector {

    volatile FileChannel _channel;

    volatile IOException _injectedIOException;
    volatile String _injectedIOExceptionFlags;
    volatile long _injectedDiskFullLimit = Long.MAX_VALUE;

    public void setChannel(final FileChannel channel) {
        _channel = channel;
    }

    private void injectFailure(final char type) throws IOException {
        final IOException e = _injectedIOException;
        if (e != null && _injectedIOExceptionFlags.indexOf(type) >= 0) {
            throw e;
        }
    }

    /**
     * Set an IOException to be thrown on subsequent I/O operations. This method
     * is intended for use only for unit tests. The <code>flags</code> parameter
     * determines which I/O operations throw exceptions:
     * <ul>
     * <li>o - open</li>
     * <li>c - close</li>
     * <li>r - read</li>
     * <li>w - write</li>
     * <li>f - force</li>
     * <li>t - truncate</li>
     * <li>l - lock</li>
     * <li>e - extending Volume file</li>
     * </ul>
     * For example, if flags is "wt" then write and truncate operations with
     * throw the injected IOException.
     * 
     * @param exception
     *            The IOException to throw
     * @param flags
     *            Selected operations
     */
    void injectTestIOException(final IOException exception, final String flags) {
        _injectedIOException = exception;
        _injectedIOExceptionFlags = flags;
    }

    /**
     * Sets a file position at which writes will simulate a disk-full condition
     * by throwing an IOException.
     * 
     * @param limit
     */
    void injectDiskFullLimit(final long limit) {
        _injectedDiskFullLimit = limit;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        _channel.close();
        injectFailure('c');
    }

    /*
     * --------------------------------
     * 
     * Implementations of these FileChannel methods simply delegate to the inner
     * FileChannel. But they retry upon receiving a ClosedChannelException
     * caused by an I/O operation on a different thread having been interrupted.
     * 
     * --------------------------------
     */
    @Override
    public void force(boolean metaData) throws IOException {
        injectFailure('f');
        _channel.force(metaData);
    }

    @Override
    public int read(ByteBuffer byteBuffer, long position) throws IOException {
        injectFailure('r');
        return _channel.read(byteBuffer, position);
    }

    @Override
    public long size() throws IOException {
        injectFailure('s');
        return _channel.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        injectFailure('t');
        return _channel.truncate(size);
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        injectFailure('l');
        return _channel.tryLock(position, size, shared);
    }

    @Override
    public int write(ByteBuffer byteBuffer, long position) throws IOException {
        injectFailure('w');
        if (byteBuffer.remaining() == 1) {
            injectFailure('e');
        }
        final long capacity = Math.max(0L, _injectedDiskFullLimit - position);
        final int delta = (int)Math.max(0L, (long)byteBuffer.remaining() - capacity);
        byteBuffer.limit(byteBuffer.limit() - delta);
        final int written = _channel.write(byteBuffer, position);
        byteBuffer.limit(byteBuffer.limit() + delta);
        if (delta > 0) {
            throw new IOException("Disk Full");
        } else {
            return written;
        }
    }

    /*
     * --------------------------------
     * 
     * Persistit does not use these methods and so they are Unsupported. Note
     * that it would be difficult to support the relative read/write methods
     * because the channel size is unavailable after it is closed. Therefore a
     * client of this class must maintain its own position counter and cannot
     * use the relative-addressing calls.
     * 
     * --------------------------------
     */
    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode arg0, long arg1, long arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel position(long arg0) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] arg0, int arg1, int arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel arg0, long arg1, long arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long arg0, long arg1, WritableByteChannel arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] arg0, int arg1, int arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

}
