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

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;

/**
 * <p>
 * A {@link FileChannel} implementation that provides different semantics on
 * interrupt. This class wraps an actual FileChannelImpl instance obtained from
 * a {@link RandomAccessFile} and delegates all supported operations to that
 * FileChannelImpl. If a blocking I/O operation, e.g., a read or write, is
 * interrupted, the actual FileChannel instance is closed by its
 * {@link AbstractInterruptibleChannel} superclass. The result is that the
 * interrupted thread receives a {@link ClosedByInterruptException}, and other
 * threads concurrently or subsequently reading or writing the same channel
 * receive {@link ClosedChannelException}s.
 * </p>
 * <p>
 * However, this mediator class class catches the
 * <code>ClosedChannelException</code>, and implicitly opens a FileChanel if the
 * client did not actually call {@link #close()}. If the operation that threw
 * the <code>ClosedChannelException</code> was on an interrupted thread then the
 * method that received the exception throws a {@link InterruptedIOException}
 * after re-opening the channel. Otherwise the method retries the operation
 * using the new channel.
 * </p>
 * <p>
 * To maintain the <code>FileChannel</code> contract, methods of this class may
 * only throw an <code>IOException</code>. Therefore, to signify a interrupt,
 * this method throws <code>InteruptedIOException</code> rather than
 * <code>InterruptedException</code>.
 * </p>
 * <p>
 * A number of methods of <code>FileChannel</code> including all methods that
 * depend on the channel's file position, are unsupported and throw
 * {@link UnsupportedOperationException}s.
 * </p>
 * 
 * @author peter
 * 
 */
class MediatedFileChannel extends FileChannel {

    private final static String LOCK_EXTENSION = ".lck";

    final File _file;
    final File _lockFile;
    final String _mode;

    volatile FileChannel _channel;
    volatile FileChannel _lockChannel;

    MediatedFileChannel(final String path, final String mode) throws IOException {
        this(new File(path), mode);
    }

    MediatedFileChannel(final File file, final String mode) throws IOException {
        _file = file;
        _lockFile = new File(file.getParentFile(), file.getName() + LOCK_EXTENSION);
        _mode = mode;
        openChannel();
    }

    /**
     * Handles <code>ClosedChannelException</code> and its subclasses
     * <code>AsynchronousCloseException</code> and
     * <code>ClosedByInterruptException</code>. Empirically we determined (and
     * by reading {@link AbstractInterruptibleChannel}) that an interrupted
     * thread can throw either <code>ClosedChannelException</code> or
     * <code>ClosedByInterruptException</code>. Therefore we simply use the
     * interrupted state of the thread itself to determine whether the Exception
     * occurred due to an interrupt on the current thread.
     * 
     * @param cce
     *            A ClosedChannelException
     * @throws IOException
     *             if (a) the attempt to reopen a new channel fails, or (b) the
     *             current thread was in fact interrupted.
     */
    private void handleClosedChannelException(final ClosedChannelException cce) throws IOException {
        /*
         * The ClosedChannelException may have occurred because the client
         * actually called close. In that event throwing the original exception
         * is correct.
         */
        if (!isOpen()) {
            throw cce;
        }
        /*
         * Open a new inner FileChannel
         */
        openChannel();
        /*
         * Behavior depends on whether this thread was originally the
         * interrupted thread. If so then throw an InterruptedIOException which
         * wraps the original exception. Otherwise return normally so that the
         * while-loops in the methods below can retry the I/O operation using
         * the new FileChannel.
         */
        if (Thread.interrupted()) {
            final InterruptedIOException iioe = new InterruptedIOException();
            iioe.initCause(cce);
            throw iioe;
        }
    }

    /**
     * Attempt to open a real FileChannel. This method is synchronized and
     * checks the status of the existing channel because multiple threads might
     * receive AsynchronousCloseException
     * 
     * @throws IOException
     */
    private synchronized void openChannel() throws IOException {
        if (isOpen() && (_channel == null || !_channel.isOpen())) {
            _channel = new RandomAccessFile(_file, _mode).getChannel();
        }
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
    public void force(final boolean metaData) throws IOException {
        while (true) {
            try {
                _channel.force(metaData);
                break;
            } catch (final ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public int read(final ByteBuffer byteBuffer, final long position) throws IOException {
        final int offset = byteBuffer.position();
        while (true) {
            try {
                return _channel.read(byteBuffer, position);
            } catch (final ClosedChannelException e) {
                handleClosedChannelException(e);
            }
            byteBuffer.position(offset);
        }
    }

    @Override
    public long size() throws IOException {
        while (true) {
            try {
                return _channel.size();
            } catch (final ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public FileChannel truncate(final long size) throws IOException {
        while (true) {
            try {
                return _channel.truncate(size);
            } catch (final ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public synchronized FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
        if (_lockChannel == null) {
            try {
                _lockChannel = new RandomAccessFile(_lockFile, "rw").getChannel();
            } catch (final IOException ioe) {
                if (!shared) {
                    throw ioe;
                } else {
                    /*
                     * Read-only volume, probably failed to create a lock file
                     * due to permissions. We'll assume that means no other
                     * process could be modifying the corresponding volume file.
                     */
                }
            }
        }
        return _lockChannel.tryLock(position, size, shared);
    }

    @Override
    public int write(final ByteBuffer byteBuffer, final long position) throws IOException {
        final int offset = byteBuffer.position();
        while (true) {
            try {
                return _channel.write(byteBuffer, position);
            } catch (final ClosedChannelException e) {
                handleClosedChannelException(e);
            }
            byteBuffer.position(offset);
        }
    }

    /**
     * Implement closing of this <code>MediatedFileChannel</code> by closing the
     * real channel and setting the <code>_reallyClosed</code> flag. The flag
     * prevents another thread from performing an {@link #openChannel()}
     * operation after this thread has closed the channel.
     */
    @Override
    protected synchronized void implCloseChannel() throws IOException {
        try {
            IOException exception = null;
            try {
                if (_lockChannel != null) {
                    _lockFile.delete();
                    _lockChannel.close();
                }
            } catch (final IOException e) {
                exception = e;
            }
            try {
                if (_channel != null) {
                    _channel.close();
                }
            } catch (final IOException e) {
                exception = e;
            }
            if (exception != null) {
                throw exception;
            }
        } catch (final ClosedChannelException e) {
            // ignore - whatever, the channel is closed
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
    public FileLock lock(final long position, final long size, final boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(final MapMode arg0, final long arg1, final long arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel position(final long arg0) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(final ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(final ByteBuffer[] arg0, final int arg1, final int arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(final ReadableByteChannel arg0, final long arg1, final long arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(final long arg0, final long arg1, final WritableByteChannel arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(final ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(final ByteBuffer[] arg0, final int arg1, final int arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    /*
     * --------------------------------
     * 
     * Method and interface intended solely for unit tests.
     * 
     * --------------------------------
     */

    /**
     * Method used by unit tests to rewire the FileChannel delegation. The
     * replacement delegate simulates various IOExceptions. The FileChannel
     * provided as an argument must implement TestChannelInjector so that this
     * method can hook it up properly.
     * 
     * @param channel
     */
    void injectChannelForTests(final FileChannel channel) {
        ((TestChannelInjector) channel).setChannel(_channel);
        _channel = channel;
    }

    /**
     * Interface implemented by an error-injecting FileChannel subclass used in
     * unit tests.
     * 
     * @author peter
     * 
     */
    interface TestChannelInjector {
        void setChannel(FileChannel channel);
    }

}
