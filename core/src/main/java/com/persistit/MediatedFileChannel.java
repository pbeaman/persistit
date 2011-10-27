package com.persistit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousCloseException;
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
 * FileChannel. If a blocking I/O operation, e.g., a read or write, is
 * interrupted, the actual FileChannel instance is closed by its
 * {@link AbstractInterruptibleChannel} superclass. The result is that the
 * interrupted thread receives a {@link ClosedByInterruptException}, any other
 * thread concurrently reading or writing the same channel receives an
 * {@link AsynchronousCloseException}, and subsequently other threads receive
 * {@link ClosedChannelException}s.
 * </p>
 * <p>
 * However, this mediator class class catches the
 * <code>ClosedByInterruptException</code>, and opens a new channel before
 * throwing the exception. Similarly, this class catches the
 * <code>AsynchronousCloseException</code> and
 * <code>ChannelClosedExceptions</code> received by other threads, opens a new
 * channel, and retries those operations.
 * </p>
 * <p>
 * To maintain the <code>FileChannel</code> contract, methods of this class may
 * only throw an <code>IOException</code>. Therefore, to signify a interrupt,
 * this method throws a synthetic Exception defined by the inner class
 * {@link IOInterruptedException}. The caller should catch that exception and
 * instead throw an {@link InterruptedException}.
 * </p>
 * 
 * @author peter
 * 
 */
class MediatedFileChannel extends FileChannel {

    final File _file;
    final String _mode;

    FileChannel _channel;
    FileLock _lock;
    boolean _reallyClosed;

    /**
     * Exception thrown when an IO operation is interrupted. This should be
     * treated like an <code>InterruptedException</code>.
     */
    static class IOInterruptedException extends IOException {

        private static final long serialVersionUID = 1L;

        public IOInterruptedException(final Throwable cause) {
            super(cause);
        }
    }

    MediatedFileChannel(final String path, final String mode) throws IOException {
        this(new File(path), mode);
    }

    MediatedFileChannel(final File file, final String mode) throws IOException {
        _file = file;
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
     * @param e
     *            A ClosedChannelException
     * @throws IOException
     *             if (a) the attempt to reopen a new channel fails, or (b) the
     *             current thread was in fact interrupted.
     */
    private void handleClosedChannelException(final ClosedChannelException e) throws IOException {
        final boolean interrupted = Thread.interrupted();
        //
        // Thread can't be in an interrupted state for this - otherwise it will
        // simply re-throw.
        //
        openChannel();
        if (interrupted) {
            throw new IOInterruptedException(e);
        } else {
            System.out.println("asynch");
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
        if (!_reallyClosed && (_channel == null || !_channel.isOpen())) {
            if (_channel != null) {
                Thread.interrupted();
            }
            _channel = new RandomAccessFile(_file, _mode).getChannel();
            FileLock oldLock = _lock;
            if (oldLock != null) {
                _lock = _channel.tryLock(oldLock.position(), oldLock.size(), oldLock.isShared());
            }
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        while (true) {
            try {
                _channel.force(metaData);
                break;
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }
    
    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        while (true) {
            try {
                return _channel.read(byteBuffer);
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public int read(ByteBuffer byteBuffer, long position) throws IOException {
        while (true) {
            try {
                return _channel.read(byteBuffer, position);
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public long size() throws IOException {
        while (true) {
            try {
                return _channel.size();
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        while (true) {
            try {
                return _channel.truncate(size);
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        while (true) {
            try {
                _lock = _channel.tryLock(position, size, shared);
                return _lock;
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }

    @Override
    public int write(ByteBuffer byteBuffer, long position) throws IOException {
        while (true) {
            try {
                return _channel.write(byteBuffer, position);
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }
    }
    

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        while (true) {
            try {
                return _channel.write(byteBuffer);
            } catch (ClosedChannelException e) {
                handleClosedChannelException(e);
            }
        }    }


    /**
     * Implement closing of this <code>MediatedFileChannel</code> by closing the
     * real channel and setting the <code>_reallyClosed</code> flag. The flag
     * prevents another thread from performing an {@link #openChannel()}
     * operation after this thread has closed the channel.
     */
    @Override
    protected void implCloseChannel() throws IOException {
        synchronized (this) {
            if (_reallyClosed) {
                return;
            }
            _reallyClosed = true;
        }
        try {
            _channel.close();
        } catch (ClosedChannelException e) {
            // ignore - whatever, the channel is closed
        }
    }

    // --------------
    //
    // Persistit does not use these methods.
    //
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
    public long write(ByteBuffer[] arg0, int arg1, int arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

}
