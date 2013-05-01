/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

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
class TrackingFileChannel extends FileChannel implements TestChannelInjector {

    volatile FileChannel _channel;

    final List<Long> _writePositions = new ArrayList<Long>();

    final List<Long> _readPositions = new ArrayList<Long>();

    @Override
    public void setChannel(final FileChannel channel) {
        _channel = channel;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        _channel.close();
    }

    @Override
    public void force(final boolean metaData) throws IOException {
        _channel.force(metaData);
    }

    @Override
    public int read(final ByteBuffer byteBuffer, final long position) throws IOException {
        _readPositions.add(position);
        return _channel.read(byteBuffer, position);
    }

    @Override
    public long size() throws IOException {
        return _channel.size();
    }

    @Override
    public FileChannel truncate(final long size) throws IOException {
        return _channel.truncate(size);
    }

    @Override
    public synchronized FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
        return _channel.tryLock(position, size, shared);
    }

    @Override
    public int write(final ByteBuffer byteBuffer, final long position) throws IOException {
        _writePositions.add(position);
        final int written = _channel.write(byteBuffer, position);
        return written;
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

    public List<Long> getWritePositionList() {
        return _writePositions;
    }

    public List<Long> getReadPositionList() {
        return _readPositions;
    }

    public void assertOrdered(final boolean read, final boolean forward) {
        final List<Long> list = read ? _readPositions : _writePositions;
        final long previous = forward ? -1 : Long.MAX_VALUE;
        for (final Long position : list) {
            if (forward) {
                assertTrue("Position should be larger", position > previous);
            } else {
                assertTrue("Position should be smaller", position < previous);
            }
        }
    }
}
