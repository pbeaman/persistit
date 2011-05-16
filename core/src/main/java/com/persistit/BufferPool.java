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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.VolumeClosedException;

/**
 * A pool of {@link Buffer} objects, maintained on various lists that permit
 * rapid lookup and replacement of pages images within <code>Buffer</code>s.
 * 
 * @version 2.1
 */
public class BufferPool {
    /**
     * Default PageWriter polling interval
     */
    private final static long DEFAULT_WRITER_POLL_INTERVAL = 2000;

    private final static int MAX_FLUSH_RETRY_COUNT = 10;

    /**
     * Sleep time when buffers are exhausted
     */
    private final static long RETRY_SLEEP_TIME = 50;
    /**
     * The ratio of hash table slots per buffer in this pool
     */
    private final static int HASH_MULTIPLE = 3;
    /**
     * Minimum number of buffers this pool may have
     */
    public final static int MINIMUM_POOL_COUNT = 7;
    /**
     * Maximum number of buffers this pool may have
     */
    public final static int MAXIMUM_POOL_COUNT = Integer.MAX_VALUE;
    /**
     * The maximum number of lock buckets
     */
    private final static int HASH_LOCKS = 4096;

    /**
     * Ratio of FastIndex to buffers
     */
    private final static float FAST_INDEX_RATIO = 0.35f;

    /**
     * The Persistit instance that references this BufferBool.
     */
    private Persistit _persistit;

    /**
     * Hash table - fast access to buffer by hash of address.
     */
    private Buffer[] _hashTable;

    /**
     * Locks used to lock hashtable entries.
     */
    private ReentrantLock[] _hashLocks;

    /**
     * All Buffers in this pool
     */
    private Buffer[] _buffers;
    /**
     * Count of Buffers allocated to this pool.
     */
    private int _bufferCount;

    /**
     * Size of each buffer
     */
    private int _bufferSize;

    /**
     * Count of FastIndex instances, computed as a fraction of buffer count
     */
    private int _fastIndexCount;
    /**
     * FastIndex array
     */
    private FastIndex[] _fastIndexes;

    /**
     * The maximum number of keys allowed Buffers in this pool
     */
    private int _maxKeys;

    /**
     * Pointer to next location to look for a replacement buffer
     */
    private AtomicInteger _clock = new AtomicInteger();

    /**
     * Pointer to next location to look for a dirty buffer
     */
    private AtomicInteger _dirtyClock = new AtomicInteger();

    /**
     * Pointer to next FastIndex to allocate
     */
    private AtomicInteger _fastIndexClock = new AtomicInteger();

    /**
     * Count of buffer pool misses (buffer not found in pool)
     */
    private AtomicLong _missCounter = new AtomicLong();

    /**
     * Count of buffer pool hits (buffer found in pool)
     */
    private AtomicLong _hitCounter = new AtomicLong();

    /**
     * Count of newly created pages
     */
    private AtomicLong _newCounter = new AtomicLong();

    /**
     * Count of valid buffers evicted to make room for another page.
     */
    private AtomicLong _evictCounter = new AtomicLong();

    /**
     * Indicates that Persistit wants to shut down fast, without flushing all
     * the dirty buffers in the buffer pool.
     */
    private AtomicBoolean _fastClose = new AtomicBoolean(false);

    /**
     * Indicates that Persistit has closed this buffer pool.
     */
    private AtomicBoolean _closed = new AtomicBoolean(false);

    /**
     * Polling interval for PageWriter
     */
    private long _writerPollInterval = DEFAULT_WRITER_POLL_INTERVAL;

    private PageWriter _writer;

    private boolean _enableTrace = true; // TODO - turn this off

    /**
     * Construct a BufferPool with the specified count of <code>Buffer</code>s
     * of the specified size.
     * 
     * @param count
     *            The number of buffers in the pool
     * @param size
     *            The size (in bytes) of each buffer
     */
    BufferPool(int count, int size, Persistit persistit) {
        int created = 0;
        _persistit = persistit;
        if (count < MINIMUM_POOL_COUNT) {
            throw new IllegalArgumentException("Buffer pool count too small: "
                    + count);
        }
        if (count > MAXIMUM_POOL_COUNT) {
            throw new IllegalArgumentException("Buffer pool count too large: "
                    + count);
        }

        int possibleSize = Buffer.MIN_BUFFER_SIZE;
        boolean ok = false;
        while (!ok && possibleSize <= Buffer.MAX_BUFFER_SIZE) {
            if (size == possibleSize)
                ok = true;
            else
                possibleSize *= 2;
        }
        if (!ok)
            throw new IllegalArgumentException(
                    "Invalid buffer size requested: " + size);

        _bufferCount = count;
        _bufferSize = size;
        _buffers = new Buffer[_bufferCount];
        _hashTable = new Buffer[_bufferCount * HASH_MULTIPLE];
        _hashLocks = new ReentrantLock[HASH_LOCKS];
        _maxKeys = (_bufferSize - Buffer.HEADER_SIZE) / Buffer.MAX_KEY_RATIO;

        for (int index = 0; index < HASH_LOCKS; index++) {
            _hashLocks[index] = new ReentrantLock();
        }

        try {
            for (int index = 0; index < _bufferCount; index++) {
                Buffer buffer = new Buffer(size, index, this, _persistit);
                _buffers[index] = buffer;
                created++;
            }
        } catch (OutOfMemoryError e) {
            System.err.println("Out of memory after creating " + created
                    + " buffers");
            throw e;
        }
        _fastIndexCount = (int) (count * FAST_INDEX_RATIO);
        _fastIndexes = new FastIndex[_fastIndexCount];
        created = 0;
        try {
            for (int index = 0; index < _fastIndexCount; index++) {
                _fastIndexes[index] = new FastIndex(_maxKeys + 1);
                _fastIndexes[index].setBuffer(_buffers[index]);
                created++;
            }
        } catch (OutOfMemoryError e) {
            System.err.println("Out of memory after creating " + created
                    + " FastIndex instances");
            throw e;
        }
        _writer = new PageWriter();

    }

    void startThreads() {
        _writer.start();
    }

    void close(final boolean flush) {
        _fastClose.set(!flush);
        _closed.set(true);
        _persistit.waitForIOTaskStop(_writer);
    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the writer and collector
     * threads. This method should be used only by tests.
     */
    void crash() {
        IOTaskRunnable.crash(_writer);
    }

    int flush() {
        int unavailable = 0;
        for (int retries = 0; retries < MAX_FLUSH_RETRY_COUNT; retries++) {
            unavailable = writeDirtyBuffers(false, true, Long.MAX_VALUE);
            if (unavailable == 0) {
                break;
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException e) {
                break;
            }
        }
        return unavailable;
    }

    int hashIndex(Volume vol, long page) {
        return (int) ((page ^ vol.getId()) % _hashTable.length);
    }

    int countDirty(Volume vol) {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++) {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer.getVolume() == vol) && buffer.isDirty()
                    && !buffer.isTransient()) {
                count++;
            }
        }
        return count;
    }

    int countInUse(Volume vol, boolean writer) {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++) {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer.getVolume() == vol)
                    && ((buffer._status & SharedResource.CLAIMED_MASK) != 0 && (!writer || (buffer._status & SharedResource.WRITER_MASK) != 0))) {
                count++;
            }
        }
        return count;
    }

    void populateBufferPoolInfo(ManagementImpl.BufferPoolInfo info) {
        info.bufferCount = _bufferCount;
        info.bufferSize = _bufferSize;
        info.missCount = _missCounter.get();
        info.hitCount = _hitCounter.get();
        info.newCount = _newCounter.get();
        info.evictCount = _evictCounter.get();
        int validPages = 0;
        int dirtyPages = 0;
        int readerClaimedPages = 0;
        int writerClaimedPages = 0;
        for (int index = 0; index < _bufferCount; index++) {
            Buffer buffer = _buffers[index];
            int status = buffer.getStatus();
            if ((status & SharedResource.VALID_MASK) != 0)
                validPages++;
            if ((status & SharedResource.DIRTY_MASK) != 0)
                dirtyPages++;
            if ((status & SharedResource.WRITER_MASK) != 0)
                writerClaimedPages++;
            else if ((status & SharedResource.CLAIMED_MASK) != 0)
                readerClaimedPages++;
        }
        info.validPageCount = validPages;
        info.dirtyPageCount = dirtyPages;
        info.readerClaimedPageCount = readerClaimedPages;
        info.writerClaimedPageCount = writerClaimedPages;
        info.updateAcquisitonTime();
    }

    int populateInfo(ManagementImpl.BufferInfo[] array, int traveralType,
            int includeMask, int excludeMask) {
        int index = 0;
        switch (traveralType) {
        case 0:
            for (int i = 0; i < _bufferCount; i++) {
                Buffer buffer = _buffers[i];
                if (selected(buffer, includeMask, excludeMask)) {
                    populateInfo1(array, index, buffer);
                    index++;
                }
            }
            break;

        default:
            index = -1;
            break;
        }
        return index;
    }

    private static void populateInfo1(ManagementImpl.BufferInfo[] array,
            int index, Buffer buffer) {
        if (index < array.length) {
            if (array[index] == null)
                array[index] = new ManagementImpl.BufferInfo();
            buffer.populateInfo(array[index]);
        }
    }

    private boolean selected(Buffer buffer, int includeMask, int excludeMask) {
        return ((includeMask == 0) || (buffer._status & includeMask) != 0)
                && (buffer._status & excludeMask) == 0;
    }

    /**
     * @return Size (in bytes) of each <code>Buffer</code> managed by this pool.
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /**
     * @return The count of <code>Buffer</code>s managed by this pool.
     */
    public int getBufferCount() {
        return _bufferCount;
    }

    /**
     * @return The count of lookup operations for pages images in this pool that
     *         required a physical read operation. This number, in comparison
     *         with the hit counter, indicates how effective the cache is in
     *         reducing disk I/O.
     */
    public long getMissCounter() {
        return _missCounter.get();
    }

    /**
     * @return The count of lookup operations for pages images in this pool for
     *         which the page image was already found in this
     *         <code>BufferPool</code>. This number, in comparison with the get
     *         counter, indicates how effective the cache is in reducing disk
     *         I/O.
     */
    public long getHitCounter() {
        return _hitCounter.get();
    }

    /**
     * 
     * @return The count of buffers newly created in this pool. Each time a new
     *         page is added to a Volume, this counter is incremented.
     */
    public long getNewCounter() {
        return _newCounter.get();
    }

    /**
     * Resets the get and hit counters to zero.
     */
    public void resetCounters() {
        _missCounter.set(0);
        _hitCounter.set(0);
        _newCounter.set(0);
        _evictCounter.set(0);
    }

    int getMaxKeys() {
        return _maxKeys;
    }

    private void bumpHitCounter() {
        _hitCounter.incrementAndGet();
    }

    private void bumpMissCounter() {
        _missCounter.incrementAndGet();
    }

    private void bumpNewCounter() {
        _newCounter.incrementAndGet();
    }

    /**
     * Get the "hit ratio" - the number of hits divided by the number of overall
     * gets. A value close to 1.0 indicates that most attempts to find data in
     * the <code>BufferPool</code> are successful - i.e., that the cache is
     * effectively reducing the need for disk read operations.
     * 
     * @return The ratio
     */
    public double getHitRatio() {
        final long hitCounter = _hitCounter.get();
        final long getCounter = hitCounter + _missCounter.get()
                + _newCounter.get();
        if (getCounter == 0)
            return 0.0;
        else
            return ((double) hitCounter) / ((double) getCounter);
    }

    /**
     * Invalidate all buffers from a specified Volume.
     * 
     * @param volume
     *            The volume
     */
    void invalidate(Volume volume) {
        for (int index = 0; index < _buffers.length; index++) {
            Buffer buffer = _buffers[index];
            if ((buffer.getVolume() == volume || volume == null)
                    && !buffer.isFixed() && buffer.isValid()) {
                invalidate(buffer);
            }
        }
    }

    void delete(Volume volume) {
        for (int index = 0; index < _buffers.length; index++) {
            Buffer buffer = _buffers[index];
            if (buffer.getVolume() == volume) {
                delete(buffer);
            }
        }
    }

    private void invalidate(Buffer buffer) {
        if (Debug.ENABLED) {
            Debug.$assert(buffer.isValid());
        }
        buffer._status &= (~SharedResource.VALID_MASK & ~SharedResource.DIRTY_MASK);
        buffer.setPageAddressAndVolume(0, null);
    }

    private void delete(Buffer buffer) {
        buffer._status |= SharedResource.DELETE_MASK;
        buffer.setPageAddressAndVolume(0, null);
    }

    private void detach(Buffer buffer) {
        final int hash = hashIndex(buffer.getVolume(), buffer.getPageAddress());
        _hashLocks[hash % HASH_LOCKS].lock();
        try {
            //
            // If already invalid, we're done.
            //
            if ((buffer._status & SharedResource.VALID_MASK) == 0) {
                return;
            }
            //
            // Detach this buffer from the hash table.
            //
            if (_hashTable[hash] == buffer) {
                _hashTable[hash] = buffer.getNext();
            } else {
                Buffer prev = _hashTable[hash];
                for (Buffer next = prev.getNext(); next != null; next = prev
                        .getNext()) {
                    if (next == buffer) {
                        prev.setNext(next.getNext());
                        break;
                    }
                    prev = next;
                }
            }
        } finally {
            _hashLocks[hash % HASH_LOCKS].unlock();
        }
    }

    /**
     * Removes a Buffer from the LRU list and inserts it at the
     * most-recently-used end.
     * 
     * @param nub
     *            The Buffer to move
     */
    private void makeMostRecentlyUsed(Buffer buffer) {
        buffer.setBit(1);
    }

    /**
     * Find or load a page given its Volume and address. The returned page has a
     * reader or a writer lock, depending on whether the writer parameter is
     * true on entry.
     * 
     * @param vol
     *            The Volume
     * @param page
     *            The address of the page
     * @param writer
     *            <i>true</i> if a write lock is required.
     * @param wantRead
     *            <i>true</i> if the caller wants the page read from disk.
     *            <i>false</i> to allocate a new blank page.)
     * @return Buffer The Buffer describing the buffer containing the page.
     * @throws InUseException
     */
    Buffer get(Volume vol, long page, boolean writer, boolean wantRead)
            throws PersistitException {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;

        for (;;) {
            boolean mustRead = false;
            boolean mustClaim = false;
            _hashLocks[hash % HASH_LOCKS].lock();
            try {
                buffer = _hashTable[hash];
                //
                // Search for the page
                //
                while (buffer != null) {
                    if (Debug.ENABLED)
                        Debug.$assert(buffer.getNext() != buffer);

                    if (buffer.getPageAddress() == page
                            && buffer.getVolume() == vol) {
                        //
                        // Found it - now claim it.
                        //
                        if (buffer.checkedClaim(writer, 0)) {
                            vol.bumpGetCounter();
                            bumpHitCounter();
                            return buffer; // trace(buffer);
                        } else {
                            mustClaim = true;
                            break;
                        }
                    }
                    buffer = buffer.getNext();
                }

                if (buffer == null) {
                    //
                    // Page not found. Allocate an available buffer and read
                    // in the page from the Volume.
                    //
                    buffer = allocBuffer();
                    //
                    // buffer may be null if allocBuffer found no available
                    // clean buffers. In that case we need to back off and
                    // get some buffers written.
                    //
                    if (buffer != null) {

                        if (Debug.ENABLED) {
                            Debug.$assert(buffer != _hashTable[hash]);
                            Debug.$assert(buffer.getNext() != buffer);
                        }

                        buffer.setPageAddressAndVolume(page, vol);
                        buffer.setNext(_hashTable[hash]);
                        _hashTable[hash] = buffer;
                        //
                        // It's not really valid yet, but it does have a writer
                        // claim on it so no other Thread can access it. In the
                        // meantime, any other Thread seeking access to the same
                        // page will find it.
                        //
                        buffer.setValid(true);
                        buffer.setTransient(vol.isTransient());

                        if (Debug.ENABLED) {
                            Debug.$assert(buffer.getNext() != buffer);
                        }
                        mustRead = true;
                    }
                }
            } finally {
                _hashLocks[hash % HASH_LOCKS].unlock();
            }
            if (buffer == null) {
                _writer.urgent();
                synchronized (this) {
                    try {
                        wait(RETRY_SLEEP_TIME);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            } else {
                if (Debug.ENABLED)
                    Debug.$assert(!mustClaim || !mustRead);

                if (mustClaim) {
                    if (!buffer.claim(writer)) {
                        throw new InUseException("Thread "
                                + Thread.currentThread().getName()
                                + " failed to acquire "
                                + (writer ? "writer" : "reader") + " claim on "
                                + buffer);
                    }

                    //
                    // Test whether the buffer we picked out is still valid
                    //
                    if (buffer.isValid() && buffer.getPageAddress() == page
                            && buffer.getVolume() == vol) {
                        //
                        // If so, then we're done.
                        //
                        vol.bumpGetCounter();
                        bumpHitCounter();
                        return buffer; // trace(buffer);
                    }
                    //
                    // If not, release the claim and retry.
                    //
                    buffer.release();
                    continue;
                }
                if (mustRead) {
                    // We're here because the required page was not found
                    // in the pool so we have to read it from the Volume.
                    // We have a writer claim on the buffer, so anyone
                    // else attempting to get this page will simply wait
                    // for us to finish reading it.
                    //
                    // At this point, the Buffer has been fully set up. It is
                    // on the hash table chain under its new page address,
                    // it is marked valid, and this Thread has a writer claim.
                    // If the read attempt fails, we need to mark the page
                    // INVALID so that any Thread waiting for access to
                    // this buffer will not use it. We also need to demote
                    // the writer claim to a reader claim unless the caller
                    // originally asked for a writer claim.
                    //
                    if (wantRead) {
                        boolean loaded = false;
                        try {
                            if (Debug.ENABLED) {
                                Debug.$assert(buffer.getPageAddress() == page
                                        && buffer.getVolume() == vol
                                        && hashIndex(buffer.getVolume(),
                                                buffer.getPageAddress()) == hash);
                            }
                            buffer.load(vol, page);
                            loaded = true;
                            vol.bumpGetCounter();
                            bumpMissCounter();
                        } finally {
                            if (!loaded) {
                                invalidate(buffer);
                                buffer.release();
                            }
                        }
                    } else {
                        buffer.clear();
                        buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
                        bumpNewCounter();
                    }
                    if (!writer) {
                        buffer.releaseWriterClaim();
                    }
                    return buffer; // trace(buffer);
                }
            }
        }
    }

    /**
     * Returns a copy of Buffer. The returned buffer is newly created, is not a
     * member of the buffer pool, and is not claimed. There is no guarantee that
     * the content of this copy is internally consistent because another thread
     * may be modifying the buffer while the copy is being made. The returned
     * Buffer should be used only for display and diagnostic purposes.
     * 
     * @param vol
     * @param page
     * @return Copy of the Buffer
     * @throws InvalidPageAddressException
     * @throws InvalidPageStructureException
     * @throws VolumeClosedException
     * @throws RetryException
     * @throws IOException
     */
    public Buffer getBufferCopy(Volume vol, long page)
            throws InvalidPageAddressException, InvalidPageStructureException,
            VolumeClosedException, TimeoutException, PersistitIOException {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;
        _hashLocks[hash % HASH_LOCKS].lock();
        try {
            buffer = _hashTable[hash];
            //
            // Search for the page
            //
            while (buffer != null) {
                if (Debug.ENABLED)
                    Debug.$assert(buffer.getNext() != buffer);

                if (buffer.getPageAddress() == page
                        && buffer.getVolume() == vol) {
                    if (Debug.ENABLED)
                        Debug.$assert((buffer._status & SharedResource.VALID_MASK) != 0);
                    //
                    // Found it - now return a copy of it.
                    //
                    return new Buffer(buffer);
                }
                buffer = buffer.getNext();
            }
        } finally {
            _hashLocks[hash % HASH_LOCKS].unlock();
        }
        //
        // Didn't find it in the pool, so we'll read a copy.
        //
        buffer = new Buffer(_bufferSize, -1, this, _persistit);
        buffer.load(vol, page);
        return buffer;
    }

    void release(Buffer buffer) {
        if ((buffer._status & SharedResource.VALID_MASK) != 0
                && (buffer._status & SharedResource.FIXED_MASK) == 0) {
            makeMostRecentlyUsed(buffer);
        }
        buffer.release();
    }

    void setFixed(final Buffer buffer, boolean fixed) {
        buffer.setFixed(fixed);
    }

    /**
     * Returns an available buffer. The replacement policy is to return a buffer
     * that's already been marked invalid, if available. Otherwise traverse the
     * least-recently-used queue to find the least- recently-used buffer that is
     * not claimed. Throws a RetryException if there is no available buffer.
     * 
     * @return Buffer An available buffer, or <i>null</i> if no buffer is
     *         currently available. The buffer has a writer claim.
     * @throws InvalidPageStructureException
     */
    private Buffer allocBuffer() throws PersistitException {

        for (int retry = 0; retry < _bufferCount;) {
            int clock = _clock.get();
            if (!_clock.compareAndSet(clock, (clock + 1) % _bufferCount)) {
                continue;
            }
            boolean resetDirtyClock = false;
            Buffer buffer = _buffers[clock % _bufferCount];
            if (buffer.testBit(1)) {
                buffer.clearBit(1);
            } else {
                if (!buffer.isFixed()
                        && (buffer.getStatus() & SharedResource.CLAIMED_MASK) == 0
                        && buffer.checkedClaim(true, 0)) {
                    if (buffer.isDirty()) {
                        if (!resetDirtyClock) {
                            resetDirtyClock = true;
                            _dirtyClock.set(clock);
                            _writer.urgent();
                        }
                        buffer.release();
                    } else {
                        if (buffer.isValid()) {
                            detach(buffer);
                            _evictCounter.incrementAndGet();
                            _persistit.getIOMeter().chargeEvictPageFromPool(
                                    buffer.getVolume(),
                                    buffer.getPageAddress(),
                                    buffer.getBufferSize(), buffer.getIndex());
                        }
                        return buffer;
                    }
                }
            }
            retry++;
        }
        return null;
    }

    FastIndex allocFastIndex() {
        for (int retry = 0; retry < _fastIndexCount; retry++) {
            int clock = _fastIndexClock.get();
            if (!_fastIndexClock.compareAndSet(clock, (clock + 1)
                    % _fastIndexCount)) {
                continue;
            }
            FastIndex findex = _fastIndexes[clock];
            if (findex.testBit(1)) {
                findex.clearBit(1);
            } else {
                Buffer buffer = findex.getBuffer();
                synchronized (buffer._lock) {
                    if ((buffer.getStatus() & buffer.CLAIMED_MASK) == 0) {
                        buffer.takeFastIndex();
                        findex.invalidate();
                        return findex;
                    }
                }
            }
        }
        throw new IllegalStateException("No FastIndex buffers available");
    }

    enum Result {
        WRITTEN, UNAVAILABLE, ERROR
    };

    private boolean needToWrite(final Buffer buffer, final int count,
            final int max) {
        if (buffer.isTransient()) {
            return false;
        }
        if (buffer.getTimestamp() > _persistit.getCurrentCheckpoint()
                .getTimestamp()) {
            return false;
        }
        if (buffer.isDataPage() && !buffer.isStructure()
                && buffer.getVolume().isLoose()) {
            return false;
        }
        return count < max;
    }

    long earliestDirtyTimestamp() {
        long earliestDirtyTimestamp = Long.MAX_VALUE;
        for (int index = 0; index < _buffers.length; index++) {
            final Buffer buffer = _buffers[index];
            synchronized (buffer._lock) {
                if (buffer.isDirty() && !buffer.isTransient()) {
                    long timestamp = buffer.getTimestamp();
                    if (timestamp < earliestDirtyTimestamp) {
                        earliestDirtyTimestamp = timestamp;
                    }
                }
            }
        }
        return earliestDirtyTimestamp;
    }

    private int writeDirtyBuffers(final boolean urgent, final boolean all,
            final long timestamp) {
        int unavailable = 0;
        int start = _dirtyClock.get();
        int max = all ? _bufferCount : _bufferCount / 8;
        final int end = all ? start + _bufferCount : start + (_bufferCount / 8);
        int index = start;
        for (; index < end; index++) {
            final Buffer buffer = _buffers[index % _bufferCount];
            if (buffer.isDirty() && buffer.getTimestamp() < timestamp) {
                if (buffer.claim(true, 0)) {
                    try {
                        if (buffer.isValid() && buffer.isDirty()) {
                            buffer.writePage();
                            if (--max <= 0) {
                                index++;
                                break;
                            }
                        } else {
                            buffer.setClean();
                        }
                    } catch (Exception e) {
                        final Volume volume = buffer.getVolume();
                        final long page = buffer.getPageAddress();
                        _persistit.getLogBase().log(
                                LogBase.LOG_EXCEPTION,
                                e + " while writing page " + page
                                        + " in volume " + volume);
                    } finally {
                        buffer.release();
                        if (urgent) {
                            synchronized (BufferPool.this) {
                                notify();
                            }
                        }
                    }
                } else {
                    unavailable++;
                }
            }
        }
        _dirtyClock.compareAndSet(start, index % _bufferCount);
        return unavailable;
    }

    private class PageWriter extends IOTaskRunnable {
        private boolean _clean;
        private boolean _wasClosed;

        PageWriter() {
            super(BufferPool.this._persistit);
        }

        void start() {
            start("PAGE_WRITER:" + _bufferSize, _writerPollInterval);
        }

        public void runTask() {
            _wasClosed = _closed.get();
            _clean = writeDirtyBuffers(_urgent.get(), _wasClosed,
                    Long.MAX_VALUE) == 0;
            if (_clean) {
                _urgent.set(false);
            }
        }

        protected boolean shouldStop() {
            return _fastClose.get() || _clean && _wasClosed;
        }

        protected long pollInterval() {
            return _wasClosed || _urgent.get() ? 0 : _writerPollInterval;
        }

        @Override
        protected void urgent() {
            super.urgent();
        }
    }

    private Buffer trace(final Buffer buffer) {
        if (_enableTrace) {
            _persistit.getIOMeter().chargeGetPage(buffer.getVolume(),
                    buffer.getPageAddress(), buffer.getBufferSize(),
                    buffer.getIndex());
        }
        return buffer;
    }

    public String toString() {
        return "BufferPool[" + _bufferCount + "@" + _bufferSize
                + (_closed.get() ? ":closed" : "") + "]";
    }
}
