/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RetryException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * A pool of {@link Buffer} objects, maintained on various lists that permit
 * rapid lookup and replacement of pages images within <code>Buffer</code>s.
 * 
 * @author peter
 * @version 2.1
 */
public class BufferPool {
    /**
     * Default PageWriter polling interval
     */
    private final static long DEFAULT_WRITER_POLL_INTERVAL = 5000;

    private final static int PAGE_WRITER_TRANCHE_SIZE = 5000;

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
     * Ratio determines which of two volume invalidation algorithms to invoke.
     */
    private final static float SMALL_VOLUME_RATIO = 0.1f;

    /**
     * Ratio of age-based write priority bump
     */
    private final static int WRITE_AGE_THRESHOLD_RATIO = 4;

    /**
     * The Persistit instance that references this BufferPool.
     */
    private final Persistit _persistit;

    /**
     * Hash table - fast access to buffer by hash of address.
     */
    private final Buffer[] _hashTable;

    /**
     * Locks used to lock hashtable entries.
     */
    private final ReentrantLock[] _hashLocks;

    /**
     * All Buffers in this pool
     */
    private final Buffer[] _buffers;
    /**
     * Count of Buffers allocated to this pool.
     */
    private final int _bufferCount;

    /**
     * Size of each buffer
     */
    private final int _bufferSize;

    /**
     * Bit map for invalidated pages. Elements in this array, one bit per page,
     * indicate buffers that have been invalidated and are therefore able to be
     * allocated without evicting a valid page.
     */
    private final AtomicLongArray _availablePagesBits;

    private final AtomicBoolean _availablePages = new AtomicBoolean();

    /**
     * The maximum number of keys allowed Buffers in this pool
     */
    private final int _maxKeys;

    /**
     * Pointer to next location to look for a replacement buffer
     */
    private final AtomicInteger _clock = new AtomicInteger();

    /**
     * Count of buffer pool misses (buffer not found in pool)
     */
    private final AtomicLong _missCounter = new AtomicLong();

    /**
     * Count of buffer pool hits (buffer found in pool)
     */
    private final AtomicLong _hitCounter = new AtomicLong();

    /**
     * Count of newly created pages
     */
    private AtomicLong _newCounter = new AtomicLong();

    /**
     * Count of valid buffers evicted to make room for another page.
     */
    private final AtomicLong _evictCounter = new AtomicLong();

    /**
     * Count of dirty pages
     */
    private final AtomicInteger _dirtyPageCount = new AtomicInteger();

    /**
     * Count of pages written from this pool
     */
    private final AtomicLong _writeCounter = new AtomicLong();
    /**
     * Count of pages written due to being dirty when selected by the buffer
     * allocator.
     */
    private final AtomicLong _forcedWriteCounter = new AtomicLong();

    /**
     * (with n Count of pages written due to being dirty before a checkpoint
     */
    private final AtomicLong _forcedCheckpointWriteCounter = new AtomicLong();
    /**
     * Indicates that Persistit has closed this buffer pool.
     */
    private final AtomicBoolean _closed = new AtomicBoolean(false);

    /**
     * Oldest update timestamp found during PAGE_WRITER's most recent scan.
     */
    private volatile long _earliestDirtyTimestamp = Long.MIN_VALUE;

    /**
     * Timestamp to which all dirty pages should be written. PAGE_WRITER writes
     * any page with a lower update timestamp regardless of urgency.
     */
    private AtomicLong _flushTimestamp = new AtomicLong();

    /**
     * Polling interval for PageWriter
     */
    private volatile long _writerPollInterval = DEFAULT_WRITER_POLL_INTERVAL;

    private volatile int _pageWriterTrancheSize = PAGE_WRITER_TRANCHE_SIZE;
    /**
     * The PAGE_WRITER IOTaskRunnable
     */
    private PageWriter _writer;

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
        _persistit = persistit;
        if (count < MINIMUM_POOL_COUNT) {
            throw new IllegalArgumentException("Buffer pool count too small: " + count);
        }
        if (count > MAXIMUM_POOL_COUNT) {
            throw new IllegalArgumentException("Buffer pool count too large: " + count);
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
            throw new IllegalArgumentException("Invalid buffer size requested: " + size);

        _bufferCount = count;
        _bufferSize = size;
        _buffers = new Buffer[_bufferCount];
        _availablePagesBits = new AtomicLongArray((count + 63) / 64);
        _hashTable = new Buffer[_bufferCount * HASH_MULTIPLE];
        _hashLocks = new ReentrantLock[HASH_LOCKS];
        _maxKeys = (_bufferSize - Buffer.HEADER_SIZE) / Buffer.MAX_KEY_RATIO;

        for (int index = 0; index < HASH_LOCKS; index++) {
            _hashLocks[index] = new ReentrantLock();
        }

        int buffers = 0;
        //
        // Allocate this here so that in the event of an OOME we can release it
        // to free enough memory to write the error information out.
        //
        byte[] reserve = new byte[1024 * 1024];
        try {
            for (int index = 0; index < _bufferCount; index++) {
                Buffer buffer = new Buffer(size, index, this, _persistit);
                _buffers[index] = buffer;
                buffers++;
            }
        } catch (OutOfMemoryError e) {
            //
            // Note: written this way to try to avoid another OOME.
            // Do not use String.format here.
            //
            reserve = null;
            System.err.print("Out of memory with ");
            System.err.print(Runtime.getRuntime().freeMemory());
            System.err.print(" bytes free after creating ");
            System.err.print(buffers);
            System.err.print("/");
            System.err.print(_bufferCount);
            System.err.print(" buffers from maximum heap ");
            System.err.println(_persistit.getAvailableHeap());
            throw e;
        }
        _writer = new PageWriter();

    }

    void startThreads() {
        _writer.start();
    }

    void close() {
        _closed.set(true);
        _persistit.waitForIOTaskStop(_writer);
        _writer = null;
    }

    /**
     * Abruptly stop (using {@link Thread#stop()}) the writer and collector
     * threads. This method should be used only by tests.
     */
    void crash() {
        IOTaskRunnable.crash(_writer);
    }

    void flush(final long timestamp) throws PersistitInterruptedException {
        setFlushTimestamp(timestamp);
        _writer.kick();
        while (isFlushing()) {
            Util.sleep(RETRY_SLEEP_TIME);
        }
    }

    boolean isFlushing() {
        return _flushTimestamp.get() != 0;
    }

    int hashIndex(Volume vol, long page) {
        return (int) (((page ^ vol.hashCode()) & Integer.MAX_VALUE) % _hashTable.length);
    }

    int countInUse(Volume vol, boolean writer) {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++) {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer.getVolume() == vol)
                    && ((buffer.getStatus() & SharedResource.CLAIMED_MASK) != 0 && (!writer || (buffer.getStatus() & SharedResource.WRITER_MASK) != 0))) {
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
        info.dirtyPageCount = _dirtyPageCount.get();
        info.writeCount = _writeCounter.get();
        info.forcedCheckpointWriteCount = _forcedCheckpointWriteCounter.get();
        info.forcedWriteCount = _forcedWriteCounter.get();
        int validPages = 0;
        int readerClaimedPages = 0;
        int writerClaimedPages = 0;

        for (int index = 0; index < _bufferCount; index++) {
            Buffer buffer = _buffers[index];
            int status = buffer.getStatus();
            if ((status & SharedResource.VALID_MASK) != 0)
                validPages++;
            if ((status & SharedResource.WRITER_MASK) != 0)
                writerClaimedPages++;
            else if ((status & SharedResource.CLAIMED_MASK) != 0)
                readerClaimedPages++;
        }
        info.validPageCount = validPages;
        info.readerClaimedPageCount = readerClaimedPages;
        info.writerClaimedPageCount = writerClaimedPages;
        info.earliestDirtyTimestamp = getEarliestDirtyTimestamp();

        info.updateAcquisitonTime();
    }

    int populateInfo(ManagementImpl.BufferInfo[] array, int traveralType, int includeMask, int excludeMask) {
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

    private static void populateInfo1(ManagementImpl.BufferInfo[] array, int index, Buffer buffer) {
        if (index < array.length) {
            if (array[index] == null)
                array[index] = new ManagementImpl.BufferInfo();
            buffer.populateInfo(array[index]);
        }
    }

    private boolean selected(Buffer buffer, int includeMask, int excludeMask) {
        return ((includeMask == 0) || (buffer.getStatus() & includeMask) != 0)
                && (buffer.getStatus() & excludeMask) == 0;
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
     * @return The count of buffers newly created in this pool. Each time a new
     *         page is added to a Volume, this counter is incremented.
     */
    public long getNewCounter() {
        return _newCounter.get();
    }

    /**
     * This counter is incremented ach time the eviction algorithm selects a
     * dirty buffer to evict. Normally dirty pages are written by the background
     * PAGE_WRITER thread, and therefore an abnormally large forcedWrite count
     * indicates the PAGE_WRITER thread is falling behind.
     * 
     * @return The count of buffers written to disk when evicted.
     */
    public long getForcedWriteCounter() {
        return _forcedWriteCounter.get();
    }

    /**
     * This counter is incremented each time a application modifies a buffer
     * that is (a) dirty, and (b) required to be written as part of a
     * checkpoint. An abnormally large count indicates that the PAGE_WRITER
     * thread is falling behind.
     * 
     * @return The count of buffers written to disk due to a checkpoint.
     */
    public long getForcedCheckpointWriteCounter() {
        return _forcedCheckpointWriteCounter.get();
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

    void bumpWriteCounter() {
        _writeCounter.incrementAndGet();
    }

    void bumpForcedCheckpointWrites() {
        _forcedCheckpointWriteCounter.incrementAndGet();
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
        final long getCounter = hitCounter + _missCounter.get() + _newCounter.get();
        if (getCounter == 0)
            return 0.0;
        else
            return ((double) hitCounter) / ((double) getCounter);
    }

    void incrementDirtyPageCount() {
        _dirtyPageCount.incrementAndGet();
    }

    void decrementDirtyPageCount() {
        _dirtyPageCount.decrementAndGet();
    }

    int getDirtyPageCount() {
        return _dirtyPageCount.get();
    }

    /**
     * Invalidate all buffers from a specified Volume.
     * 
     * @param volume
     *            The volume
     * @throws PersistitInterruptedException
     */
    boolean invalidate(Volume volume) throws PersistitInterruptedException {
        final float ratio = (float) volume.getStorage().getNextAvailablePage() / (float) _bufferCount;
        if (ratio < SMALL_VOLUME_RATIO) {
            return invalidateSmallVolume(volume);
        } else {
            return invalidateLargeVolume(volume);
        }
    }

    boolean invalidateSmallVolume(final Volume volume) throws PersistitInterruptedException {
        boolean result = true;
        int markedAvailable = 0;
        for (long page = 1; page < volume.getStorage().getNextAvailablePage(); page++) {
            int hashIndex = hashIndex(volume, page);
            _hashLocks[hashIndex % HASH_LOCKS].lock();
            try {
                for (Buffer buffer = _hashTable[hashIndex]; buffer != null; buffer = buffer.getNext()) {
                    if ((buffer.getVolume() == volume || volume == null) && !buffer.isFixed() && buffer.isValid()) {
                        if (buffer.claim(true, 0)) {
                            // re-check after claim
                            boolean invalidated = false;
                            try {
                                if ((buffer.getVolume() == volume || volume == null) && !buffer.isFixed()
                                        && buffer.isValid()) {
                                    invalidate(buffer);
                                    invalidated = true;
                                }
                            } finally {
                                buffer.release();
                            }
                            if (invalidated) {
                                int q = buffer.getIndex() / 64;
                                int p = buffer.getIndex() % 64;
                                long bits = _availablePagesBits.get(q);
                                if (_availablePagesBits.compareAndSet(q, bits, bits | (1L << p))) {
                                    markedAvailable++;
                                }
                            }
                        } else {
                            result = false;
                        }
                    }
                }
            } finally {
                _hashLocks[hashIndex % HASH_LOCKS].unlock();
            }
        }
        if (markedAvailable > 0) {
            _availablePages.set(true);
        }
        return result;

    }

    boolean invalidateLargeVolume(final Volume volume) throws PersistitInterruptedException {
        boolean result = true;
        int markedAvailable = 0;
        for (int index = 0; index < _bufferCount; index++) {
            Buffer buffer = _buffers[index];
            if ((buffer.getVolume() == volume || volume == null) && !buffer.isFixed() && buffer.isValid()) {
                if (buffer.claim(true, 0)) {
                    // re-check after claim
                    boolean invalidated = false;
                    try {
                        if ((buffer.getVolume() == volume || volume == null) && !buffer.isFixed() && buffer.isValid()) {
                            invalidate(buffer);
                            invalidated = true;
                        }
                    } finally {
                        buffer.release();
                    }
                    if (invalidated) {
                        int q = buffer.getIndex() / 64;
                        int p = buffer.getIndex() % 64;
                        long bits = _availablePagesBits.get(q);
                        if (_availablePagesBits.compareAndSet(q, bits, bits | (1L << p))) {
                            markedAvailable++;
                        }
                    }
                } else {
                    result = false;
                }
            }
        }
        if (markedAvailable > 0) {
            _availablePages.set(true);
        }
        return result;
    }

    private void invalidate(Buffer buffer) throws PersistitInterruptedException {
        Debug.$assert0.t(buffer.isValid() && buffer.isMine());

        while (!detach(buffer)) {
            Util.spinSleep();
        }
        buffer.clearValid();
        buffer.clearDirty();
        buffer.setPageAddressAndVolume(0, null);
    }

    private boolean detach(Buffer buffer) {
        final int hash = hashIndex(buffer.getVolume(), buffer.getPageAddress());
        if (!_hashLocks[hash % HASH_LOCKS].tryLock()) {
            return false;
        }
        try {

            // Detach this buffer from the hash table.
            //
            if (_hashTable[hash] == buffer) {
                _hashTable[hash] = buffer.getNext();
            } else {
                Buffer prev = _hashTable[hash];
                for (Buffer next = prev.getNext();; next = prev.getNext()) {
                    assert next != null : "Attempting to detach an unattached Buffer";
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
        return true;
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
    Buffer get(Volume vol, long page, boolean writer, boolean wantRead) throws PersistitException {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;

        for (;;) {
            boolean mustClaim = false;
            _hashLocks[hash % HASH_LOCKS].lock();
            try {
                buffer = _hashTable[hash];
                //
                // Search for the page
                //
                while (buffer != null) {
                    Debug.$assert0.t(buffer.getNext() != buffer);
                    if (buffer.getPageAddress() == page && buffer.getVolume() == vol) {
                        //
                        // Found it - now claim it.
                        //
                        if (buffer.claim(writer, 0)) {
                            vol.getStatistics().bumpGetCounter();
                            bumpHitCounter();
                            return buffer;
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
                    Debug.$assert1.t(!buffer.isDirty());
                    Debug.$assert0.t(buffer != _hashTable[hash]);
                    Debug.$assert0.t(buffer.getNext() != buffer);

                    buffer.setPageAddressAndVolume(page, vol);
                    buffer.setNext(_hashTable[hash]);
                    _hashTable[hash] = buffer;
                    //
                    // It's not really valid yet, but it does have a writer
                    // claim on it so no other Thread can access it. In the
                    // meantime, any other Thread seeking access to the same
                    // page will find it.
                    //
                    buffer.setValid();
                    if (vol.isTemporary()) {
                        buffer.setTemporary();
                    } else {
                        buffer.clearTemporary();
                    }
                    Debug.$assert0.t(buffer.getNext() != buffer);
                }
            } finally {
                _hashLocks[hash % HASH_LOCKS].unlock();
            }
            if (mustClaim) {
                /*
                 * We're here because we found the page we want, but another
                 * thread has an incompatible claim on it. Here we wait, then
                 * recheck to make sure the buffer still represents the same
                 * page.
                 */
                if (!buffer.claim(writer)) {
                    throw new InUseException("Thread " + Thread.currentThread().getName() + " failed to acquire "
                            + (writer ? "writer" : "reader") + " claim on " + buffer);
                }

                //
                // Test whether the buffer we picked out is still valid
                //
                if (buffer.isValid() && buffer.getPageAddress() == page && buffer.getVolume() == vol) {
                    //
                    // If so, then we're done.
                    //
                    vol.getStatistics().bumpGetCounter();
                    bumpHitCounter();
                    return buffer;
                }
                //
                // If not, release the claim and retry.
                //
                buffer.release();
                continue;
            } else {
                /*
                 * We're here because the required page was not found in the
                 * pool so we have to read it from the Volume. We have a writer
                 * claim on the buffer, so anyone else attempting to get this
                 * page will simply wait for us to finish reading it.
                 * 
                 * At this point, the Buffer has been fully set up. It is on the
                 * hash table chain under its new page address, it is marked
                 * valid, and this Thread has a writer claim. If the read
                 * attempt fails, we need to mark the page INVALID so that any
                 * Thread waiting for access to this buffer will not use it. We
                 * also need to demote the writer claim to a reader claim unless
                 * the caller originally asked for a writer claim.
                 */
                if (wantRead) {
                    boolean loaded = false;
                    try {
                        Debug.$assert0.t(buffer.getPageAddress() == page && buffer.getVolume() == vol
                                && hashIndex(buffer.getVolume(), buffer.getPageAddress()) == hash);
                        buffer.load(vol, page);
                        loaded = true;
                        vol.getStatistics().bumpGetCounter();
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
                return buffer;
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
     * @throws PersistitInterruptedException
     * @throws RetryException
     * @throws IOException
     */
    public Buffer getBufferCopy(Volume vol, long page) throws InvalidPageAddressException,
            InvalidPageStructureException, VolumeClosedException, InUseException, PersistitIOException,
            PersistitInterruptedException {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;
        _hashLocks[hash % HASH_LOCKS].lock();
        try {
            buffer = _hashTable[hash];
            //
            // Search for the page
            //
            while (buffer != null) {
                Debug.$assert0.t(buffer.getNext() != buffer);

                if (buffer.getPageAddress() == page && buffer.getVolume() == vol) {
                    Debug.$assert0.t(buffer.isValid());
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
        boolean acquired = buffer.claim(true);
        assert acquired : "buffer not unavailable";
        buffer.load(vol, page);
        buffer.setValid();
        buffer.release();
        return buffer;
    }

    public Buffer getBufferCopy(final int index) throws IllegalArgumentException {
        if (index < 0 || index >= _bufferCount) {
            throw new IllegalArgumentException("Index " + index + " is out of range in " + this);
        }
        return new Buffer(_buffers[index]);
    }

    /**
     * Returns an available buffer. The replacement policy is to return a buffer
     * that's already been marked invalid, if available. Otherwise use the Clock
     * algorithm to choose a page for replacement that is approximately the
     * least-recently-used page.
     * 
     * @return Buffer An available buffer, or <i>null</i> if no buffer is
     *         currently available. The buffer has a writer claim.
     * @throws PersistitException
     * @throws IllegalStateException
     *             if there is no available buffer.
     */

    private Buffer allocBuffer() throws PersistitException {
        //
        // Start by searching for an invalid page. It's preferable
        // since no valid page will need to be evicted.
        //
        if (_availablePages.get()) {
            int start = (_clock.get() / 64) * 64;
            for (int q = start;;) {
                q += 64;
                if (q >= _bufferCount) {
                    q = 0;
                }
                long bits = _availablePagesBits.get(q / 64);
                if (bits != 0) {
                    for (int p = 0; p < 64; p++) {
                        if ((bits & (1L << p)) != 0) {
                            final Buffer buffer = _buffers[q + p];
                            //
                            // Note: need to verify that there are no claims -
                            // including those of the current thread.
                            //
                            if ((buffer.getStatus() & SharedResource.CLAIMED_MASK) == 0 && buffer.claim(true, 0)) {
                                if (!buffer.isValid()) {
                                    bits = _availablePagesBits.get(q / 64);
                                    if (_availablePagesBits.compareAndSet(q / 64, bits, bits & ~(1L << p))) {
                                        buffer.clearDirty();
                                        return buffer;
                                    }
                                }
                                buffer.release();
                            }
                        }
                    }
                }
                if (q == start) {
                    break;
                }

            }
            _availablePages.set(false);
        }
        //
        // Look for a page to evict.
        //
        for (int retry = 0; retry < _bufferCount * 2;) {
            int clock = _clock.get();
            assert clock < _bufferCount;
            if (!_clock.compareAndSet(clock, (clock + 1) % _bufferCount)) {
                continue;
            }
            Buffer buffer = _buffers[clock];
            if (buffer.isTouched()) {
                buffer.clearTouched();
            } else {
                //
                // Note: need to verify that there are no claims - including
                // those of the current thread.
                //
                if (!buffer.isFixed() && (buffer.getStatus() & SharedResource.CLAIMED_MASK) == 0
                        && buffer.claim(true, 0)) {
                    if (buffer.isDirty()) {
                        // An invalid dirty buffer is available and does not
                        // need to be written.
                        if (!buffer.isValid()) {
                            buffer.clearDirty();
                            return buffer;
                        }
                        // A dirty valid buffer needs to be written and then
                        // marked invalid
                        try {
                            buffer.writePage();
                            if (detach(buffer)) {
                                buffer.clearValid();
                                _forcedWriteCounter.incrementAndGet();
                                _evictCounter.incrementAndGet();
                                _persistit.getIOMeter().chargeEvictPageFromPool(buffer.getVolume(),
                                        buffer.getPageAddress(), buffer.getBufferSize(), buffer.getIndex());
                            }
                        } finally {
                            if (!buffer.isValid()) {
                                return buffer;
                            } else {
                                buffer.release();
                            }
                        }
                    } else {
                        if (buffer.isValid() && detach(buffer)) {
                            buffer.clearValid();
                            _evictCounter.incrementAndGet();
                            _persistit.getIOMeter().chargeEvictPageFromPool(buffer.getVolume(),
                                    buffer.getPageAddress(), buffer.getBufferSize(), buffer.getIndex());
                        }
                        if (!buffer.isValid()) {
                            return buffer;
                        } else {
                            buffer.release();
                        }
                    }
                }
            }
            retry++;
        }
        throw new IllegalStateException("No available Buffers");
    }

    enum Result {
        WRITTEN, UNAVAILABLE, ERROR
    };

    /**
     * @return Earliest timestamp of any dirty page in this
     *         <code>BufferPool</code>.
     */
    public long getEarliestDirtyTimestamp() {
        return _earliestDirtyTimestamp;
    }

    void setFlushTimestamp(final long timestamp) {
        while (true) {
            long current = _flushTimestamp.get();
            if (timestamp > current) {
                if (_flushTimestamp.compareAndSet(current, timestamp)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    /**
     * Heuristic to determine when the PAGE_WRITER thread(s) should do work.
     * 
     * @return whether PAGE_WRITER thread should write pages on the current
     *         polling cycle
     */
    boolean shouldWritePages() {
        int cleanCount = _bufferCount - _dirtyPageCount.get();
        if (getEarliestDirtyTimestamp() < _flushTimestamp.get()) {
            return true;
        }
        if (getEarliestDirtyTimestamp() <= _persistit.getCurrentCheckpoint().getTimestamp()) {
            return true;
        }
        if (cleanCount < _pageWriterTrancheSize * 2) {
            return true;
        }
        if (cleanCount < _bufferCount / 8) {
            return true;
        }
        return false;
    }

    void writeDirtyBuffers(final int[] priorities, final BufferHolder[] selectedBuffers) throws PersistitException {
        int count = selectDirtyBuffers(priorities, selectedBuffers);
        if (count > 0) {
            Arrays.sort(selectedBuffers, 0, count);
            for (int index = 0; index < count; index++) {
                final BufferHolder holder = selectedBuffers[index];
                final Buffer buffer = holder._buffer;
                if (buffer.claim(true, 0)) {
                    try {
                        if (holder.matches(buffer) && buffer.isDirty() && buffer.isValid()) {
                            buffer.writePage();
                        }
                    } finally {
                        buffer.release();
                    }
                }
            }
        }
    }

    int selectDirtyBuffers(final int[] priorities, final BufferHolder[] holders) throws PersistitException {
        Debug.suspend();
        int count = 0;
        final int clock = _clock.get();

        final long checkpointTimestamp = _persistit.getCurrentCheckpoint().getTimestamp();
        final long currentTimestamp = _persistit.getCurrentTimestamp();

        long earliestDirtyTimestamp = currentTimestamp;
        long flushTimestamp = _flushTimestamp.get();

        boolean flushed = true;
        for (int index = clock; index < clock + _bufferCount; index++) {
            final Buffer buffer = _buffers[index % _bufferCount];
            long timestamp = buffer.getTimestamp();
            /*
             * The following is subtle timing dance. If the buffer claim is
             * acquired here then no other thread can modify its timestamp or
             * dirty flag until it is released, and the timestamp reflects the
             * true sequence at which the buffer became dirty. However, if the
             * buffer is in use by another thread, then the timestamp reflects
             * either (a) the timestamp at which the buffer became dirty if is
             * is dirty, or (b) the the timestamp at which the thread holding
             * the claim acquired it.
             */
            if (!buffer.claim(false, 0)) {
                /*
                 * Without a claim, we are still guaranteed that the buffer will
                 * never receive a dirty timestamp less than its current
                 * timestamp.
                 */
                if (timestamp < earliestDirtyTimestamp) {
                    earliestDirtyTimestamp = timestamp;
                }
                if (timestamp < flushTimestamp) {
                    flushed = false;
                }
            } else {
                try {
                    if (buffer.isDirty()) {
                        final int priority = writePriority(buffer, clock, checkpointTimestamp, currentTimestamp);
                        if (priority > 0) {
                            count = addSelectedBufferByPriority(buffer, priority, priorities, holders, count);
                            if (!buffer.isTemporary()) {
                                timestamp = buffer.getTimestamp();
                                if (timestamp < earliestDirtyTimestamp) {
                                    earliestDirtyTimestamp = timestamp;
                                }
                                if (timestamp <= flushTimestamp) {
                                    flushed = false;
                                }
                            }
                        }
                    }
                } finally {
                    buffer.release();
                }
            }
        }

        _earliestDirtyTimestamp = earliestDirtyTimestamp;

        if (flushed) {
            _flushTimestamp.compareAndSet(flushTimestamp, 0);
        }
        return count;
    }
    
    int addSelectedBufferByPriority(final Buffer buffer, final int priority, final int[] priorities, final BufferHolder[] holders, final int initialCount) {
        int count = initialCount;
        if (priority > 0) {
            if (count == 0 || priorities[count - 1] > priority) {
                if (count < priorities.length) {
                    priorities[count] = priority;
                    holders[count].set(buffer);
                    count++;
                }
            } else {
                count = Math.min(count, priorities.length - 1);
                int where = count; 
                while (where > 0 && priorities[where - 1] < priority) {
                    where--;
                }
                int move = count - where ;
                if (move > 0) {
                    BufferHolder lastHolder = holders[count];
                    System.arraycopy(priorities, where, priorities, where + 1, move);
                    System.arraycopy(holders, where, holders, where + 1, move);
                    holders[where] = lastHolder;
                }
                priorities[where] = priority;
                holders[where].set(buffer);
                count++;
            }
        }
        return count;
    }

    /**
     * Compute a priority for writing the specified Buffer. A larger value
     * denotes a greater priority. Priority 0 indicates the buffer is ineligible
     * to be written.
     * 
     * @return priority
     */
     int writePriority(final Buffer buffer, int clock, long checkpointTimestamp, final long currentTimestamp) {
        int status = buffer.getStatus();
        if ((status & Buffer.VALID_MASK) == 0 || (status & Buffer.DIRTY_MASK) == 0) {
            // ineligible
            return 0;
        }
        //
        // compute "distance" between this buffer and the clock. A larger
        // distance results in lower priority.
        //
        int distance = (buffer.getIndex() - _clock.get() + _bufferCount) % _bufferCount;
        int age = 0;
        //
        // If this buffer has been touched, then it won't be evicted for at
        // least another _bufferCount cycles, and its distance is therefore
        // increased.
        //
        if ((status & Buffer.TOUCHED_MASK) != 0) {
            distance += _bufferCount;
        }

        if (!buffer.isTemporary()) {
            long timestampThreshold = (currentTimestamp * WRITE_AGE_THRESHOLD_RATIO + checkpointTimestamp)
                    / WRITE_AGE_THRESHOLD_RATIO;
            if (_flushTimestamp.get() > timestampThreshold) {
                timestampThreshold = _flushTimestamp.get();
            }
            //
            // Give higher priority to a older dirty buffers that need to be
            // written soon to allow a checkpoint.
            //
            if (buffer.getTimestamp() < timestampThreshold) {
                age = (int) Math.min(timestampThreshold - buffer.getTimestamp(), Integer.MAX_VALUE / 2);
                distance = 0;
            }
        } else {
            //
            // Temporary buffer - don't write it at all until the clock goes
            // through at least a full cycle.
            //
            if (distance > _bufferCount) {
                return 0;
            }
        }
        //
        // Bias to a large positive integer (magnitude doesn't matter)
        //
        return _bufferCount * 2 - distance + age;
    }

    static class BufferHolder implements Comparable<BufferHolder> {

        long _page;
        long _volumeId;
        Buffer _buffer;

        private void set(final Buffer buffer) {
            _page = buffer.getPageAddress();
            _volumeId = buffer.getVolumeId();
            _buffer = buffer;
        }

        /**
         * @return the page address
         */
        long getPage() {
            return _page;
        }

        /**
         * @return the volumeId
         */
        long getVolumeId() {
            return _volumeId;
        }

        /**
         * @return the Buffer
         */
        Buffer getBuffer() {
            return _buffer;
        }

        private boolean matches(final Buffer buffer) {
            return buffer == _buffer && buffer.getPageAddress() == _page && buffer.getVolumeId() == _volumeId;
        }

        /**
         * Used to sort buffers in ascending page address order by volume.
         * 
         * @param buffer
         * @return -1, 0 or 1 as this <code>Buffer</code> falls before, a, or
         *         after the supplied <code>Buffer</code> in the desired page
         *         address order.
         */
        @Override
        public int compareTo(BufferHolder buffer) {
            return _volumeId > buffer._volumeId ? 1 : _volumeId < buffer._volumeId ? -1 : _page > buffer._page ? 1
                    : _page < buffer._page ? -1 : 0;

        }

        @Override
        public String toString() {
            final Buffer buffer = _buffer;
            return buffer == null ? null : buffer.toString();
        }
    }

    /**
     * Implementation of PAGE_WRITER thread.
     */
    class PageWriter extends IOTaskRunnable {

        int[] _priorities = new int[0];
        BufferHolder[] _selectedBuffers = new BufferHolder[0];

        PageWriter() {
            super(BufferPool.this._persistit);
        }

        void start() {
            start("PAGE_WRITER:" + _bufferSize, _writerPollInterval);
        }

        @Override
        public void runTask() throws PersistitException {
            int size = _pageWriterTrancheSize;
            if (size != _priorities.length) {
                _priorities = new int[size];
                _selectedBuffers = new BufferHolder[size];
                for (int index = 0; index < size; index++) {
                    _selectedBuffers[index] = new BufferHolder();
                }
            }

            if (shouldWritePages()) {
                writeDirtyBuffers(_priorities, _selectedBuffers);
            }
        }

        @Override
        protected boolean shouldStop() {
            return _closed.get() && !isFlushing();
        }

        @Override
        protected long pollInterval() {
            return isFlushing() ? 0 : _writerPollInterval;
        }
    }

    @Override
    public String toString() {
        return "BufferPool[" + _bufferCount + "@" + _bufferSize + (_closed.get() ? ":closed" : "") + "]";
    }

    /**
     * @param i
     * @param detail
     * @return toString value for buffer at index <code>i</code>.
     */
    String toString(int i, boolean detail) {
        if (detail) {
            return _buffers[i].toStringDetail();
        } else {
            return _buffers[i].toString();
        }
    }

    /**
     * Dump the content of this <code>BufferPool</code> to the suppled stream.
     * Format is identical to the journal, consisting of a stream of IV and PA
     * records.
     * 
     * @param stream
     *            DataOutputStream to write to
     * @param bb
     *            ByteBuffer used to buffer intermediate results
     * @param secure
     *            true to obscure data values in the dump
     * @throws Exception
     */
    void dump(final DataOutputStream stream, final ByteBuffer bb, final boolean secure, final boolean verbose)
            throws Exception {
        final String toString = toString();
        if (verbose) {
            System.out.println(toString);
        }

        final Set<Volume> identifiedVolumes = new HashSet<Volume>();
        for (final Buffer buffer : _buffers) {
            buffer.dump(bb, secure, verbose, identifiedVolumes);
            if (bb.remaining() < _bufferSize * 2) {
                bb.flip();
                stream.write(bb.array(), 0, bb.limit());
                bb.clear();
            }
        }
        if (bb.remaining() > 0) {
            bb.flip();
            stream.write(bb.array(), 0, bb.limit());
            bb.clear();
        }
        stream.flush();
    }
}
