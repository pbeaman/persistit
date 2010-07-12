/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.persistit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.VolumeClosedException;

/**
 * A pool of {@link Buffer} objects, maintained on various lists that permit
 * rapid lookup and replacement of pages images within <tt>Buffer</tt>s.
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
    private final static int HASH_MULTIPLE = 13;
    /**
     * Minimum number of buffers this pool may have
     */
    public final static int MINIMUM_POOL_COUNT = 7;
    /**
     * Maximum number of buffers this pool may have
     */
    public final static int MAXIMUM_POOL_COUNT = Integer.MAX_VALUE;
    /**
     * Pages per allocation bucket
     */
    public final static int PAGES_PER_BUCKET = 4096;

    /**
     * The Persistit instance that references this BufferBool.
     */
    private Persistit _persistit;

    /**
     * Hash table - fast access to buffer by hash of address.
     */
    private Buffer[] _hashTable;

    /**
     * PBuffer indexed by size and
     */
    private Buffer[] _buffers;

    /**
     * Count of PBuffers allocated to this pool.
     */
    private int _bufferCount;

    /**
     * Size of each buffer
     */
    private int _bufferSize;
    /**
     * Count of separate locks and list structures
     */
    private int _bucketCount;

    /**
     * Head of singly-linked list of invalid pages
     */
    private Buffer[] _invalidBufferQueue;

    /**
     * Doubly-linked list of permanent Buffers. Note that a Buffer on this list
     * cannot also be on the LRU list. A permanent Buffer cannot be replaced.
     * The "head" Buffer (page 0) of each Volume is so-marked.
     */
    private Buffer[] _perm;

    /**
     * Head of doubly-linked list of valid page in least-recently-used order
     */
    private Buffer[] _lru;

    /**
     * Head of doubly-linked list of dirty pages in write priority order
     */
    private Buffer[] _dirty;

    /**
     * Head of doubly-linked list of dirty pages urgently needing to be written
     */
    private Buffer[] _urgent;

    /**
     * An Object used to lock the buffer management memory structures
     */
    private final Object[] _lock;

    /**
     * Count of buffer pool gets
     */
    private AtomicLong _getCounter = new AtomicLong();

    /**
     * Count of buffer pool hits (buffer found in pool)
     */
    private AtomicLong _hitCounter = new AtomicLong();

    /**
     * Indicates that Persistit has closed this buffer pool.
     */
    private volatile boolean _closed;

    /**
     * Polling interval for TemporaryVolumeBufferWriter
     */
    private long _writerPollInterval = DEFAULT_WRITER_POLL_INTERVAL;

    private PageWriter _writer;

    private DirtyPageCollector _collector;

    // TODO - reconsider whether to keep this statistic
    private AtomicLong _stalledForCheckpoint = new AtomicLong();

    /**
     * Construct a BufferPool with the specified count of <tt>Buffer</tt>s of
     * the specified size.
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
        _buffers = new Buffer[count];
        _bucketCount = (count / PAGES_PER_BUCKET) + 1;
        _hashTable = new Buffer[((count * HASH_MULTIPLE) / _bucketCount)
                * _bucketCount];

        _invalidBufferQueue = new Buffer[_bucketCount];
        _lru = new Buffer[_bucketCount];
        _perm = new Buffer[_bucketCount];
        _dirty = new Buffer[_bucketCount];
        _urgent = new Buffer[_bucketCount];
        _lock = new Object[_bucketCount];
        for (int bucket = 0; bucket < _bucketCount; bucket++) {
            _lock[bucket] = new Object();
        }

        try {

            for (int index = 0; index < count; index++) {
                Buffer buffer = new Buffer(size, index, this, _persistit);
                int bucket = index % _bucketCount;
                buffer.setNext(_invalidBufferQueue[bucket]);
                _invalidBufferQueue[bucket] = buffer;
                _buffers[index] = buffer;
                created++;
            }
            _collector = new DirtyPageCollector();

            // TODO - PageWriter per Log if/when we add striped logs
            _writer = new PageWriter(0);
        } catch (OutOfMemoryError e) {
            System.out.println("Out of memory after creating " + created
                    + " buffers");
            throw e;
        }

    }

    void close() {
        _closed = true;
        if (_collector != null) {
            _collector.kick();
            while (!_collector.isStopped()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (_writer != null) {
            while (!_writer.isStopped()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    int flush() {
        final BitSet bits = new BitSet(_bufferCount);
        int unavailable = 0;
        for (int retry = 0; retry < MAX_FLUSH_RETRY_COUNT; retry++) {
            unavailable = 0;
            for (int poolIndex = 0; poolIndex < _bufferCount; poolIndex++) {
                if (!bits.get(poolIndex)) {
                    final Buffer buffer = _buffers[poolIndex];
                    final int bucket = bucket(buffer);
                    if (buffer.isDirty() && !buffer.isUrgent()) {
                        if ((buffer.getStatus() & SharedResource.WRITER_MASK) == 0) {
                            enqueueUrgentPage(buffer, bucket);
                        } else {
                            unavailable++;
                        }
                    } else {
                        bits.set(poolIndex);
                    }
                }
            }
            if (unavailable == 0) {
                break;
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ie) {
                break;
            }
        }

        while (true) {
            boolean hasEnqueuedPages = false;
            for (int bucket = 0; !hasEnqueuedPages && bucket < _bucketCount; bucket++) {
                synchronized (_lock[bucket]) {
                    hasEnqueuedPages |= (_urgent[bucket] != null || _dirty[bucket] != null);
                }
            }
            if (!hasEnqueuedPages) {
                break;
            } else {
                try {
                    Thread.sleep(RETRY_SLEEP_TIME);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }
        return unavailable;
    }

    private int hashIndex(Volume vol, long page) {
        // Important - all pages that hash to the same hash table entry
        // must also belong to the same bucket.
        //
        return (int) ((page ^ vol.getId()) % _hashTable.length);
    }

    int countDirty(Volume vol) {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++) {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer.getVolume() == vol)
                    && (buffer._status & SharedResource.DIRTY_MASK) != 0) {
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
        info._bufferCount = _bufferCount;
        info._bufferSize = _bufferSize;
        info._getCounter = _getCounter.get();
        info._hitCounter = _hitCounter.get();
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
        info._validPageCount = validPages;
        info._dirtyPageCount = dirtyPages;
        info._readerClaimedPageCount = readerClaimedPages;
        info._writerClaimedPageCount = writerClaimedPages;
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

        case 1:
            for (int bucket = 0; bucket < _bucketCount; bucket++) {
                synchronized (_lock[bucket]) {
                    Buffer lru = _lru[bucket];
                    for (Buffer buffer = lru; buffer != null; buffer = buffer
                            .getNextLru()) {
                        if (selected(buffer, includeMask, excludeMask)) {
                            populateInfo1(array, index, buffer);
                            index++;
                        }
                        if (buffer.getNextLru() == lru)
                            break;
                    }
                }
            }
            break;

        case 2:
            for (int bucket = 0; bucket < _bucketCount; bucket++) {
                synchronized (_lock[bucket]) {
                    for (Buffer buffer = _invalidBufferQueue[bucket]; buffer != null; buffer = buffer
                            .getNext()) {
                        if (selected(buffer, includeMask, excludeMask)) {
                            populateInfo1(array, index, buffer);
                            index++;
                        }
                    }
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

    // /**
    // * Selects and collects <tt>Buffer</tt>s from this pool that conform to
    // the
    // * selection specifications. This method is intended for use only by
    // * the diagnostic utility package.
    // * @param type
    // * @param includeMask
    // * @param excludeMask
    // * @return An array of <tt>Buffer</tt>s that conform to the selection
    // * criteria.
    // */
    // public Buffer[] selectBuffers(int type, int includeMask, int excludeMask)
    // {
    // Vector list = new Vector(_bufferCount);
    // switch (type)
    // {
    // case 1:
    // for (int bucket = 0; bucket < _bucketCount; bucket++)
    // {
    // synchronized(_lock[bucket])
    // {
    // Buffer lru = _lru[bucket];
    // for (Buffer buffer = lru;
    // buffer != null;
    // buffer = buffer.getNextLru())
    // {
    // if (selected(buffer, includeMask, excludeMask))
    // {
    // list.add(buffer);
    // }
    // if (buffer.getNextLru() == lru) break;
    // }
    // }
    // }
    // break;
    // case 2:
    // for (int bucket = 0; bucket < _bucketCount; bucket++)
    // {
    // synchronized(_lock[bucket])
    // {
    // for (Buffer buffer = _invalidBufferQueue[bucket];
    // buffer != null;
    // buffer = buffer.getNext())
    // {
    // if (selected(buffer, includeMask, excludeMask))
    // {
    // list.add(buffer);
    // }
    // }
    // }
    // }
    // break;
    // default:
    // for (int i = 0; i < _bufferCount; i++)
    // {
    // Buffer buffer = _buffers[i];
    // if (selected(buffer, includeMask, excludeMask))
    // {
    // list.add(buffer);
    // }
    // }
    // break;
    // }
    // Buffer[] array = new Buffer[list.size()];
    // for (int i = 0; i < list.size(); i++)
    // {
    // array[i] = (Buffer)list.elementAt(i);
    // }
    // return array;
    // }

    private boolean selected(Buffer buffer, int includeMask, int excludeMask) {
        return ((includeMask == 0) || (buffer._status & includeMask) != 0)
                && (buffer._status & excludeMask) == 0;
    }

    /**
     * @return Size (in bytes) of each <tt>Buffer</tt> managed by this pool.
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /**
     * @return The count of <tt>Buffer</tt>s managed by this pool.
     */
    public int getBufferCount() {
        return _bufferCount;
    }

    /**
     * @return The count of lookup operations for pages images in this pool.
     *         This number, in comparison with the hit counter, indicates how
     *         effective the cache is in reducing disk I/O.
     */
    public long getGetCounter() {
        return _getCounter.get();
    }

    /**
     * @return The count of lookup operations for pages images in this pool for
     *         which the page image was already found in this
     *         <tt>BufferPool</tt>. This number, in comparison with the get
     *         counter, indicates how effective the cache is in reducing disk
     *         I/O.
     */
    public long getHitCounter() {
        return _hitCounter.get();
    }

    /**
     * Resets the get and hit counters to zero.
     */
    public void resetCounters() {
        _getCounter.set(0);
        _hitCounter.set(0);
    }

    /**
     * Resets the get and hit counters to supplied values.
     * 
     * @param gets
     * @param hits
     */
    public void resetCounters(long gets, long hits) {
        _getCounter.set(gets);
        _hitCounter.set(hits);
    }

    private void bumpHitCounter() {
        _getCounter.incrementAndGet();
        _hitCounter.incrementAndGet();
    }

    private void bumpGetCounter() {
        _getCounter.incrementAndGet();
    }

    /**
     * Get the "hit ratio" - the number of hits divided by the number of overall
     * gets. A value close to 1.0 indicates that most attempts to find data in
     * the <tt>BufferPool</tt> are successful - i.e., that the cache is
     * effectively reducing the need for disk read operations.
     * 
     * @return The ratio
     */
    public double getHitRatio() {
        final long getCounter = _getCounter.get();
        final long hitCounter = _hitCounter.get();
        if (getCounter == 0)
            return 0.0;
        else
            return ((double) hitCounter) / ((double) getCounter);
    }

    private int bucket(Buffer b) {
        return (int) (b.getPageAddress() % _bucketCount);
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
                    && !buffer.isPermanent() && buffer.isValid()) {
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
        int bucket = bucket(buffer);
        synchronized (_lock[bucket]) {
            if (Debug.ENABLED)
                Debug.$assert(buffer.isValid());
            detach(buffer);
            buffer._status &= (~SharedResource.VALID_MASK & ~SharedResource.DIRTY_MASK);
            buffer.setPageAddressAndVolume(0, null);
            buffer.setNext(_invalidBufferQueue[bucket]);
            _invalidBufferQueue[bucket] = buffer;
        }
    }

    private void delete(Buffer buffer) {
        int bucket = bucket(buffer);
        synchronized (_lock[bucket]) {
            buffer._status |= SharedResource.DELETE_MASK;
            buffer.setPageAddressAndVolume(0, null);
        }

    }

    private void detach(Buffer buffer) {
        int bucket = bucket(buffer);
        synchronized (_lock[bucket]) {
            //
            // If already invalid, we're done.
            //
            if ((buffer._status & SharedResource.VALID_MASK) == 0) {
                return;
            }

            final int hash = hashIndex(buffer.getVolume(), buffer
                    .getPageAddress());
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
            //       
            // Detach buffer from the LRU queue.
            //
            if (_lru[bucket] == buffer)
                _lru[bucket] = buffer.getNextLru();
            if (_lru[bucket] == buffer)
                _lru[bucket] = null;

            buffer.removeFromLru();
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

        int bucket = bucket(buffer);
        synchronized (_lock[bucket]) {
            //
            // Return if buffer is already least-recently-used.
            //       
            if (_lru[bucket] == null) {
                _lru[bucket] = buffer;
            } else if (_lru[bucket] != buffer) {
                //
                // Detach buffer from the list. (Is a no-op for a buffer
                // that wasn't already on the LRU queue.)
                //
                buffer.moveInLru(_lru[bucket]);
            }
        }
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
            throws InUseException, InvalidPageAddressException,
            InvalidPageStructureException, VolumeClosedException,
            PersistitIOException, RetryException {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;
        int bucket = (int) (page % _bucketCount);
        for (;;) {
            boolean mustRead = false;
            boolean mustClaim = false;
            synchronized (_lock[bucket]) {
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
                        if (buffer.claim(writer, 0)) {
                            // But if buffer needs to be check-pointed, release
                            // the
                            // claim and retry below.
                            final Checkpoint checkpoint = _persistit
                                    .getTimestampAllocator()
                                    .getCurrentCheckpoint();
                            if (writer
                                    && buffer.isDirty()
                                    && buffer.getTimestamp() < checkpoint
                                            .getTimestamp()) {
                                buffer.release();
                                mustClaim = true;
                                break;
                            }
                            vol.bumpGetCounter();
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
                    buffer = allocBuffer(bucket);
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
                        buffer._status |= SharedResource.VALID_MASK;

                        if (Debug.ENABLED)
                            Debug.$assert(buffer.getNext() != buffer);
                        mustRead = true;
                    }
                }
            }
            if (buffer == null) {
                _collector.kick();
                synchronized (_lock[bucket]) {
                    try {
                        _lock[bucket].wait(RETRY_SLEEP_TIME);
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
                    final Checkpoint checkpoint = _persistit
                            .getTimestampAllocator().getCurrentCheckpoint();
                    if (writer
                            && buffer.isDirty()
                            && buffer.getTimestamp() < checkpoint
                                    .getTimestamp()) {
                        buffer.release();
                        enqueueUrgentPage(buffer, bucket);
                        synchronized (_lock[bucket]) {
                            try {
                                _lock[bucket].wait(RETRY_SLEEP_TIME);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        final long stalled = _stalledForCheckpoint
                                .incrementAndGet();
                        if (stalled % 10000 == 0) {
                            System.out.println("stalls: " + stalled);
                        }
                        continue;
                    }
                    //
                    // Test whether the buffer we picked out is still valid
                    //
                    if ((buffer._status & SharedResource.VALID_MASK) != 0
                            && buffer.getPageAddress() == page
                            && buffer.getVolume() == vol) {
                        //
                        // If so, then we're done.
                        //
                        vol.bumpGetCounter();
                        bumpHitCounter();
                        return buffer;
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
                            if (Debug.ENABLED)
                                Debug.$assert(buffer.getPageAddress() == page
                                        && buffer.getVolume() == vol
                                        && hashIndex(buffer.getVolume(), buffer
                                                .getPageAddress()) == hash);

                            buffer.load(vol, page);
                            loaded = true;
                        } finally {
                            if (!loaded) {
                                invalidate(buffer);
                                buffer.release();
                            }
                        }
                        vol.bumpGetCounter();
                        bumpGetCounter();
                    } else {
                        buffer.clear();
                        buffer.init(Buffer.PAGE_TYPE_UNALLOCATED,
                                "initGetWithoutRead");
                    }
                    if (!writer)
                        buffer.releaseWriterClaim();
                    return buffer;
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
        int bucket = (int) (page % _bucketCount);
        Buffer buffer = null;
        synchronized (_lock[bucket]) {
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
                        Debug
                                .$assert((buffer._status & SharedResource.VALID_MASK) != 0);
                    //
                    // Found it - now return a copy of it.
                    //
                    return new Buffer(buffer);
                }
                buffer = buffer.getNext();
            }
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
                && (buffer._status & SharedResource.PERMANENT_MASK) == 0) {
            makeMostRecentlyUsed(buffer);
        }
        buffer.release();
    }

    void setPermanent(final Buffer buffer, boolean permanent) {
        if (permanent != buffer.isPermanent()) {
            final int bucket = bucket(buffer);
            buffer.setPermanent(permanent);
            synchronized (_lock[bucket]) {
                if (permanent) {
                    // Insert
                    if (_perm[bucket] == null) {
                        buffer.removeFromLru();
                        _perm[bucket] = buffer;
                    } else {
                        buffer.moveInLru(_perm[bucket]);
                    }
                } else {
                    if (_perm[bucket] == buffer) {
                        _perm[bucket] = buffer.getNext() == buffer ? null
                                : buffer.getNext();
                    }
                    buffer.removeFromLru();
                }
            }
        }
    }

    /**
     * Returns an available buffer. The replacement policy is to return a buffer
     * that's already been marked invalid, if available. Otherwise traverse the
     * least-recently-used queue to find the least- recently-used buffer that is
     * not claimed. Throws a RetryException if there is no available buffer.
     * 
     * @param bucket
     *            The cache bucket
     * @return Buffer An available buffer, or <i>null</i> if no buffer is
     *         currently available. The buffer has a writer claim.
     * @throws InvalidPageStructureException
     */
    private Buffer allocBuffer(int bucket) throws PersistitIOException,
            InvalidPageStructureException, RetryException {
        // No need to synchronize here because the only caller is get(...).
        Buffer lastBuffer = null;

        boolean found = false;

        /**
         * First search the Invalid Buffer Queue
         */
        Buffer buffer = _invalidBufferQueue[bucket];
        while (buffer != null) {
            if (Debug.ENABLED)
                Debug.$assert(buffer.getNext() != buffer);

            if (buffer.isAvailable() && buffer.isClean()
                    && buffer.claim(true, 0)) {
                if (lastBuffer != null)
                    lastBuffer.setNext(buffer.getNext());
                else
                    _invalidBufferQueue[bucket] = buffer.getNext();
                found = true;
                break;
            }
            lastBuffer = buffer;
            buffer = buffer.getNext();
        }

        /**
         * Next search for a clean replaceable buffer
         */
        if (!found) {
            buffer = _lru[bucket];
            while (buffer != null) {
                if (buffer.isAvailable() && buffer.isClean()
                        && buffer.claim(true, 0)) {
                    detach(buffer);
                    found = true;
                    break;
                }
                buffer = buffer.getNextLru();
                if (buffer == _lru[bucket])
                    break;
            }
        }
        if (found) {
            return buffer;
        } else {
            return null;
        }
    }

    enum Result {
        WRITTEN, UNAVAILABLE, ERROR
    };

    private boolean enqueueDirtyPage(final Buffer buffer, final int bucket) {
        final boolean dirty;
        synchronized (_lock[bucket]) {
            if (buffer.isDirty() && !buffer.isEnqueued()) {
                dirty = true;
                if (_dirty[bucket] == null) {
                    buffer.removeFromDirty();
                    _dirty[bucket] = buffer;
                } else {
                    buffer.moveInDirty(_dirty[bucket]);
                }
                buffer.setEnqueued();
            } else {
                dirty = false;
            }
        }
        if (dirty) {
            _writer.kick();
        }
        return dirty;
    }

    private boolean enqueueUrgentPage(final Buffer buffer, final int bucket) {
        final boolean dirty;
        synchronized (_lock[bucket]) {
            if (buffer.isDirty() && !buffer.isUrgent()) {
                dirty = true;
                buffer.setUrgent();
                if (_urgent[bucket] == null) {
                    buffer.removeFromDirty();
                    _urgent[bucket] = buffer;
                } else {
                    buffer.moveInDirty(_urgent[bucket]);
                }
            } else {
                dirty = false;
            }
        }
        if (dirty) {
            _writer.kick();
        }
        return dirty;

    }

    /**
     * Given a Checkpoint, determines whether there are any dirty pages in the
     * BufferPool having timestamps smaller than the checkpoint. If not, then
     * the logs are complete and the checkpoint record can be written.
     * 
     * @param checkpoint
     * @return <tt>true</tt> if there are no dangerous pages in the BufferPool
     */
    private Checkpoint findValidCheckpoint(
            final List<Checkpoint> outstandingCheckpoints) {
        long earliestDirtyTimestamp = Long.MAX_VALUE;
        for (int index = 0; index < _buffers.length; index++) {
            final Buffer buffer = _buffers[index];
            if (buffer.isDirty()) {
                long timestamp = buffer.getTimestamp();
                if (timestamp < earliestDirtyTimestamp) {
                    earliestDirtyTimestamp = timestamp;
                }
            }
        }
        for (int index = outstandingCheckpoints.size(); --index >= 0;) {
            final Checkpoint checkpoint = outstandingCheckpoints.get(index);
            if (checkpoint.getTimestamp() <= earliestDirtyTimestamp) {
                for (int k = index; k >= 0; --k) {
                    outstandingCheckpoints.remove(k);
                }
                return checkpoint;
            }
        }
        return null;
    }

    private class DirtyPageCollector implements Runnable {

        private boolean _kicked;
        private boolean _stopped;

        DirtyPageCollector() {
            final Thread thread = new Thread(this, "PAGE_COLLECTOR:"
                    + _bufferSize);
            thread.start();
        }

        public void kick() {
            synchronized (this) {
                if (!_kicked) {
                    notify();
                    _kicked = true;
                }
            }
        }

        public boolean isStopped() {
            synchronized (this) {
                return _stopped;
            }
        }

        public void run() {

            while (true) {
                boolean clean = true;
                boolean wasClosed = _closed;
                for (int bucket = 0; bucket < _bucketCount; bucket++) {
                    clean &= enqueueDirtyBuffers(bucket) == 0;
                }
                synchronized (this) {
                    if (clean && wasClosed) {
                        _stopped = true;
                        break;
                    }
                    _kicked = false;
                    try {
                        wait(wasClosed ? RETRY_SLEEP_TIME : _writerPollInterval);
                    } catch (InterruptedException ie) {
                        // do nothing
                    }
                }
            }
        }

        private int enqueueDirtyBuffers(int bucket) {
            int count = 0;
            int maxCount = 64;
            int invalidDepth = 64;
            int unavailable = 0;

            synchronized (_lock[bucket]) {
                Buffer buffer;

                buffer = _invalidBufferQueue[bucket];
                int lruCount = _closed ? maxCount : _bufferCount / _bucketCount
                        / 2;
                int lazyCount = _closed ? maxCount : _bufferCount
                        / _bucketCount / 8;
                while (buffer != null && invalidDepth-- > 0 && count < maxCount) {
                    if (Debug.ENABLED)
                        Debug.$assert(buffer.getNext() != buffer);
                    if (buffer.isDirty()) {
                        if ((buffer.getStatus() & SharedResource.CLAIMED_MASK) == 0) {
                            if (enqueueDirtyPage(buffer, bucket)) {
                                count++;
                            }
                        } else {
                            unavailable++;
                        }
                    }
                    buffer = buffer.getNext();
                }

                buffer = _lru[bucket];
                while (buffer != null && count < maxCount) {
                    if (buffer.isDirty()) {
                        if ((buffer.getStatus() & SharedResource.WRITER_MASK) == 0) {
                            if (enqueueDirtyPage(buffer, bucket)) {
                                count++;
                            }
                        } else {
                            unavailable++;
                        }
                    }
                    buffer = buffer.getNextLru();
                    if (buffer == _lru[bucket]) {
                        break;
                    }
                    if (--lruCount < 0 && count > lazyCount) {
                        break;
                    }
                }

                buffer = _perm[bucket];
                while (buffer != null && count < maxCount) {
                    if (buffer.isDirty() && !buffer.isEnqueued()) {
                        if ((buffer.getStatus() & SharedResource.WRITER_MASK) == 0) {
                            if (enqueueDirtyPage(buffer, bucket)) {
                                count++;
                            }
                        } else {
                            unavailable++;
                        }
                    }
                    buffer = buffer.getNextLru();
                    if (buffer == _perm[bucket]) {
                        break;
                    }
                }
            }
            if (count == 0) {
                return -unavailable;
            } else {
                return count;
            }
        }
    }

    private class PageWriter implements Runnable {
        final int _logIndex;
        boolean _kicked;
        boolean _stopped;

        PageWriter(final int logIndex) {
            _logIndex = logIndex;
            final Thread thread = new Thread(this, "PAGE_WRITER:" + _bufferSize
                    + ":" + _logIndex);
            thread.start();
        }

        public void kick() {
            synchronized (this) {
                if (!_kicked) {
                    notify();
                    _kicked = true;
                }
            }
        }

        public boolean isStopped() {
            synchronized (this) {
                return _stopped;
            }
        }

        public void run() {
            try {

                Checkpoint currentCheckpoint = null;
                List<Checkpoint> outstandingCheckpoints = new ArrayList<Checkpoint>();

                while (true) {
                    int written = 0;
                    int remaining = 0;
                    int unavailable = 0;
                    int errors = 0;

                    for (int bucket = 0; bucket < _bucketCount; bucket++) {
                        for (int ubucket = 0; ubucket < _bucketCount; ubucket++) {
                            Buffer buffer;
                            synchronized (_lock[ubucket]) {
                                buffer = _urgent[ubucket];
                            }
                            while (buffer != null) {
                                final Result result = writePage(buffer, ubucket);
                                switch (result) {
                                case WRITTEN:
                                    written++;
                                    synchronized (_lock[ubucket]) {
                                        if (_urgent[ubucket] == buffer) {
                                            _urgent[ubucket] = buffer
                                                    .getNextDirty();
                                        }
                                        buffer.removeFromDirty();
                                        if (_urgent[ubucket] == buffer) {
                                            _urgent[ubucket] = null;
                                        } else {
                                            remaining++;
                                        }
                                        buffer.setUnenqueued();
                                    }
                                    break;
                                case UNAVAILABLE:
                                    unavailable++;
                                    break;
                                case ERROR:
                                    errors++;
                                    break;
                                }
                                if (result == Result.WRITTEN) {
                                    break;
                                }
                                synchronized (_lock[ubucket]) {
                                    buffer = buffer.getNextDirty();
                                    if (buffer == _urgent[ubucket]) {
                                        break;
                                    }
                                }
                            }
                        }

                        Buffer buffer;
                        synchronized (_lock[bucket]) {
                            buffer = _dirty[bucket];
                        }
                        while (buffer != null) {
                            final Result result = writePage(buffer, bucket);
                            switch (result) {
                            case WRITTEN:
                                written++;
                                synchronized (_lock[bucket]) {
                                    if (_dirty[bucket] == buffer) {
                                        _dirty[bucket] = buffer.getNextDirty();
                                    }
                                    buffer.removeFromDirty();
                                    if (_dirty[bucket] == buffer) {
                                        _dirty[bucket] = null;
                                    } else {
                                        remaining++;
                                    }
                                    buffer.setUnenqueued();
                                }
                            case UNAVAILABLE:
                                unavailable++;
                                break;
                            case ERROR:
                                errors++;
                                break;
                            }
                            if (result == Result.WRITTEN) {
                                break;
                            }
                            synchronized (_lock[bucket]) {
                                buffer = buffer.getNextDirty();
                                if (buffer == _dirty[bucket]) {
                                    break;
                                }
                            }
                        }
                    }

                    final Checkpoint newCheckpointTimestamp = _persistit
                            .getTimestampAllocator().updateCheckpoint();

                    if (newCheckpointTimestamp != currentCheckpoint) {
                        currentCheckpoint = newCheckpointTimestamp;
                        outstandingCheckpoints.add(currentCheckpoint);
                        final Checkpoint validCheckpoint = findValidCheckpoint(outstandingCheckpoints);
                        if (validCheckpoint != null) {
                            try {
                                _persistit.getLogManager()
                                        .writeCheckpointToLog(validCheckpoint);
                            } catch (PersistitIOException e) {
                                _persistit.getLogBase().log(
                                        LogBase.LOG_EXCEPTION,
                                        e + " while writing " + validCheckpoint
                                                + ":" + e);

                            }
                        }
                    }
                    try {
                        final boolean collectorStopped = _collector.isStopped();
                        synchronized (this) {
                            _kicked = false;
                            if (remaining + unavailable + errors == 0) {
                                if (_closed && collectorStopped) {
                                    // done - shut down
                                    break;
                                } else {
                                    wait(_writerPollInterval);
                                }
                            } else if (remaining == 0) {
                                wait(RETRY_SLEEP_TIME);
                            }
                        }
                    } catch (InterruptedException ie) {
                        // ignore
                    }

                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                _stopped = true;
            }
        }

        private Result writePage(final Buffer buffer, final int bucket) {
            final Volume volume = buffer.getVolume();
            final long page = buffer.getPageAddress();
            final boolean claimed = buffer.claim(true, 0);
            try {
                if (claimed) {
                    if (volume != null) {
                        buffer.clearSlack();
                        buffer.save();
                        // buffer.releaseWriterClaim();
                        _persistit.getLogManager().writePageToLog(buffer);
                        buffer.getVolume().bumpWriteCounter();
                        synchronized (_lock[bucket]) {
                            _lock[bucket].notify();
                        }
                    }
                    buffer.setClean();
                    return Result.WRITTEN;
                } else {
                    return Result.UNAVAILABLE;
                }
            } catch (Exception e) {
                _persistit.getLogBase().log(
                        LogBase.LOG_EXCEPTION,
                        e + " while writing page " + page + " in volume "
                                + volume);
                return Result.ERROR;
            } finally {
                if (claimed) {
                    buffer.release();
                }
            }
        }
    }
}
