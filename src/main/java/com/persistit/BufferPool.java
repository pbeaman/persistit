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

import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.VolumeClosedException;

/**
 * A pool of {@link Buffer} objects, maintained on various lists that
 * permit rapid lookup and replacement of pages images within <tt>Buffer</tt>s.
 *
 * @version 1.1
 */
public class BufferPool
{
    /**
     * Default TemporaryVolumeBufferWriter polling interval
     */
    private final static long DEFAULT_WRITER_POLL_INTERVAL = 5000;
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
     * Number of Buffers to allocate per Findex
     */
    private final static int FINDEX_RATIO = 2;
    
    /**
     * Minimum number of Findex arrays to preallocate
     */
    private final static int MINIMUM_FINDEX_COUNT = 4;
    
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
     * Head of doubly-linked list of valid page in least-recently-used order
     */
    private Buffer[] _lru = null;
    
    /**
     * Head of doubly-linked list of Buffers with Findex elements in 
     * least-recently-used order
     */
    private Buffer[] _findexLru = null;
    /**
     * An Object used to lock the buffer management memory structures 
     */
    private final Object[] _lock;

    /**
     * Count of buffer pool gets 
     */
    private long _getCounter;
    
    /**
     * Count of buffer pool hits (buffer found in pool)
     */
    private long _hitCounter;
    
    /**
     * Current age - used to determine whether buffer needs to be moved
     * up on most recently used list.
     */
    private volatile long _currentAge;
    
    /**
     * Indicates that Persistit has closed this buffer pool.
     */
    private boolean _closed;
    
    /**
     * Polling interval for TemporaryVolumeBufferWriter
     */
    private long _writerPollInterval = DEFAULT_WRITER_POLL_INTERVAL;
    
    private TemporaryVolumeBufferWriter[] _writers;
    
    final static RetryException TV_RETRY_EXCEPTION = new RetryException(-1);
    /**
     * Construct a BufferPool with the specified count of <tt>Buffer</tt>s of
     * the specified size. 
     * @param count     The number of buffers in the pool
     * @param size      The size (in bytes) of each buffer
     */
    BufferPool(int count, int size, Persistit persistit)
    {
    	_persistit = persistit;
        if (count < MINIMUM_POOL_COUNT)
        {
            throw new IllegalArgumentException(
                "Buffer pool count too small: " + count);
        }
        if ( count > MAXIMUM_POOL_COUNT)
        {
            throw new IllegalArgumentException(
                "Buffer pool count too large: " + count);
        }
        
        int possibleSize = Buffer.MIN_BUFFER_SIZE;
        boolean ok = false;
        while (!ok && possibleSize <= Buffer.MAX_BUFFER_SIZE)
        {
            if (size == possibleSize) ok = true;
            else possibleSize *= 2;
        }
        if (!ok) throw new IllegalArgumentException(
            "Invalid buffer size requested: " + size);
        
        _bufferCount = count;
        _currentAge = count + 1;
        _bufferSize = size;
        _buffers = new Buffer[count];
        _bucketCount = (count / PAGES_PER_BUCKET) + 1;
        _hashTable =
            new Buffer[((count * HASH_MULTIPLE) / _bucketCount) * _bucketCount];

        _invalidBufferQueue = new Buffer[_bucketCount];
        _lru = new Buffer[_bucketCount];
        _findexLru = new Buffer[_bucketCount];
        _lock = new Object[_bucketCount];
        for (int bucket = 0; bucket < _bucketCount; bucket++)
        {
            _lock[bucket] = new Object();
        }
        
        int findexCount = _bufferCount / FINDEX_RATIO;
        
        if (findexCount < MINIMUM_FINDEX_COUNT * _bucketCount)
        {
            findexCount = MINIMUM_FINDEX_COUNT * _bucketCount;
        }
        
        for (int index = 0; index < count; index++)
        {
            Buffer buffer = new Buffer(size, index, this, _persistit);
            int bucket = index % _bucketCount;
            buffer._next = _invalidBufferQueue[bucket];
            _invalidBufferQueue[bucket] = buffer;
            _buffers[index] = buffer;
            if (findexCount-- > 0)
            {
                buffer.createFindex();
                if (_findexLru[bucket] == null)
                {
                    _findexLru[bucket] = buffer;
                    buffer._findexNextLru = buffer;
                    buffer._findexPrevLru = buffer;
                }
                else
                {
                    buffer._findexNextLru = _findexLru[bucket];
                    buffer._findexPrevLru = _findexLru[bucket]._findexPrevLru;
                    _findexLru[bucket]._findexPrevLru._findexNextLru = buffer;
                    _findexLru[bucket]._findexPrevLru = buffer;
                }
            }
        }
    }
    
    void close()
    {
        _closed = true;
        if (_writers != null)
        {
            for (int index = 0; index < _writers.length; index++)
            {
                _writers[index].kick();
            }
        }
    }
    
    void prepareTemporaryVolumeBufferWriters()
    {
        if (_writers == null)
        {
            _writers = new TemporaryVolumeBufferWriter[_bucketCount];
            for (int index = 0; index < _bucketCount; index++)
            {
                _writers[index] = new TemporaryVolumeBufferWriter(index);
            }
        }
    }
    
    private int hashIndex(Volume vol, long page)
    {
        // Important - all pages that hash to the same hash table entry
        // must also belong to the same bucket.
        //
        return (int)(page % _hashTable.length);
    }
    
    int countDirty(Volume vol)
    {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++)
        {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer._vol == vol) &&
                (buffer._status & Buffer.DIRTY_MASK) != 0 )
            {
                count++;
            }
        }
        return count;
    }
    
    int countInUse(Volume vol, boolean writer)
    {
        int count = 0;
        for (int i = 0; i < _bufferCount; i++)
        {
            Buffer buffer = _buffers[i];
            if ((vol == null || buffer._vol == vol) &&
                ((buffer._status & Buffer.CLAIMED_MASK) != 0 &&
                 (!writer || (buffer._status & Buffer.WRITER_MASK) != 0)))
            {
                count++;
            }
        }
        return count;
    }
    
    void populateBufferPoolInfo(ManagementImpl.BufferPoolInfo info)
    {
        info._bufferCount = _bufferCount;
        info._bufferSize = _bufferSize;
        info._getCounter = _getCounter;
        info._hitCounter = _hitCounter;
        int validPages = 0;
        int dirtyPages = 0;
        int readerClaimedPages = 0;
        int writerClaimedPages = 0;
        int reservedPages = 0;
        for (int index = 0; index < _bufferCount; index++)
        {
            Buffer buffer = _buffers[index];
            if (buffer.getReservedGeneration() > 0) reservedPages++;
            int status = buffer.getStatus();
            if ((status & Buffer.VALID_MASK) != 0) validPages++;
            if ((status & Buffer.DIRTY_MASK) != 0) dirtyPages++;
            if ((status & Buffer.WRITER_MASK) != 0) writerClaimedPages++;
            else if ((status & Buffer.CLAIMED_MASK) != 0) readerClaimedPages++;
        }
        info._validPageCount = validPages;
        info._dirtyPageCount = dirtyPages;
        info._readerClaimedPageCount = readerClaimedPages;
        info._writerClaimedPageCount = writerClaimedPages;
        info._reservedPageCount = reservedPages;
        info.updateAcquisitonTime();
    }

    int populateInfo(
        ManagementImpl.BufferInfo[] array,
        int traveralType,
        int includeMask,
        int excludeMask)
    {
        int index = 0;
        switch (traveralType)
        {
            case 0:
                for (int i = 0; i < _bufferCount; i++)
                {
                    Buffer buffer = _buffers[i];
                    if (selected(buffer, includeMask, excludeMask))
                    {
                        populateInfo1(array, index, buffer);
                        index++;
                    }
                }
                break;
                
            case 1:
                for (int bucket = 0; bucket < _bucketCount; bucket++)
                {
                    synchronized(_lock[bucket])
                    {
                        Buffer lru = _lru[bucket];
                        for (Buffer buffer = lru;
                             buffer != null;
                             buffer = buffer._nextLru)
                        {
                            if (selected(buffer, includeMask, excludeMask))
                            {
                                populateInfo1(array, index, buffer);
                                index++;
                            }
                            if (buffer._nextLru == lru) break;
                        }
                    }
                }
                break;
                
            case 2:
                for (int bucket = 0; bucket < _bucketCount; bucket++)
                {
                    synchronized(_lock[bucket])
                    {
                        for (Buffer buffer = _invalidBufferQueue[bucket];
                             buffer != null;
                             buffer = buffer._next)
                        {
                            if (selected(buffer, includeMask, excludeMask))
                            {
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
    
    private static void populateInfo1(
        ManagementImpl.BufferInfo[] array, 
        int index, 
        Buffer buffer)
    {
        if (index < array.length)
        {
            if (array[index] == null) array[index] = 
                new ManagementImpl.BufferInfo();
            buffer.populateInfo(array[index]);
        }
    }
    
//    /**
//     * Selects and collects <tt>Buffer</tt>s from this pool that conform to the
//     * selection specifications.  This method is intended for use only by
//     * the diagnostic utility package.
//     * @param type
//     * @param includeMask
//     * @param excludeMask
//     * @return  An array of <tt>Buffer</tt>s that conform to the selection
//     *          criteria.
//     */
//    public Buffer[] selectBuffers(int type, int includeMask, int excludeMask)
//    {
//        Vector list = new Vector(_bufferCount);
//        switch (type)
//        {
//            case 1:
//                for (int bucket = 0; bucket < _bucketCount; bucket++)
//                {
//                    synchronized(_lock[bucket])
//                    {
//                        Buffer lru = _lru[bucket];
//                        for (Buffer buffer = lru;
//                             buffer != null;
//                             buffer = buffer._nextLru)
//                        {
//                            if (selected(buffer, includeMask, excludeMask))
//                            {
//                                list.add(buffer);
//                            }
//                            if (buffer._nextLru == lru) break;
//                        }
//                    }
//                }
//                break;
//            case 2:
//                for (int bucket = 0; bucket < _bucketCount; bucket++)
//                {
//                    synchronized(_lock[bucket])
//                    {
//                        for (Buffer buffer = _invalidBufferQueue[bucket];
//                             buffer != null;
//                             buffer = buffer._next)
//                        {
//                            if (selected(buffer, includeMask, excludeMask))
//                            {
//                                list.add(buffer);
//                            }
//                        }
//                    }
//                }
//                break;
//            default:
//                for (int i = 0; i < _bufferCount; i++)
//                {
//                    Buffer buffer = _buffers[i];
//                    if (selected(buffer, includeMask, excludeMask))
//                    {
//                        list.add(buffer);
//                    }
//                }
//                break;
//        }
//        Buffer[] array = new Buffer[list.size()];
//        for (int i = 0; i < list.size(); i++)
//        {
//            array[i] = (Buffer)list.elementAt(i);
//        }
//        return array;
//    }
    
    private boolean selected(Buffer buffer, int includeMask, int excludeMask)
    {
        return ((includeMask == 0) ||
                 (buffer._status & includeMask) != 0) &&
                (buffer._status & excludeMask) == 0;
    }
    
    /**
     * @return  Size (in bytes) of each <tt>Buffer</tt> managed by this pool.
     */
    public int getBufferSize()
    {
        return _bufferSize;
    }
    
    /**
     * @return  The count of <tt>Buffer</tt>s managed by this pool.
     */
    public int getBufferCount()
    {
        return _bufferCount;
    }
    
    /**
     * @return  The count of lookup operations for pages images in this pool.
     *          This number, in comparison with the hit counter, indicates
     *          how effective the cache is in reducing disk I/O. 
     */
    public synchronized long getGetCounter()
    {
        return _getCounter;
    }
    
    /**
     * @return  The count of lookup operations for pages images in this pool
     *          for which the page image was already found in this
     *          <tt>BufferPool</tt>. This number, in comparison with the 
     *          get counter, indicates how effective the cache is in
     *          reducing disk I/O. 
     */
    public synchronized long getHitCounter()
    {
        return _hitCounter;
    }
    
    /**
     * Resets the get and hit counters to zero.
     */
    public synchronized void resetCounters()
    {
        _getCounter = 0;
        _hitCounter = 0;
    }
    
    /**
     * Resets the get and hit counters to supplied values.
     * @param gets
     * @param hits
     */
    public synchronized void resetCounters(long gets, long hits)
    {
        _getCounter = gets;
        _hitCounter = hits;
    }
    
    private synchronized void bumpHitCounter()
    {
        _getCounter++;
        _hitCounter++;
    }

    private synchronized void bumpGetCounter()
    {
        _getCounter++;
    }
    
    /**
     * Get the "hit ratio" - the number of hits divided by the number of
     * overall gets.  A value close to 1.0 indicates that most attempts to
     * find data in the <tt>BufferPool</tt> are successful - i.e., that
     * the cache is effectively reducing the need for disk read operations.
     * @return  The ratio
     */
    public synchronized double getHitRatio()
    {
        if (_getCounter == 0) return 0.0;
        else return ((double)_hitCounter) / ((double)_getCounter);
    }
    
    private int bucket(Buffer b)
    {
        return (int)(b._page % _bucketCount);
    }
    /**
     * Invalidate all buffers from a specified Volume.
     * @param volume    The volume
     */
    void invalidate(Volume volume)
    {
        for (int index = 0; index < _buffers.length; index++)
        {
            Buffer buffer = _buffers[index];
            if ((buffer.getVolume() == volume || volume == null) &&
                !buffer.isPermanent() &&
                buffer.isValid())
            {
                invalidate(buffer);
            }
        }
    }
    
    void delete(Volume volume) {
        for (int index = 0; index < _buffers.length; index++)
        {
            Buffer buffer = _buffers[index];
            if (buffer.getVolume() == volume)
            {
                delete(buffer);
            }
        }
    }
    
    private void invalidate(Buffer buffer)
    {
        int bucket = bucket(buffer);
        synchronized(_lock[bucket])
        {
            if (Debug.ENABLED) Debug.$assert (buffer.isValid());
            detach(buffer);
            buffer._status &= (~Buffer.VALID_MASK & ~Buffer.DIRTY_MASK);
            buffer._vol = null;
            buffer._page = 0;
            buffer._next = _invalidBufferQueue[bucket];
            _invalidBufferQueue[bucket] = buffer;
        }
    }
    
    private void delete(Buffer buffer) {
        int bucket = bucket(buffer);
        synchronized(_lock[bucket])
        {
            buffer._status |= Buffer.DELETE_MASK;
            buffer._vol = null;
            buffer._page = 0;
        }

    }

    private void detach(Buffer buffer)
    {
        int bucket = bucket(buffer);
        synchronized(_lock[bucket])
        {
            //
            // If already invalid, we're done.
            //
            if ((buffer._status & Buffer.VALID_MASK) == 0) return;
            
            int hash = hashIndex(buffer._vol, buffer._page);
            //
            // Detach this buffer from the hash table.
            //
            if (_hashTable[hash] == buffer)
            {
                _hashTable[hash] = buffer._next;
            }
            else
            {
                Buffer prev = _hashTable[hash];
                for (Buffer next = prev._next; next != null; next = prev._next)
                {
                    if (next == buffer)
                    {
                        prev._next = next._next;
                        break;
                    }
                    prev = next;
                }
            }
            //       
            // Detach buffer from the LRU queue.
            //
            if (_lru[bucket] == buffer) _lru[bucket] = buffer._nextLru;
            if (_lru[bucket] == buffer) _lru[bucket] = null;
            
            buffer._prevLru._nextLru = buffer._nextLru;
            buffer._nextLru._prevLru = buffer._prevLru;
            //
            // Make the queue for this buffer reflexive.
            //
            buffer._nextLru = buffer;
            buffer._prevLru = buffer;
        }
    }
    
    /**
     * Removes a Buffer from the LRU list and inserts it at the
     * most-recently-used end.
     * @param nub   The Buffer to move
     */
    private void makeMostRecentlyUsed(Buffer buffer)
    {
        // PDB 20051230 - this was a very bad idea because we detach the buffer
        // in allocBuffer and always need to reattach it on release.
        //
        //if (_currentAge - buffer._age < _bufferCount / (_bucketCount * 5)) return;
        
        int bucket = bucket(buffer);
        synchronized(_lock[bucket])
        {
            //
            // Return if buffer is already least-recently-used.
            //       
            if (_lru[bucket] == null)
            {
                _lru[bucket] = buffer;
            }
            else if (_lru[bucket] != buffer)
            {
                //
                // Detach buffer from the list.  (Is a no-op for a buffer
                // that wasn't already on the LRU queue.)
                //
                buffer._prevLru._nextLru = buffer._nextLru;
                buffer._nextLru._prevLru = buffer._prevLru;
                //
                // Now splice it in
                buffer._nextLru = _lru[bucket];
                buffer._prevLru = _lru[bucket]._prevLru;
                _lru[bucket]._prevLru._nextLru = buffer;
                _lru[bucket]._prevLru = buffer;
            }
            if (buffer._findexElements != null)
            {
                if (_findexLru[bucket] == null)
                {
                    _findexLru[bucket] = buffer;
                    if (Debug.ENABLED) checkLru(bucket, buffer, true);
                }
                else if (buffer.isFindexValid() && 
                         _findexLru[bucket] != buffer)
                {
                    //
                    // Detach buffer from the list.  (Is a no-op for a buffer
                    // that wasn't already on the LRU queue.)
                    //
                    buffer._findexPrevLru._findexNextLru = buffer._findexNextLru;
                    buffer._findexNextLru._findexPrevLru = buffer._findexPrevLru;
                    //
                    // Now splice it in
                    buffer._findexNextLru = _findexLru[bucket];
                    buffer._findexPrevLru = _findexLru[bucket]._findexPrevLru;
                    _findexLru[bucket]._findexPrevLru._findexNextLru = buffer;
                    _findexLru[bucket]._findexPrevLru = buffer;
                    if (Debug.ENABLED) checkLru(bucket, buffer, true);
                }
            }
            buffer._age = ++_currentAge;
        }
    }

    /**
     * Find or load a page given its Volume and address.  The returned
     * page has a reader or a writer lock, depending on whether the
     * writer parameter is true on entry. 
     * @param vol           The Volume
     * @param page          The address of the page
     * @param writer        <i>true</i> if a write lock is required.
     * @param wantRead      <i>true</i> if the caller wants the page read
     *                      from disk.  <i>false</i> to allocate a new blank
     *                      page.)
     * @return Buffer       The Buffer describing the buffer containing
     *                      the page.
     * @throws InUseException
     */
    Buffer get(Volume vol, long page, boolean writer, boolean wantRead)
    throws InUseException, 
           InvalidPageAddressException,
           InvalidPageStructureException, 
           RetryException, 
           VolumeClosedException, 
           PersistitIOException
    {
        int hash = hashIndex(vol, page);
        Buffer buffer = null;
        int bucket = (int)(page % _bucketCount);
        for (;;)
        {
            boolean mustRead = false;
            boolean mustClaim = false;
            
            synchronized(_lock[bucket])
            {
                buffer = _hashTable[hash];
                //
                // Search for the page
                //
                while (buffer != null)
                {
                    if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                    
                    if (buffer._page == page && buffer._vol == vol)
                    {
                        //
                        // Found it - now claim it.
                        //
                        if (buffer.claimIfAvailable(writer))
                        {
                            vol.bumpGetCounter();
                            bumpHitCounter();
                            return buffer;
                        }
                        else
                        {
                            mustClaim = true;
                            break;
                        }
                    }
                    buffer = buffer._next; 
                }
                
                if (buffer == null)
                {
                    //
                    // Page not found.  Allocate an available buffer and read
                    // in the page from the Volume.
                    //
                    buffer = allocBuffer(bucket);
                    
                    if (Debug.ENABLED) Debug.$assert(buffer != _hashTable[hash]);
                    if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                    if (Debug.ENABLED) Debug.$assert(buffer.isClean());
                           
                    buffer._page = page;
                    buffer._vol = vol;
                    buffer._hash = hash;
                    buffer._next = _hashTable[hash];
                    _hashTable[hash] = buffer;
                    //
                    // It's not really valid yet, but it does have a writer
                    // claim on it so no other Thread can access it.  In the
                    // meantime, any other Thread seeking access to the same
                    // page will find it.
                    //
                    buffer._status |= Buffer.VALID_MASK;

                    if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                    mustRead = true;
                }
            }
            if (buffer != null)
            {
                if (Debug.ENABLED) Debug.$assert(!mustClaim || !mustRead);
                
                if (mustClaim)
                {
                    if (!buffer.claim(writer))
                    {
                        throw new InUseException(
                            "Thread " + Thread.currentThread().getName() +
                            " failed to acquire " + 
                            (writer ? "writer" : "reader") + " claim on " + buffer);
                    } 
                    //
                    // Test whether the buffer we picked out is still valid
                    //
                    if ((buffer._status & Buffer.VALID_MASK) != 0 &&
                        buffer._page == page && buffer._vol == vol)
                    {
                        //
                        // If so, then we're done.
                        //
                        vol.bumpGetCounter();
                        bumpHitCounter();
                        return buffer;
                    }
                    else
                    {
                        //
                        // If not, release the claim and retry.
                        //
                        buffer.release();
                        continue;
                    }
                }
                if (mustRead)
                {
                    // We're here because the required page was not found
                    // in the pool so we have to read it from the Volume.
                    // We have a writer claim on the buffer, so anyone
                    // else attempting to get this page will simply wait
                    // for us to finish reading it.
                    //
                    // At this point, the Buffer has been fully set up.  It is
                    // on the hash table chain under its new page address,
                    // it is marked valid, and this Thread has a writer claim.
                    // If the read attempt fails, we need to mark the page
                    // INVALID so that any Thread waiting for access to
                    // this buffer will not use it. We also need to demote
                    // the writer claim to a reader claim unless the caller
                    // originally asked for a writer claim.
                    //
                    if (wantRead)
                    {
                        boolean loaded = false;
                        try
                        {
                            if (Debug.ENABLED) Debug.$assert(
                                buffer.getPageAddress() == page &&
                                buffer.getVolume() == vol &&
                                buffer.getHash() == hash);

                            buffer.load(vol, page);
                            loaded = true;
                        }
                        finally
                        {
                            if (!loaded)
                            {
                                invalidate(buffer);
                                buffer.release();
                            }
                        }
                        vol.bumpGetCounter();
                        bumpGetCounter();
                    }
                    else
                    {
                        buffer.clear();
                        buffer.init(Buffer.PAGE_TYPE_UNALLOCATED, "initGetWithoutRead");
                    }
                    if (!writer) buffer.releaseWriterClaim();
                    return buffer;
                }
            }
        }
    }
    
    /**
     * Returns a copy of Buffer.  The returned buffer is newly created, is not
     * a member of the buffer pool, and is not claimed.  There is no guarantee
     * that the content of this copy is internally consistent because another
     * thread may be modifying the buffer while the copy is being made.  The
     * returned Buffer should be used only for display and diagnostic purposes.
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
    throws InvalidPageAddressException,
           InvalidPageStructureException, 
           VolumeClosedException,
           TimeoutException,
           PersistitIOException
    {
        int hash = hashIndex(vol, page);
        int bucket = (int)(page % _bucketCount);
        Buffer buffer = null;
        long expirationTime =
            System.currentTimeMillis() + Buffer.DEFAULT_MAX_WAIT_TIME;
        
        for(;;)
        {
            synchronized(_lock[bucket])
            {
                buffer = _hashTable[hash];
                //
                // Search for the page
                //
                while (buffer != null)
                {
                    if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                        
                    if (buffer._page == page && buffer._vol == vol)
                    {
                        if (Debug.ENABLED) Debug.$assert(
                            (buffer._status & Buffer.VALID_MASK) != 0);
                        //
                        // Found it - now return a copy of it.
                        //
                        return new Buffer(buffer);
                    }
                    buffer = buffer._next; 
                }
            }
            //
            // Didn't find it in the pool, so we'll read a copy.
            //
            buffer = new Buffer(_bufferSize, -1, this, _persistit);
            try
            {
                buffer.load(vol, page);
                return buffer;
            }
            catch (RetryException re)
            {
                if (System.currentTimeMillis() > expirationTime)
                {
                    throw new TimeoutException();
                }
                _persistit.getPrewriteJournal().waitForReservation(re);
            }
        }
    }

    
    void release(Buffer buffer)
    {
        if ((buffer._status & Buffer.VALID_MASK) != 0 &&
            (buffer._status & Buffer.PERMANENT_MASK) == 0) 
        {
            makeMostRecentlyUsed(buffer);
        }
        buffer.release();
    }

    /**
     * Returns an available buffer.  The replacement policy is to
     * return a buffer that's already been marked invalid, if available.
     * Otherwise traverse the least-recently-used queue to find the least-
     * recently-used buffer that is not claimed.  Throws a RetryException if
     * there is no available buffer.
     * 
     * @param bucket    The cache bucket
     * @return Buffer   An available buffer, or <i>null</i> if no buffer is
     *                  currently available.  The buffer has a writer claim.
     * @throws  RetryException  If no buffer is available. 
     */
    private Buffer allocBuffer(int bucket)
    throws RetryException
    {
        // No need to synchronize here because the only caller is get(...).
        Buffer lastBuffer = null;
        int dirtyTemporaryVolumeBuffers = 0;
        int dirtyReservedBuffers = 0;
        Buffer buffer = _invalidBufferQueue[bucket];
        boolean found = false;
        while (buffer != null)
        {
            if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
            
            if (buffer.isAvailable() && buffer.claimIfAvailable(true))
            {
                if (lastBuffer != null) lastBuffer._next = buffer._next;
                else _invalidBufferQueue[bucket] = buffer._next;
                found = true;
                break;
            }
            else if (buffer.isDirty())
            {
                if (buffer.getReservedGeneration() == 0)
                {
                    dirtyTemporaryVolumeBuffers++;
                }
                else
                {
                    dirtyReservedBuffers++;
                }
            }
            lastBuffer = buffer;
            buffer = buffer._next;
        }
        
        if (!found)
        {
            buffer = _lru[bucket];
            while (buffer != null)
            {
                if (buffer.isAvailable() && buffer.claimIfAvailable(true))
                {
                    detach(buffer);                
                    found = true;
                    break;
                }
                else if (buffer.isDirty())
                {
                    if (buffer.getReservedGeneration() == 0)
                    {
                        dirtyTemporaryVolumeBuffers++;
                    }
                    else
                    {
                        dirtyReservedBuffers++;
                    }
                }
                buffer = buffer._nextLru;
                if (buffer == _lru[bucket]) break;
            }
        }
        
        boolean manyDirtyTempBuffers = 
            dirtyTemporaryVolumeBuffers >= (_bufferCount / _bucketCount) / 8;
            
        if (found)
        {
            if (manyDirtyTempBuffers) _writers[bucket].kick();
            return buffer;
        }
        
        boolean manyDirtyReservedBuffers =
            dirtyReservedBuffers >= (_bufferCount / _bucketCount) / 8;
            
        if (manyDirtyTempBuffers)
        {
            _writers[bucket].kick();
        }
        if (manyDirtyReservedBuffers)
        {
            _persistit.getPrewriteJournal().kick();
            //throw PrewriteJournal.get().getRetryException();
        }
        
        throw TV_RETRY_EXCEPTION;
    }
    
    void allocFindex(Buffer needyBuffer)
    {
        // No need to synchronize here because the only caller is get(...).
        //
        if (needyBuffer._findexElements != null) return;
        int bucket = needyBuffer.getIndex() % _bucketCount;
        synchronized(_lock[bucket])
        {
            needyBuffer.invalidateFindex();
            //
            // Look in the Findex LRU queue for an LRU buffer to reappropriate 
            // to this buffer.
            //
            Buffer buffer = _findexLru[bucket];
            while (buffer != null)
            {
                if (buffer._findexElements != null &&
                    !buffer.isClaimed())    // a "quick" check
                {
                    if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                    
                    if (buffer.claimIfAvailable(true))
                    {
                        int[] elements = buffer.takeFindexElements();
                        buffer.release();
                        
                        if (elements != null)
                        {
                            if (_findexLru[bucket] == buffer)
                            {
                                _findexLru[bucket] = buffer._findexNextLru;
                                if (_findexLru[bucket] == buffer)
                                {
                                    _findexLru[bucket] = null;
                                }
                            }
                            buffer._findexPrevLru._findexNextLru = buffer._findexNextLru;
                            buffer._findexNextLru._findexPrevLru = buffer._findexPrevLru;
                            buffer._findexPrevLru = buffer;
                            buffer._findexNextLru = buffer;
                            needyBuffer.giveFindexElements(elements);
                            if (Debug.ENABLED) checkLru(bucket, buffer, false);
                            return;
                        }
                    }
                }
                buffer = buffer._findexNextLru;
                if (buffer == _findexLru[bucket]) break;
            }
            //
            // Finally, if we can't find any available Findex arrays we'll create
            // a new one.
            //
            needyBuffer.createFindex();
        }
    }

    private void checkLru(int bucket, Buffer b, boolean included)
    {
        if (Debug.ENABLED && Debug.VERIFY_PAGES)
        {
            boolean found = false;
            Buffer buffer = _findexLru[bucket];
            if (buffer == null)
            {
                Debug.$assert(!included);
                return;
            } 
            
            else if (buffer._findexNextLru == buffer._findexPrevLru)
            {
                Debug.$assert(!included || buffer == b);
                return;
            }
            
            else for (int i = 0; i < _buffers.length; i++)
            {
                Buffer previous = buffer;
                buffer = buffer._findexNextLru;
                if (buffer == b) found = true;
                Debug.$assert(buffer != null && buffer != previous);
                Debug.$assert(buffer._findexPrevLru == previous);
                if (buffer == _findexLru[bucket])
                {
                    Debug.$assert(included == found);
                    return;
                }
                Debug.$assert(previous != buffer);
            }
            Debug.$assert(false);
            
            return;
        }
    }
    
    private int gatherDirtyTemporaryVolumeBuffers(int bucket, Buffer[] needWriting)
    {
        int count = 0;
        int maxCount = needWriting.length;
        int invalidDepth = 64;
        int lruDepth = (_bufferCount / _bucketCount) / 2;
        synchronized(_lock[bucket])
        {
            Buffer buffer = _invalidBufferQueue[bucket];
            while (buffer != null && invalidDepth-- > 0 && count < maxCount)
            {
                if (Debug.ENABLED) Debug.$assert(buffer._next != buffer);
                if (buffer.isDirty() &&
                    buffer.getReservedGeneration() == 0 && 
                    (buffer.getStatus() & Buffer.WRITER_MASK) == 0)
                {
                    needWriting[count++] = buffer;
                }
                buffer = buffer._next;
            }
            
            buffer = _lru[bucket];
            while (buffer != null && lruDepth-- > 0 && count < maxCount)
            {
                if (buffer.isDirty() &&
                    buffer.getReservedGeneration() == 0 && 
                    (buffer.getStatus() & Buffer.WRITER_MASK) == 0)
                {
                    needWriting[count++] = buffer;
                }
                buffer = buffer._nextLru;
                if (buffer == _lru[bucket]) break;
            }
        }
        return count;
    }
    
    private class TemporaryVolumeBufferWriter
    implements Runnable
    {
        int _bucket;
        boolean _kicked;
        Buffer[] _needWriting = new Buffer[(_bufferCount / _bucketCount) / 4];
        
        TemporaryVolumeBufferWriter(int bucket)
        {
            _bucket = bucket;
            new Thread(this, "TVWRITER" + _bufferSize + ":" + bucket).start();
        }
        
        public void kick()
        {
            synchronized(this)
            {
                if (!_kicked)
                {
                    notify();
                    _kicked = true;
                }
            }
        }
        
        public void run()
        {
            while (!_closed)
            {
                int count = 
                    gatherDirtyTemporaryVolumeBuffers(_bucket, _needWriting);
                for (int index = 0; index < count; index++)
                {
                    Buffer buffer = _needWriting[index];
                    Volume volume = buffer.getVolume();
                    long page = buffer.getPageAddress();
                    _needWriting[index] = null;
                    boolean claimed = buffer.claimIfAvailable(false);
                    try
                    {
                        if (claimed && buffer.isDirty())
                        {
                            if (volume != null)
                            {
                                if (Debug.ENABLED)
                                {
                                    Debug.$assert(volume.isTemporary());
                                }
                                if (!volume.isClosed())
                                {
                                    buffer.clearSlack();
                                    buffer.save();
                                    volume.writePage(
                                        buffer.getBytes(), 
                                        0, 
                                        buffer.getBufferSize(), 
                                        page);
                                }
                            }
                            buffer.clean();
                        }
                    }
                    catch (Exception e)
                    {
                        _persistit.getLogBase().log(LogBase.LOG_EXCEPTION,
                            e + " while writing page " + page + 
                            " in volume " + volume);
                    }
                    finally
                    {
                        if (claimed) buffer.release();
                    }
                }
                
                try
                {
                    synchronized(this)
                    {
                        _kicked = false;
                        wait(_writerPollInterval);
                    }
                }
                catch (InterruptedException ie)
                {
                }
            }
        }
    }
}
