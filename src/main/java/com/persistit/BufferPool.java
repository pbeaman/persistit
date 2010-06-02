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
import java.util.concurrent.atomic.AtomicLong;

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
	private AtomicLong _getCounter = new AtomicLong();

	/**
	 * Count of buffer pool hits (buffer found in pool)
	 */
	private AtomicLong _hitCounter = new AtomicLong();

	/**
	 * Current age - used to determine whether buffer needs to be moved up on
	 * most recently used list.
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

	private PageWriter[] _writers;

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
		_currentAge = count + 1;
		_bufferSize = size;
		_buffers = new Buffer[count];
		_bucketCount = (count / PAGES_PER_BUCKET) + 1;
		_hashTable = new Buffer[((count * HASH_MULTIPLE) / _bucketCount)
				* _bucketCount];

		_invalidBufferQueue = new Buffer[_bucketCount];
		_lru = new Buffer[_bucketCount];
		_findexLru = new Buffer[_bucketCount];
		_lock = new Object[_bucketCount];
		for (int bucket = 0; bucket < _bucketCount; bucket++) {
			_lock[bucket] = new Object();
		}

		int findexCount = _bufferCount / FINDEX_RATIO;

		if (findexCount < MINIMUM_FINDEX_COUNT * _bucketCount) {
			findexCount = MINIMUM_FINDEX_COUNT * _bucketCount;
		}
		try {

		for (int index = 0; index < count; index++) {
			Buffer buffer = new Buffer(size, index, this, _persistit);
			int bucket = index % _bucketCount;
			buffer._next = _invalidBufferQueue[bucket];
			_invalidBufferQueue[bucket] = buffer;
			_buffers[index] = buffer;
			if (findexCount-- > 0) {
				buffer.createFindex();
				if (_findexLru[bucket] == null) {
					_findexLru[bucket] = buffer;
					buffer._findexNextLru = buffer;
					buffer._findexPrevLru = buffer;
				} else {
					buffer._findexNextLru = _findexLru[bucket];
					buffer._findexPrevLru = _findexLru[bucket]._findexPrevLru;
					_findexLru[bucket]._findexPrevLru._findexNextLru = buffer;
					_findexLru[bucket]._findexPrevLru = buffer;
				}
			}
			created++;
		}

		_writers = new PageWriter[_bucketCount];
		for (int index = 0; index < _bucketCount; index++) {
			_writers[index] = new PageWriter(index);
		}
		} catch (OutOfMemoryError e) {
			System.out.println("Out of memory after creating " + created + " buffers");
			throw e;
		}

	}

	void close() {
		synchronized (this) {
			_closed = true;
		}
		if (_writers != null) {
			for (int index = 0; index < _writers.length; index++) {
				_writers[index].kick();
			}
			boolean stopped = false;
			while (!stopped) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException ie) {
				}
				stopped = true;
				for (int index = 0; index < _writers.length; index++) {
					stopped &= _writers[index]._stopped;
				}
			}
		}
	}

	private int hashIndex(Volume vol, long page) {
		// Important - all pages that hash to the same hash table entry
		// must also belong to the same bucket.
		//
		return (int) (page % _hashTable.length);
	}

	int countDirty(Volume vol) {
		int count = 0;
		for (int i = 0; i < _bufferCount; i++) {
			Buffer buffer = _buffers[i];
			if ((vol == null || buffer._vol == vol)
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
			if ((vol == null || buffer._vol == vol)
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
		int reservedPages = 0;
		for (int index = 0; index < _bufferCount; index++) {
			Buffer buffer = _buffers[index];
			if (buffer.getReservedGeneration() > 0)
				reservedPages++;
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
		info._reservedPageCount = reservedPages;
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
					for (Buffer buffer = lru; buffer != null; buffer = buffer._nextLru) {
						if (selected(buffer, includeMask, excludeMask)) {
							populateInfo1(array, index, buffer);
							index++;
						}
						if (buffer._nextLru == lru)
							break;
					}
				}
			}
			break;

		case 2:
			for (int bucket = 0; bucket < _bucketCount; bucket++) {
				synchronized (_lock[bucket]) {
					for (Buffer buffer = _invalidBufferQueue[bucket]; buffer != null; buffer = buffer._next) {
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
	// buffer = buffer._nextLru)
	// {
	// if (selected(buffer, includeMask, excludeMask))
	// {
	// list.add(buffer);
	// }
	// if (buffer._nextLru == lru) break;
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
	// buffer = buffer._next)
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
		return (int) (b._page % _bucketCount);
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
			buffer._vol = null;
			buffer._page = 0;
			buffer._next = _invalidBufferQueue[bucket];
			_invalidBufferQueue[bucket] = buffer;
		}
	}

	private void delete(Buffer buffer) {
		int bucket = bucket(buffer);
		synchronized (_lock[bucket]) {
			buffer._status |= SharedResource.DELETE_MASK;
			buffer._vol = null;
			buffer._page = 0;
		}

	}

	private void detach(Buffer buffer) {
		int bucket = bucket(buffer);
		synchronized (_lock[bucket]) {
			//
			// If already invalid, we're done.
			//
			if ((buffer._status & SharedResource.VALID_MASK) == 0)
				return;

			int hash = hashIndex(buffer._vol, buffer._page);
			//
			// Detach this buffer from the hash table.
			//
			if (_hashTable[hash] == buffer) {
				_hashTable[hash] = buffer._next;
			} else {
				Buffer prev = _hashTable[hash];
				for (Buffer next = prev._next; next != null; next = prev._next) {
					if (next == buffer) {
						prev._next = next._next;
						break;
					}
					prev = next;
				}
			}
			//       
			// Detach buffer from the LRU queue.
			//
			if (_lru[bucket] == buffer)
				_lru[bucket] = buffer._nextLru;
			if (_lru[bucket] == buffer)
				_lru[bucket] = null;

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
				buffer._prevLru._nextLru = buffer._nextLru;
				buffer._nextLru._prevLru = buffer._prevLru;
				//
				// Now splice it in
				//
				buffer._nextLru = _lru[bucket];
				buffer._prevLru = _lru[bucket]._prevLru;
				_lru[bucket]._prevLru._nextLru = buffer;
				_lru[bucket]._prevLru = buffer;
			}
			if (buffer._findexElements != null) {
				if (_findexLru[bucket] == null) {
					_findexLru[bucket] = buffer;
					if (Debug.ENABLED)
						checkLru(bucket, buffer, true);
				} else if (buffer.isFindexValid()
						&& _findexLru[bucket] != buffer) {
					//
					// Detach buffer from the list. (Is a no-op for a buffer
					// that wasn't already on the LRU queue.)
					//
					buffer._findexPrevLru._findexNextLru = buffer._findexNextLru;
					buffer._findexNextLru._findexPrevLru = buffer._findexPrevLru;
					//
					// Now splice it in
					//
					buffer._findexNextLru = _findexLru[bucket];
					buffer._findexPrevLru = _findexLru[bucket]._findexPrevLru;
					_findexLru[bucket]._findexPrevLru._findexNextLru = buffer;
					_findexLru[bucket]._findexPrevLru = buffer;
					if (Debug.ENABLED)
						checkLru(bucket, buffer, true);
				}
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
			PersistitIOException {
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
						Debug.$assert(buffer._next != buffer);

					if (buffer._page == page && buffer._vol == vol) {
						//
						// Found it - now claim it.
						//
						if (buffer.claim(writer, 0)) {
							vol.bumpGetCounter();
							bumpHitCounter();
							return buffer;
						} else {
							mustClaim = true;
							break;
						}
					}
					buffer = buffer._next;
				}

				if (buffer == null) {
					//
					// Page not found. Allocate an available buffer and read
					// in the page from the Volume.
					//
					buffer = allocBuffer(bucket);

					if (Debug.ENABLED)
						Debug.$assert(buffer != _hashTable[hash]);
					if (Debug.ENABLED)
						Debug.$assert(buffer._next != buffer);
					if (Debug.ENABLED)
						Debug.$assert(buffer.isClean());

					buffer._page = page;
					buffer._vol = vol;
					buffer._hash = hash;
					buffer._next = _hashTable[hash];
					_hashTable[hash] = buffer;
					//
					// It's not really valid yet, but it does have a writer
					// claim on it so no other Thread can access it. In the
					// meantime, any other Thread seeking access to the same
					// page will find it.
					//
					buffer._status |= SharedResource.VALID_MASK;

					if (Debug.ENABLED)
						Debug.$assert(buffer._next != buffer);
					mustRead = true;
				}
			}
			if (buffer != null) {
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
					if ((buffer._status & SharedResource.VALID_MASK) != 0
							&& buffer._page == page && buffer._vol == vol) {
						//
						// If so, then we're done.
						//
						vol.bumpGetCounter();
						bumpHitCounter();
						return buffer;
					} else {
						//
						// If not, release the claim and retry.
						//
						buffer.release();
						continue;
					}
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
										&& buffer.getHash() == hash);

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
					Debug.$assert(buffer._next != buffer);

				if (buffer._page == page && buffer._vol == vol) {
					if (Debug.ENABLED)
						Debug
								.$assert((buffer._status & SharedResource.VALID_MASK) != 0);
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
	 * @throws RetryException
	 *             If no buffer is available.
	 */
	private Buffer allocBuffer(int bucket) throws PersistitIOException,
			InvalidPageStructureException {
		// No need to synchronize here because the only caller is get(...).
		Buffer lastBuffer = null;
		int dirtyBuffers = 0;
		Buffer buffer = _invalidBufferQueue[bucket];
		boolean found = false;

		/**
		 * First search the Invalid Buffer Queue
		 */
		while (buffer != null) {
			if (Debug.ENABLED)
				Debug.$assert(buffer._next != buffer);

			if (buffer.isAvailable() && buffer.isClean()
					&& buffer.claim(true, 0)) {
				if (lastBuffer != null)
					lastBuffer._next = buffer._next;
				else
					_invalidBufferQueue[bucket] = buffer._next;
				found = true;
				break;
			} else if (buffer.isDirty()) {
				dirtyBuffers++;
			}
			lastBuffer = buffer;
			buffer = buffer._next;
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
				} else if (buffer.isDirty()) {
					dirtyBuffers++;
				}
				buffer = buffer._nextLru;
				if (buffer == _lru[bucket])
					break;
			}
		}
		/**
		 * And finally, search for a dirty buffer near the top of the LRU queue
		 */
		if (!found) {
			buffer = _lru[bucket];
			while (buffer != null) {
				if (buffer.isAvailable() && buffer.claim(true, 0)) {
					detach(buffer);
					found = true;
					break;
				} else if (buffer.isDirty()) {
					dirtyBuffers++;
				}
				buffer = buffer._nextLru;
				if (buffer == _lru[bucket])
					break;
			}
		}

		boolean manyDirtyReservedBuffers = dirtyBuffers >= (_bufferCount / _bucketCount) / 2;
		if (manyDirtyReservedBuffers) {
			// kick the flusher
		}

		if (found) {
			/**
			 * The Buffer could have been dirty or could be been dirtied by
			 * another thread while claiming it. If it's dirty, just force it
			 * out to the Log right now.
			 */
			if (buffer.isDirty()) {
				buffer.clearSlack();
				buffer.save();
				_persistit.getLogManager().writePageToLog(buffer);
				buffer.clean();
			}
			return buffer;
		}

		// TODO - fix this for real.
		throw new IllegalStateException("Buffers exhausted.");
	}

	void allocFindex(Buffer needyBuffer) {
		// No need to synchronize here because the only caller is get(...).
		//
		if (needyBuffer._findexElements != null)
			return;
		int bucket = needyBuffer.getIndex() % _bucketCount;
		synchronized (_lock[bucket]) {
			needyBuffer.invalidateFindex();
			//
			// Look in the Findex LRU queue for an LRU buffer to reappropriate
			// to this buffer.
			//
			Buffer buffer = _findexLru[bucket];
			while (buffer != null) {
				if (buffer._findexElements != null && !buffer.isClaimed()) // a
				// "quick"
				// check
				{
					if (Debug.ENABLED)
						Debug.$assert(buffer._next != buffer);

					if (buffer.claim(true, 0)) {
						int[] elements = buffer.takeFindexElements();
						buffer.release();

						if (elements != null) {
							if (_findexLru[bucket] == buffer) {
								_findexLru[bucket] = buffer._findexNextLru;
								if (_findexLru[bucket] == buffer) {
									_findexLru[bucket] = null;
								}
							}
							buffer._findexPrevLru._findexNextLru = buffer._findexNextLru;
							buffer._findexNextLru._findexPrevLru = buffer._findexPrevLru;
							buffer._findexPrevLru = buffer;
							buffer._findexNextLru = buffer;
							needyBuffer.giveFindexElements(elements);
							if (Debug.ENABLED)
								checkLru(bucket, buffer, false);
							return;
						}
					}
				}
				buffer = buffer._findexNextLru;
				if (buffer == _findexLru[bucket])
					break;
			}
			//
			// Finally, if we can't find any available Findex arrays we'll
			// create
			// a new one.
			//
			needyBuffer.createFindex();
		}
	}

	private void checkLru(int bucket, Buffer b, boolean included) {
		if (Debug.ENABLED && Debug.VERIFY_PAGES) {
			boolean found = false;
			Buffer buffer = _findexLru[bucket];
			if (buffer == null) {
				Debug.$assert(!included);
				return;
			}

			else if (buffer._findexNextLru == buffer._findexPrevLru) {
				Debug.$assert(!included || buffer == b);
				return;
			}

			else
				for (int i = 0; i < _buffers.length; i++) {
					Buffer previous = buffer;
					buffer = buffer._findexNextLru;
					if (buffer == b)
						found = true;
					Debug.$assert(buffer != null && buffer != previous);
					Debug.$assert(buffer._findexPrevLru == previous);
					if (buffer == _findexLru[bucket]) {
						Debug.$assert(included == found);
						return;
					}
					Debug.$assert(previous != buffer);
				}
			Debug.$assert(false);

			return;
		}
	}

	private int gatherDirtyBuffers(int bucket, Buffer[] needWriting) {
		int count = 0;
		int maxCount = needWriting.length;
		int invalidDepth = 64;
		int unavailable = 0;
		synchronized (_lock[bucket]) {
			Buffer buffer = _invalidBufferQueue[bucket];
			int lruCount = _closed ? maxCount : _bufferCount / _bucketCount / 2;
			int lazyCount = _closed ? maxCount : _bufferCount / _bucketCount
					/ 8;
			while (buffer != null && invalidDepth-- > 0 && count < maxCount) {
				if (Debug.ENABLED)
					Debug.$assert(buffer._next != buffer);
				if (buffer.isDirty()) {
					if ((buffer.getStatus() & SharedResource.WRITER_MASK) == 0) {
						needWriting[count++] = buffer;
					} else {
						unavailable++;
					}
				}
				buffer = buffer._next;
			}

			buffer = _lru[bucket];
			while (buffer != null && count < maxCount) {
				if (buffer.isDirty()) {
					if ((buffer.getStatus() & SharedResource.WRITER_MASK) == 0) {
						needWriting[count++] = buffer;
					} else {
						unavailable++;
					}
				}
				buffer = buffer._nextLru;
				if (buffer == _lru[bucket]) {
					break;
				}
				if (--lruCount < 0 && count > lazyCount) {
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

	private class PageWriter implements Runnable {
		int _bucket;
		boolean _kicked;
		boolean _stopped = false;
		Buffer[] _needWriting = new Buffer[(_bufferCount / _bucketCount) / 4];

		PageWriter(int bucket) {
			_bucket = bucket;
			final Thread thread = new Thread(this, "PAGE_WRITER" + _bufferSize
					+ ":" + bucket);
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

		public void run() {
			try {
				while (true) {
					int count = gatherDirtyBuffers(_bucket, _needWriting);
					for (int index = 0; index < count; index++) {
						final Buffer buffer = _needWriting[index];
						final Volume volume = buffer.getVolume();
						long page = buffer.getPageAddress();
						_needWriting[index] = null;
						boolean claimed = buffer.claim(false, 0);
						try {
							if (claimed && buffer.isDirty()) {
								if (volume != null) {
									buffer.clearSlack();
									buffer.save();
									_persistit.getLogManager().writePageToLog(
											buffer);
								}
								buffer.clean();
							}
						} catch (Exception e) {
							_persistit.getLogBase().log(
									LogBase.LOG_EXCEPTION,
									e + " while writing page " + page
											+ " in volume " + volume);
						} finally {
							if (claimed) {
								buffer.release();
							}
						}
					}

					try {
						synchronized (this) {
							_kicked = false;
							if (_closed && count == 0) {
								break;
							}
							if (!_closed || count < 0) {
								wait(_writerPollInterval);
							}
						}
					} catch (InterruptedException ie) {
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				_stopped = true;
			}
		}
	}
}
