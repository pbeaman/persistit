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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import com.persistit.Exchange.Sequence;
import com.persistit.Management.RecordInfo;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.InvalidPageTypeException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RebalanceException;
import com.persistit.exception.VolumeClosedException;

/**
 * <p>
 * Memory structure that holds and manipulates the state of a fixed-length
 * <i>page</i> of the database. Persistit manipulates the content of a page by
 * copying it into a <code>Buffer</code>, reading and/or modifying modifying the
 * <code>Buffer</code>, and then writing the <code>Buffer</code>'s content back
 * into the page. There are several types of pages within the BTree structure -
 * e.g., index pages and data pages. A <code>Buffer</code> can hold and
 * manipulate any kind of page.
 * </p>
 * <p>
 * Multiple <code>Buffer</code>s are managed by a {@link BufferPool} that
 * provides methods to locate, load and store the contents of the
 * <code>Buffer</code>.
 * </p>
 * <p>
 * <code>Buffer</code> provides the low-level methods that organize keys and
 * values into byte arrays to be stored in database files.
 * </p>
 * 
 * @version 1.0
 */

public final class Buffer extends SharedResource {

    /**
     * Architectural lower bound on buffer size
     */
    public final static int MIN_BUFFER_SIZE = 1024;
    /**
     * Architectural upper bound on buffer size
     */
    public final static int MAX_BUFFER_SIZE = 16384;
    /**
     * Page type code for an unallocated page.
     */
    public final static int PAGE_TYPE_UNALLOCATED = 0;
    /**
     * Page type code for a data page. Data pages are the leaf pages of the
     * BTree.
     */
    public final static int PAGE_TYPE_DATA = 1;
    /**
     * Minimum page type code for an index page. Index pages are the non-leaf
     * pages of the BTree.
     */
    public final static int PAGE_TYPE_INDEX_MIN = 2;

    /**
     * Maximum page type code for an index page. Index pages are the non-leaf
     * pages of the BTree.
     */
    public final static int PAGE_TYPE_INDEX_MAX = 21;
    /**
     * Page type code for a page that has been converted to hold a list of
     * garbage chains. Pages deallocated as a result of (@link Exchange#remove)
     * operations are added organized into Garbage pages.
     */
    public final static int PAGE_TYPE_GARBAGE = 30;
    /**
     * Page type code for pages used to store continuation bytes for long
     * records
     */
    public final static int PAGE_TYPE_LONG_RECORD = 31;
    /**
     * Largest value page type code.
     */
    public final static int PAGE_TYPE_MAX = 31;

    public final static int PAGE_TYPE_HEAD = 32;

    public final static String[] TYPE_NAMES = { "Unused", // 0
            "Data", // 1
            "Index1", // 2
            "Index2", // 3
            "Index3", // 4
            "Index4", // 5
            "Index5", // 6
            "Index6", // 7
            "Index7", // 8
            "Index8", // 9
            "Index9", // 10
            "Index10", // 11
            "Index11", // 12
            "Index12", // 13
            "Index13", // 14
            "Index14", // 15
            "Index15", // 16
            "Index16", // 17
            "Index17", // 18
            "Index18", // 19
            "Index19", // 20
            "Index20", // 21
            "Invalid", // 22
            "Invalid", // 23
            "Invalid", // 24
            "Invalid", // 25
            "Invalid", // 26
            "Invalid", // 27
            "Invalid", // 28
            "Invalid", // 29
            "Garbage", // 30
            "LongRec", // 31
            "Head", // 32
    };

    /**
     * Size of standard header portion of each page type
     */
    public final static int HEADER_SIZE = 32;

    // TODO - allow larger pointer size
    final static long MAX_VALID_PAGE_ADDR = Integer.MAX_VALUE - 1;
    /**
     * A <code>Buffer</code> contains header information, a series of contiguous
     * key blocks, each of which contains a Elided Byte Count (EBC), one byte of
     * the key sequence, called the discriminator byte (DB), and an offset to
     * another section of the buffer that contains the bytes of the key value
     * and the payload (TAIL). These three values are coded into an int (4
     * bytes) so that every key block is int-aligned. The TAIL value is the
     * offset within this buffer of the tail block. It is always a multiple of
     * 4, making the tail block int-aligned too.
     * 
     * Offsets to fields in header:
     */
    final static int TYPE_OFFSET = 0;
    final static int BUFFER_LENGTH_OFFSET = 1;
    final static int KEY_BLOCK_END_OFFSET = 2;
    final static int FREE_OFFSET = 4;
    final static int SLACK_OFFSET = 6;
    final static int PAGE_ADDRESS_OFFSET = 8;
    final static int RIGHT_SIBLING_OFFSET = 16;
    final static int TIMESTAMP_OFFSET = 24;

    // Offset within page of first KeyBlock
    final static int KEY_BLOCK_START = HEADER_SIZE;

    // (Constant) length of the key block
    final static int KEYBLOCK_LENGTH = 4;

    // Mask to form pointer to a key block
    final static int KEYBLOCK_MASK = 0xFFFFFFFC;

    // Length of tail block
    final static int TAILBLOCK_HDR_SIZE_DATA = 4;
    final static int TAILBLOCK_HDR_SIZE_INDEX = 8;
    final static int TAILBLOCK_POINTER = 4;
    final static int TAILBLOCK_FACTOR = 4;
    final static int TAILBLOCK_MASK = 0xFFFFFFFC;

    final static int TAILBLOCK_SIZE_MASK = 0x0000FFFF;
    final static int TAILBLOCK_KLENGTH_MASK = 0x0FFF0000;
    final static int TAILBLOCK_INUSE_MASK = 0x10000000;
    // final static int TAILBLOCK_SIZE_SHIFT = 0;
    final static int TAILBLOCK_KLENGTH_SHIFT = 16;

    // Flag indicates exact match
    final static int EXACT_MASK = 1;

    final static int P_MASK = 0x0000FFFC;

    private final static int DEPTH_MASK = 0x0FFF0000;
    private final static int DEPTH_SHIFT = 16;
    private final static int FIXUP_MASK = 0x40000000;

    // Mask for the discriminator byte field within a keyblock
    private final static int DB_MASK = 0x000000FF;

    // Mask for the key common count field within a keyblock
    private final static int EBC_MASK = 0x000FFF00;

    // Mask for the tail pointer within a keyblock
    private final static int TAIL_MASK = 0xFFF00000;

    // Shift for discriminator byte field
    // private final static int DB_SHIFT = 0;

    // Shift for key common count field
    private final static int EBC_SHIFT = 8;

    // Shift for tail pointer
    private final static int TAIL_SHIFT = 20 - 2;

    final static int GARBAGE_BLOCK_SIZE = 32;

    private final static int GARBAGE_BLOCK_STATUS = 4;
    private final static int GARBAGE_BLOCK_LEFT_PAGE = 8;
    private final static int GARBAGE_BLOCK_RIGHT_PAGE = 16;
    private final static int GARBAGE_BLOCK_EXPECTED_COUNT = 24;

    /**
     * 
     */
    final static int LONGREC_TYPE = 255;
    final static int LONGREC_PREFIX_SIZE_OFFSET = 2;
    final static int LONGREC_SIZE_OFFSET = 4;
    final static int LONGREC_PAGE_OFFSET = 12;
    final static int LONGREC_PREFIX_SIZE = 100;
    final static int LONGREC_PREFIX_OFFSET = 20;
    final static int LONGREC_SIZE = LONGREC_PREFIX_OFFSET + LONGREC_PREFIX_SIZE;

    /**
     * Implicit overhead size
     */
    final static int INDEX_PAGE_OVERHEAD = KEY_BLOCK_START + 2
            * KEYBLOCK_LENGTH + 2 * TAILBLOCK_HDR_SIZE_INDEX;

    final static int DATA_PAGE_OVERHEAD = KEY_BLOCK_START + 2 * KEYBLOCK_LENGTH
            + 2 * TAILBLOCK_HDR_SIZE_DATA;

    private final static Stack<int[]> REPACK_BUFFER_STACK = new Stack<int[]>();

    public final static int MAX_KEY_RATIO = 16;

    // For debugging - set true in debugger to create verbose toString() output.
    //
    boolean _toStringDebug = false;

    /**
     * The BufferPool in which this buffer is allocated.
     */
    final private BufferPool _pool;

    /**
     * Index within the buffer pool
     */
    final private int _poolIndex;

    /**
     * The page address of the page currently loaded in this buffer.
     */
    private long _page;

    /**
     * The Volume from which the page was loaded.
     */
    private Volume _vol;

    /**
     * Timestamp of last Transaction to modify this resource
     */
    private volatile long _timestamp;

    /**
     * A ByteBuffer facade for this Buffer used in NIO operations
     */
    private ByteBuffer _byteBuffer;

    /**
     * The bytes in this buffer. Note these bytes are also the backing store of
     * _byteBuffer.
     */
    private byte[] _bytes;

    /**
     * The right sibling page address
     */
    private long _rightSibling;

    /**
     * The size of this buffer
     */
    private int _bufferSize;

    /**
     * FastIndex structure used for rapid page searching
     */
    private FastIndex _fastIndex;

    /**
     * Type code for this page.
     */
    private int _type;

    /**
     * Tailblock header size
     */
    private int _tailHeaderSize = TAILBLOCK_HDR_SIZE_DATA;

    /**
     * Offset within the buffer to the first byte past the last keyblock
     */
    private int _keyBlockEnd;

    /**
     * Offset within the buffer to the lowest tailblock. To allocate a new
     * tailblock, consume the space below this point.
     */
    private int _alloc;

    /**
     * Count of unused bytes above _alloc.
     */
    private int _slack;

    // Following are package-private so BufferPool can access them.
    // TODO - replace with accessor methods.
    /**
     * Singly-linked list of Buffers current having the same hash code.
     * (Maintained by BufferPool.)
     */
    private Buffer _next = null;

    /**
     * This is for the new CLOCK algorithm experiment. Bits: 1 Touched 2
     * FastIndex touched
     */
    AtomicInteger _bits = new AtomicInteger();

    /**
     * Construct a new buffer.
     * 
     * @param size
     *            The buffer size, in bytes.
     */
    Buffer(int size, int index, BufferPool pool, Persistit persistit) {
        super(persistit);
        boolean ok = false;
        for (int s = MIN_BUFFER_SIZE; !ok && s <= MAX_BUFFER_SIZE; s *= 2) {
            if (s == size) {
                ok = true;
            }
        }
        if (!ok) {
            throw new IllegalArgumentException("Invalid buffer size: " + size);
        }
        size &= KEYBLOCK_MASK;
        _pool = pool;
        _poolIndex = index;
        _byteBuffer = ByteBuffer.allocate(size);
        _bytes = _byteBuffer.array();
        _bufferSize = size;
    }

    Buffer(Buffer original) {
        this(original._bufferSize, original._poolIndex, original._pool,
                original._persistit);
        setStatus(original.getStatus() & ~(WRITER_MASK | CLAIMED_MASK));
        _type = original._type;
        _timestamp = original._timestamp;
        _page = original._page;
        _vol = original._vol;
        _rightSibling = original._rightSibling;
        _alloc = original._alloc;
        _slack = original._slack;
        _keyBlockEnd = original._keyBlockEnd;
        _tailHeaderSize = original._tailHeaderSize;
        System.arraycopy(original._bytes, 0, _bytes, 0, _bytes.length);
    }

    /**
     * Initializes the buffer so that it contains no keys or data.
     */
    void init(int type) {
        _type = type;
        _keyBlockEnd = KEY_BLOCK_START;
        _tailHeaderSize = isIndexPage() ? TAILBLOCK_HDR_SIZE_INDEX
                : TAILBLOCK_HDR_SIZE_DATA;
        _rightSibling = 0;
        _alloc = _bufferSize;
        _slack = 0;
        // _generation = 0;
        bumpGeneration();
    }

    /**
     * Extract fields from the buffer.
     */
    void load(Volume vol, long page) throws PersistitIOException,
            InvalidPageAddressException, InvalidPageStructureException,
            VolumeClosedException {
        _vol = vol;
        _page = page;
        final boolean readFromLog = _persistit.getJournalManager()
                .readPageFromJournal(this);
        if (!readFromLog) {
            vol.readPage(this, page);
        }
        load();
    }

    void load() throws InvalidPageStructureException {
        Debug.$assert(isMine());

        _timestamp = getLong(TIMESTAMP_OFFSET);

        if (_page != 0) {
            int type = getByte(TYPE_OFFSET);
            if (type > PAGE_TYPE_MAX) {
                throw new InvalidPageStructureException("Invalid type " + type);
            }
            _type = type;
            _keyBlockEnd = getChar(KEY_BLOCK_END_OFFSET);

            if (type == PAGE_TYPE_UNALLOCATED) {
                _rightSibling = 0;
                _alloc = _bufferSize;
                _slack = 0;
                _rightSibling = 0;
            } else {
                if (Debug.ENABLED) {
                    Debug.$assert(getByte(BUFFER_LENGTH_OFFSET) * 256 == _bufferSize);
                    Debug.$assert(getLong(PAGE_ADDRESS_OFFSET) == _page);
                }
                _alloc = getChar(FREE_OFFSET);
                _slack = getChar(SLACK_OFFSET);
                _rightSibling = getLong(RIGHT_SIBLING_OFFSET);

                if (isDataPage()) {
                    _tailHeaderSize = TAILBLOCK_HDR_SIZE_DATA;
                } else if (isIndexPage()) {
                    _tailHeaderSize = TAILBLOCK_HDR_SIZE_INDEX;
                }
                invalidateFastIndex();
            }
        } else {
            _type = PAGE_TYPE_HEAD;
        }
        bumpGeneration();
    }

    boolean checkedClaim(final boolean writer) throws PersistitException {
        return checkedClaim(writer, DEFAULT_MAX_WAIT_TIME);
    }

    boolean checkedClaim(final boolean writer, final long timeout)
            throws PersistitException {
        final boolean result = super.claim(writer, timeout);
        if (writer && result) {
            writePageOnCheckpoint();
        }
        return result;
    }

    private void writePageOnCheckpoint() throws PersistitException {
        if (isDirty()) {
            final Checkpoint checkpoint = _persistit.getCurrentCheckpoint();
            if (getTimestamp() < checkpoint.getTimestamp()
                    && _persistit.getCurrentTimestamp() >= checkpoint
                            .getTimestamp()) {
                writePage();
            }
        }
    }

    void writePage() throws PersistitIOException, InvalidPageStructureException {
        final Volume volume = getVolume();
        if (volume != null) {
            clearSlack();
            save();
            _persistit.getJournalManager().writePageToJournal(this);
            setClean();
            if (!volume.isClosed()) {
                volume.bumpWriteCounter();
            }
        }
    }

    void setDirty() {
        super.setDirty();
        bumpGeneration();
        final long timestamp = _persistit.getTimestampAllocator()
                .updateTimestamp();
        _timestamp = Math.max(_timestamp, timestamp);
    }

    void setDirtyStructure() {
        super.setDirtyStructure();
        bumpGeneration();
        final long timestamp = _persistit.getTimestampAllocator()
                .updateTimestamp();
        _timestamp = Math.max(_timestamp, timestamp);
    }

    /**
     * Release a writer claim on a buffer. This method also relinquishes the
     * reservation this buffer may have had if it is clean.
     */
    void release() {
        if (Debug.ENABLED && isDirty() && (isDataPage() || isIndexPage())) {
            assertVerify();
        }
        super.release();
    }

    /**
     * Zero out all bytes in this buffer.
     */
    void clear() {
        Util.clearBytes(_bytes, 0, _bufferSize);
    }

    void clearBytes(int from, int to) {
        Util.clearBytes(_bytes, from, to);
    }

    /**
     * Clears all tailblock bytes that are no longer in use. This makes sure we
     * don't retain data that has been deleted or moved in this page.
     * 
     * @throws InvalidPageStructureException
     */
    void clearSlack() throws InvalidPageStructureException {
        if (isGarbagePage()) {
            Util.clearBytes(_bytes, _keyBlockEnd, _alloc);
        } else if (isDataPage() || isIndexPage()) {
            repack();
            clearBytes(_keyBlockEnd, _alloc);
        }
    }

    /**
     * Post fields back into the buffer in preparation for writing it to disk.
     */
    void save() {
        if (_page != 0) {
            putLong(TIMESTAMP_OFFSET, _timestamp);
            putByte(TYPE_OFFSET, _type);
            putByte(BUFFER_LENGTH_OFFSET, _bufferSize / 256);
            putChar(KEY_BLOCK_END_OFFSET, _keyBlockEnd);
            putChar(FREE_OFFSET, _alloc);
            putChar(SLACK_OFFSET, _slack);
            putLong(PAGE_ADDRESS_OFFSET, _page);
            putLong(RIGHT_SIBLING_OFFSET, _rightSibling);
        }
    }

    /**
     * @return The {@link Volume} to which the page currently occupying this
     *         <code>Buffer</code> belongs
     */
    public Volume getVolume() {
        return _vol;
    }

    /**
     * @return The ByteBuffer facade for this Buffer.
     */
    public ByteBuffer getByteBuffer() {
        return _byteBuffer;
    }

    /**
     * @return The page address of the current page.
     */
    public long getPageAddress() {
        return _page;
    }

    /**
     * @return The size of the buffer, in bytes
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /**
     * Synchronized because background threads read the state.
     * 
     * @return
     */
    public long getTimestamp() {
        return _timestamp;
    }

    /**
     * @return Number of remaining bytes available for allocation
     */
    public int getAvailableSize() {
        if (_type == Buffer.PAGE_TYPE_DATA
                || _type >= Buffer.PAGE_TYPE_INDEX_MIN
                && _type <= Buffer.PAGE_TYPE_INDEX_MAX) {
            return _alloc - _keyBlockEnd + _slack;
        } else {
            return 0;
        }
    }

    /**
     * @return Number of key-value pairs currently stored in this page.
     */
    public int getKeyCount() {
        return (_keyBlockEnd - KEY_BLOCK_START) / KEYBLOCK_LENGTH;
    }

    /**
     * @return Offset to next available allocation block within the page.
     */
    public int getAlloc() {
        return _alloc;
    }

    /**
     * Gets the page type
     * 
     * @return the type as an integer
     */
    public int getPageType() {
        return _type;
    }

    /**
     * @return A displayable String version of the page type.
     */
    public String getPageTypeName() {
        if (_page == 0 && isValid())
            return TYPE_NAMES[PAGE_TYPE_HEAD];
        return getPageTypeName(_page, _type);
    }

    public static String getPageTypeName(final long page, final int type) {
        if (page == 0)
            return TYPE_NAMES[PAGE_TYPE_HEAD];

        if (type == Buffer.PAGE_TYPE_UNALLOCATED
                || type == Buffer.PAGE_TYPE_DATA
                || type >= Buffer.PAGE_TYPE_INDEX_MIN
                && type <= Buffer.PAGE_TYPE_INDEX_MAX
                || type == Buffer.PAGE_TYPE_GARBAGE
                || type == Buffer.PAGE_TYPE_LONG_RECORD) {
            return TYPE_NAMES[type];
        } else
            return "Invalid" + type;
    }

    /**
     * Gets the index of this Buffer within the buffer pool
     * 
     * @return The index
     */
    public int getIndex() {
        return _poolIndex;
    }

    /**
     * Get the page address of the right sibling page
     * 
     * @return the sibling's address
     */
    public long getRightSibling() {
        return _rightSibling;
    }

    public void setPageAddressAndVolume(final long pageAddress,
            final Volume volume) {
        _page = pageAddress;
        _vol = volume;
    }

    /**
     * Set the right sibling's page address
     * 
     * @param pageAddress
     *            the sibling's address
     */
    void setRightSibling(long pageAddress) {
        Debug.$assert(isMine());
        _rightSibling = pageAddress;
    }

    int getKeyBlockStart() {
        return KEY_BLOCK_START;
    }

    int getKeyBlockEnd() {
        return _keyBlockEnd;
    }

    /**
     * Finds the keyblock in this page that exactly matches or immediately
     * follows the supplied key.
     * <p>
     * The result is encoded as
     * <p>
     * <br>
     * (fixupRequired ? FIXUP_MASK : 0) | <br>
     * (exact ? EXACT_MASK : 0) | <br>
     * (depth &lt;&lt; DEPTH_SHIFT) | <br>
     * offset
     * <p>
     * where: <br>
     * fixupRequired is true if the successor keyblock needs to be adjusted when
     * inserting the supplied key <br>
     * exact is true if the key matches exactly <br>
     * depth is the count of bytes in the matching left subkey of the
     * predecessor key block <br>
     * offset is the offset of the keyblock at or after the supplied key.
     * <p>
     * 
     * @param key
     *            The key to seek
     * @return An encoded result (see above). Returns 0 if the supplied key
     *         precedes the first key in the page. Returns Integer.MAX_VALUE if
     *         it follows the last key in the page.
     */
    int findKey(Key key) {
        final FastIndex fastIndex = getFastIndex();
        byte[] kbytes = key.getEncodedBytes();
        int klength = key.getEncodedSize();
        int depth = 0;
        int left = KEY_BLOCK_START;
        int right = _keyBlockEnd;
        int start = left;
        int tailHeaderSize = _tailHeaderSize;

        for (int p = start; p < right;) {
            //
            // Here we MUST land on a KeyBlock at an ebc value change point.
            //
            int index = (p - start) >> 2;
            int runCount = fastIndex.getRunCount(index);
            int ebc = fastIndex.getEbc(index);

            if (depth < ebc) {
                // We know that depth < ebc for a bunch of KeyBlocks - we
                // can skip them. We can skip all KeyBlocks with ebc values
                // equal to or greater than this one, so whether there's a
                // runCount (positive) or crossCount (negative), we skip
                // this KeyBlock and all its successors.
                if (runCount < 0) {
                    runCount = -runCount;
                }
                p += KEYBLOCK_LENGTH * (runCount + 1);
                continue;
            }

            else if (depth > ebc) {
                int result = p | (depth << DEPTH_SHIFT);
                return result;
            }

            else // if (depth == ebc)
            {
                // Now we are looking for the keyblock with the matching db
                int db = fastIndex.getDescriminatorByte(index);
                int kb = kbytes[depth] & 0xFF;

                if (kb < db) {
                    int result = p | (depth << DEPTH_SHIFT);
                    return result;
                }
                if (kb > db) {
                    if (runCount > 0) {
                        int p2 = p + (runCount << 2);
                        // p2 now points to the last key block with the same
                        // ebc in this run.
                        int db2 = fastIndex.getDescriminatorByte(index
                                + runCount);

                        // For the common case that runCount == 1, we avoid
                        // setting up the binary search loop. Instead, the
                        // result is completely determined here by the
                        // value of db2.
                        if (runCount == 1) {
                            if (db2 > kb) {
                                //
                                // This is right because we already know
                                // that kb > db.
                                //
                                int result = p2 | (depth << DEPTH_SHIFT);

                                return result;
                            } else if (db2 < kb) {
                                // we just want to move forward from kb2.
                                // index = (p2 - start) >> 2;
                                // runCount = _findexElements[index] >>
                                // FINDEX_RUNCOUNT_SHIFT;
                                p = p2 + KEYBLOCK_LENGTH;
                                continue;
                            } else {
                                // found it right here. We'll fall through to
                                // the
                                // equality check below.
                                //
                                p = p2;
                                db = db2;
                            }
                        }
                        //
                        // If runCount > 1 then we have more work to do.
                        //
                        else if (db2 > kb) {
                            // the key block we want is >= p and < p2.
                            // Time to do a binary search.
                            left = p;
                            right = p2;
                            //
                            // Adjust left and right bounds of the range
                            // using db - kb difference; every db must differ
                            // from its predecessor because the ebcs are all
                            // the same. Therefore the we can define upper
                            // bounds on the maximum distance from the left
                            // and right ends of the range to the keyblock
                            // we are seeking.
                            //
                            if (runCount > 4) {
                                int distance = (right - left) >> 2;
                                int oldRight = right;
                                if (distance > kb - db + 1) {
                                    right = left + ((kb - db + 1) << 2);
                                }
                                if (distance > db2 - kb + 1) {
                                    left = oldRight - ((db2 - kb + 1) << 2);
                                }
                            }

                            while (right > left) {
                                p = ((left + right) >> 1) & P_MASK;
                                if (p == left) {
                                    //
                                    // This is right because we already know
                                    // that kb > db.
                                    //
                                    int result = right | (depth << DEPTH_SHIFT);

                                    return result;
                                }
                                int db1 = getDb(p);

                                if (db1 == kb) {
                                    db = db1;
                                    break;
                                } else if (db1 > kb) {
                                    right = p;
                                } else {
                                    left = p;
                                    db = db1;
                                }
                            }
                        } else if (db2 < kb) {
                            // we just want to move forward from kb2, skipping
                            // the crossCount if non-zero.
                            index = (p2 - start) >> 2;
                            runCount = fastIndex.getRunCount(index);
                            assert runCount <= 0;
                            p = p2 + KEYBLOCK_LENGTH * (-runCount + 1);
                            continue;
                        } else {
                            // found it right here. We'll fall through to the
                            // equality check below.
                            p = p2;
                            db = db2;
                        }
                    } else {
                        p += KEYBLOCK_LENGTH * (-runCount + 1);
                        continue;
                    }
                }

                if (kb == db) {
                    int kbData = getInt(p);
                    int tail = decodeKeyBlockTail(kbData);
                    int tbData = getInt(tail);
                    int tlength = decodeTailBlockKLength(tbData) + depth + 1;
                    int qlength = tlength < klength ? tlength : klength;
                    //
                    // Walk down the key, increasing depth
                    //
                    boolean matched = true;
                    if (++depth < qlength) {
                        int q = tail + tailHeaderSize;
                        kb = kbytes[depth];
                        db = _bytes[q++];

                        while (kb == db && ++depth < qlength) {
                            kb = kbytes[depth];
                            db = _bytes[q++];
                        }

                        if (kb != db) {
                            kb = kb & 0xFF;
                            db = db & 0xFF;

                            if (kb < db) {
                                // key is less than tail, so we return
                                // this keyblock
                                int result = p | (depth << DEPTH_SHIFT)
                                        | FIXUP_MASK;

                                return result;
                            }
                            matched = false;
                        }
                    }

                    if (matched && depth == qlength) {
                        // We matched all the way to the end of either key or
                        // tail.
                        //
                        if (qlength == tlength) {
                            // We matched all the way to the end of the tail
                            if (qlength == klength) {
                                // lengths match exactly so this is an
                                // exact match
                                //
                                int result = p | (depth << DEPTH_SHIFT)
                                        | EXACT_MASK;
                                return result;
                            }
                            // key is longer, so we move to the right
                        } else if (tlength > qlength) {
                            // Tail is longer, so the key
                            // key is less than tail, so we return the
                            // this keyblock since it is greater than the key
                            //
                            int result = p | (depth << DEPTH_SHIFT)
                                    | FIXUP_MASK;
                            return result;
                        }
                    }
                }
                // Advance to the next keyblock
                p += KEYBLOCK_LENGTH;
            }

        }
        // Should never fall through. If so, then we've walked to the right of
        // maxkey.
        int result = right | (depth << DEPTH_SHIFT);
        return result;
    }

    boolean hasValue(Key key) {
        int foundAt = findKey(key);
        return ((foundAt & EXACT_MASK) != 0);
    }

    boolean hasChild(Key key) throws PersistitException {
        if (!isDataPage()) {
            throw new InvalidPageTypeException("type=" + _type);
        }
        int foundAt = findKey(key);
        return hasChild(foundAt, key);
    }

    boolean hasChild(int foundAt, Key key) {
        int p = foundAt & P_MASK;
        if ((foundAt & EXACT_MASK) != 0) {
            p += KEYBLOCK_LENGTH;
            if (p >= _keyBlockEnd)
                return false;
            int kbData = getInt(p);
            int ebc = decodeKeyBlockEbc(kbData);
            return ebc == key.getEncodedSize();
        } else {
            if (p >= _keyBlockEnd)
                return false;
            int depth = (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;
            return depth == key.getEncodedSize() && depth > 0;
        }
    }

    Value fetch(Key key, Value value) throws PersistitException {
        if (!isDataPage()) {
            throw new InvalidPageTypeException("type=" + _type);
        }
        int foundAt = findKey(key);
        return fetch(foundAt, value);
    }

    Value fetch(int foundAt, Value value) {
        if ((foundAt & EXACT_MASK) == 0) {
            value.clear();
        } else {
            int kbData = getInt(foundAt & P_MASK);
            int tail = decodeKeyBlockTail(kbData);
            int tbData = getInt(tail);
            int klength = decodeTailBlockKLength(tbData);
            int size = decodeTailBlockSize(tbData);
            int valueSize = size - klength - _tailHeaderSize;
            value.putEncodedBytes(_bytes, tail + _tailHeaderSize + klength,
                    valueSize);
        }
        return value;
    }

    long fetchLongRecordPointer(int foundAt) {
        if (!isDataPage()) {
            return 0;
        }
        int kbData = getInt(foundAt & P_MASK);
        int tail = decodeKeyBlockTail(kbData);
        int tbData = getInt(tail);
        int klength = decodeTailBlockKLength(tbData);
        int size = decodeTailBlockSize(tbData);
        int valueSize = size - klength - _tailHeaderSize;
        if (valueSize != LONGREC_SIZE) {
            return 0;
        }
        if ((_bytes[tail + _tailHeaderSize + klength] & 0xFF) != LONGREC_TYPE) {
            return 0;
        }

        long pointer = getLong(tail + _tailHeaderSize + klength
                + LONGREC_PAGE_OFFSET);
        return pointer;
    }

    long getPointer(int foundAt) throws PersistitException {
        if (!isIndexPage()) {
            throw new InvalidPageTypeException("type=" + _type);
        }
        int kbData = getInt(foundAt & P_MASK);
        int tail = decodeKeyBlockTail(kbData);
        return getInt(tail + 4); // TODO - allow larger pointer size
    }

    /**
     * Internal implementation of getKey using a previously computed result from
     * the findKey() method.
     * 
     * @param key
     * @param mode
     * @param foundAt
     * @return
     */
    int traverse(Key key, Key.Direction mode, int foundAt) {
        boolean exactMatch = (foundAt & EXACT_MASK) > 0;
        if (mode == Key.EQ || exactMatch
                && (mode == Key.LTEQ || mode == Key.GTEQ)) {
            return foundAt;
        }
        if (mode == Key.LT || mode == Key.LTEQ) {
            return previousKey(key, foundAt);
        } else if (mode == Key.GT || mode == Key.GTEQ) {
            return nextKey(key, foundAt);
        } else
            throw new IllegalArgumentException("Invalid mode: " + mode);
    }

    /**
     * Helper method to compute the actual bytes in the previous key.
     * 
     * @param key
     * @param foundAt
     * @return
     */
    private int previousKey(Key key, int foundAt) {
        int p = (foundAt & P_MASK) - KEYBLOCK_LENGTH;
        int depth = (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;

        if (p < KEY_BLOCK_START)
            return foundAt;

        byte[] kbytes = key.getEncodedBytes();

        // Compute the count of prefix bytes in the supplied key that
        // are known to match. The leftmost knownGood bytes do not need
        // to be recovered by traversing keys in the page.
        //
        int kbData2 = getInt(p + KEYBLOCK_LENGTH);
        int ebc2 = decodeKeyBlockEbc(kbData2);
        int kbData = getInt(p);
        int ebc = decodeKeyBlockEbc(kbData);
        int knownGood = ebc;
        if (ebc2 < ebc)
            knownGood = ebc2;
        if (depth < knownGood)
            knownGood = depth;

        int tail = decodeKeyBlockTail(kbData);
        //
        // This is the length of the key value we need to reconstruct.
        // We walk backward through the keys on this page until we find
        // a key with an ebc value that is less than or equal to knownGood.
        // For each key we copy the bytes from its tail to fill in the gap
        // between ebc and unknown. This reduces the value of unknown. Once
        // the unknown count becomes less than or equal to the knownGood count,
        // we are done.
        //
        int unknown = decodeTailBlockKLength(getInt(tail)) + ebc + 1; // (+1 is
                                                                      // for the
                                                                      // discriminator
                                                                      // byte)
        key.setEncodedSize(unknown);

        int result = p | (unknown << DEPTH_SHIFT) | EXACT_MASK;
        //
        // Reconstruct the previous key.
        //
        for (;;) {
            if (ebc < unknown) {
                // move bytes from this keyblock into the result key.
                kbytes[ebc] = (byte) decodeKeyBlockDb(kbData);
                int more = unknown - ebc - 1;
                if (more > 0) {
                    System.arraycopy(_bytes, tail + _tailHeaderSize, kbytes,
                            ebc + 1, more);
                }
                unknown = ebc;
            }
            if (unknown <= knownGood)
                break;

            p -= KEYBLOCK_LENGTH;
            if (p < KEY_BLOCK_START)
                break;
            kbData = getInt(p);
            ebc = decodeKeyBlockEbc(kbData);
            tail = decodeKeyBlockTail(kbData);
        }
        return result;
    }

    /**
     * Helper method to compute the actual bytes in the successor key.
     * 
     * @param key
     * @param foundAt
     * @return
     */
    int nextKey(Key key, int foundAt) {
        int p = foundAt & P_MASK;
        if ((foundAt & EXACT_MASK) != 0)
            p += KEYBLOCK_LENGTH;

        if (p >= _keyBlockEnd)
            return foundAt;

        byte[] kbytes = key.getEncodedBytes();
        int kbData = getInt(p);
        int ebc = decodeKeyBlockEbc(kbData);
        int tail = decodeKeyBlockTail(kbData);
        int tbData = getInt(tail);
        int klength = decodeTailBlockKLength(tbData);
        int keyLength = klength + ebc + 1;
        key.setEncodedSize(keyLength);

        int result = p | (keyLength << DEPTH_SHIFT) | EXACT_MASK;
        //
        // Note: the findKey method is guaranteed to return the offset of a
        // keyblock whose first ebc bytes match the supplied key.
        //
        kbytes[ebc] = (byte) decodeKeyBlockDb(kbData);
        System.arraycopy(_bytes, tail + _tailHeaderSize, kbytes, ebc + 1,
                klength);

        return result;
    }

    /**
     * Helper method for IntegrityCheck to find next long record key block
     * 
     * @param value
     * @param foundAt
     * @return
     */
    int nextLongRecord(Value value, int foundAt) {
        if (Debug.ENABLED)
            Debug.$assert(isDataPage());
        for (int p = foundAt & P_MASK; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
            int kbData = getInt(p);
            int tail = decodeKeyBlockTail(kbData);
            int tbData = getInt(tail);
            int klength = decodeTailBlockKLength(tbData);
            int size = decodeTailBlockSize(tbData);
            int valueSize = size - klength - _tailHeaderSize;
            if ((valueSize > 0)
                    && ((_bytes[tail + _tailHeaderSize + klength] & 0xFF) == LONGREC_TYPE)) {
                value.putEncodedBytes(_bytes, tail + _tailHeaderSize + klength,
                        valueSize);
                return p;
            }
        }
        return -1;
    }

    int previousKeyBlock(int foundAt) {
        int p = (foundAt & P_MASK) - KEYBLOCK_LENGTH;
        if (p < KEY_BLOCK_START || p > _keyBlockEnd)
            return -1;
        return p;
    }

    int nextKeyBlock(int foundAt) {
        int p = (foundAt & P_MASK) + KEYBLOCK_LENGTH;
        if (p >= _keyBlockEnd || p < KEY_BLOCK_START)
            return -1;
        return p;
    }

    int toKeyBlock(int foundAt) {
        foundAt &= P_MASK;
        if (KEY_BLOCK_START >= _keyBlockEnd)
            return -1;
        if (foundAt < KEY_BLOCK_START)
            return KEY_BLOCK_START;
        if (foundAt >= _keyBlockEnd)
            return _keyBlockEnd - KEYBLOCK_LENGTH;
        return foundAt;
    }

    /**
     * Insert or replace a value for a key. If there already is a value with the
     * same key, then replace it. Otherwise insert a new key and value into the
     * buffer.
     * 
     * @param key
     *            The key on under which the value will be stored
     * @param value
     *            The value, converted to a byte array
     */
    int putValue(Key key, Value value) {
        int p = findKey(key);
        return putValue(key, value, p, false);
    }

    /**
     * Insert or replace a value for a key. If there already is a value with the
     * same key, then replace it. Otherwise insert a new key and value into the
     * buffer.
     * 
     * @param key
     *            The key under which the value will be stored
     * @param value
     *            The value to be stored
     * @param foundAt
     *            The keyblock before which this record will be inserted
     */
    int putValue(Key key, Value value, int foundAt, boolean postSplit) {
        if (Debug.ENABLED) {
            assertVerify();
        }
        final FastIndex fastIndex = _fastIndex;

        boolean exactMatch = (foundAt & EXACT_MASK) > 0;
        int p = foundAt & P_MASK;

        if (exactMatch) {
            return replaceValue(key, value, p);
        } else {
            int length;
            if (isIndexPage()) {
                length = 0;
            } else {
                length = value.getEncodedSize();
            }

            int depth = (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;
            boolean fixupSuccessor = (foundAt & FIXUP_MASK) > 0;
            byte[] kbytes = key.getEncodedBytes();

            int ebcNew;
            int ebcSuccessor;
            int kbSuccessor = 0;
            int delta = 0;
            int successorTail = 0;
            int successorTailBlock = 0;
            int successorTailSize = 0;
            int free1 = 0;
            int free2 = 0;
            if (fixupSuccessor) {
                kbSuccessor = getInt(p);
                ebcNew = decodeKeyBlockEbc(kbSuccessor);
                ebcSuccessor = depth;
                delta = ebcSuccessor - ebcNew;
                successorTail = decodeKeyBlockTail(kbSuccessor);
                successorTailBlock = getInt(successorTail);
                successorTailSize = decodeTailBlockSize(successorTailBlock);
                free1 = (successorTailSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
                free2 = (successorTailSize - delta + ~TAILBLOCK_MASK)
                        & TAILBLOCK_MASK;

            } else {
                ebcSuccessor = 0;
                ebcNew = depth;
            }
            int klength = key.getEncodedSize() - ebcNew - 1;
            int newTailSize = klength + length + _tailHeaderSize;

            if (getKeyCount() >= _pool.getMaxKeys()
                    || !willFit(newTailSize + KEYBLOCK_LENGTH - (free1 - free2))) {
                Debug.$assert(!postSplit);
                return -1;
            }

            //
            // Possibly increase the ebc value of the successor
            // (Needs to be done first so that computation of new run value
            // is correct.
            //
            if (fixupSuccessor && ebcNew != ebcSuccessor) {
                int successorKeyLength = decodeTailBlockKLength(successorTailBlock);
                int successorDb = getByte(successorTail + _tailHeaderSize
                        + delta - 1);

                // Write updated successor tail block
                putInt(successorTail,
                        encodeTailBlock(successorTailSize - delta,
                                successorKeyLength - delta));

                System.arraycopy(_bytes, successorTail + _tailHeaderSize
                        + delta, _bytes, successorTail + _tailHeaderSize,
                        successorTailSize - _tailHeaderSize - delta);

                if (free2 < free1) {
                    deallocTail(successorTail + free2, free1 - free2);
                }

                kbSuccessor = encodeKeyBlock(ebcSuccessor, successorDb,
                        successorTail);

                putInt(p, kbSuccessor);
            }
            int dbNew = kbytes[ebcNew] & 0xFF;
            //
            // Allocate space for the new tail block
            //
            _keyBlockEnd += KEYBLOCK_LENGTH;

            int newTail = allocTail(newTailSize);
            if (newTail == -1) {
                _keyBlockEnd -= KEYBLOCK_LENGTH;
                repack();
                _keyBlockEnd += KEYBLOCK_LENGTH;
                newTail = allocTail(newTailSize);
                if (Debug.ENABLED)
                    Debug.$assert(newTail != -1);
            }

            // Shift the subsequent key blocks
            System.arraycopy(_bytes, p, _bytes, p + KEYBLOCK_LENGTH,
                    _keyBlockEnd - p - KEYBLOCK_LENGTH);

            // Write new key block
            int newKeyBlock = encodeKeyBlock(ebcNew, dbNew, newTail);
            putInt(p, newKeyBlock);

            // Write new tail block
            putInt(newTail, encodeTailBlock(newTailSize, klength));

            if (Debug.ENABLED) {
                Debug.$assert(klength >= 0 && ebcNew + 1 >= 0
                        && ebcNew + 1 + klength <= kbytes.length
                        && newTail + _tailHeaderSize >= 0
                        && newTail + _tailHeaderSize + klength <= _bytes.length);
            }
            try {
                System.arraycopy(kbytes, ebcNew + 1, _bytes, newTail
                        + _tailHeaderSize, klength);
            } catch (Exception e) {
                Debug.$assert(false);
            }
            if (isIndexPage()) {
                // TODO - allow larger pointer size
                int pointer = (int) value.getPointerValue();

                if (Debug.ENABLED) {
                    Debug.$assert(p + KEYBLOCK_LENGTH < _keyBlockEnd ? pointer > 0
                            : true);
                    if (value != Value.EMPTY_VALUE) {
                        Debug.$assert(_type - 1 == value.getPointerPageType());
                    }
                }

                putInt(newTail + TAILBLOCK_POINTER, pointer);
            } else if (value != Value.EMPTY_VALUE) {
                System.arraycopy(value.getEncodedBytes(), 0, _bytes, newTail
                        + _tailHeaderSize + klength, length);
            }

            if (fastIndex != null) {
                fastIndex.insertKeyBlock(p, ebcNew, fixupSuccessor);
            }

            bumpGeneration();

            if (Debug.ENABLED && p > KEY_BLOCK_START) {
                Debug.$assert(adjacentKeyCheck(p - KEYBLOCK_LENGTH));
            }
            if (Debug.ENABLED && p + KEYBLOCK_LENGTH < _keyBlockEnd) {
                Debug.$assert(adjacentKeyCheck(p));
            }

            if (Debug.ENABLED) {
                assertVerify();
            }

            return p | (key.getEncodedSize() << DEPTH_SHIFT) | EXACT_MASK;
        }
    }

    /**
     * This method is for debugging only. It should be asserted after a key has
     * been inserted or removed.
     * 
     * @param p
     * @return
     */
    private boolean adjacentKeyCheck(int p) {
        p &= P_MASK;
        int kbData1 = getInt(p);
        int kbData2 = getInt(p + KEYBLOCK_LENGTH);
        int db1 = decodeKeyBlockDb(kbData1);
        int ebc1 = decodeKeyBlockEbc(kbData1);
        int db2 = decodeKeyBlockDb(kbData2);
        int ebc2 = decodeKeyBlockEbc(kbData2);

        if (db1 == 0 && p > KEY_BLOCK_START) {
            return false; // Can set breakpoint here
        }
        if (db2 == 0) {
            return false; // Can set breakpoint here
        }

        if (ebc1 == ebc2 && db1 < db2)
            return true;
        if (ebc2 < ebc1)
            return true;
        if (ebc2 > ebc1) {
            int tail1 = decodeKeyBlockTail(kbData1);
            int tbData1 = getInt(tail1);
            int klength1 = decodeTailBlockKLength(tbData1);
            int db = -1;
            if (klength1 >= ebc2 - ebc1) {
                db = _bytes[tail1 + _tailHeaderSize + ebc2 - ebc1 - 1]
                        & DB_MASK;
                if (db2 > db)
                    return true;
                return false; // Can set breakpoint here
            } else if (klength1 + 1 == ebc2 - ebc1) {
                // All bytes of key1 are elided by key2
                return true;
            }
            return false; // Can set breakpoint here
        }
        return false; // Can set breakpoint here
    }

    private int replaceValue(Key key, Value value, int p) {
        int kbData = getInt(p);
        int tail = decodeKeyBlockTail(kbData);
        int tbData = getInt(tail);
        int klength = decodeTailBlockKLength(tbData);
        int oldTailSize = decodeTailBlockSize(tbData);

        int length;
        if (isIndexPage()) {
            length = 0;
        } else {
            if (value.isAtomicIncrementArmed()) {
                length = oldTailSize - klength - _tailHeaderSize;
                value.putEncodedBytes(_bytes, tail + _tailHeaderSize + klength,
                        length);
                value.performAtomicIncrement();
            } else {
                length = value.getEncodedSize();
            }
        }

        int newTailSize = klength + length + _tailHeaderSize;
        int oldNext = (tail + oldTailSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
        int newNext = (tail + newTailSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
        int newTail = tail;
        if (newNext < oldNext) {
            // Free the remainder of the old tail block
            deallocTail(newNext, oldNext - newNext);
        } else if (newNext > oldNext) {
            if (!willFit(newNext - oldNext)) {
                return -1; // need to split the page
            }
            //
            // Free the old tail block entirely
            //
            deallocTail(tail, oldTailSize);
            newTail = allocTail(newTailSize);
            if (newTail == -1) {
                repack();
                //
                // Guaranteed to succeed because of the test on willFit() above
                //
                newTail = allocTail(newTailSize);
            }
            putInt(p, encodeKeyBlockTail(kbData, newTail));
        }
        putInt(newTail, encodeTailBlock(newTailSize, klength));
        if (newTail != tail) {
            System.arraycopy(key.getEncodedBytes(), key.getEncodedSize()
                    - klength, _bytes, newTail + _tailHeaderSize, klength);
        }

        if (isIndexPage()) {
            // TODO - allow larger pointer size
            long pointer = value.getPointerValue();
            if (Debug.ENABLED) {
                Debug.$assert(p + KEYBLOCK_LENGTH < _keyBlockEnd ? pointer > 0
                        : pointer == -1);
                if (value != Value.EMPTY_VALUE) {
                    Debug.$assert(_type - 1 == value.getPointerPageType());
                }
            }
            putInt(newTail + TAILBLOCK_POINTER, (int) pointer);
        } else if (value != Value.EMPTY_VALUE) {
            System.arraycopy(value.getEncodedBytes(), 0, _bytes, newTail
                    + _tailHeaderSize + klength, length);
        }
        if (Debug.ENABLED)
            assertVerify();
        return p | (key.getEncodedSize() << DEPTH_SHIFT) | EXACT_MASK;
    }

    /**
     * Removes the keys and assocated values from the key blocks indicated by
     * the two argument. The removal range starts at foundAt1 inclusive and ends
     * at foundAt2 exclusive.
     * 
     * @param foundAt1
     *            Encoded location of the first key block to remove
     * @param foundAt2
     *            Encoded location of the first key block beyond the removal
     *            range.
     * @param spareKey
     *            A key used in accumulating bytes for the successor key to the
     *            removal range. As a side effect, this key contains the last
     *            key to be removed by this method.
     * @return <i>true</i> if the key was found and the value removed, else
     *         <i>false</i>
     */
    boolean removeKeys(int foundAt1, int foundAt2, Key spareKey) {
        if (Debug.ENABLED)
            assertVerify();

        int p1 = foundAt1 & P_MASK;
        int p2 = foundAt2 & P_MASK;
        if ((foundAt2 & EXACT_MASK) != 0)
            p2 += KEYBLOCK_LENGTH;

        if (p1 < KEY_BLOCK_START || p1 > _keyBlockEnd || p2 < KEY_BLOCK_START
                || p2 > _keyBlockEnd) {
            throw new IllegalArgumentException("p1=" + p1 + " p2=" + p2
                    + " in " + summarize());
        }
        if (p2 <= p1)
            return false;

        int ebc = Integer.MAX_VALUE;

        byte[] spareBytes = spareKey.getEncodedBytes();
        int keySize = 0;
        for (int p = p1; p < p2; p += KEYBLOCK_LENGTH) {
            int kbData = getInt(p);
            int ebcCandidate = decodeKeyBlockEbc(kbData);
            if (ebcCandidate < ebc)
                ebc = ebcCandidate;
            int db = decodeKeyBlockDb(kbData);
            int tail = decodeKeyBlockTail(kbData);
            int tbData = getInt(tail);
            int klength = decodeTailBlockKLength(tbData);
            spareBytes[ebcCandidate] = (byte) db;
            if (klength > 0) {
                System.arraycopy(_bytes, tail + _tailHeaderSize, spareBytes,
                        ebcCandidate + 1, klength);
            }
            keySize = klength + ebcCandidate + 1;
            int size = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;
            deallocTail(tail, size);
        }
        spareKey.setEncodedSize(keySize);
        //
        // Remove the deleted key blocks
        //
        System.arraycopy(_bytes, p2, _bytes, p1, _keyBlockEnd - p2);

        _keyBlockEnd -= (p2 - p1);

        //
        // All the tailblocks that need to be deleted have been marked free.
        // Now we need to reallocate the tail block for the successor key
        // if due to a loss of ebc count we need to represent more bytes
        // of its key. Note: p1 now points to that successor key block.
        //
        if (p1 < _keyBlockEnd) {
            int kbNext = getInt(p1);
            int ebcNext = decodeKeyBlockEbc(kbNext);

            //
            // If ebcNext > ebc then the successor key will need to expand.
            //
            if (ebcNext > ebc) {
                int tailNext = decodeKeyBlockTail(kbNext);
                int dbNext = decodeKeyBlockDb(kbNext);
                int tbNext = getInt(tailNext);
                int nextTailSize = decodeTailBlockSize(tbNext);

                int nextTailBlockSize = (nextTailSize + ~TAILBLOCK_MASK)
                        & TAILBLOCK_MASK;

                int newNextTailBlockSize = (nextTailSize + ebcNext - ebc + ~TAILBLOCK_MASK)
                        & TAILBLOCK_MASK;

                int delta = newNextTailBlockSize - nextTailBlockSize;
                boolean freeNextTailBlock = false;
                int newNextTail = tailNext;
                if (delta > 0) {
                    // Now attempt to allocate a new tail block for the
                    // successor key.
                    //
                    newNextTail = allocTail(newNextTailBlockSize);
                    if (newNextTail == -1) {
                        // If allocTail failed to return a new block into which
                        // we can copy the successor block, then we'll try to
                        // wedge an allocation in right before it.
                        //
                        newNextTail = wedgeTail(tailNext, delta);
                        //
                        // If that fails, then we repack and try again.
                        //
                        if (newNextTail == -1) {
                            repack();
                            //
                            // Repacking will move the tail blocks so we need
                            // to refetch pointers to the ones we care about.
                            //
                            kbNext = getInt(p1);
                            tailNext = decodeKeyBlockTail(kbNext);
                            newNextTail = wedgeTail(tailNext, delta);
                        }
                    } else {
                        freeNextTailBlock = true;
                    }
                    if (newNextTail == -1) {
                        throw new IllegalStateException(
                                "Can't wedge enough space in " + this
                                        + " foundAt1=" + foundAt1
                                        + " foundAt2=" + foundAt2
                                        + " spareKey=" + spareKey
                                        + " nextTailBlockSize="
                                        + nextTailBlockSize
                                        + " newNextTailBlockSize="
                                        + newNextTailBlockSize + " ebc=" + ebc
                                        + " ebcNext=" + ebcNext);
                    }
                }
                //
                // At this point newNextTail points to a tail block with
                // sufficient space to hold the revised successor key.
                // It may or may not be at a new location. We start by copying
                // the payload associated with the old tail block to its new
                // location.
                //
                if (isIndexPage()) {
                    putInt(newNextTail + TAILBLOCK_POINTER, getInt(tailNext
                            + TAILBLOCK_POINTER)); // TODO - allow larger
                    // pointer size
                }
                System.arraycopy(_bytes, tailNext + _tailHeaderSize, _bytes,
                        newNextTail + _tailHeaderSize + ebcNext - ebc,
                        nextTailSize - _tailHeaderSize);

                _bytes[newNextTail + _tailHeaderSize + ebcNext - ebc - 1] = (byte) dbNext;

                //
                // Next copy copy the additional bytes of key that are to be
                // removed.
                //
                System.arraycopy(spareBytes, ebc + 1, _bytes, newNextTail
                        + _tailHeaderSize, ebcNext - ebc - 1);
                //
                // Now construct the new tail block
                //
                int newNextKLength = decodeTailBlockKLength(tbNext) + ebcNext
                        - ebc;
                int newNextTailSize = nextTailSize + ebcNext - ebc;
                putInt(newNextTail,
                        encodeTailBlock(newNextTailSize, newNextKLength));
                if (freeNextTailBlock) {
                    int toFree = (nextTailSize + ~TAILBLOCK_MASK)
                            & TAILBLOCK_MASK;
                    deallocTail(tailNext, toFree);
                }
                //
                // Fix up the successor key block
                //
                int kbNewNext = encodeKeyBlock(ebc, spareBytes[ebc],
                        newNextTail);
                putInt(p1, kbNewNext);
            }
        }
        invalidateFastIndex();
        bumpGeneration();
        if (Debug.ENABLED) {
            assertVerify();
        }
        return true;
    }

    /**
     * Moves the tail blocks below the specified tail offset downward in order
     * to free delta bytes of space before the specified tail block. This is
     * sometimes done in the removeKey() method to allow the key that follows
     * the one being removed to have its ebc value increased.
     * 
     * @param tail
     *            Offset of tail block to be expanded downward
     * @param delta
     *            The amount by which that block is to be expanded.
     * @return int Offset of the expanded tail block, or -1 if it does not fit.
     */
    private int wedgeTail(int tail, int delta) {
        delta = (delta + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
        if (delta == 0) {
            return tail;
        }
        if (delta < 0) {
            throw new IllegalArgumentException(
                    "wedgeTail delta must be positive: " + delta
                            + " is invalid");
        }
        if (_alloc - _keyBlockEnd < delta) {
            return -1;
        }

        System.arraycopy(_bytes, _alloc, _bytes, _alloc - delta, tail - _alloc);
        _alloc -= delta;

        for (int p = KEY_BLOCK_START; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
            int kbData = getInt(p);
            int oldTail = decodeKeyBlockTail(kbData);
            if (oldTail < tail) {
                putInt(p, encodeKeyBlockTail(kbData, oldTail - delta));
            }
        }
        return tail - delta;
    }

    /**
     * Copies keys and values to a newly allocated buffer in order to make room
     * for an insertion, and then performs the insertion.
     * 
     * @param rightSibling
     *            The buffer containing the new right sibling page
     * @param key
     *            The key being inserted or replaced
     * @param value
     *            The new value
     * @param foundAt
     *            Offset to keyblock where insertion will occur
     * @param indexKey
     *            A Key into which this method writes the rightmost key in the
     *            left sibling page.
     * @param sequence
     *            The insert sequence (FORWARD, REVERSE or NONE)
     * @param policy
     *            The SplitPolicy for this insertion
     * @throws PersistitException
     */
    final int split(Buffer rightSibling, Key key, Value value, int foundAt,
            Key indexKey, Sequence sequence, SplitPolicy policy)
            throws PersistitException {
        // Make sure the right sibling page is empty.

        if (Debug.ENABLED) {
            Debug.$assert(rightSibling._keyBlockEnd == KEY_BLOCK_START);
            Debug.$assert(rightSibling._alloc == rightSibling._bufferSize);
            assertVerify();
        }

        // First we calculate how large the virtual page containing the
        // modified or inserted key/value pair would be.
        //
        int currentSize = _bufferSize - _alloc - _slack + _keyBlockEnd
                - KEY_BLOCK_START;

        int foundAtPosition = foundAt & P_MASK;
        boolean exact = (foundAt & EXACT_MASK) != 0;
        int depth = (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;
        boolean fixupSuccessor = (foundAt & FIXUP_MASK) > 0;

        int ebcNew;
        int ebcSuccessor;
        int deltaSuccessorTailSize = 0;
        int deltaSuccessorEbc = 0;

        if (fixupSuccessor) {
            int kbSuccessor = getInt(foundAtPosition);
            int tbSuccessor = getInt(decodeKeyBlockTail(kbSuccessor));
            ebcNew = decodeKeyBlockEbc(kbSuccessor);
            ebcSuccessor = depth;
            int tbSize = decodeTailBlockSize(tbSuccessor);

            // This is the number of bytes by which the successor key
            // can have its elided byte count increased.
            deltaSuccessorEbc = ebcSuccessor - ebcNew;

            int oldSize = (tbSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
            int newSize = (tbSize - deltaSuccessorEbc + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;

            // This is the number of bytes by which the successor tail block
            // can be reduced (because its elision count has increased.)
            deltaSuccessorTailSize = oldSize - newSize;
        } else {
            ebcSuccessor = 0;
            ebcNew = depth;
        }
        int keyBlockSizeDelta = KEYBLOCK_LENGTH;
        int oldTailBlockSize = 0;
        int newTailBlockSize;
        if (exact) {
            int kbData = getInt(foundAtPosition);
            int tbData = getInt(decodeKeyBlockTail(kbData));
            oldTailBlockSize = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;
            keyBlockSizeDelta = 0;
            // PDB 20050802 - because when replacing a keyblock we
            // leave the ebc unchanged.
            ebcNew = decodeKeyBlockEbc(kbData);
        }
        newTailBlockSize = ((isIndexPage() ? 0 : value.getEncodedSize())
                + _tailHeaderSize + key.getEncodedSize() - ebcNew - 1 + ~TAILBLOCK_MASK)
                & TAILBLOCK_MASK;

        int virtualSize = currentSize + newTailBlockSize - oldTailBlockSize
                + keyBlockSizeDelta - deltaSuccessorTailSize;

        int splitBest = 0; // Maximal fitness measure
        int splitAt = 0; // Offset to first keyblock in right sibling

        int leftSize = 0; // Bytes in left sibling page

        boolean armed = true;
        int whereInserted = -1;

        int rightKeyBlock = _keyBlockEnd - KEYBLOCK_LENGTH;
        for (int p = KEY_BLOCK_START; p < rightKeyBlock;) {
            int splitCandidate = 0;
            if (p == foundAtPosition && armed) {

                // Here we are adding the newly inserted (or replacing) record
                // to the left side. The key block pointed to by p is our
                // candidate for the first key in the right sibling page.

                leftSize += newTailBlockSize + KEYBLOCK_LENGTH;
                if (exact) {
                    p += KEYBLOCK_LENGTH;
                }

                // Compute the number of bytes by which the successor tailblock
                // will grow due to its elision count becoming zero.
                //
                int kbData = getInt(p);
                int tbData = getInt(decodeKeyBlockTail(kbData));
                int ebc = decodeKeyBlockEbc(kbData);
                int tbSize = decodeTailBlockSize(tbData);
                int tbSizeDelta = ((tbSize + ebc + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                        - ((tbSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);

                int edgeTailBlockSize = (decodeTailBlockKLength(tbData)
                        - deltaSuccessorEbc + _tailHeaderSize + ~TAILBLOCK_MASK)
                        & TAILBLOCK_MASK;

                if (p < rightKeyBlock) {
                    int rightSize = virtualSize - leftSize + tbSizeDelta;

                    splitCandidate = policy.splitFit(this, p, foundAtPosition,
                            exact, leftSize + KEYBLOCK_LENGTH
                                    + edgeTailBlockSize, rightSize,
                            currentSize, virtualSize, _bufferSize
                                    - KEY_BLOCK_START, splitBest, sequence);
                    if (splitCandidate > splitBest) {
                        splitBest = splitCandidate;
                        splitAt = p | EXACT_MASK;
                    }
                }
                armed = false;
            } else {
                // Here we are proposing the key block pointed to by p as the
                // last key of the left page.
                int kbData = getInt(p);
                int tbData = getInt(decodeKeyBlockTail(kbData));
                int tbSizeDelta;
                int tailBlockSize = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                        & TAILBLOCK_MASK;
                leftSize += tailBlockSize + KEYBLOCK_LENGTH;

                p += KEYBLOCK_LENGTH;
                int edgeTailBlockSize;

                if (p == foundAtPosition && armed) {
                    tbSizeDelta = (((isIndexPage() ? 0 : value.getEncodedSize())
                            + _tailHeaderSize + key.getEncodedSize() + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                            - newTailBlockSize;

                    edgeTailBlockSize = (key.getEncodedSize() - depth
                            + _tailHeaderSize + ~TAILBLOCK_MASK)
                            & TAILBLOCK_MASK;
                } else {
                    kbData = getInt(p);
                    tbData = getInt(decodeKeyBlockTail(kbData));
                    int ebc = decodeKeyBlockEbc(kbData);
                    int tbSize = decodeTailBlockSize(tbData);

                    tbSizeDelta = ((tbSize + ebc + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                            - ((tbSize + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);

                    edgeTailBlockSize = (decodeTailBlockKLength(tbData)
                            + _tailHeaderSize + ~TAILBLOCK_MASK)
                            & TAILBLOCK_MASK;
                }

                if (p < rightKeyBlock) {
                    int rightSize = virtualSize - leftSize + tbSizeDelta;

                    splitCandidate = policy.splitFit(this, p, foundAtPosition,
                            exact, leftSize + KEYBLOCK_LENGTH
                                    + edgeTailBlockSize, rightSize,
                            currentSize, virtualSize, _bufferSize
                                    - KEY_BLOCK_START, splitBest, sequence);
                    if (splitCandidate > splitBest) {
                        splitBest = splitCandidate;
                        splitAt = p;
                    }
                }
            }
            // Following is true when we have gone past the best split
            // point.
            if (splitCandidate == 0 && splitBest != 0) {
                break;
            }
        }

        if (splitBest == 0) {
            throw new InvalidPageStructureException("Can't split page " + this
                    + " exact=" + exact + " insertAt=" + foundAtPosition
                    + " currentSize=" + currentSize + " virtualSize="
                    + virtualSize + " leftSize=" + leftSize);
        }
        //
        // Now move the keys and records.
        //
        byte[] indexKeyBytes = indexKey.getEncodedBytes();
        int splitAtPosition = splitAt & P_MASK;

        boolean lastLeft = (splitAt & EXACT_MASK) != 0;
        boolean firstRight = !lastLeft && splitAtPosition == foundAtPosition;
        int indexKeyDepth = 0;
        //
        // First we need to compute the full key in the right sibling page.
        //
        int scanStart = KEY_BLOCK_START;
        if (foundAtPosition <= splitAt) {
            // We know that the value of key is at or before foundAt, so we
            // can just copy the bytes from key and avoid scanning the records
            // to the left of foundAt.
            //
            scanStart = foundAtPosition;
            indexKeyDepth = key.getEncodedSize();
            System.arraycopy(key.getEncodedBytes(), 0, indexKeyBytes, 0,
                    indexKeyDepth);
        }
        //
        // If we are inserting the new key as the first key of the right
        // page, then do not scan because the insert key is the same as the
        // split key.
        //
        if (!firstRight) {
            for (int p = scanStart; p <= splitAtPosition; p += KEYBLOCK_LENGTH) {
                int kbData = getInt(p);
                int ebc = decodeKeyBlockEbc(kbData);
                int db = decodeKeyBlockDb(kbData);
                int tail = decodeKeyBlockTail(kbData);
                if (ebc > indexKeyDepth) {
                    throw new InvalidPageStructureException("ebc at " + p
                            + " ebc=" + ebc + " > indexKeyDepth="
                            + indexKeyDepth);
                }
                indexKeyDepth = ebc;
                indexKeyBytes[indexKeyDepth++] = (byte) db;
                int tbData = getInt(tail);
                int klength = decodeTailBlockKLength(tbData);

                System.arraycopy(_bytes, tail + _tailHeaderSize, indexKeyBytes,
                        indexKeyDepth, klength);
                indexKeyDepth += klength;
            }
        }
        indexKey.setEncodedSize(indexKeyDepth);
        //
        // Set the keyblock end for the right sibling so that alloc() knows
        // its bounds.
        //
        rightSibling._keyBlockEnd = _keyBlockEnd
                - (splitAtPosition - KEY_BLOCK_START);
        int rightP = rightSibling.KEY_BLOCK_START;
        //
        // Now move all the records from the split point forward to the
        // right page and deallocate their space in the left page.
        //
        for (int p = splitAtPosition; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
            int kbData = getInt(p);
            int db = decodeKeyBlockDb(kbData);
            int ebc = decodeKeyBlockEbc(kbData);
            int tail = decodeKeyBlockTail(kbData);

            int tbData = getInt(tail);
            int klength = decodeTailBlockKLength(tbData);
            int tailBlockSize = decodeTailBlockSize(tbData);
            int dataSize = tailBlockSize - _tailHeaderSize - klength;
            int newKeyLength;
            int newDataSize;
            int newDb;
            int newEbc;
            //
            // Adjust for first key in right page
            //
            if (p == splitAtPosition) {
                newKeyLength = klength + ebc;
                newDb = ebc > 0 ? indexKeyBytes[0] : db;
                newEbc = 0;
            } else {
                newKeyLength = klength;
                newDb = db;
                newEbc = ebc;
            }
            //
            // Adjust for replacement case.
            //
            if (exact && isDataPage() && foundAtPosition == p) {
                newDataSize = value.getEncodedSize();
                if (Debug.ENABLED)
                    Debug.$assert(newDataSize > dataSize);
            } else {
                newDataSize = dataSize;
            }
            newTailBlockSize = newKeyLength + newDataSize + _tailHeaderSize;
            //
            // Allocate the new tail block.
            //
            int newTailBlock = rightSibling.allocTail(newTailBlockSize);
            if (Debug.ENABLED) {
                Debug.$assert(newTailBlock >= 0
                        && newTailBlock < rightSibling._bufferSize);
                Debug.$assert(newTailBlock != -1);
            }

            rightSibling.putInt(newTailBlock,
                    encodeTailBlock(newTailBlockSize, newKeyLength));
            if (isIndexPage()) {
                rightSibling.putInt(newTailBlock + TAILBLOCK_POINTER,
                        getInt(tail + TAILBLOCK_POINTER));
            }
            if (p == splitAtPosition && ebc > 0) {
                System.arraycopy(indexKeyBytes,
                        1, // Note: byte 0 is the discriminator byte
                        rightSibling._bytes, newTailBlock + _tailHeaderSize,
                        ebc - 1);

                rightSibling.putByte(newTailBlock + _tailHeaderSize + ebc - 1,
                        db);

                System.arraycopy(_bytes, tail + _tailHeaderSize,
                        rightSibling._bytes, newTailBlock + _tailHeaderSize
                                + ebc, klength);
            } else {
                System.arraycopy(_bytes, tail + _tailHeaderSize,
                        rightSibling._bytes, newTailBlock + _tailHeaderSize,
                        klength);
            }

            if (isDataPage()) {
                if (exact && foundAtPosition == p) {
                    if (value != Value.EMPTY_VALUE) {
                        System.arraycopy(value.getEncodedBytes(), 0,
                                rightSibling._bytes, newTailBlock
                                        + _tailHeaderSize + newKeyLength,
                                newDataSize);
                    }
                } else {
                    System.arraycopy(_bytes, tail + _tailHeaderSize + klength,
                            rightSibling._bytes, newTailBlock + _tailHeaderSize
                                    + newKeyLength, newDataSize);
                }
            }
            //
            // Put the key block into the right page.
            //
            rightSibling.putInt(rightP,
                    encodeKeyBlock(newEbc, newDb, newTailBlock));
            rightP += KEYBLOCK_LENGTH;
            //
            // Deallocate the tailblock from the left page. If this is the
            // split key, then we deallocate the whole tail only if the
            // insert key is going in as the first key in the right sibling.
            // Otherwise, we simply deallocate the data.
            //
            //
            if (p != splitAtPosition || (firstRight && !exact)) {
                // deallocate the whole tail block. If this is the split
                // position, then we are going to add a new, different edge
                // key below.
                //
                deallocTail(tail, tailBlockSize);
            } else {
                // This is the split position, and the insert key is either
                // an exact match for the first key in the right sibling page,
                // or it is not being inserted there. In this case, the key
                // we are examining in the left sibling page will become the
                // new edge key. All we need to do is deallocate any extra
                // space needed by this key's data.
                //
                // Later, we will fix up the edge key in the case where the
                // insert key will be the first key of the right sibling.
                //
                if (isDataPage()) {
                    currentSize = (tailBlockSize + ~TAILBLOCK_MASK)
                            & TAILBLOCK_MASK;

                    int newSize = (tailBlockSize - dataSize + ~TAILBLOCK_MASK)
                            & TAILBLOCK_MASK;
                    if (newSize != currentSize) {
                        deallocTail(tail + newSize, currentSize - newSize);
                    }
                    putInt(tail,
                            encodeTailBlock(_tailHeaderSize + klength, klength));
                } else {
                    // edge key for index block. We destroy the pointer value
                    // so that we don't get confused later.
                    putInt(tail + TAILBLOCK_POINTER, -1);
                }
            }
        }
        //
        // Fix up the right edge key in the left page.
        //
        int kbData = getInt(splitAtPosition);
        int edgeTail = decodeKeyBlockTail(kbData);
        int ebc = decodeKeyBlockEbc(kbData);
        depth = (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;

        if (firstRight && !exact) {
            //
            // Handle the special case where the inserted key is the first
            // key of the right page.
            //
            if (fixupSuccessor) {
                depth = ebc;
            }
            int db = indexKeyBytes[depth];

            int edgeKeyLength = indexKey.getEncodedSize() - depth - 1;
            int edgeTailBlockSize = edgeKeyLength + _tailHeaderSize;
            edgeTail = allocTail(edgeTailBlockSize);
            if (edgeTail == -1) {
                _keyBlockEnd = splitAtPosition;
                repack();
                edgeTail = allocTail(edgeTailBlockSize);
            }

            if (Debug.ENABLED)
                Debug.$assert(edgeTail != -1);
            putInt(edgeTail, encodeTailBlock(edgeTailBlockSize, edgeKeyLength));

            System.arraycopy(indexKeyBytes, depth + 1, _bytes, edgeTail
                    + _tailHeaderSize, edgeKeyLength);

            putInt(splitAtPosition, encodeKeyBlock(depth, db, edgeTail));
        }
        //
        // In any case, the new key block end is at the split position.
        //
        _keyBlockEnd = splitAtPosition + KEYBLOCK_LENGTH;
        //
        // If this is in index level, then set the pointer value to -1 so that
        // it does not later get confused and interpreted as a valid pointer.
        //
        if (isIndexPage()) {
            putInt(edgeTail + TAILBLOCK_POINTER, -1); // TODO - fix for long
            // pointers
        }

        invalidateFastIndex();
        rightSibling.invalidateFastIndex();

        if (Debug.ENABLED) {
            Debug.$assert(rightSibling._keyBlockEnd > rightSibling.KEY_BLOCK_START
                    + KEYBLOCK_LENGTH ? rightSibling
                    .adjacentKeyCheck(rightSibling.KEY_BLOCK_START) : true);
        }
        if (!exact) {
            if (foundAtPosition >= splitAtPosition
                    && (!lastLeft || foundAtPosition > splitAtPosition)) {
                foundAt -= (splitAtPosition - KEY_BLOCK_START);
                if (firstRight && !fixupSuccessor) {
                    foundAt = (foundAt & P_MASK) | FIXUP_MASK
                            | (ebc << DEPTH_SHIFT);
                }
                final int t = rightSibling.putValue(key, value, foundAt, true);
                whereInserted = -foundAt;
                Debug.$assert(t != -1);
            } else {
                final int t = putValue(key, value, foundAt, true);
                whereInserted = foundAt;
                Debug.$assert(t != -1);
            }
        } else {
            int p = -1;
            if (foundAtPosition < splitAtPosition) {
                p = replaceValue(key, value, foundAtPosition);
                whereInserted = foundAtPosition;
                //
                // It is really bad if p is less than 0. Means that we failed
                // to replace the value.
                //
                if (Debug.ENABLED) {
                    Debug.$assert(p > 0);
                }
                if (p <= 0) {
                    throw new IllegalStateException("p = " + p
                            + " foundAtPosition=" + foundAtPosition
                            + " splitAtPosition=" + splitAtPosition);
                }
            }
            // If foundAtPosition >= splitAtPosition then the split code already
            // copied
            // the new value into the right sibling page.
        }

        if (Debug.ENABLED) {
            Debug.$assert(KEY_BLOCK_START + KEYBLOCK_LENGTH < _keyBlockEnd);
            Debug.$assert(rightSibling.KEY_BLOCK_START + KEYBLOCK_LENGTH < rightSibling._keyBlockEnd);
        }

        // Indicate that both buffers have changed.
        bumpGeneration();
        rightSibling.bumpGeneration();

        if (Debug.ENABLED)
            assertVerify();
        if (Debug.ENABLED)
            rightSibling.assertVerify();
        return whereInserted;
    }

    /**
     * Joins or rebalances two pages as part of a deletion operation. This
     * buffer contains the left edge of the deletion. All the keys at or above
     * foundAt1 are to be removed. The supplied buffer parameter contains the
     * key at the right edge of the deletion. All keys up to, but not including
     * foundAt2 are to be removed.
     * <p>
     * This method attempts to combine all the remaining keys and data into one
     * page. If they will not fit, then it reallocates the keys and values
     * across the two pages. As a side effect, it copies the first key of the
     * rebalanced right page into the supplied spareKey. The caller will then
     * reinsert that key value into index pages above this one.
     * 
     * @param buffer
     *            The buffer containing the right edge key
     * @param foundAt1
     *            Offset of the first key block to remove
     * @param foundAt2
     *            Offset of the first key block in buffer to keep
     * @param indexKey
     *            A Key into which the new right page's first key is copied in
     *            the event this method results in a rebalance operation.
     * @param spareKey
     *            A spare Key used internally for intermediate results
     * @param policy
     *            The JoinPolicy that allocates records between the two pages.
     * @return <i>true</i> if the result is a rebalanced pair of pages.
     * @throws PersistitException
     */

    final boolean join(Buffer buffer, int foundAt1, int foundAt2, Key indexKey,
            Key spareKey, JoinPolicy policy) throws PersistitException {
        foundAt1 &= P_MASK;
        foundAt2 &= P_MASK;

        if (buffer == this || foundAt1 <= KEY_BLOCK_START
                || foundAt1 >= _keyBlockEnd || foundAt2 <= KEY_BLOCK_START
                || foundAt2 >= buffer._keyBlockEnd /*- KEYBLOCK_LENGTH */) {
            if (Debug.ENABLED)
                Debug.debug2(true);
            throw new IllegalArgumentException("foundAt1=" + foundAt1
                    + " foundAt2=" + foundAt2 + " _keyBlockEnd=" + _keyBlockEnd
                    + " buffer._keyBlockEnd=" + buffer._keyBlockEnd);
        }

        if (Debug.ENABLED)
            assertVerify();
        if (Debug.ENABLED)
            buffer.assertVerify();

        //
        // Initialize indexKey to contain the first key of the right
        // page.
        //
        int newEbc = Integer.MAX_VALUE;
        byte[] indexKeyBytes = indexKey.getEncodedBytes();
        int kbData = buffer.getInt(KEY_BLOCK_START);
        indexKeyBytes[0] = (byte) decodeKeyBlockDb(kbData);
        int tail = decodeKeyBlockTail(kbData);
        int tbData = buffer.getInt(tail);
        int klength = decodeTailBlockKLength(tbData);
        System.arraycopy(buffer._bytes, tail + _tailHeaderSize, indexKeyBytes,
                1, klength);
        //
        // Start by assuming all the records will fit into one page. Compute
        // the ebc of the first key after the deletion range. This will be
        // the minimum ebc of all the deleted keys, except that the first
        // key of the second page is ignored (because it is a duplicate of
        // the right edge key of the left page.)
        //
        // At the same time we run these loops we can deallocate the associated
        // tail blocks. That will allow us to measure the available space.
        //
        for (int index = foundAt1; index < _keyBlockEnd; index += KEYBLOCK_LENGTH) {
            kbData = getInt(index);
            int ebc = decodeKeyBlockEbc(kbData);
            if (ebc < newEbc)
                newEbc = ebc;
            tail = decodeKeyBlockTail(kbData);
            tbData = getInt(tail);
            int size = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;
            deallocTail(tail, size);
        }
        for (int index = KEY_BLOCK_START; index < foundAt2; index += KEYBLOCK_LENGTH) {
            kbData = buffer.getInt(index);
            int ebc = decodeKeyBlockEbc(kbData);
            if (ebc < newEbc && index > KEY_BLOCK_START)
                newEbc = ebc;
            tail = decodeKeyBlockTail(kbData);
            tbData = buffer.getInt(tail);
            int size = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;
            buffer.deallocTail(tail, size);
        }
        if (Debug.ENABLED)
            Debug.$assert(newEbc < Integer.MAX_VALUE);
        //
        // We clear the about-to-be deleted key blocks so that repack()
        // operations don't damage just-freed tail blocks.
        //
        clearBytes(foundAt1, _keyBlockEnd);
        buffer.clearBytes(KEY_BLOCK_START, foundAt2);

        kbData = buffer.getInt(foundAt2);
        int oldEbc = decodeKeyBlockEbc(kbData);
        //
        // This is the amount by which the tailblock for the first record
        // after the deletion range would need to increase in if the record
        // were placed immediately after the last record before the deletion
        // range.
        //
        // Now we actually modify the key at foundAt2 to have additional unique
        // bytes (i.e., we reduce its ebc). Once we've done that, we can think
        // of the collection of records in the left and right pages as a
        // continuous virtual page.
        //
        if (newEbc < oldEbc) {
            buffer.reduceEbc(foundAt2, newEbc, indexKeyBytes);
        } else {
            newEbc = oldEbc;
        }

        // Compute the size of the page that would result from combining the
        // two pages into one.
        //
        int virtualSize = foundAt1 + (buffer._keyBlockEnd - foundAt2)
                + (_bufferSize - _alloc) - _slack
                + (buffer._bufferSize - buffer._alloc) - buffer._slack;
        final int virtualKeyCount = ((foundAt1 - KEY_BLOCK_START) + (buffer
                .getKeyBlockEnd() - foundAt2)) / KEYBLOCK_LENGTH;
        boolean okayToRejoin = virtualKeyCount < _pool.getMaxKeys()
                && policy.acceptJoin(this, virtualSize);
        boolean result;

        if (okayToRejoin) {
            // REJOIN CASE
            // -----------
            // This is the case where according to the JoinPolicy, the records
            // from the right page can be merged into the left page. The caller
            // will then deallocate the right page.
            //
            if (Debug.ENABLED)
                Debug.$assert(virtualSize <= _bufferSize);
            //
            // Now we need to process each keyblock from the right buffer,
            // copying its tail block to the left buffer.
            //
            _keyBlockEnd = foundAt1;

            moveRecords(buffer, foundAt2, buffer._keyBlockEnd, _keyBlockEnd,
                    false);
            buffer._keyBlockEnd = KEY_BLOCK_START;
            //
            // unsplice the right buffer from the right sibling chain.
            //
            long rightSibling = buffer.getRightSibling();
            setRightSibling(rightSibling);
            invalidateFastIndex();
            result = false;
        }

        else {
            // REBALANCE CASE
            // --------------
            // This is the case where the records of the two pages cannot be
            // combined into one page, and therefore we will rebalance the
            // records between the two.
            //
            // There is a further possible wrinkle, in that removing a key
            // that falls at the beginning of the right page may in rare
            // circumstances cause a condition in which the pages cannot be
            // rebalanced at all. This happens if the key being removed is
            // short and the one following it is very long. In this event
            // the method throws a RebalanceException. The caller must then
            // split the left page and then retry the join operation. Ugly,
            // but necessary.
            //
            boolean moveLeft = false;
            int joinBest = 0;
            int joinOffset = -1;
            int leftSize = 0;
            int indexKeySize = 0;
            byte[] spareKeyBytes = spareKey.getEncodedBytes();
            //
            // Search the left page for good rebalance point.
            //
            for (int p = KEY_BLOCK_START; p < foundAt1; p += KEYBLOCK_LENGTH) {
                kbData = getInt(p);
                int db = decodeKeyBlockDb(kbData);
                int ebc = decodeKeyBlockEbc(kbData);
                tail = decodeKeyBlockTail(kbData);
                tbData = getInt(tail);
                int size = decodeTailBlockSize(tbData);
                klength = decodeTailBlockKLength(tbData);

                spareKeyBytes[ebc] = (byte) db;
                System.arraycopy(_bytes, tail + _tailHeaderSize, spareKeyBytes,
                        ebc + 1, klength);
                int keySize = ebc + klength + 1;

                int delta = ((size + ebc + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                        - ((size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);

                int candidateRightSize = virtualSize - leftSize + delta;

                int candidateLeftSize = leftSize + KEYBLOCK_LENGTH
                        + ((klength + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                        + _tailHeaderSize;

                int joinFit = policy.rebalanceFit(this, buffer, p, foundAt1,
                        foundAt2, virtualSize, candidateLeftSize,
                        candidateRightSize, _bufferSize - KEY_BLOCK_START /*
                                                                           * PDB
                                                                           * 20050902
                                                                           */);

                if (joinFit > joinBest) {
                    joinBest = joinFit;
                    joinOffset = p;

                    indexKeySize = keySize;

                    System.arraycopy(spareKeyBytes, 0, indexKeyBytes, 0,
                            indexKeySize);
                }

                leftSize += KEYBLOCK_LENGTH
                        + ((size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);
            }

            //
            // Search the right page for good rebalance point.
            //
            for (int p = foundAt2; p < buffer._keyBlockEnd; p += KEYBLOCK_LENGTH) {
                kbData = buffer.getInt(p);
                int db = decodeKeyBlockDb(kbData);
                int ebc = decodeKeyBlockEbc(kbData);
                tail = decodeKeyBlockTail(kbData);
                tbData = buffer.getInt(tail);
                int size = decodeTailBlockSize(tbData);
                klength = decodeTailBlockKLength(tbData);

                spareKeyBytes[ebc] = (byte) db;
                System.arraycopy(buffer._bytes, tail + _tailHeaderSize,
                        spareKeyBytes, ebc + 1, klength);

                int keySize = ebc + klength + 1;
                //
                // This is the ebc for this key if it were to be inserted
                // as the right edge key of the left page.
                //
                int adjustedEbc = p == foundAt2 ? newEbc : ebc;
                //
                // This is the amount by which the tailblock for the candidate
                // rebalance key would have to grow if it became the first
                // key on the right page and its ebc became zero.
                //
                int delta = ((size + ebc + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                        - ((size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);

                int candidateRightSize = virtualSize - leftSize + delta;

                int candidateLeftSize = leftSize
                        + ((klength + (ebc - adjustedEbc) + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                        + _tailHeaderSize + KEYBLOCK_LENGTH;

                int joinFit = policy.rebalanceFit(this, buffer, p, foundAt1,
                        foundAt2, virtualSize, candidateLeftSize,
                        candidateRightSize, _bufferSize);

                if (joinFit > joinBest) {
                    joinBest = joinFit;
                    joinOffset = p;
                    moveLeft = true;

                    indexKeySize = keySize;

                    System.arraycopy(spareKeyBytes, 0, indexKeyBytes, 0,
                            indexKeySize);
                }

                leftSize += KEYBLOCK_LENGTH
                        + ((ebc - adjustedEbc + size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);
            }

            if (joinBest <= 0)
                throw RebalanceException.SINGLETON;
            if (moveLeft) {
                //
                // In this case we are moving records from the right page
                // to the left page.
                //
                _keyBlockEnd = foundAt1;
                int rightSize = buffer._keyBlockEnd - joinOffset;

                moveRecords(buffer, foundAt2, joinOffset, _keyBlockEnd, true);

                System.arraycopy(buffer._bytes, joinOffset, buffer._bytes,
                        KEY_BLOCK_START, rightSize);

                buffer.clearBytes(KEY_BLOCK_START + rightSize,
                        buffer._keyBlockEnd);
                buffer._keyBlockEnd = KEY_BLOCK_START + rightSize;

                buffer.reduceEbc(KEY_BLOCK_START, 0, indexKeyBytes);
            } else {
                //
                // We will move records from the left page to the right page.
                //
                int rightSize = buffer._keyBlockEnd - foundAt2;

                System.arraycopy(buffer._bytes, foundAt2, buffer._bytes,
                        KEY_BLOCK_START, rightSize);

                buffer.clearBytes(KEY_BLOCK_START + rightSize,
                        buffer._keyBlockEnd);

                buffer._keyBlockEnd = KEY_BLOCK_START + rightSize;

                if (joinOffset != foundAt1) {
                    buffer.moveRecords(this, joinOffset, foundAt1,
                            KEY_BLOCK_START, false);
                }
                _keyBlockEnd = joinOffset;
                //
                // Now set up the edge key in the left page.
                //
                moveRecords(buffer, KEY_BLOCK_START, KEY_BLOCK_START,
                        joinOffset, true);

                buffer.reduceEbc(KEY_BLOCK_START, 0, indexKeyBytes);
            }
            //
            // This is now the key that will be reinserted into the index
            // to point to the newly rebalanced right page.
            //
            indexKey.setEncodedSize(indexKeySize);

            setRightSibling(buffer.getPageAddress());

            invalidateFastIndex();
            buffer.invalidateFastIndex();

            result = true;
        }

        if (Debug.ENABLED) {
            Debug.$assert(KEY_BLOCK_START + KEYBLOCK_LENGTH < _keyBlockEnd);
            if (result) {
                Debug.$assert(KEY_BLOCK_START + KEYBLOCK_LENGTH < buffer._keyBlockEnd);
            }
        }
        //
        // Indicate that both buffers have changed
        //
        bumpGeneration();
        buffer.bumpGeneration();

        if (Debug.ENABLED)
            assertVerify();
        if (Debug.ENABLED)
            buffer.assertVerify();

        return result;
    }

    void invalidateFastIndex() {
        if (_fastIndex != null) {
            _fastIndex.invalidate();
        }
    }

    FastIndex getFastIndex() {
        // TODO - replace synchronized with CAS instructions
        synchronized (this) {
            if (_fastIndex == null) {
                _fastIndex = _pool.allocFastIndex();
                _fastIndex.setBuffer(this);
            }
            if (!_fastIndex.isValid()) {
                _fastIndex.recompute();
            }
            return _fastIndex;
        }
    }

    void takeFastIndex() {
        _fastIndex = null;
    }

    /**
     * Only for unit tests.
     * 
     * @param fastIndex
     */
    void setFastIndex(final FastIndex fastIndex) {
        _fastIndex = fastIndex;
    }

    private void reduceEbc(int p, int newEbc, byte[] indexKeyBytes) {
        int kbData = getInt(p);
        int oldDb = decodeKeyBlockDb(kbData);
        int oldEbc = decodeKeyBlockEbc(kbData);
        int tail = decodeKeyBlockTail(kbData);
        int tbData = getInt(tail);
        int size = decodeTailBlockSize(tbData);
        int klength = decodeTailBlockKLength(tbData);

        if (newEbc == oldEbc)
            return;
        if (newEbc > oldEbc) {
            throw new IllegalArgumentException("newEbc=" + newEbc
                    + " must be less than oldEbc=" + oldEbc);
        }

        int delta = ((size + oldEbc - newEbc + ~TAILBLOCK_MASK) & TAILBLOCK_MASK)
                - ((size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);
        int newTail;
        boolean wedged = false;

        if (delta == 0) {
            newTail = tail;
        } else {
            newTail = allocTail(size + delta);
            if (newTail == -1) {
                newTail = wedgeTail(tail, delta);
                if (newTail == -1) {
                    repack();
                    //
                    // Repacking will move the tail blocks so we need
                    // to refetch pointers to the ones we care about.
                    //
                    kbData = getInt(p);
                    tail = decodeKeyBlockTail(kbData);
                    newTail = wedgeTail(tail, delta);

                    if (newTail == -1) {
                        throw new IllegalStateException(
                                "Can't wedge enough space");
                    }
                }
                wedged = true;
            }
        }

        if (newTail != tail && isIndexPage()) {
            // TODO - allow larger pointer size
            //
            putInt(newTail + TAILBLOCK_POINTER,
                    getInt(tail + TAILBLOCK_POINTER));
        }
        System.arraycopy(_bytes, tail + _tailHeaderSize, _bytes, newTail
                + _tailHeaderSize + oldEbc - newEbc, size - _tailHeaderSize);

        _bytes[newTail + _tailHeaderSize + oldEbc - newEbc - 1] = (byte) oldDb;

        System.arraycopy(indexKeyBytes, newEbc + 1, _bytes, newTail
                + _tailHeaderSize, oldEbc - newEbc - 1);

        if (newTail != tail && !wedged) {
            deallocTail(tail, size);
        }

        size += oldEbc - newEbc;
        klength += oldEbc - newEbc;
        int newDb = indexKeyBytes[newEbc] & 0xFF;

        putInt(newTail, encodeTailBlock(size, klength));

        putInt(p, encodeKeyBlock(newEbc, newDb, newTail));
    }

    /**
     * Move records from buffer2 to this buffer. The
     * 
     * @param buffer
     * @param p1
     * @param p2
     * @param insertAt
     * @param includeRightEdge
     */
    private void moveRecords(Buffer buffer, int p1, int p2, int insertAt,
            boolean includesRightEdge) {
        if (p2 - p1 + _keyBlockEnd > _alloc) {
            repack();
        }
        if (insertAt < _keyBlockEnd) {
            System.arraycopy(_bytes, insertAt, _bytes, insertAt + p2 - p1,
                    _keyBlockEnd - insertAt);

        }
        clearBytes(insertAt, insertAt + p2 - p1);

        _keyBlockEnd += p2 - p1;
        if (includesRightEdge)
            _keyBlockEnd += KEYBLOCK_LENGTH;

        for (int p = p1; p < p2 || includesRightEdge && p == p2; p += KEYBLOCK_LENGTH) {
            int kbData = buffer.getInt(p);
            int ebc = decodeKeyBlockEbc(kbData);
            int db = decodeKeyBlockDb(kbData);
            int tail = decodeKeyBlockTail(kbData);
            int tbData = buffer.getInt(tail);
            int size = decodeTailBlockSize(tbData);
            int klength = decodeTailBlockKLength(tbData);
            int newSize = size;
            boolean edgeCase = includesRightEdge && p == p2;
            if (edgeCase) {
                // this is just for the right edge key of the left page
                newSize = _tailHeaderSize + klength;
            }

            int newTail = allocTail(newSize);
            if (newTail == -1) {
                repack();
                newTail = allocTail(newSize);
            }

            if (Debug.ENABLED)
                Debug.$assert(newTail != -1);

            System.arraycopy(buffer._bytes, tail + 4, _bytes, newTail + 4,
                    newSize - 4);

            putInt(newTail, encodeTailBlock(newSize, klength));

            if (edgeCase && isIndexPage()) {
                putInt(newTail + TAILBLOCK_POINTER, -1);
            }

            putInt(insertAt, encodeKeyBlock(ebc, db, newTail));
            insertAt += KEYBLOCK_LENGTH;

            if (!edgeCase) {
                //
                // Remove the record from the source page.
                //
                buffer.deallocTail(tail, size);
                buffer.putInt(p, 0);
            }
        }
    }

    /**
     * Allocates a tail block of the specified size and returns its offset. Note
     * that if this method cannot allocate sufficient space without repacking,
     * it returns -1. The caller can call repack() and then call allocTail
     * again. The caller is responsible for repacking because it may need to
     * perform various other actions if tail blocks move.
     * 
     * @param size
     *            Size of the required tail block
     * @return Offset to the allocated tail block, or -1 if there is not
     *         sufficient free space.
     */
    private int allocTail(int size) {
        size = (size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;
        int alloc = _alloc - size;
        if (alloc >= _keyBlockEnd) {
            _alloc = alloc;
            return alloc;
        } else {
            return -1;
        }
    }

    private void deallocTail(int tail, int size) {
        size = (size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK;

        if (Debug.ENABLED) {
            Debug.$assert((size > 0 && size <= _bufferSize - _alloc)
                    && (tail >= _alloc && tail < _bufferSize)
                    && (tail + size <= _bufferSize));
        }

        if (tail == _alloc) {
            //
            // If we are deallocating the lower tail block, then aggregate
            // any free space above this block.
            //
            while (tail + size < _bufferSize) {
                int kbNext = getInt(tail + size);
                if ((kbNext & TAILBLOCK_INUSE_MASK) == 0) {
                    int sizeNext = decodeTailBlockSize(kbNext);
                    if (Debug.ENABLED) {
                        Debug.$assert((sizeNext & ~TAILBLOCK_MASK) == 0
                                && sizeNext != 0);
                    }
                    _slack -= sizeNext;
                    putInt(tail + size, 0);
                    size += sizeNext;
                } else
                    break;
            }
            _alloc += size;
        }

        else {
            putInt(tail, encodeFreeBlock(size));
            _slack += size;
        }
    }

    /**
     * Repacks the tail blocks so that they are contiguous.
     */
    private void repack() {
        Debug.$assert(isMine());

        int[] plan = getRepackPlanBuffer();
        //
        // Phase 1:
        // For each allocated tail block, post the offset of its
        // left sibling and the count of free bytes between the left
        // sibling and this block.
        //
        int free = 0;
        int back = 0;
        for (int tail = _alloc; tail < _bufferSize;) {
            int tbData = getInt(tail);

            int size = (decodeTailBlockSize(tbData) + ~TAILBLOCK_MASK)
                    & TAILBLOCK_MASK;

            if ((tbData & TAILBLOCK_INUSE_MASK) != 0) {
                plan[tail / TAILBLOCK_FACTOR] = (back << 16) | free;
                free = 0;
                back = tail;
            } else {
                free += size;
            }
            tail += size;
        }
        // Phase 2:
        // Move each tailblock rightward by the accumulated free space
        // that has been removed. Update the plan to represent the
        // shift distance for each allocated block.
        //
        int alloc = _bufferSize;
        int moveFrom = 0;
        int moveSize = 0;

        for (int tail = back; tail != 0;) {
            moveFrom = tail;
            if (free > 0)
                moveSize += alloc - tail - free;
            alloc = tail + free;
            int planData = plan[tail / TAILBLOCK_FACTOR];
            plan[tail / TAILBLOCK_FACTOR] = free + tail;
            int deltaFree = planData & 0xFFFF;
            if (deltaFree > 0 && moveSize > 0 && free > 0) {
                System.arraycopy(_bytes, moveFrom, _bytes, moveFrom + free,
                        moveSize);
                moveSize = 0;
            }

            free += deltaFree;
            tail = planData >>> 16;
        }
        if (moveSize > 0 && free > 0) {
            System.arraycopy(_bytes, moveFrom, _bytes, moveFrom + free,
                    moveSize);
        }
        _alloc = alloc;
        _slack = 0;
        //
        // Phase 3:
        // Fix up the key blocks.
        //
        if (free > 0) {
            for (int p = KEY_BLOCK_START; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
                int kbData = getInt(p);
                //
                // For certain remove operations, we may have invalid keyblocks
                // in range. We can safely ignore them here.
                //
                if (kbData != 0) {
                    int tail = decodeKeyBlockTail(kbData);
                    int newTail = plan[tail / KEYBLOCK_LENGTH];

                    if (newTail != tail) {
                        putInt(p, encodeKeyBlockTail(kbData, newTail));
                    }
                }
            }
        }
        releaseRepackPlanBuffer(plan);
    }

    /**
     * Determines whether it is possible to allocate a new tailblock with a
     * specified size.
     * 
     * @param needed
     *            The size required by the proposed new tailblock.
     * @return boolean <i>true</i> if it will fit, else <i>false</i>.
     */
    private boolean willFit(int needed) {
        return needed <= (_alloc - _keyBlockEnd + _slack);
    }

    /**
     * Determines whether supplied index points to the right of the last key in
     * the page.
     * 
     * @param foundAt
     *            The keyblock index
     */
    boolean isAfterRightEdge(int foundAt) {
        int p = foundAt & P_MASK;
        return (p >= _keyBlockEnd)
                || ((p == _keyBlockEnd - KEYBLOCK_LENGTH && (foundAt & EXACT_MASK) != 0));
    }

    /**
     * Determines whether supplied index points to the left of the first key in
     * the page.
     * 
     * @param foundAt
     */
    boolean isBeforeLeftEdge(int foundAt) {
        return (((foundAt & EXACT_MASK) == 0 && (foundAt & P_MASK) <= KEY_BLOCK_START) || (foundAt & P_MASK) < KEY_BLOCK_START);
    }

    // ------------------------------------------------------------------------
    byte[] getBytes() {
        return _bytes;
    }

    int getByte(int index) {
        return (_bytes[index] & 0xFF);
    }

    int getChar(int index) {
        if (Persistit.BIG_ENDIAN) {
            return (_bytes[index + 1] & 0xFF) | (_bytes[index] & 0xFF) << 8;
        } else {
            return (_bytes[index] & 0xFF) | (_bytes[index + 1] & 0xFF) << 8;
        }
    }

    int getInt(int index) {
        if (Persistit.BIG_ENDIAN) {
            return (_bytes[index + 3] & 0xFF) | (_bytes[index + 2] & 0xFF) << 8
                    | (_bytes[index + 1] & 0xFF) << 16
                    | (_bytes[index] & 0xFF) << 24;
        } else {
            return (_bytes[index] & 0xFF) | (_bytes[index + 1] & 0xFF) << 8
                    | (_bytes[index + 2] & 0xFF) << 16
                    | (_bytes[index + 3] & 0xFF) << 24;
        }
    }

    long getLong(int index) {
        if (Persistit.BIG_ENDIAN) {
            return (long) (_bytes[index + 7] & 0xFF)
                    | (long) (_bytes[index + 6] & 0xFF) << 8
                    | (long) (_bytes[index + 5] & 0xFF) << 16
                    | (long) (_bytes[index + 4] & 0xFF) << 24
                    | (long) (_bytes[index + 3] & 0xFF) << 32
                    | (long) (_bytes[index + 2] & 0xFF) << 40
                    | (long) (_bytes[index + 1] & 0xFF) << 48
                    | (long) (_bytes[index] & 0xFF) << 56;
        } else {
            return (long) (_bytes[index] & 0xFF)
                    | (long) (_bytes[index + 1] & 0xFF) << 8
                    | (long) (_bytes[index + 2] & 0xFF) << 16
                    | (long) (_bytes[index + 3] & 0xFF) << 24
                    | (long) (_bytes[index + 4] & 0xFF) << 32
                    | (long) (_bytes[index + 5] & 0xFF) << 40
                    | (long) (_bytes[index + 6] & 0xFF) << 48
                    | (long) (_bytes[index + 7] & 0xFF) << 56;
        }
    }

    int getDb(int index) {
        if (Persistit.BIG_ENDIAN) {
            return _bytes[index + 3] & 0xFF;
        } else {
            return _bytes[index + 3] & 0xFF;
        }
    }

    void putByte(int index, int value) {
        if (Debug.ENABLED)
            Debug.$assert(index >= 0 && index + 1 <= _bytes.length);
        _bytes[index] = (byte) (value);
    }

    void putChar(int index, int value) {
        if (Debug.ENABLED)
            Debug.$assert(index >= 0 && index + 2 <= _bytes.length);
        if (Persistit.BIG_ENDIAN) {
            _bytes[index + 1] = (byte) (value);
            _bytes[index] = (byte) (value >>> 8);
        } else {
            _bytes[index] = (byte) (value);
            _bytes[index + 1] = (byte) (value >>> 8);
        }
    }

    void putInt(int index, int value) {
        if (Debug.ENABLED)
            Debug.$assert(index >= 0 && index + 4 <= _bytes.length);
        if (Persistit.BIG_ENDIAN) {
            _bytes[index + 3] = (byte) (value);
            _bytes[index + 2] = (byte) (value >>> 8);
            _bytes[index + 1] = (byte) (value >>> 16);
            _bytes[index] = (byte) (value >>> 24);
        } else {
            _bytes[index] = (byte) (value);
            _bytes[index + 1] = (byte) (value >>> 8);
            _bytes[index + 2] = (byte) (value >>> 16);
            _bytes[index + 3] = (byte) (value >>> 24);
        }
    }

    void putLong(int index, long value) {
        if (Debug.ENABLED)
            Debug.$assert(index >= 0 && index + 8 <= _bytes.length);

        if (Persistit.BIG_ENDIAN) {
            _bytes[index + 7] = (byte) (value);
            _bytes[index + 6] = (byte) (value >>> 8);
            _bytes[index + 5] = (byte) (value >>> 16);
            _bytes[index + 4] = (byte) (value >>> 24);
            _bytes[index + 3] = (byte) (value >>> 32);
            _bytes[index + 2] = (byte) (value >>> 40);
            _bytes[index + 1] = (byte) (value >>> 48);
            _bytes[index] = (byte) (value >>> 56);
        } else {
            _bytes[index] = (byte) (value);
            _bytes[index + 1] = (byte) (value >>> 8);
            _bytes[index + 2] = (byte) (value >>> 16);
            _bytes[index + 3] = (byte) (value >>> 24);
            _bytes[index + 4] = (byte) (value >>> 32);
            _bytes[index + 5] = (byte) (value >>> 40);
            _bytes[index + 6] = (byte) (value >>> 48);
            _bytes[index + 7] = (byte) (value >>> 56);
        }
    }

    static void writeLongRecordDescriptor(byte[] bytes, int size, long pageAddr) {
        if (bytes.length != LONGREC_SIZE) {
            throw new IllegalArgumentException(
                    "Bad LONG_RECORD descriptor size: " + size);
        }
        bytes[0] = (byte) LONGREC_TYPE;
        bytes[1] = (byte) 0;
        Util.putChar(bytes, LONGREC_PREFIX_SIZE_OFFSET, LONGREC_PREFIX_SIZE);
        Util.putLong(bytes, LONGREC_SIZE_OFFSET, size);
        Util.putLong(bytes, LONGREC_PAGE_OFFSET, pageAddr);
    }

    static int decodeLongRecordDescriptorSize(byte[] bytes, int offset) {
        int type;
        if ((type = (bytes[offset] & 0xFF)) != LONGREC_TYPE) {
            throw new IllegalArgumentException(
                    "Bad LONG_RECORD descriptor type: " + type);
        }
        return (int) Util.getLong(bytes, offset + LONGREC_SIZE_OFFSET);
    }

    static long decodeLongRecordDescriptorPointer(byte[] bytes, int offset) {
        int type;
        if ((type = (bytes[offset] & 0xFF)) != LONGREC_TYPE) {
            throw new IllegalArgumentException(
                    "Bad LONG_RECORD descriptor type: " + type);
        }
        return Util.getLong(bytes, offset + LONGREC_PAGE_OFFSET);
    }

    /**
     * @return <i>true</i> if the current page is unallocated
     */
    public boolean isUnallocatedPage() {
        return _type == PAGE_TYPE_UNALLOCATED;
    }

    /**
     * @return <i>true</i> if the current page is a data page
     */
    public boolean isDataPage() {
        return _type == PAGE_TYPE_DATA;
    }

    /**
     * @return <i>true</i> if the current page is an index page
     */
    public boolean isIndexPage() {
        return _type >= PAGE_TYPE_INDEX_MIN && _type <= PAGE_TYPE_INDEX_MAX;
    }

    /**
     * @return <i>true</i> if the current page is a garbage page
     */
    public boolean isGarbagePage() {
        return _type == PAGE_TYPE_GARBAGE;
    }

    /**
     * @return <i>true</i> if the current page is the head page of a
     *         {@link Volume}
     */
    public boolean isHeadPage() {
        return _type == PAGE_TYPE_HEAD;
    }

    /**
     * @return <i>true</i> if the current page is a long record page
     */
    public boolean isLongRecordPage() {
        return _type == PAGE_TYPE_LONG_RECORD;
    }

    static int encodeKeyBlock(int ebc, int db, int tail) {
        return ((ebc << EBC_SHIFT) & EBC_MASK)
                | ((db /* << DB_SHIFT */) & DB_MASK)
                | ((tail << TAIL_SHIFT) & TAIL_MASK);
    }

    static int encodeKeyBlockTail(int kbData, int tail) {
        return (kbData & ~TAIL_MASK) | ((tail << TAIL_SHIFT) & TAIL_MASK);
    }

    static int encodeTailBlock(int size, int klength) {
        return ((klength << TAILBLOCK_KLENGTH_SHIFT) & TAILBLOCK_KLENGTH_MASK)
                | ((size /* << TAILBLOCK_SIZE_SHIFT */) & TAILBLOCK_SIZE_MASK)
                | TAILBLOCK_INUSE_MASK;
    }

    static int encodeFreeBlock(int size) {
        return ((size /* << TAILBLOCK_SIZE_SHIFT */) & TAILBLOCK_SIZE_MASK);
    }

    static int decodeKeyBlockEbc(int kbData) {
        return (kbData & EBC_MASK) >>> EBC_SHIFT;
    }

    static int decodeKeyBlockDb(int kbData) {
        return (kbData & DB_MASK) /* >>> DB_SHIFT */;
    }

    static int decodeKeyBlockTail(int kbData) {
        return (kbData & TAIL_MASK) >>> TAIL_SHIFT;
    }

    static int decodeTailBlockSize(int tbData) {
        return (tbData & TAILBLOCK_SIZE_MASK) /* >>> TAILBLOCK_SIZE_SHIFT */;
    }

    static int decodeTailBlockKLength(int tbData) {
        return (tbData & TAILBLOCK_KLENGTH_MASK) >>> TAILBLOCK_KLENGTH_SHIFT;
    }

    static boolean decodeTailBlockInUse(int tbData) {
        return (tbData & TAILBLOCK_INUSE_MASK) != 0;
    }

    static int decodeDepth(int foundAt) {
        return (foundAt & DEPTH_MASK) >>> DEPTH_SHIFT;
    }

    final int[] getRepackPlanBuffer() {
        synchronized (REPACK_BUFFER_STACK) {
            if (REPACK_BUFFER_STACK.isEmpty()) {
                return new int[MAX_BUFFER_SIZE / TAILBLOCK_FACTOR];
            } else
                return (int[]) REPACK_BUFFER_STACK.pop();
        }
    }

    final void releaseRepackPlanBuffer(int[] plan) {
        synchronized (REPACK_BUFFER_STACK) {
            REPACK_BUFFER_STACK.push(plan);
        }
    }

    InvalidPageStructureException verify(Key key) {
        if (_page == 0) {
            return new InvalidPageStructureException(
                    "head page is neither a data page nor an index page");
        }
        if (!isIndexPage() && !isDataPage()) {
            return new InvalidPageStructureException("page type " + _type
                    + " is neither data page nor an index page");
        }
        BitSet bitSet = null;
        if (isIndexPage())
            bitSet = new BitSet(); // TODO - this may kill GC
        if (key == null)
            key = new Key(_persistit);

        byte[] kb = key.getEncodedBytes();
        int[] plan = getRepackPlanBuffer();
        for (int index = 0; index < plan.length; index++) {
            plan[index] = 0;
        }

        for (int p = KEY_BLOCK_START; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
            int kbData = getInt(p);
            int db = decodeKeyBlockDb(kbData);
            int ebc = decodeKeyBlockEbc(kbData);
            int tail = decodeKeyBlockTail(kbData);

            if (p == KEY_BLOCK_START && ebc != 0) {
                return new InvalidPageStructureException("invalid initial ebc "
                        + ebc + " for keyblock at " + p + " --[" + summarize()
                        + "]");
            }

            if (tail < _keyBlockEnd || tail < _alloc
                    || tail > _bufferSize - _tailHeaderSize
                    || (tail & ~TAILBLOCK_MASK) != 0) {
                return new InvalidPageStructureException(
                        "invalid tail block offset " + tail
                                + " for keyblock at " + p + " --["
                                + summarize() + "]");
            }
            int tbData = getInt(tail);
            int klength = decodeTailBlockKLength(tbData);
            if ((tbData & TAILBLOCK_INUSE_MASK) == 0) {
                return new InvalidPageStructureException(
                        "not in-use tail block offset " + tail
                                + " for keyblock at " + p + " --["
                                + summarize() + "]");
            }
            // Verify that first key in this pages matches the final key
            // of the preceding page.
            if (p == KEY_BLOCK_START && key.getEncodedSize() != 0) {
                int index = 0;
                int compare = 0;
                int size = key.getEncodedSize();
                if (klength < size)
                    size = klength + 1;
                compare = (kb[0] & 0xFF) - db;
                while (compare == 0 && ++index < size) {
                    compare = (kb[index] & 0xFF)
                            - (_bytes[tail + _tailHeaderSize + index - 1] & 0xFF);
                }
                if (compare != 0) {
                    String s = compare < 0 ? "too big" : "too small";
                    return new InvalidPageStructureException("initial key " + s
                            + " at offset " + index + " for keyblock at " + p
                            + " --[" + summarize() + "]");
                }
            }
            // Verify that successor keys follow in sequence.
            if (p > KEY_BLOCK_START && ebc < key.getEncodedSize()) {
                int dbPrev = kb[ebc] & 0xFF;
                if (db < dbPrev) {
                    return new InvalidPageStructureException(
                            "db not greater: db=" + db + " dbPrev=" + dbPrev
                                    + " for keyblock at " + p + " --["
                                    + summarize() + "]");
                }
            }
            //
            // If this is an index page, make sure the pointer isn't redundant
            //
            if (isIndexPage()) {
                int pointer = getInt(tail + 4); // TODO - long pointers
                if (pointer == -1) {
                    if (p + KEYBLOCK_LENGTH != _keyBlockEnd) {
                        return new InvalidPageStructureException(
                                "index pointer has pointer to -1 "
                                        + " for keyblock at " + p + " --["
                                        + summarize() + "]");
                    }
                } else if (bitSet.get(pointer)) {
                    return new InvalidPageStructureException(
                            "index page has multiple keys pointing to same page "
                                    + " for keyblock at " + p + " --["
                                    + summarize() + "]");
                } else
                    bitSet.set(pointer);
            }

            if (_pool != null && getKeyCount() > _pool.getMaxKeys()) {
                return new InvalidPageStructureException(
                        "page has too many keys: has " + getKeyCount()
                                + " but max is " + _pool.getMaxKeys());
            }
            
            kb[ebc] = (byte) db;
            System.arraycopy(_bytes, tail + _tailHeaderSize, kb, ebc + 1,
                    klength);
            key.setEncodedSize(ebc + klength + 1);
            plan[tail / TAILBLOCK_FACTOR] = p;
        }

        // Now check the free blocks

        int formerBlock = _alloc;
        for (int tail = _alloc; tail < _bufferSize;) {
            if ((tail & ~TAILBLOCK_MASK) != 0 || tail < 0 || tail > _bufferSize) {
                return new InvalidPageStructureException("Tail block at "
                        + formerBlock + " is invalid");
            }
            int tbData = getInt(tail);
            int size = decodeTailBlockSize(tbData);
            if (size <= ~TAILBLOCK_MASK || size >= _bufferSize - _keyBlockEnd) {
                return new InvalidPageStructureException("Tailblock at " + tail
                        + " has invalid size=" + size);
            }
            if ((tbData & TAILBLOCK_INUSE_MASK) != 0) {
                if (plan[tail / TAILBLOCK_FACTOR] == 0) {
                    return new InvalidPageStructureException("Tailblock at "
                            + tail + " is in use, but no key "
                            + " block points to it.");
                }
                int klength = decodeTailBlockKLength(tbData);
                {
                    if (klength + _tailHeaderSize > size) {
                        return new InvalidPageStructureException(
                                "Tailblock at " + tail + " has klength="
                                        + klength + " longer than size=" + size
                                        + " - headerSize=" + _tailHeaderSize);
                    }
                }
            } else {
                if (plan[tail / TAILBLOCK_FACTOR] != 0) {
                    return new InvalidPageStructureException("Tailblock at "
                            + tail + " is marked free, but the "
                            + " key block at " + plan[tail / TAILBLOCK_FACTOR]
                            + " points to it.");
                }
            }
            tail += ((size + ~TAILBLOCK_MASK) & TAILBLOCK_MASK);
        }
        releaseRepackPlanBuffer(plan);
        return null;
    }

    /**
     * @return A displyable summary of information about the page contained in
     *         this <code>Buffer</code>.
     */
    public String summarize() {
        return "page=" + _page + " type=" + getPageTypeName()
                + " rightSibling=" + _rightSibling + " status="
                + getStatusDisplayString() + " start=" + KEY_BLOCK_START
                + " end=" + _keyBlockEnd + " size=" + _bufferSize + " alloc="
                + _alloc + " slack=" + _slack + " index=" + _poolIndex
                + " timestamp=" + _timestamp + " generation=" + _generation;
    }

    public String toString() {
        if (_toStringDebug) {
            return toStringDetail();
        }

        return "Page " + _page + " in Volume " + _vol + " at index "
                + _poolIndex + " status=" + getStatusDisplayString() + " type="
                + getPageTypeName();
    }

    /**
     * @return a human-readable inventory of the contents of this buffer
     */
    public String toStringDetail() {
        final StringBuilder sb = new StringBuilder(String.format(
                "Page %,d in volume %s at index @%,d status %s type %s", _page,
                _vol, _poolIndex, getStatusDisplayString(), getPageTypeName()));
        if (!isValid()) {
            sb.append(" - invalid");
        } else if (isDataPage() || isIndexPage()) {
            sb.append(String.format("\n  type=%,d  alloc=%,d  slack=%,d  "
                    + "keyBlockStart=%,d  keyBlockEnd=%,d "
                    + "timestamp=%,d generation=%,d right=%,d hash=%,d", _type,
                    _alloc, _slack, KEY_BLOCK_START, _keyBlockEnd,
                    getTimestamp(), getGeneration(), getRightSibling(),
                    _pool.hashIndex(_vol, _page)));

            try {
                final Key key = new Key((Persistit) null);
                final Value value = new Value((Persistit) null);

                for (RecordInfo r : getRecords()) {
                    r.getKeyState().copyTo(key);
                    if (isDataPage()) {
                        r.getValueState().copyTo(value);
                        sb.append(String
                                .format("\n   %5d: db=%3d ebc=%3d tb=%,5d [%,d]%s=[%,d]%s",
                                        r.getKbOffset(), r.getDb(), r.getEbc(),
                                        r.getTbOffset(), r.getKLength(), key, r
                                                .getValueState()
                                                .getEncodedBytes().length,
                                        abridge(value)));
                    } else {
                        sb.append(String.format(
                                "\n  %5d: db=%3d ebc=%3d tb=%,5d [%,d]%s->%,d",
                                r.getKbOffset(), r.getDb(), r.getEbc(),
                                r.getTbOffset(), r.getKLength(), key,
                                r.getPointerValue()));
                    }
                }
            } catch (Exception e) {
                sb.append(" - " + e);
            }
        } else if (isHeadPage()) {
            //
            // TODO - Desperately needs to be refactored so that Volume and this
            // method don't need all these constant byte references.
            //
            sb.append(String.format("\n  type=%,d  "
                    + "timestamp=%,d generation=%,d right=%,d hash=%,d", _type,
                    getTimestamp(), getGeneration(), getRightSibling(),
                    _pool.hashIndex(_vol, _page)));
            sb.append(String
                    .format("\n  highestPageUsed=%,d pageCount=%,d "
                            + "firstAvailablePage=%,d directoryRootPage=%,d garbageRootPage=%,d id=%,d ",
                            Util.getLong(_bytes, 104),
                            Util.getLong(_bytes, 112),
                            Util.getLong(_bytes, 136),
                            Util.getLong(_bytes, 144),
                            Util.getLong(_bytes, 152), Util.getLong(_bytes, 32)));
        } else if (isGarbagePage()) {
            sb.append(String.format("\n  type=%,d  "
                    + "timestamp=%,d generation=%,d right=%,d hash=%,d", _type,
                    getTimestamp(), getGeneration(), getRightSibling(),
                    _pool.hashIndex(_vol, _page)));
            for (int p = _alloc; p < _bufferSize; p += GARBAGE_BLOCK_SIZE) {
                sb.append(String.format("\n  garbage chain @%,6d : %,d -> %,d",
                        p, getLong(p + GARBAGE_BLOCK_LEFT_PAGE), getLong(p
                                + GARBAGE_BLOCK_RIGHT_PAGE)));
            }
        } else {
            sb.append(String.format("\n  type=%,d  "
                    + "timestamp=%,d generation=%,d right=%,d hash=%,d\n",
                    _type, getTimestamp(), getGeneration(), getRightSibling(),
                    _pool.hashIndex(_vol, _page)));
        }
        return sb.toString();
    }

    private String abridge(final Value value) {
        String s = value.toString();
        if (s.length() > 120) {
            return s.substring(0, 120) + "...";
        } else {
            return s;
        }
    }

    String foundAtString(int p) {
        StringBuilder sb = new StringBuilder("<");
        sb.append(p & P_MASK);
        if ((p & EXACT_MASK) != 0)
            sb.append(":exact");
        if ((p & FIXUP_MASK) > 0)
            sb.append(":fixup");
        sb.append(":depth=");
        sb.append(decodeDepth(p));
        p = p & P_MASK;
        if (p < KEY_BLOCK_START)
            sb.append(":before");
        else if (p >= _keyBlockEnd)
            sb.append(":after");
        else if (p + KEYBLOCK_LENGTH == _keyBlockEnd)
            sb.append(":end");
        else {
            int kbData = getInt(p);
            sb.append(":ebc=" + decodeKeyBlockEbc(kbData));
            sb.append(":db=" + decodeKeyBlockDb(kbData));
            sb.append(":tail=" + decodeKeyBlockTail(kbData));
        }
        sb.append(">");
        return sb.toString();
    }

    /**
     * @return an array of <code>Record</code>s extracted from this buffer.
     */
    public ManagementImpl.RecordInfo[] getRecords() {
        ManagementImpl.RecordInfo[] result = null;

        if (isIndexPage() || isDataPage()) {
            Key key = new Key(_persistit);
            Value value = new Value(_persistit);

            int count = (_keyBlockEnd - KEY_BLOCK_START) / KEYBLOCK_LENGTH;
            result = new ManagementImpl.RecordInfo[count];
            int n = 0;
            for (int p = KEY_BLOCK_START; p < _keyBlockEnd; p += KEYBLOCK_LENGTH) {
                int kbData = getInt(p);
                int db = decodeKeyBlockDb(kbData);
                int ebc = decodeKeyBlockEbc(kbData);
                int tail = decodeKeyBlockTail(kbData);
                int tbData = tail != 0 ? getInt(tail) : 0;
                int size = decodeTailBlockSize(tbData);
                int klength = decodeTailBlockKLength(tbData);
                boolean inUse = decodeTailBlockInUse(tbData);

                ManagementImpl.RecordInfo rec = new ManagementImpl.RecordInfo();
                rec._kbOffset = p;
                rec._tbOffset = tail;
                rec._ebc = ebc;
                rec._db = db;
                rec._klength = klength;
                rec._size = size;
                rec._inUse = inUse;

                byte[] kbytes = key.getEncodedBytes();
                kbytes[ebc] = (byte) db;
                System.arraycopy(_bytes, tail + _tailHeaderSize, kbytes,
                        ebc + 1, klength);
                key.setEncodedSize(ebc + 1 + klength);
                rec._key = new KeyState(key);

                if (isIndexPage()) {
                    // TODO - allow larger pointer size
                    rec._pointerValue = getInt(tail + 4);
                } else {
                    int vsize = size - _tailHeaderSize - klength;
                    if (vsize < 0)
                        vsize = 0;
                    value.putEncodedBytes(_bytes, tail + _tailHeaderSize
                            + klength, vsize);
                    if (value.isDefined()
                            && (value.getEncodedBytes()[0] & 0xFF) == LONGREC_TYPE) {
                        value.setLongRecordMode(true);
                    } else {
                        value.setLongRecordMode(false);
                    }
                    rec._value = new ValueState(value);
                }
                result[n++] = rec;
            }
        } else if (isGarbagePage()) {
            int count = (_bufferSize - _alloc) / GARBAGE_BLOCK_SIZE;
            result = new ManagementImpl.RecordInfo[count];
            int n = 0;
            for (int p = _alloc; p < _bufferSize; p += GARBAGE_BLOCK_SIZE) {
                ManagementImpl.RecordInfo rec = new ManagementImpl.RecordInfo();
                rec._tbOffset = p;
                rec._garbageStatus = getInt(p + GARBAGE_BLOCK_STATUS);
                rec._garbageLeftPage = getLong(p + GARBAGE_BLOCK_LEFT_PAGE);
                rec._garbageRightPage = getLong(p + GARBAGE_BLOCK_RIGHT_PAGE);
                result[n++] = rec;
            }
        }
        return result;
    }

    void assertVerify() {
        if (Debug.VERIFY_PAGES) {
            Exception verifyException = verify(null);
            Debug.$assert(verifyException == null);
        }
    }

    boolean addGarbageChain(long left, long right, long expectedCount) {
        if (Debug.ENABLED)
            Debug.$assert(left > 0 && left <= MAX_VALID_PAGE_ADDR
                    && left != _page && right != _page && isGarbagePage());

        if (_alloc - GARBAGE_BLOCK_SIZE < _keyBlockEnd) {
            return false;
        } else {
            _alloc -= GARBAGE_BLOCK_SIZE;
            putInt(_alloc + GARBAGE_BLOCK_STATUS, 0);
            putLong(_alloc + GARBAGE_BLOCK_LEFT_PAGE, left);
            putLong(_alloc + GARBAGE_BLOCK_RIGHT_PAGE, right);
            putLong(_alloc + GARBAGE_BLOCK_EXPECTED_COUNT, expectedCount);
            bumpGeneration();
            setDirtyStructure();
            return true;
        }
    }

    int getGarbageChainStatus() {
        if (Debug.ENABLED)
            Debug.$assert(isGarbagePage());
        if (_alloc + GARBAGE_BLOCK_SIZE > _bufferSize)
            return -1;
        else
            return getInt(_alloc + GARBAGE_BLOCK_STATUS);
    }

    long getGarbageChainLeftPage() {
        if (Debug.ENABLED)
            Debug.$assert(isGarbagePage());
        if (_alloc + GARBAGE_BLOCK_SIZE > _bufferSize)
            return -1;
        long page = getLong(_alloc + GARBAGE_BLOCK_LEFT_PAGE);
        if (Debug.ENABLED)
            Debug.$assert(page > 0 && page <= MAX_VALID_PAGE_ADDR
                    && page != _page);
        return page;
    }

    long getGarbageChainRightPage() {
        if (Debug.ENABLED)
            Debug.$assert(isGarbagePage());
        if (_alloc + GARBAGE_BLOCK_SIZE > _bufferSize)
            return -1;
        else
            return getLong(_alloc + GARBAGE_BLOCK_RIGHT_PAGE);
    }

    long getGarbageChainLeftPage(int p) {
        long page = getLong(p + GARBAGE_BLOCK_LEFT_PAGE);
        if (Debug.ENABLED)
            Debug.$assert(page > 0 && page <= MAX_VALID_PAGE_ADDR
                    && page != _page);
        return page;
    }

    long getGarbageChainRightPage(int p) {
        return getLong(p + GARBAGE_BLOCK_RIGHT_PAGE);
    }

    boolean removeGarbageChain() {
        if (Debug.ENABLED)
            Debug.$assert(isGarbagePage());
        if (_alloc + GARBAGE_BLOCK_SIZE > _bufferSize)
            return false;
        clearBytes(_alloc, _alloc + GARBAGE_BLOCK_SIZE);
        _alloc += GARBAGE_BLOCK_SIZE;
        bumpGeneration();
        setDirtyStructure();
        return true;
    }

    void setGarbageLeftPage(long left) {
        Debug.$assert(isMine());
        if (Debug.ENABLED) {
            if (Debug.ENABLED)
                Debug.$assert(left > 0 && left <= MAX_VALID_PAGE_ADDR
                        && left != _page);
            Debug.$assert(isGarbagePage());
            Debug.$assert(_alloc + GARBAGE_BLOCK_SIZE <= _bufferSize);
            Debug.$assert(_alloc >= _keyBlockEnd);
        }
        putLong(_alloc + GARBAGE_BLOCK_LEFT_PAGE, left);
        bumpGeneration();
        setDirtyStructure();
    }

    void populateInfo(ManagementImpl.BufferInfo info) {
        info.poolIndex = _poolIndex;
        info.pageAddress = _page;
        info.rightSiblingAddress = _rightSibling;
        Volume vol = _vol;
        if (vol != null) {
            info.volumeId = vol.getId();
            info.volumeName = vol.getPath();
        } else {
            info.volumeId = 0;
            info.volumeName = null;
        }
        info.type = _type;
        info.typeName = getPageTypeName();
        info.bufferSize = _bufferSize;
        info.keyBlockStart = KEY_BLOCK_START;
        info.keyBlockEnd = _keyBlockEnd;
        info.availableBytes = getAvailableSize();
        info.alloc = _alloc;
        info.slack = _slack;
        info.timestamp = _timestamp;
        info.status = getStatus();
        info.statusName = getStatusCode();
        Thread writerThread = getWriterThread();
        if (writerThread != null) {
            info.writerThreadName = writerThread.getName();
        } else {
            info.writerThreadName = null;
        }
        info.updateAcquisitonTime();

    }

    void setNext(final Buffer buffer) {
        _next = buffer;
    }

    Buffer getNext() {
        return _next;
    }

}