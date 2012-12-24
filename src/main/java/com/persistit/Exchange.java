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

import static com.persistit.Buffer.EXACT_MASK;
import static com.persistit.Buffer.HEADER_SIZE;
import static com.persistit.Buffer.KEYBLOCK_LENGTH;
import static com.persistit.Buffer.MAX_VALID_PAGE_ADDR;
import static com.persistit.Buffer.PAGE_TYPE_DATA;
import static com.persistit.Buffer.PAGE_TYPE_INDEX_MIN;
import static com.persistit.Buffer.P_MASK;
import static com.persistit.Buffer.TAILBLOCK_HDR_SIZE_INDEX;
import static com.persistit.Key.AFTER;
import static com.persistit.Key.BEFORE;
import static com.persistit.Key.EQ;
import static com.persistit.Key.GT;
import static com.persistit.Key.GTEQ;
import static com.persistit.Key.LEFT_GUARD_KEY;
import static com.persistit.Key.LT;
import static com.persistit.Key.LTEQ;
import static com.persistit.Key.RIGHT_GUARD_KEY;
import static com.persistit.Key.maxStorableKeySize;
import static com.persistit.util.SequencerConstants.DEALLOCATE_CHAIN_A;
import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_A;
import static com.persistit.util.ThreadSequencer.sequence;

import java.util.ArrayList;
import java.util.List;

import com.persistit.Key.Direction;
import com.persistit.MVV.PrunedVersion;
import com.persistit.ValueHelper.MVVValueWriter;
import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.VolumeStructure.Chain;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.RebalanceException;
import com.persistit.exception.RetryException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.exception.VersionsOutOfOrderException;
import com.persistit.exception.WWRetryException;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * <p>
 * The main facade for fetching, storing and removing records from a
 * Persistit&trade; database.
 * </p>
 * <p>
 * Applications interact with Persistit through instances of this class. A
 * <code>Exchange</code> has two important associated member objects, a
 * {@link com.persistit.Key} and a {@link com.persistit.Value}. A
 * <code>Key</code> is a mutable representation of a key, and a
 * <code>Value</code> is a mutable representation of a value. Applications
 * manipulate these objects and interact with the database through one of the
 * following four general patterns:
 * <ol>
 * <li>
 * Modify the <code>Key</code>, perform a {@link com.persistit.Exchange#fetch
 * fetch} operation, and query the <code>Value</code>.</li>
 * <li>
 * Modify the <code>Key</code>, modify the <code>Value</code>, and then perform
 * a {@link com.persistit.Exchange#store store} operation to insert or replace
 * data in the database.</li>
 * <li>
 * Modify the <code>Key</code>, and then perform a
 * {@link com.persistit.Exchange#remove remove} to remove one or more key/value
 * pairs.</li>
 * <li>
 * Optionally modify the <code>Key</code>, perform a
 * {@link com.persistit.Exchange#traverse traverse} operation, then query the
 * resulting state of <code>Key</code> and/or <code>Value</code> to enumerate
 * key/value pairs currently stored in the database.</li>
 * </ol>
 * <p>
 * Additional methods of <code>Exchange</code> include {@link #fetchAndStore
 * fetchAndStore} and {@link #fetchAndRemove fetchAndRemove} which atomically
 * modify the database and return the former value associated with the current
 * <code>Key</code>.
 * </p>
 * <p>
 * <h3>Exchange is Not Threadsafe</h3>
 * <em>Important:</em> an <code>Exchange</code> and its associated
 * <code>Key</code> and <code>Value</code> instances are <i>not</i> thread-safe.
 * Generally each <code>Thread</code> should allocate and use its own
 * <code>Exchange</code> instances. Were it to occur, modification of the
 * <code>Key</code> or <code>Value</code> objects associated with an
 * <code>Exchange</code> by another thread could cause severe and unpredictable
 * errors, including possible corruption of the underlying data storage. While
 * the methods of one <code>Exchange</code> instance are not threadsafe,
 * Persistit is designed to allow multiple threads, using <em>multiple</em>
 * <code>Exchange</code> instances, to access and update the underlying database
 * in a highly concurrent fashion.
 * </p>
 * <p>
 * <h3>Exchange Pools</h3>
 * Normally each thread should allocate its own <code>Exchange</code> instances.
 * However, depending on the garbage collection performance characteristics of a
 * particular JVM it may be desirable to maintain a pool of
 * <code>Exchange</code>s available for reuse, thereby reducing the frequency
 * with which <code>Exchange</code>s need to be constructed and then garbage
 * collected. An application may get an Exchange using
 * {@link Persistit#getExchange(String, String, boolean)} or
 * {@link Persistit#getExchange(Volume, String, boolean)}. These methods reuse a
 * previously constructed <code>Exchange</code> if one is available in a pool;
 * otherwise they construct methods construct a new one. Applications using the
 * Exchange pool should call
 * {@link Persistit#releaseExchange(Exchange, boolean)} to relinquish an
 * <code>Exchange</code> once it is no longer needed, thereby placing it in the
 * pool for subsequent reuse.
 * </p>
 * 
 * @version 1.0
 */
public class Exchange implements ReadOnlyExchange {

    public enum Sequence {
        NONE, FORWARD, REVERSE
    }

    private static class MvvVisitor implements MVV.VersionVisitor {
        enum Usage {
            FETCH, STORE
        }

        private final static long READ_COMMITTED_TS = TransactionStatus.UNCOMMITTED - 1;

        private final TransactionIndex _ti;
        private final Exchange _exchange;
        private TransactionStatus _status;
        private int _step;
        private int _foundOffset;
        private int _foundLength;
        private long _foundVersion;
        private int _foundStep;
        private Usage _usage;

        private MvvVisitor(final TransactionIndex ti, final Exchange exchange) {
            _ti = ti;
            _exchange = exchange;
        }

        /**
         * @param status
         *            Status to inspect the versions as. <code>null</code> is
         *            allowed iff <code>usage</code> is {@link Usage#FETCH},
         *            which signifies 'read committed' mode.
         * @param step
         *            Current step value associated with <code>status</code>.
         * @param usage
         *            What reason this visit is being done for.
         */
        public void initInternal(final TransactionStatus status, final int step, final Usage usage) {
            Debug.$assert0.t(status != null || usage != Usage.STORE);
            _status = status;
            _step = step;
            _usage = usage;
        }

        public int getOffset() {
            return _foundOffset;
        }

        public int getLength() {
            return _foundLength;
        }

        public boolean foundVersion() {
            return _foundVersion != MVV.VERSION_NOT_FOUND;
        }

        @Override
        public void init() {
            _foundVersion = MVV.VERSION_NOT_FOUND;
            _foundOffset = -1;
            _foundLength = -1;
            _foundStep = 0;
        }

        @Override
        public void sawVersion(final long version, final int offset, final int valueLength) throws PersistitException {
            try {
                switch (_usage) {
                case FETCH:
                    final long ts = _status != null ? _status.getTs() : READ_COMMITTED_TS;
                    final long status = _ti.commitStatus(version, ts, _step);
                    if (status >= 0 && status != TransactionStatus.UNCOMMITTED && status >= _foundVersion) {
                        assert status <= ts;
                        final int step = TransactionIndex.vh2step(version);
                        if (step >= _foundStep || status > _foundVersion) {
                            _foundOffset = offset;
                            _foundLength = valueLength;
                            _foundVersion = status;
                            _foundStep = step;
                        }
                    }
                    break;

                case STORE:
                    final long depends = _ti.wwDependency(version, _status, 0);
                    if (depends == TransactionStatus.TIMED_OUT) {
                        throw new WWRetryException(version);
                    }
                    if (depends != 0 && depends != TransactionStatus.ABORTED) {
                        // version is from concurrent txn that already committed
                        // or timed out waiting to see. Either
                        // way, must abort.
                        _exchange._transaction.rollback();
                        throw new RollbackException();
                    }
                    if (version > _foundVersion) {
                        _foundVersion = version;
                    }
                    break;
                }
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
        }
    }

    /**
     * Maximum number of levels in one tree. (This count represents a highly
     * pathological case: most trees, even large ones, are no more than four or
     * five levels deep.)
     */
    final static int MAX_TREE_DEPTH = 20;
    /**
     * Upper bound on horizontal page searches.
     */
    final static int MAX_WALK_RIGHT = 50;

    private final static int LEFT_CLAIMED = 1;

    private final static int RIGHT_CLAIMED = 2;

    private final static int VERSIONS_OUT_OF_ORDER_RETRY_COUNT = 3;

    private Persistit _persistit;

    private final Key _key;
    private final Value _value;

    private final LevelCache[] _levelCache = new LevelCache[MAX_TREE_DEPTH];

    private BufferPool _pool;
    private Volume _volume;
    private Tree _tree;

    private volatile long _cachedTreeGeneration = -1;
    private volatile int _cacheDepth = 0;

    private Key _spareKey1;
    private Key _spareKey2;
    private final Key _spareKey3;
    private final Key _spareKey4;

    private final Value _spareValue;

    private SplitPolicy _splitPolicy;
    private JoinPolicy _joinPolicy;

    private boolean _isDirectoryExchange = false;

    private Transaction _transaction;

    private boolean _ignoreTransactions;
    private boolean _ignoreMVCCFetch;
    private boolean _storeCausedSplit;
    private int _keysVisitedDuringTraverse;

    private Object _appCache;

    private ReentrantResourceHolder _treeHolder;

    private final MvvVisitor _mvvVisitor;
    private final RawValueWriter _rawValueWriter = new RawValueWriter();
    private final MVVValueWriter _mvvValueWriter = new MVVValueWriter();
    private LongRecordHelper _longRecordHelper;

    private volatile Thread _thread;

    private Exchange(final Persistit persistit) {
        _persistit = persistit;
        _key = new Key(_persistit);
        _spareKey1 = new Key(_persistit);
        _spareKey2 = new Key(_persistit);
        _spareKey3 = new Key(_persistit);
        _spareKey4 = new Key(_persistit);
        _value = new Value(_persistit);
        _spareValue = new Value(_persistit);
        _mvvVisitor = new MvvVisitor(_persistit.getTransactionIndex(), this);
    }

    /**
     * <p>
     * Construct a new <code>Exchange</code> object to create and/or access the
     * {@link Tree} specified by treeName within the {@link Volume} specified by
     * <code>volumeName</code>. This constructor optionally creates a new
     * <code>Tree</code>. If the <code>create</code> parameter is false and a
     * <code>Tree</code> by the specified name does not exist, this constructor
     * throws a {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * The <code>volumeName</tt< you supply must match exactly one open 
     * <code>Volume</code>. The name matches if either (a) the
     * <code>Volume</code> has an optional alias that is equal to the supplied
     * name, or (b) if the supplied name matches a substring of the
     * <code>Volume</code>'s pathname. If there is not unique match for the name
     * you supply, this method throws a
     * {@link com.persistit.exception.VolumeNotFoundException}.
     * </p>
     * 
     * @param volumeName
     *            The volume name that either matches the alias or a partially
     *            matches the pathname of exactly one open <code>Volume</code>.
     * 
     * @param treeName
     *            The tree name
     * 
     * @param create
     *            <code>true</code> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange(final Persistit persistit, final String volumeName, final String treeName, final boolean create)
            throws PersistitException {
        this(persistit, persistit.getVolume(volumeName), treeName, create);
    }

    /**
     * <p>
     * Construct a new <code>Exchange</code> object to create and/or access the
     * {@link Tree} specified by treeName within the specified {@link Volume}.
     * This constructor optionally creates a new <code>Tree</code>. If the
     * <code>create</code> parameter is false and a <code>Tree</code> by the
     * specified name does not exist, this constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * 
     * @param volume
     *            The Volume
     * @param treeName
     *            The tree name
     * @param create
     *            <code>true</code> to create a new Tree if one by the specified
     *            name does not already exist.
     * @throws PersistitException
     */
    public Exchange(final Persistit persistit, final Volume volume, final String treeName, final boolean create)
            throws PersistitException {
        this(persistit);
        if (volume == null) {
            throw new NullPointerException();
        }
        init(volume, treeName, create);
    }

    /**
     * Construct a new <code>Exchange</code> to access the same {@link Volume}
     * and {@link Tree} as the supplied <code>Exchange</code>. The states of the
     * supplied <code>Exchange</code>'s {@link Key} and {@link Value} objects
     * are copied to new the <code>Key</code> and new <code>Value</code>
     * associated with this <code>Exchange</code> so that operations on the two
     * <code>Exchange</code>s initially behave identically.
     * 
     * @param exchange
     *            The <code>Exchange</code> to copy from.
     */
    public Exchange(final Exchange exchange) {
        this(exchange._persistit);
        init(exchange);
    }

    /**
     * Construct a new <code>Exchange</code> to access the specified
     * {@link Tree}.
     * 
     * @param tree
     *            The <code>Tree</code> to access.
     */
    public Exchange(final Tree tree) {
        this(tree._persistit);
        init(tree);
        _volume = tree.getVolume();
        _isDirectoryExchange = tree == _volume.getDirectoryTree();
        initCache();
    }

    void init(final Volume volume, final String treeName, final boolean create) throws PersistitException {
        assertCorrectThread(true);
        if (volume == null) {
            throw new NullPointerException();
        }

        final Tree tree = volume.getTree(treeName, create);
        if (tree == null) {
            throw new TreeNotFoundException(treeName);
        }
        init(tree);
    }

    void init(final Tree tree) {
        assertCorrectThread(true);
        final Volume volume = tree.getVolume();
        _ignoreTransactions = volume.isTemporary();
        _ignoreMVCCFetch = false;
        _pool = _persistit.getBufferPool(volume.getPageSize());
        _transaction = _persistit.getTransaction();
        _key.clear();
        _value.clear();

        if (_volume != volume || _tree != tree) {
            _volume = volume;
            _tree = tree;
            _treeHolder = new ReentrantResourceHolder(_tree);
            _cachedTreeGeneration = -1;
            initCache();
        }
        _splitPolicy = _persistit.getDefaultSplitPolicy();
        _joinPolicy = _persistit.getDefaultJoinPolicy();
    }

    void init(final Exchange exchange) {
        assertCorrectThread(true);
        _persistit = exchange._persistit;
        _volume = exchange._volume;
        _ignoreTransactions = _volume.isTemporary();
        _ignoreMVCCFetch = false;
        _tree = exchange._tree;
        _treeHolder = new ReentrantResourceHolder(_tree);
        _pool = exchange._pool;

        _cachedTreeGeneration = -1;
        _transaction = _persistit.getTransaction();
        _cacheDepth = exchange._cacheDepth;

        initCache();

        for (int index = 0; index < _cacheDepth; index++) {
            exchange._levelCache[index].copyTo(_levelCache[index]);
        }

        exchange._key.copyTo(_key);
        exchange._value.copyTo(_value);
        _splitPolicy = exchange._splitPolicy;
        _joinPolicy = exchange._joinPolicy;
    }

    void removeState(final boolean secure) {
        assertCorrectThread(false);
        _key.clear(secure);
        _value.clear(secure);
        _spareKey1.clear(secure);
        _spareKey2.clear(secure);
        _spareValue.clear(secure);
        _transaction = null;
        _ignoreTransactions = false;
        _ignoreMVCCFetch = false;
        _splitPolicy = _persistit.getDefaultSplitPolicy();
        _joinPolicy = _persistit.getDefaultJoinPolicy();
        _treeHolder.verifyReleased();
    }

    /**
     * Drop all cached optimization information
     */
    public void initCache() {
        assertCorrectThread(true);
        for (int level = 0; level < MAX_TREE_DEPTH; level++) {
            if (_levelCache[level] != null)
                _levelCache[level].invalidate();
            else
                _levelCache[level] = new LevelCache(level);
        }
    }

    private void checkLevelCache() throws PersistitException {

        if (!_tree.isValid()) {
            if (_tree.getVolume().isTemporary()) {
                _tree = _tree.getVolume().getTree(_tree.getName(), true);
                _treeHolder = new ReentrantResourceHolder(_tree);
                _cachedTreeGeneration = -1;
            } else {
                throw new TreeNotFoundException();
            }
        }
        if (_cachedTreeGeneration != _tree.getGeneration()) {
            _cachedTreeGeneration = _tree.getGeneration();
            _cacheDepth = _tree.getDepth();
            for (int index = 0; index < MAX_TREE_DEPTH; index++) {
                final LevelCache lc = _levelCache[index];
                lc.invalidate();
            }
        }
    }

    private class LevelCache {
        int _level;
        Buffer _buffer;
        long _page;
        long _bufferGeneration;
        long _keyGeneration;
        int _foundAt;
        int _lastInsertAt;
        //
        // The remaining fields are used only by raw_removeKeyRangeInternal and
        // its helpers.
        //
        Buffer _leftBuffer;
        Buffer _rightBuffer;
        int _leftFoundAt;
        int _rightFoundAt;
        int _flags;
        long _deallocLeftPage;
        long _deallocRightPage;

        private LevelCache(final int level) {
            _level = level;
        }

        @Override
        public String toString() {
            if (_buffer == null)
                return "<empty>";

            return "Buffer=<" + _buffer + ">" + ", keyGeneration=" + _keyGeneration + ", bufferGeneration="
                    + _bufferGeneration + ", foundAt=" + _buffer.foundAtString(_foundAt) + ">";
        }

        private void copyTo(final LevelCache to) {
            Debug.$assert0.t(to._level == _level || to._level == -1);
            to._buffer = _buffer;
            to._page = _page;
            to._foundAt = _foundAt;
            to._keyGeneration = _keyGeneration;
            to._bufferGeneration = _bufferGeneration;
        }

        private void invalidate() {
            _buffer = null;
            _bufferGeneration = -1;
        }

        private void updateInsert(final Buffer buffer, final Key key, final int foundAt) {
            update(buffer, key, foundAt);
            _lastInsertAt = foundAt;
        }

        private void update(final Buffer buffer, final Key key, final int foundAt) {
            Debug.$assert0.t(_level + PAGE_TYPE_DATA == buffer.getPageType());
            // Debug.$assert0.t(foundAt == -1 || (foundAt & EXACT_MASK) == 0
            // || Buffer.decodeDepth(foundAt) == key.getEncodedSize());

            _page = buffer.getPageAddress();
            _buffer = buffer;
            _bufferGeneration = buffer.getGeneration();

            if (key == _key && foundAt > 0 && !buffer.isAfterRightEdge(foundAt)) {
                _keyGeneration = key.getGeneration();
                _foundAt = foundAt;
            } else {
                _keyGeneration = -1;
                _foundAt = -1;
            }
        }

        private Sequence sequence(final int foundAt) {
            final int delta = ((foundAt & P_MASK) - (_lastInsertAt & P_MASK));
            if ((foundAt & EXACT_MASK) == 0 && delta == KEYBLOCK_LENGTH) {
                return Sequence.FORWARD;
            }
            if ((foundAt & EXACT_MASK) == 0 && delta == 0) {
                return Sequence.REVERSE;
            }
            return Sequence.NONE;
        }

        private void initRemoveFields() {
            _leftBuffer = null;
            _rightBuffer = null;
            _leftFoundAt = -1;
            _rightFoundAt = -1;
            _flags = 0;
        }
    }

    /**
     * Bit flags that are passed to {@link #storeInternal(Key, Value, int, int)}
     * to control various behavior. See each member for specifics.
     */
    static class StoreOptions {
        /** The default, implies none of the further options **/
        public static final int NONE = 0;

        /** Fetch the current value before replacing **/
        public static final int FETCH = 1 << 1;

        /** Use MVCC (store as version or fetch restricted version) **/
        public static final int MVCC = 1 << 2;

        /** Block and use for any acquire operation **/
        public static final int WAIT = 1 << 3;

        /** Perform the store only if key is currently visible **/
        public static final int ONLY_IF_VISIBLE = 1 << 4;

        /**
         * Don't write the store operation to the journal - used when storing
         * AntiValues
         **/
        public static final int DONT_JOURNAL = 1 << 5;
    }

    static enum PruneStatus {
        REMOVED, CHANGED, UNCHANGED
    }

    /**
     * A visitor used with the
     * {@link Exchange#traverse(Key.Direction, boolean, int, TraverseVisitor)}
     * The {@link #visit(ReadOnlyExchange)} method is called once for each
     * <code>Key</code> traversed by the <code>traverse</code> method.
     */
    public interface TraverseVisitor {
        /**
         * Receive an Exchange having <code>Key</code> and <code>Value</code>
         * values set by
         * {@link Exchange#traverse(Key.Direction, boolean, int, TraverseVisitor)}
         * . This method will be called once for each key encountered in the
         * traversal. This method may return <code>false</code> to stop
         * traversing additional keys. </p>
         * <p>
         * The implementation of this method:
         * <ul>
         * <li>Must return quickly, especially in a multi-threaded environment,
         * to avoid blocking other threads that may attempt to update records in
         * the same <code>Buffer</code>,
         * <li>Must not perform update operations on any <codeExchange</code>,
         * especially in a multi-threaded environment, to prevent deadlocks,
         * <li>May read and modify the <code>Key</code> and <code>Value</code>
         * fields of the supplied <code>ReadOnlyExchange</code>. Note, however,
         * that modifying the <code>Key</code> affects the results of subsequent
         * traversal operations.
         * </ul>
         * 
         * @param ex
         *            a {@link ReadOnlyExchange} from which the current
         *            <code>Key</code> and <code>Value</code> may be read
         * @return <code>true</code> to continue traversing keys, or
         *         <code>false</code> to stop
         * @throws PersistitException
         */
        public boolean visit(final ReadOnlyExchange ex) throws PersistitException;
    }

    /**
     * Delegate to {@link Key#reset} on the associated <code>Key</code> object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange reset() {
        getKey().reset();
        return this;
    }

    /**
     * Delegate to {@link Key#clear} on the associated <code>Key</code> object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange clear() {
        getKey().clear();
        return this;
    }

    /**
     * Delegate to {@link Key#setDepth} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange setDepth(final int depth) {
        getKey().setDepth(depth);
        return this;
    }

    /**
     * Delegate to {@link Key#cut(int)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange cut(final int level) {
        getKey().cut(level);
        return this;
    }

    /**
     * Delegate to {@link Key#cut()} on the associated <code>Key</code> object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange cut() {
        getKey().cut();
        return this;
    }

    /**
     * Delegate to {@link Key#append(boolean)} on the associated
     * <code>Key</code> object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final boolean item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(byte)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final byte item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(short)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final short item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(char)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final char item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(int)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final int item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(long)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final long item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(float)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final float item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(double)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final double item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#append(Object)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange append(final Object item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(boolean)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final boolean item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(byte)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final byte item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(short)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final short item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(char)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final char item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(int)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final int item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(long)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final long item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(float)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final float item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(double)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final double item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegate to {@link Key#to(Object)} on the associated <code>Key</code>
     * object.
     * 
     * @return This <code>Exchange</code> to permit method call chaining.
     */
    public Exchange to(final Object item) {
        getKey().to(item);
        return this;
    }

    /**
     * Return the {@link Key} associated with this <code>Exchange</code>.
     * 
     * @return This <code>Key</code>.
     */
    @Override
    public Key getKey() {
        assertCorrectThread(true);
        return _key;
    }

    /**
     * Return the {@link Value} associated with this <code>Exchange</code>.
     * 
     * @return The <code>Value</code>.
     */
    @Override
    public Value getValue() {
        assertCorrectThread(true);
        return _value;
    }

    BufferPool getBufferPool() {
        return _pool;
    }

    /**
     * Return the {@link Volume} containing the data accessed by this
     * <code>Exchange</code>.
     * 
     * @return The <code>Volume</code>.
     */
    @Override
    public Volume getVolume() {
        assertCorrectThread(true);
        return _volume;
    }

    /**
     * Return the {@link Tree} on which this <code>Exchange</code> operates.
     * 
     * @return The <code>Tree</code>
     */
    @Override
    public Tree getTree() {
        assertCorrectThread(true);
        return _tree;
    }

    /**
     * Return the Persistit instance from which this Exchange was created.
     * 
     * @return The <code>Persistit</code> instance.
     */
    @Override
    public Persistit getPersistitInstance() {
        assertCorrectThread(true);
        return _persistit;
    }

    /**
     * Return the count of structural changes committed to the {@link Tree} on
     * which this <code>Exchange</code> operates. This count includes changes
     * committed by all Threads, including the current one. A structural change
     * is one in which at least one key is inserted or deleted. Replacement of
     * an existing value is not counted.
     * 
     * @return The change count
     */
    @Override
    public long getChangeCount() {
        assertCorrectThread(true);
        return _tree.getChangeCount();
    }

    /**
     * An additional <code>Key</code> maintained for the convenience of
     * {@link Transaction}, {@link PersistitMap} and {@link JournalManager}.
     * 
     * @return spareKey1
     */
    Key getAuxiliaryKey1() {
        return _spareKey1;
    }

    /**
     * An additional <code>Key</code> maintained for the convenience of
     * {@link Transaction}, {@link PersistitMap} and {@link JournalManager}.
     * 
     * @return spareKey3
     */
    Key getAuxiliaryKey3() {
        return _spareKey3;
    }

    /**
     * An additional <code>Key</code> maintained for the convenience of
     * {@link Transaction}, {@link PersistitMap} and {@link JournalManager}.
     * 
     * @return spareKey4
     */
    Key getAuxiliaryKey4() {
        return _spareKey4;
    }

    /**
     * An additional <code>Key</code> maintained for the convenience of
     * {@link Transaction}, {@link PersistitMap} and {@link JournalManager}.
     * 
     * @return spareKey2
     */
    Key getAuxiliaryKey2() {
        return _spareKey2;
    }

    /**
     * An additional <code>Value</code> maintained for the convenience of
     * {@link Transaction}.
     * 
     * @return spareValue
     */
    Value getAuxiliaryValue() {
        return _spareValue;
    }

    /**
     * Internal value that, if <code>true</code>, indicates the last store
     * operation caused a page split. Reset on every call to a store method.
     * 
     * @return storeCausedSplit
     */
    boolean getStoreCausedSplit() {
        return _storeCausedSplit;
    }

    /**
     * Internal value that is a counter of the total loops of the inner loop of
     * {@link #traverse(com.persistit.Key.Direction, boolean, int, int, int)}.
     * 
     * @return {@link #_keysVisitedDuringTraverse}
     */
    int getKeysVisitedDuringTraverse() {
        return _keysVisitedDuringTraverse;
    }

    /**
     * Return a displayable String containing the volume name, tree name and
     * current key state for this <code>Exchange</code>.
     * 
     * @return The displayable String
     */
    @Override
    public String toString() {
        return "Exchange(Volume=" + _volume.getPath() + ",Tree=" + _tree.getName() + "," + ",Key=<" + _key.toString()
                + ">)";

    }

    /**
     * Search for a data record by key. Uses and maintains level cache. This
     * method returns a foundAt location within a Buffer.
     * <p />
     * As a side effect, this method populates the root LevelCache instance
     * (_levelCache[0]) and establishes a claim on a Buffer at that level to
     * which the foundAt value refers. The caller of this method MUST release
     * that Buffer when finished with it.
     * 
     * @return Encoded key location within the data page. The page itself is
     *         made valid in the level cache.
     */
    private int search(final Key key, final boolean writer) throws PersistitException {
        Buffer buffer = null;
        checkLevelCache();
        final LevelCache lc = _levelCache[0];
        buffer = quicklyReclaimBuffer(lc, writer);

        if (buffer == null) {
            return searchTree(key, 0, writer);
        }

        checkPageType(buffer, PAGE_TYPE_DATA, true);

        final int foundAt = findKey(buffer, key, lc);

        if (buffer.isBeforeLeftEdge(foundAt) || buffer.isAfterRightEdge(foundAt)) {
            buffer.release();
            return searchTree(key, 0, writer);
        }
        return foundAt;
    }

    /**
     * Helper method to return the result of the {@link Buffer#findKey(Key)}
     * method given a Buffer, a Key and a LevelCache instance. The caller must
     * establish a claim on the Buffer before calling this method. This method
     * determines whether information cached in the LevelCache is still valid;
     * if so the previous result is still valid.
     * 
     * @param buffer
     * @param key
     * @param lc
     * @return
     * @throws PersistitInterruptedException
     */
    private int findKey(final Buffer buffer, final Key key, final LevelCache lc) throws PersistitInterruptedException {
        //
        // Possibly we can accelerate.
        //
        // TODO - metrics on hits vs. misses
        //
        int foundAt = lc._foundAt;
        if (foundAt != -1 && buffer.getGeneration() == lc._bufferGeneration && key == _key
                && key.getGeneration() == lc._keyGeneration) {
            Debug.$assert0.t(buffer.findKey(key) == foundAt);
            return foundAt;
        }
        //
        // Otherwise look it up again.
        //
        foundAt = buffer.findKey(key);
        // TODO - why do this if key != _key
        lc.update(buffer, key, foundAt);
        return foundAt;
    }

    /**
     * Searches for the current key from top down and populates the level cache
     * while doing so.
     * <p />
     * As a side effect, this method populates the root LevelCache instance
     * (_levelCache[0]) and establishes a claim on a Buffer at that level to
     * which the foundAt value refers. The caller of this method MUST release
     * that Buffer when finished with it.
     * 
     * @return Encoded key location within the level. The page itself is valid
     *         within the level cache.
     */
    private int searchTree(final Key key, final int toLevel, final boolean writer) throws PersistitException {
        Buffer oldBuffer = null;
        int currentLevel;
        int foundAt = -1;

        if (!_treeHolder.claim(false)) {
            Debug.$assert0.t(false);
            throw new InUseException("Thread " + Thread.currentThread().getName() + " failed to get reader claim on "
                    + _tree);
        }
        checkLevelCache();

        long pageAddress = _tree.getRootPageAddr();
        long oldPageAddress = pageAddress;
        Debug.$assert0.t(pageAddress != 0);

        try {
            for (currentLevel = _cacheDepth; --currentLevel >= toLevel;) {
                if (pageAddress <= 0) {
                    corrupt("Volume " + _volume + " level=" + currentLevel + " page=" + pageAddress + " oldPage="
                            + oldPageAddress + " key=<" + key.toString() + "> " + " invalid page address");
                }

                foundAt = searchLevel(key, false, pageAddress, currentLevel, writer && currentLevel == toLevel);
                if (oldBuffer != null) {
                    oldBuffer.releaseTouched();
                    oldBuffer = null;
                }

                final LevelCache lc = _levelCache[currentLevel];
                final Buffer buffer = lc._buffer;

                if (buffer == null || buffer.isBeforeLeftEdge(foundAt)) {
                    oldBuffer = buffer; // So it will be released
                    corrupt("Volume " + _volume + " level=" + currentLevel + " page=" + pageAddress + " key=<"
                            + key.toString() + "> " + " is before left edge");
                }

                checkPageType(buffer, currentLevel + PAGE_TYPE_DATA, true);

                if (currentLevel == toLevel) {
                    for (int level = currentLevel; --level > 0;) {
                        _levelCache[level].invalidate();
                    }
                    return foundAt;
                } else if (buffer.isIndexPage()) {
                    int p = foundAt & P_MASK;
                    if ((foundAt & EXACT_MASK) == 0) {
                        p -= KEYBLOCK_LENGTH;
                    }
                    oldBuffer = buffer; // So it will be released
                    oldPageAddress = pageAddress;
                    pageAddress = buffer.getPointer(p);

                    Debug.$assert0.t(pageAddress > 0 && pageAddress < MAX_VALID_PAGE_ADDR);
                } else {
                    oldBuffer = buffer; // So it will be released
                    corrupt("Volume " + _volume + " level=" + currentLevel + " page=" + pageAddress + " key=<"
                            + key.toString() + ">" + " page type=" + buffer.getPageType() + " is invalid");
                }
            }
            // Should never get here.
            return -1;

        } finally {
            if (oldBuffer != null) {
                oldBuffer.releaseTouched();
                oldBuffer = null;
            }
            _treeHolder.release();
        }
    }

    /**
     * Search for the key in the specified page (data or index). This method
     * gets and claims the identified page. If the key is found to be after the
     * right key of that page, this method "walks" right by getting and claiming
     * the right sibling page and then releasing the original page. This pattern
     * implements the B-link-tree semantic that allows searches to proceed while
     * inserts are adjusting the index structure.
     * <p />
     * As a side effect, this method populates the LevelCache instance for the
     * specified <code>currentLevel</code> and establishes a claim on a Buffer
     * at that level. The caller of this method MUST release that Buffer when
     * finished with it.
     * 
     * @param key
     *            Key to search for
     * @param edge
     *            if <code>true</code> select the right-edge key of the left
     *            page, otherwise select the left key of the right page.
     * @param pageAddress
     *            The address of the page to search
     * @param currentLevel
     *            current level in the tree
     * @return Encoded key location within the page.
     */
    private int searchLevel(final Key key, final boolean edge, long pageAddress, final int currentLevel,
            final boolean writer) throws PersistitException {
        Buffer oldBuffer = null;
        try {
            final long initialPageAddress = pageAddress; // DEBUG - debugging
                                                         // only
            long oldPageAddress = pageAddress;
            for (int rightWalk = MAX_WALK_RIGHT; rightWalk-- > 0;) {
                Buffer buffer = null;
                if (pageAddress <= 0 || pageAddress >= _volume.getStorage().getNextAvailablePage()) {
                    corrupt("Volume " + _volume + " level=" + currentLevel + " page=" + pageAddress + " previousPage="
                            + oldPageAddress + " initialPage=" + initialPageAddress + " key=<" + key.toString() + ">"
                            + " oldBuffer=<" + oldBuffer + ">" + " invalid page address");
                }
                final LevelCache lc = _levelCache[currentLevel];

                if (lc._page == pageAddress) {
                    buffer = quicklyReclaimBuffer(lc, writer);
                }

                if (buffer == null) {
                    buffer = _pool.get(_volume, pageAddress, writer, true);
                }
                checkPageType(buffer, currentLevel + PAGE_TYPE_DATA, true);

                //
                // Release previous buffer after claiming this one. This
                // prevents another Thread from inserting pages to the left
                // of our new buffer.
                //
                if (oldBuffer != null) {
                    oldBuffer.releaseTouched();
                    oldBuffer = null;
                }

                if (pageAddress != lc._page) {
                    lc.invalidate();
                }

                final int foundAt = findKey(buffer, key, lc);
                if (!buffer.isAfterRightEdge(foundAt) || edge & (foundAt & EXACT_MASK) != 0) {
                    lc.update(buffer, key, foundAt);
                    return foundAt;
                }

                oldPageAddress = pageAddress;
                pageAddress = buffer.getRightSibling();

                Debug.$assert0.t(pageAddress > 0 && pageAddress < MAX_VALID_PAGE_ADDR);
                oldBuffer = buffer;
            }
            corrupt("Volume " + _volume + " level=" + currentLevel + " page=" + oldPageAddress + " initialPage="
                    + initialPageAddress + " key=<" + key.toString() + ">" + " walked right more than "
                    + MAX_WALK_RIGHT + " pages" + " last page visited=" + pageAddress);
            // won't happen - here to make compiler happy.
            return -1;
        } finally {
            if (oldBuffer != null) {
                oldBuffer.releaseTouched();
            }
        }
    }

    int maxValueSize(final int keySize) {
        final int pageSize = _volume.getPageSize();
        final int reserveForKeys = (KEYBLOCK_LENGTH + TAILBLOCK_HDR_SIZE_INDEX) * 3 + maxStorableKeySize(pageSize) * 2
                + keySize;
        return (pageSize - HEADER_SIZE - reserveForKeys) / 2;
    }

    /**
     * Inserts or replaces a data value in the database.
     * 
     * @param key
     *            The key to store.
     * @param value
     *            The value to store.
     * @return This <code>Exchange</code> to permit method call chaining.
     * @throws PersistitException
     *             Upon error
     */
    Exchange store(final Key key, final Value value) throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        key.testValidForStoreAndFetch(_volume.getPageSize());
        if (!isDirectoryExchange()) {
            _persistit.checkSuspended();
        }
        if (!_ignoreTransactions && !_transaction.isActive()) {
            _persistit.getJournalManager().throttle();
        }
        // TODO: directoryExchange, and lots of tests, don't use transactions.
        // Skip MVCC for now.
        int options = StoreOptions.WAIT;
        options |= (!_ignoreTransactions && _transaction.isActive()) ? StoreOptions.MVCC : 0;
        storeInternal(key, value, 0, options);
        _treeHolder.verifyReleased();

        return this;
    }

    /**
     * Inserts or replaces a data value in the database starting at a specified
     * level and working up toward the root of the tree.
     * 
     * <p>
     * <b>Note: Fetch and MVCC are exclusive options.</b>
     * </p>
     * 
     * @param key
     *            The key to store.
     * @param value
     *            The value to store.
     * @param level
     *            The level of the backing tree to start the insert at.
     * @param options
     *            Bit flag integer controlling various internal behavior. See
     *            members of {@link StoreOptions} for details.
     * @return <code>true</code> if <b>any version</b> of the key already
     *         existed
     * @throws PersistitException
     *             uponError
     */
    boolean storeInternal(Key key, final Value value, int level, final int options) throws PersistitException {

        final boolean doMVCC = (options & StoreOptions.MVCC) > 0;
        final boolean doFetch = (options & StoreOptions.FETCH) > 0;

        // spares used for new splits/levels
        Debug.$assert0.t(key != _spareKey1);

        _storeCausedSplit = false;
        boolean treeClaimRequired = false;
        boolean treeClaimAcquired = false;
        boolean treeWriterClaimRequired = false;
        boolean committed = false;
        boolean incrementMVVCount = false;

        final int maxSimpleValueSize = maxValueSize(key.getEncodedSize());
        final Value spareValue = _persistit.getThreadLocalValue();
        assert !(doMVCC & value == spareValue || doFetch && value == _spareValue) : "storeInternal may use the supplied Value: "
                + value;

        //
        // First insert the record in the data page
        //
        Buffer buffer = null;

        //
        // The LONG_RECORD pointer that was present before the update, if
        // there is a long record being replaced.
        //
        long oldLongRecordPointer = 0;
        long oldLongRecordPointerMVV = 0;
        //
        // The LONG_RECORD pointer for a new long record value, if the
        // the new value is long.
        //
        long newLongRecordPointer = 0;
        long newLongRecordPointerMVV = 0;

        final boolean isLongRecord = value.getEncodedSize() > maxSimpleValueSize;
        if (isLongRecord) {
            //
            // This method may delay significantly for I/O and must
            // be called when there are no other claimed resources.
            //
            newLongRecordPointer = getLongRecordHelper().storeLongRecord(value, _transaction.isActive());
        }

        if (!_ignoreTransactions && ((options & StoreOptions.DONT_JOURNAL) == 0)) {
            _transaction.store(this, key, value);
        }

        boolean keyExisted = false;

        try {

            Value valueToStore = value;

            mainRetryLoop: for (;;) {
                Debug.$assert0.t(buffer == null);
                if (Debug.ENABLED) {
                    Debug.suspend();
                }

                /*
                 * Can't save the old pointer as the state may have changed
                 * since the last claim, could have even been de-allocated, and
                 * just as equally can't hold onto the new one either.
                 */
                oldLongRecordPointerMVV = 0;
                if (!committed && newLongRecordPointerMVV != 0) {
                    _volume.getStructure().deallocateGarbageChain(newLongRecordPointerMVV, 0);
                    newLongRecordPointerMVV = 0;
                    spareValue.changeLongRecordMode(false);
                }

                if (treeClaimRequired && !treeClaimAcquired) {
                    if (!_treeHolder.claim(treeWriterClaimRequired)) {
                        Debug.$assert0.t(false);
                        throw new InUseException("Thread " + Thread.currentThread().getName() + " failed to get "
                                + (treeWriterClaimRequired ? "writer" : "reader") + " claim on " + _tree);
                    }
                    treeClaimAcquired = true;
                }

                checkLevelCache();
                final List<PrunedVersion> prunedVersions = new ArrayList<PrunedVersion>();

                try {
                    if (level >= _cacheDepth) {
                        Debug.$assert0.t(level == _cacheDepth);
                        //
                        // Need to lock the tree because we may need to change
                        // its root.
                        //
                        if (!treeClaimAcquired || !_treeHolder.upgradeClaim()) {
                            treeClaimRequired = true;
                            treeWriterClaimRequired = true;
                            throw RetryException.SINGLE;
                        }

                        Debug.$assert0.t(valueToStore.getPointerValue() > 0);
                        insertIndexLevel(key, valueToStore);
                        break mainRetryLoop;
                    }

                    Debug.$assert0.t(buffer == null);
                    int foundAt = -1;
                    final LevelCache lc = _levelCache[level];
                    buffer = quicklyReclaimBuffer(lc, true);

                    if (buffer != null) {
                        //
                        // Start by assuming cached value is okay
                        //
                        foundAt = findKey(buffer, key, lc);

                        if (buffer.isBeforeLeftEdge(foundAt) || buffer.isAfterRightEdge(foundAt)) {
                            buffer.release();
                            buffer = null;
                        }
                    }

                    if (buffer == null) {
                        foundAt = searchTree(key, level, true);
                        buffer = lc._buffer;
                    }

                    Debug.$assert0.t(buffer != null && (buffer.getStatus() & SharedResource.WRITER_MASK) != 0
                            && (buffer.getStatus() & SharedResource.CLAIMED_MASK) != 0);

                    boolean didPrune = false;
                    boolean splitRequired = false;

                    if (buffer.isDataPage()) {
                        keyExisted = (foundAt & EXACT_MASK) != 0;
                        if (keyExisted) {
                            oldLongRecordPointer = buffer.fetchLongRecordPointer(foundAt);
                        }

                        if (doFetch || doMVCC) {
                            buffer.fetch(foundAt, spareValue);
                            if (oldLongRecordPointer != 0) {
                                if (isLongMVV(spareValue)) {
                                    oldLongRecordPointerMVV = oldLongRecordPointer;
                                    fetchFixupForLongRecords(spareValue, Integer.MAX_VALUE);
                                }
                            }
                            /*
                             * If it was a long MVV we saved it into the
                             * variable above. Otherwise it is a primordial
                             * value that we can't get rid of.
                             */
                            oldLongRecordPointer = 0;

                            if (doFetch) {
                                spareValue.copyTo(_spareValue);
                                fetchFromValueInternal(_spareValue, Integer.MAX_VALUE, buffer);
                            }
                        }

                        if (doMVCC) {
                            valueToStore = spareValue;
                            final int valueSize = value.getEncodedSize();
                            int retries = VERSIONS_OUT_OF_ORDER_RETRY_COUNT;

                            for (;;) {
                                try {
                                    /*
                                     * If key didn't exist the value is truly
                                     * non-existent and not just undefined/zero
                                     * length
                                     */
                                    byte[] spareBytes = spareValue.getEncodedBytes();
                                    int spareSize;
                                    if (keyExisted) {
                                        spareSize = MVV.prune(spareBytes, 0, spareValue.getEncodedSize(),
                                                _persistit.getTransactionIndex(), false, prunedVersions);
                                        spareValue.setEncodedSize(spareSize);
                                    } else {
                                        spareSize = -1;
                                    }

                                    final TransactionStatus tStatus = _transaction.getTransactionStatus();
                                    final int tStep = _transaction.getStep();

                                    if ((options & StoreOptions.ONLY_IF_VISIBLE) != 0) {
                                        /*
                                         * Could be single visit of all versions
                                         * but current TI would still require
                                         * calls to both commitStatus() and
                                         * wwDependency()
                                         */
                                        _mvvVisitor.initInternal(tStatus, tStep, MvvVisitor.Usage.FETCH);
                                        MVV.visitAllVersions(_mvvVisitor, spareBytes, 0, spareSize);
                                        final int offset = _mvvVisitor.getOffset();
                                        if (!_mvvVisitor.foundVersion()
                                                || (_mvvVisitor.getLength() > 0 && spareBytes[offset] == MVV.TYPE_ANTIVALUE)) {
                                            // Completely done, nothing to store
                                            keyExisted = false;
                                            break mainRetryLoop;
                                        }
                                    }

                                    // Visit all versions for ww detection
                                    _mvvVisitor.initInternal(tStatus, tStep, MvvVisitor.Usage.STORE);
                                    MVV.visitAllVersions(_mvvVisitor, spareBytes, 0, spareSize);

                                    final int mvvSize = MVV.estimateRequiredLength(spareBytes, spareSize, valueSize);
                                    spareValue.ensureFit(mvvSize);
                                    spareBytes = spareValue.getEncodedBytes();

                                    final long versionHandle = TransactionIndex.tss2vh(
                                            _transaction.getStartTimestamp(), tStep);
                                    int storedLength = MVV.storeVersion(spareBytes, 0, spareSize, spareBytes.length,
                                            versionHandle, value.getEncodedBytes(), 0, valueSize);

                                    incrementMVVCount = (storedLength & MVV.STORE_EXISTED_MASK) == 0;
                                    storedLength &= MVV.STORE_LENGTH_MASK;
                                    spareValue.setEncodedSize(storedLength);

                                    Debug.$assert0.t(MVV.verify(_persistit.getTransactionIndex(), spareBytes, 0,
                                            storedLength));

                                    if (spareValue.getEncodedSize() > maxSimpleValueSize) {
                                        newLongRecordPointerMVV = getLongRecordHelper().storeLongRecord(spareValue,
                                                _transaction.isActive());
                                    }
                                    break;
                                } catch (final VersionsOutOfOrderException e) {
                                    if (--retries <= 0) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    }

                    Debug.$assert0.t(valueToStore.getEncodedSize() <= maxSimpleValueSize);
                    _rawValueWriter.init(valueToStore);

                    splitRequired = putLevel(lc, key, _rawValueWriter, buffer, foundAt, treeClaimAcquired);

                    Debug.$assert0.t((buffer.getStatus() & SharedResource.WRITER_MASK) != 0
                            && (buffer.getStatus() & SharedResource.CLAIMED_MASK) != 0);
                    //
                    // If a split is required but treeClaimAcquired is false
                    // then putLevel did not change anything. It just backed out
                    // so we can repeat after acquiring the claim. We need to
                    // repeat this after acquiring a tree claim.
                    //
                    if (splitRequired && !treeClaimAcquired) {
                        if (!didPrune && buffer.isDataPage()) {
                            didPrune = true;
                            if (buffer.pruneMvvValues(_tree, false)) {
                                continue;
                            }
                        }
                        //
                        // TODO - is it worth it to try an instantaneous claim
                        // and retry?
                        //
                        treeClaimRequired = true;
                        buffer.releaseTouched();
                        buffer = null;
                        continue;
                    }
                    //
                    // The value has been written to the buffer and the
                    // buffer is reserved and dirty. No backing out now.
                    // If we made it to here, any LONG_RECORD value is
                    // committed.
                    //
                    if (buffer.isDataPage()) {
                        if (!keyExisted) {
                            _tree.bumpChangeCount();
                        }
                        assert buffer.isDirty() : "Buffer must be dirty";
                        committed = true;
                        if (incrementMVVCount) {
                            _transaction.getTransactionStatus().incrementMvvCount();
                        }
                        Buffer.deallocatePrunedVersions(_persistit, _volume, prunedVersions);
                    }

                    buffer.releaseTouched();
                    buffer = null;

                    if (!splitRequired) {
                        //
                        // No split means we're totally done.
                        //
                        break;
                    } else {
                        // Otherwise we need to index the new right
                        // sibling at the next higher index level.
                        Debug.$assert0.t(valueToStore.getPointerValue() > 0);
                        //
                        // This maneuver sets key to the key value of
                        // the first record in the newly inserted page.
                        //
                        key = _spareKey1;
                        _spareKey1 = _spareKey2;
                        _spareKey2 = key;
                        //
                        // Bump key generation because it no longer matches
                        // what's in the LevelCache
                        //
                        key.bumpGeneration();
                        //
                        // And now cycle back to insert the key/pointer pair
                        // into the next higher index level.
                        //
                        level++;
                        continue;
                    }

                } catch (final WWRetryException re) {
                    if (buffer != null) {
                        buffer.releaseTouched();
                        buffer = null;
                    }
                    if (treeClaimAcquired) {
                        _treeHolder.release();
                        treeClaimAcquired = false;
                    }
                    try {
                        sequence(WRITE_WRITE_STORE_A);
                        final long depends = _persistit.getTransactionIndex().wwDependency(re.getVersionHandle(),
                        // TODO - timeout?
                                _transaction.getTransactionStatus(), SharedResource.DEFAULT_MAX_WAIT_TIME);
                        if (depends != 0 && depends != TransactionStatus.ABORTED) {
                            // version is from concurrent txn that already
                            // committed
                            // or timed out waiting to see. Either
                            // way, must abort.
                            _transaction.rollback();
                            throw new RollbackException();
                        }
                    } catch (final InterruptedException ie) {
                        throw new PersistitInterruptedException(ie);
                    }
                } catch (final RetryException re) {
                    if (buffer != null) {
                        buffer.releaseTouched();
                        buffer = null;
                    }

                    if (treeClaimAcquired) {
                        _treeHolder.release();
                        treeClaimAcquired = false;
                    }
                    final boolean doWait = (options & StoreOptions.WAIT) != 0;
                    treeClaimAcquired = _treeHolder.claim(true, doWait ? SharedResource.DEFAULT_MAX_WAIT_TIME : 0);
                    if (!treeClaimAcquired) {
                        if (!doWait) {
                            throw re;
                        } else {
                            throw new InUseException("Thread " + Thread.currentThread().getName()
                                    + " failed to get reader claim on " + _tree);
                        }
                    }
                } finally {
                    if (buffer != null) {
                        buffer.releaseTouched();
                        buffer = null;
                    }
                }
            }
        } finally {
            if (treeClaimAcquired) {
                _treeHolder.release();
                treeClaimAcquired = false;
            }

            value.changeLongRecordMode(false);
            spareValue.changeLongRecordMode(false);
            if (!committed) {
                //
                // We failed to write the new LONG_RECORD. If there was
                // previously no LONG_RECORD, then deallocate the newly
                // allocated LONG_RECORD chain if we had successfully
                // allocated one.
                //
                if (newLongRecordPointer != oldLongRecordPointer && newLongRecordPointer != 0) {
                    _volume.getStructure().deallocateGarbageChain(newLongRecordPointer, 0);
                }
                if (newLongRecordPointerMVV != 0) {
                    _volume.getStructure().deallocateGarbageChain(newLongRecordPointerMVV, 0);
                }
            } else {
                if (oldLongRecordPointer != newLongRecordPointer && oldLongRecordPointer != 0) {
                    _volume.getStructure().deallocateGarbageChain(oldLongRecordPointer, 0);
                }
                if (oldLongRecordPointerMVV != 0) {
                    _volume.getStructure().deallocateGarbageChain(oldLongRecordPointerMVV, 0);
                }
            }
        }
        _volume.getStatistics().bumpStoreCounter();
        _tree.getStatistics().bumpStoreCounter();
        if (doFetch || doMVCC) {
            _volume.getStatistics().bumpFetchCounter();
            _tree.getStatistics().bumpFetchCounter();
        }
        return keyExisted;
    }

    private long timestamp() {
        return _persistit.getTimestampAllocator().updateTimestamp();
    }

    private void insertIndexLevel(final Key key, final Value value) throws PersistitException {

        Buffer buffer = null;
        try {
            buffer = _volume.getStructure().allocPage();
            final long timestamp = timestamp();
            buffer.writePageOnCheckpoint(timestamp);

            buffer.init(PAGE_TYPE_INDEX_MIN + _tree.getDepth() - 1);

            final long newTopPage = buffer.getPageAddress();
            final long leftSiblingPointer = _tree.getRootPageAddr();

            Debug.$assert0.t(leftSiblingPointer == _tree.getRootPageAddr());
            final long rightSiblingPointer = value.getPointerValue();
            //
            // Note: left and right sibling are of the same level and therefore
            // it is not necessary to invoke value.setPointerPageType() here.
            //
            _rawValueWriter.init(value);
            value.setPointerValue(leftSiblingPointer);
            buffer.putValue(LEFT_GUARD_KEY, _rawValueWriter);

            value.setPointerValue(rightSiblingPointer);
            buffer.putValue(key, _rawValueWriter);

            value.setPointerValue(-1);
            buffer.putValue(RIGHT_GUARD_KEY, _rawValueWriter);

            buffer.setDirtyAtTimestamp(timestamp);

            _tree.changeRootPageAddr(newTopPage, 1);
            _tree.bumpGeneration();
            _volume.getStructure().updateDirectoryTree(_tree);

        } finally {
            if (buffer != null) {
                buffer.releaseTouched();
            }
        }
    }

    /**
     * Inserts a data or pointer value into a level of the tree.
     * 
     * @param buffer
     *            The buffer containing the insert location. The buffer must
     *            have a writer claim on it, and must be reserved.
     * @param foundAt
     *            The encoded insert location.
     * @return <code>true</code> if it necessary to insert a key into the
     *         ancestor index page.
     */
    // TODO - Check insertIndexLevel timestamps
    private boolean putLevel(final LevelCache lc, final Key key, final ValueHelper valueWriter, final Buffer buffer,
            final int foundAt, final boolean okToSplit) throws PersistitException {
        Debug.$assert0.t((buffer.getStatus() & SharedResource.WRITER_MASK) != 0
                && (buffer.getStatus() & SharedResource.CLAIMED_MASK) != 0);
        final Sequence sequence = lc.sequence(foundAt);

        long timestamp = timestamp();
        buffer.writePageOnCheckpoint(timestamp);

        final int result = buffer.putValue(key, valueWriter, foundAt, false);
        if (result != -1) {
            buffer.setDirtyAtTimestamp(timestamp);
            lc.updateInsert(buffer, key, result);
            return false;
        } else {
            Debug.$assert0.t(buffer.getPageAddress() != _volume.getStructure().getGarbageRoot());
            Buffer rightSibling = null;

            try {
                // We can't perform the split because we don't have a claim
                // on the Tree. We will just return, and the caller will
                // call again with that claim.
                //
                if (!okToSplit) {
                    return true;
                }
                _storeCausedSplit = true;
                //
                // Allocate a new page
                //
                rightSibling = _volume.getStructure().allocPage();

                timestamp = timestamp();
                buffer.writePageOnCheckpoint(timestamp);
                rightSibling.writePageOnCheckpoint(timestamp);

                Debug.$assert0.t(rightSibling.getPageAddress() != 0);
                Debug.$assert0.t(rightSibling != buffer);

                rightSibling.init(buffer.getPageType());
                // debug
                //
                // Split the page. As a side-effect, this will bump the
                // generation counters of both buffers, and therefore the
                // level cache for this level will become
                // (appropriately) invalid.
                //

                final int at = buffer
                        .split(rightSibling, key, valueWriter, foundAt, _spareKey1, sequence, _splitPolicy);
                if (at < 0) {
                    lc.updateInsert(rightSibling, key, -at);
                } else {
                    lc.updateInsert(buffer, key, at);
                }

                final long oldRightSibling = buffer.getRightSibling();
                final long newRightSibling = rightSibling.getPageAddress();

                Debug.$assert0.t(newRightSibling > 0 && oldRightSibling != newRightSibling);
                Debug.$assert0.t(rightSibling.getPageType() == buffer.getPageType());

                rightSibling.setRightSibling(oldRightSibling);
                buffer.setRightSibling(newRightSibling);

                valueWriter.setPointerValue(newRightSibling);

                rightSibling.setDirtyAtTimestamp(timestamp);
                buffer.setDirtyAtTimestamp(timestamp);

                return true;

            } finally {
                if (rightSibling != null) {
                    rightSibling.releaseTouched();
                }
            }
        }
    }

    private Buffer quicklyReclaimBuffer(final LevelCache lc, final boolean writer) throws PersistitException {
        final Buffer buffer = lc._buffer;
        if (buffer == null)
            return null;

        final boolean available = buffer.claim(writer, 0);
        if (available) {
            if (buffer.getPageAddress() == lc._page && buffer.getVolume() == _volume
                    && _cachedTreeGeneration == _tree.getGeneration() && buffer.getGeneration() == lc._bufferGeneration
                    && buffer.isValid()) {
                return buffer;
            } else {
                buffer.release();
            }
        }
        return null;
    }

    /**
     * <p>
     * Performs generalized tree traversal. The direction value indicates
     * whether to traverse forward or backward in collation sequence, whether to
     * descend to child nodes, and whether the key being sought must be strictly
     * greater than or less then the supplied key.
     * </p>
     * <p>
     * The <code>direction</code> value must be one of:
     * <dl>
     * <dt>Key.GT:</dt>
     * <dd>Find the next key that is strictly greater than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.GTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next greater key and return it.</dd>
     * <dt>Key.EQ:</dt>
     * <dd>Return <code>true</code> iff the specified key exists in the
     * database. Does not update the Key.</dd>
     * <dt>Key.LT:</dt>
     * <dd>Find the next key that is strictly less than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.LTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next smaller key and return it.</dd>
     * </dl>
     * </p>
     * 
     * @param direction
     *            One of Key.GT, Key.GTEQ, Key.EQ, Key.LT or Key.LTEQ.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <code>Tree</code> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * @return <code>true</code> if there is a key to traverse to, else
     *         <code>false</code>.
     * @throws PersistitException
     */
    public boolean traverse(final Direction direction, final boolean deep) throws PersistitException {
        final boolean result = traverse(direction, deep, Integer.MAX_VALUE);
        return result;
    }

    /**
     * <p>
     * Performs generalized tree traversal. The direction value indicates
     * whether to traverse forward or backward in collation sequence and whether
     * the key being sought must be strictly greater than or less than the
     * supplied key.
     * </p>
     * <p>
     * This method normally modifies both the <code>Key</code> and
     * <code>Value</code> fields of this <code>Exchange</code>: the
     * <code>Key</code> is modified to reflect the key found through traversal,
     * and the <code>Value</code> field is modified to contain the value
     * associated with that key. However, this behavior can be modified by the
     * <code>minimumBytes</code> parameter. If <code>minimumBytes</code> is less
     * than 0 then this method modifies neither the <code>Key</code> nor the
     * <code>Value</code> field. If it is equal to zero then only the
     * <code>Key</code> is modified.
     * </p>
     * <p>
     * The <code>direction</code> value must be one of:
     * <dl>
     * <dt>Key.GT:</dt>
     * <dd>Find the next key that is strictly greater than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.GTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next greater key and return it.</dd>
     * <dt>Key.EQ:</dt>
     * <dd>Return <code>true</code> iff the specified key exists in the
     * database. Does not update the Key.</dd>
     * <dt>Key.LT:</dt>
     * <dd>Find the next key that is strictly less than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.LTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next smaller key and return it.</dd>
     * </dl>
     * </p>
     * 
     * @param direction
     *            One of Key.GT, Key.GTEQ, Key.EQ, Key.LT or Key.LTEQ.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <code>Tree</code> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @param minimumBytes
     *            The minimum number of bytes to fetch. See {@link #fetch(int)}.
     * 
     * @return <code>true</code> if there is a key to traverse to, else
     *         <code>false</code>.
     * 
     * @throws PersistitException
     */
    public boolean traverse(final Direction direction, final boolean deep, final int minimumBytes)
            throws PersistitException {
        return traverse(direction, deep, minimumBytes, 0, 0, null);
    }

    /**
     * See {@link #traverse(com.persistit.Key.Direction, boolean, int)} for full
     * description
     * 
     * @param minKeyDepth
     *            Minimum valid key depth. If a key is found with a depth less
     *            than this value, <i>regardless of MVCC visibility</i>,
     *            <code>false</code> is immediately returned.
     * @param matchUpToIndex
     *            Length of minimum matching key fragment. If a key is found
     *            that does not match this many bytes, <i>regardless of MVCC
     *            visibility</i>, <code>false</code> is immediately returned.
     */
    private boolean traverse(final Direction direction, final boolean deep, final int minimumBytes,
            final int minKeyDepth, final int matchUpToIndex, final TraverseVisitor visitor) throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();

        final Key spareKey = _spareKey1;
        final boolean doFetch = minimumBytes > 0;
        final boolean doModify = minimumBytes >= 0;
        final boolean reverse = (direction == LT) || (direction == LTEQ);
        final Value outValue = doFetch ? _value : _spareValue;
        outValue.clear();

        Buffer buffer = null;
        boolean edge = direction == EQ || direction == GTEQ || direction == LTEQ;
        boolean nudged = false;

        if (_key.getEncodedSize() == 0) {
            if (reverse) {
                _key.appendAfter();
            } else {
                _key.appendBefore();
            }
            nudged = true;
        }

        _key.testValidForTraverse();

        checkLevelCache();

        try {
            //
            // Now we are committed to computing a new key value. Save the
            // original key value for comparison.
            //
            _key.copyTo(spareKey);
            int index = _key.getEncodedSize();

            int foundAt = 0;
            boolean nudgeForMVCC = false;
            _keysVisitedDuringTraverse = 0;

            for (;;) {
                ++_keysVisitedDuringTraverse;
                final LevelCache lc = _levelCache[0];
                boolean matches;
                //
                // Optimal path - pick up the buffer and location left
                // by previous operation.
                //
                if (buffer == null && lc._keyGeneration == _key.getGeneration()) {
                    buffer = quicklyReclaimBuffer(lc, false);
                    foundAt = lc._foundAt;
                }
                //
                // But if direction is leftward and the position is at the left
                // edge of the buffer, re-do with a key search - there is no
                // other way to find the left sibling page.
                //
                if (buffer != null && (nudgeForMVCC || (reverse && (foundAt & P_MASK) <= buffer.getKeyBlockStart()))) {
                    // Going left from first record in the page requires a
                    // key search.
                    buffer.releaseTouched();
                    buffer = null;
                }

                //
                // If the operations above failed to get the key, then
                // look it up with search.
                //
                if (buffer == null) {
                    if (nudgeForMVCC || (!edge && !nudged)) {
                        if (reverse) {
                            if (!_key.isSpecial()) {
                                _key.nudgeLeft();
                            }
                        } else {
                            if (!_key.isSpecial()) {
                                if (deep) {
                                    _key.nudgeDeeper();
                                } else {
                                    _key.nudgeRight();
                                }
                            }
                        }
                        nudged = true;
                        nudgeForMVCC = false;
                    }
                    foundAt = search(_key, false);
                    buffer = lc._buffer;
                }

                if (edge && (foundAt & EXACT_MASK) != 0) {
                    matches = true;
                } else if (edge && !deep && Buffer.decodeDepth(foundAt) == index) {
                    matches = true;
                } else if (direction == EQ) {
                    matches = false;
                } else {
                    edge = false;
                    foundAt = buffer.traverse(_key, direction, foundAt);
                    if (buffer.isAfterRightEdge(foundAt)) {
                        final long rightSiblingPage = buffer.getRightSibling();

                        Debug.$assert0.t(rightSiblingPage >= 0 && rightSiblingPage <= MAX_VALID_PAGE_ADDR);
                        if (rightSiblingPage > 0) {
                            final Buffer rightSibling = _pool.get(_volume, rightSiblingPage, false, true);
                            buffer.releaseTouched();
                            //
                            // Reset foundAtNext to point to the first key block
                            // of the right sibling page.
                            //
                            buffer = rightSibling;
                            checkPageType(buffer, PAGE_TYPE_DATA, false);
                            foundAt = buffer.traverse(_key, direction, buffer.toKeyBlock(0));
                            matches = !buffer.isAfterRightEdge(foundAt);
                        } else {
                            matches = false;
                        }
                    } else {
                        matches = true;
                    }

                    //
                    // If (a) the key was not nudged, and (b) this is not a deep
                    // traverse, and (c) the foundAtNext refers now to a child
                    // of the original key, then it's the wrong result - the
                    // optimistic assumption that the next key would be adjacent
                    // to the preceding result is wrong. To resolve this,
                    // invalidate the LevelCache entry and retry the loop. That
                    // will nudge the key appropriately and do a standard
                    // search.
                    //

                    if (!nudged && !deep && _key.compareKeyFragment(spareKey, 0, spareKey.getEncodedSize()) == 0) {
                        _key.setEncodedSize(spareKey.getEncodedSize());
                        lc._keyGeneration = -1;
                        buffer.release();
                        buffer = null;
                        continue;
                    }
                }

                //
                // Earliest point we can check for the quick exit. Internal
                // optimization (ignores visibility) that takes advantage of
                // physical key traversal for logical key semantics.
                //
                final boolean stopDueToKeyDepth;
                if (minKeyDepth > 0 && _key.getDepth() < minKeyDepth) {
                    stopDueToKeyDepth = true;
                } else if (matchUpToIndex > 0) {
                    stopDueToKeyDepth = spareKey.compareKeyFragment(_key, 0, matchUpToIndex) != 0;
                } else {
                    stopDueToKeyDepth = false;
                }

                // Original search loop end, MVCC must also inspect value before
                // finishing

                if (reverse && _key.isLeftEdge() || !reverse && _key.isRightEdge() || stopDueToKeyDepth) {
                    matches = false;
                } else {
                    if (deep) {
                        matches |= direction != EQ;
                        index = _key.getEncodedSize();

                        if (matches) {
                            matches = fetchFromBufferInternal(buffer, outValue, foundAt, minimumBytes);
                            if (!matches && direction != EQ) {
                                nudged = false;
                                nudgeForMVCC = (direction == GTEQ || direction == LTEQ);
                                buffer.release();
                                buffer = null;
                                continue;
                            }
                        }
                    } else {
                        int parentIndex = spareKey.previousElementIndex(index);
                        if (parentIndex < 0) {
                            parentIndex = 0;
                        }

                        matches &= (spareKey.compareKeyFragment(_key, 0, parentIndex) == 0);

                        if (matches) {
                            index = _key.nextElementIndex(parentIndex);
                            if (index > 0) {
                                final boolean isVisibleMatch = fetchFromBufferInternal(buffer, outValue, foundAt,
                                        minimumBytes);
                                //
                                // In any case (matching sibling, child or
                                // niece/nephew) we need to ignore this
                                // particular key and continue search if not
                                // visible to current transaction
                                //
                                if (!isVisibleMatch) {
                                    nudged = false;
                                    buffer.release();
                                    buffer = null;
                                    if (direction == EQ) {
                                        matches = false;
                                    } else {
                                        nudgeForMVCC = (direction == GTEQ || direction == LTEQ);
                                        continue;
                                    }
                                }
                                //
                                // It was a niece or nephew, record non-exact
                                // match
                                //
                                if (index != _key.getEncodedSize()) {
                                    foundAt &= ~EXACT_MASK;
                                }
                            } else {
                                matches = false;
                            }
                        }
                    }
                }

                if (doModify) {
                    if (matches) {
                        if (_key.getEncodedSize() == index) {
                            lc.update(buffer, _key, foundAt);
                        } else {
                            //
                            // Parent key determined from seeing a child or
                            // niece/nephew, need to fetch the actual
                            // value of this key before returning
                            //
                            _key.setEncodedSize(index);
                            if (buffer != null) {
                                buffer.releaseTouched();
                                buffer = null;
                            }
                            fetch(minimumBytes);
                        }
                    } else {
                        if (deep) {
                            _key.setEncodedSize(0);
                        } else {
                            spareKey.copyTo(_key);
                        }
                        _key.cut();
                        if (reverse) {
                            _key.appendAfter();
                        } else {
                            _key.appendBefore();
                        }
                    }
                } else {
                    // Restore original key
                    spareKey.copyTo(_key);
                }

                // Done
                _volume.getStatistics().bumpTraverseCounter();
                _tree.getStatistics().bumpTraverseCounter();
                if (matches && visitor != null && visitor.visit(this)) {
                    nudged = false;
                    continue;
                }
                return matches;
            }
        } finally {
            if (buffer != null) {
                buffer.releaseTouched();
                buffer = null;
            }
        }
    }

    /**
     * <p>
     * Performs generalized tree traversal using a {@link TraverseVisitor}. The
     * direction value indicates whether to traverse forward or backward in
     * collation sequence and whether the key being sought must be strictly
     * greater than or less than the supplied key.
     * </p>
     * <p>
     * Unlike {@link #traverse(Key.Direction, boolean, int)}, this method does
     * not return each time a new key is encountered in the traversal. Instead,
     * the {@link TraverseVisitor#visit(ReadOnlyExchange)} method is called once
     * for each key. This method avoids performing initial verification of the
     * key value and usually avoids locking a <code>Buffer</code> for every
     * record returned. It may offer better performance in circumstances where a
     * long sequence of keys is being examined. Note that
     * <code>ReadOnlyExchange</code> is an interface implemented by this class
     * which supplies the subset of methods that may be used safely within the
     * visitor.
     * </p>
     * <p>
     * During the call the {@link Buffer} containing the key is locked with a
     * non-exclusive claim, and any thread attempting to update records in the
     * same <code>Buffer</code> will block. Therefore the <code>visit</code>
     * method must be written carefully. See
     * {@link TraverseVisitor#visit(ReadOnlyExchange)} for guidelines.
     * </p>
     * <p>
     * This method normally modifies both the <code>Key</code> and
     * <code>Value</code> fields of this <code>Exchange</code>: the
     * <code>Key</code> is modified to reflect the key found through traversal,
     * and the <code>Value</code> field is modified to contain the value
     * associated with that key. However, this behavior can be modified by the
     * <code>minimumBytes</code> parameter. If <code>minimumBytes</code> is less
     * than or equal to zero then only the <code>Key</code> is modified. If it
     * is greater than zero, then the traverse method may choose to populate
     * only the specified number of bytes of the <code>Value</code>.
     * </p>
     * <p>
     * The <code>direction</code> value must be one of:
     * <dl>
     * <dt>Key.GT:</dt>
     * <dd>Find the next key that is strictly greater than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.GTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next greater key and return it.</dd>
     * <dt>Key.EQ:</dt>
     * <dd>Return <code>true</code> iff the specified key exists in the
     * database. Does not update the Key.</dd>
     * <dt>Key.LT:</dt>
     * <dd>Find the next key that is strictly less than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.LTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next smaller key and return it.</dd>
     * </dl>
     * </p>
     * 
     * @param direction
     *            One of Key.GT, Key.GTEQ, Key.EQ, Key.LT or Key.LTEQ.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <code>Tree</code> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @param minimumBytes
     *            The minimum number of bytes to fetch. See {@link #fetch(int)}.
     * 
     * @param visitor
     *            The application-supplied <code>TraverseVisitor</code>.
     * 
     * @return <code>true</code> if additional keys remaining in the traversal
     *         set, or <code>false</code> to indicate that keys are exhausted.
     * 
     * @throws PersistitException
     */

    public boolean traverse(final Direction direction, final boolean deep, final int minimumBytes,
            final TraverseVisitor visitor) throws PersistitException {
        return traverse(direction, deep, Math.min(0, minimumBytes), 0, 0, visitor);
    }

    /**
     * <p>
     * Performs generalized tree traversal constrained by a supplied
     * {@link KeyFilter}. The direction value indicates whether to traverse
     * forward or backward in collation sequence, and whether the key being
     * sought must be strictly greater than or less than the supplied key.
     * </p>
     * <p>
     * The direction value must be one of:
     * <dl>
     * <dt>Key.GT:</dt>
     * <dd>Find the next key that is strictly greater than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.GTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next greater key and return it.</dd>
     * <dt>Key.EQ:</dt>
     * <dd>Return <code>true</code> if the specified key exists in the database.
     * Does not update the Key.</dd>
     * <dt>Key.LT:</dt>
     * <dd>Find the next key that is strictly less than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.LTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next smaller key and return it.</dd>
     * </dl>
     * </p>
     * 
     * @param direction
     *            One of Key.GT, Key.GTEQ, Key.EQ, Key.LT or Key.LTEQ.
     * 
     * @param keyFilter
     *            A KeyFilter that constrains the keys returned by this
     *            operation.
     * 
     * @param minBytes
     *            The minimum number of bytes to fetch. See {@link #fetch(int)}.
     *            If minBytes is less than or equal to 0 then this method does
     *            not update the Key and Value fields of the Exchange.
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * 
     * @throws PersistitException
     */
    public boolean traverse(final Direction direction, final KeyFilter keyFilter, final int minBytes)
            throws PersistitException {
        if (keyFilter == null) {
            return traverse(direction, true, minBytes);
        }

        if (direction == EQ) {
            return keyFilter.selected(_key) && traverse(direction, true, minBytes);
        }

        assertCorrectThread(true);
        if (_key.getEncodedSize() == 0) {
            if (direction == GT || direction == GTEQ) {
                _key.appendBefore();
            } else {
                _key.appendAfter();
            }
        }

        int totalVisited = 0;
        for (;;) {
            if (!keyFilter.next(_key, direction)) {
                _key.setEncodedSize(0);
                if (direction == LT || direction == LTEQ) {
                    _key.appendAfter();
                } else {
                    _key.appendBefore();
                }
                return false;
            }
            if (keyFilter.isKeyPrefixFilter()) {
                return traverse(direction, true, minBytes, keyFilter.getMinimumDepth(),
                        keyFilter.getKeyPrefixByteCount(), null);
            }
            final boolean matched = traverse(direction, true, minBytes);
            totalVisited += _keysVisitedDuringTraverse;
            _keysVisitedDuringTraverse = totalVisited;
            if (!matched) {
                return false;
            }
            if (keyFilter.selected(_key)) {
                return true;
            }
        }
    }

    /**
     * Traverses to the next logical sibling key value. Equivalent to
     * <code>traverse(Key.GT, false)</code>.
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next() throws PersistitException {
        return traverse(GT, false);
    }

    /**
     * Traverses to the previous logical sibling key value. Equivalent to
     * <code>traverse(Key.LT, false)</code>.
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean previous() throws PersistitException {
        return traverse(LT, false);
    }

    /**
     * Traverses to the next key with control over depth. Equivalent to
     * <code>traverse(Key.GT, deep)</code>.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <code>Tree</code> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next(final boolean deep) throws PersistitException {
        return traverse(GT, deep);
    }

    /**
     * Traverses to the previous key with control over depth. Equivalent to
     * <code>traverse(Key.LT, deep)</code>.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <code>Tree</code> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * 
     * @throws PersistitException
     */
    public boolean previous(final boolean deep) throws PersistitException {
        return traverse(LT, deep);
    }

    /**
     * Traverses to the next key value within the subset of all keys defined by
     * the supplied KeyFilter. Whether logical children of the current key value
     * are included in the result is determined by the <code>KeyFilter</code>.
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next(final KeyFilter filter) throws PersistitException {
        return traverse(GT, filter, Integer.MAX_VALUE);
    }

    /**
     * Traverses to the previous key value within the subset of all keys defined
     * by the supplied KeyFilter. Whether logical children of the current key
     * value are included in the result is determined by the
     * <code>KeyFilter</code>.
     * 
     * @return <code>true</code> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean previous(final KeyFilter filter) throws PersistitException {
        return traverse(LT, filter, Integer.MAX_VALUE);
    }

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link #next()} except that no state is
     * changed.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    @Override
    public boolean hasNext() throws PersistitException {
        return traverse(GT, false, -1);
    }

    /**
     * Determines whether the current key has a successor within the subset of
     * all keys defined by a <code>KeyFilter</code>. This method does not change
     * the state of <code>Key</code> or <code>Value</code>.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    @Override
    public boolean hasNext(final KeyFilter filter) throws PersistitException {
        if (filter == null)
            return hasNext();
        _key.copyTo(_spareKey2);
        final boolean result = traverse(GT, filter, 0);
        _spareKey2.copyTo(_key);
        return result;
    }

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link #next(boolean)} except that no state
     * is changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<code>true</code>, or must be a restricted logical siblings (
     *            <code>false</code>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    @Override
    public boolean hasNext(final boolean deep) throws PersistitException {
        return traverse(GT, deep, -1);
    }

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link #previous()} except that no state is
     * changed.
     * 
     * @return <code>true</code> if the key has a predecessor
     * @throws PersistitException
     */
    @Override
    public boolean hasPrevious() throws PersistitException {
        return traverse(LT, false, -1);
    }

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <code>Key</code> or <code>Value</code>.
     * This method is equivalent to {@link #previous(boolean)} except that no
     * state is changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<code>true</code>, or must be a restricted logical siblings (
     *            <code>false</code>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <code>true</code> if the key has a predecessor
     * 
     * @throws PersistitException
     */
    @Override
    public boolean hasPrevious(final boolean deep) throws PersistitException {
        return traverse(LT, deep, -1);
    }

    /**
     * Determines whether the current key has a predecessor within the subset of
     * all keys defined by a <code>KeyFilter</code>. This method does not change
     * the state of <code>Key</code> or <code>Value</code>.
     * 
     * @return <code>true</code> if the key has a successor
     * 
     * @throws PersistitException
     */
    @Override
    public boolean hasPrevious(final KeyFilter filter) throws PersistitException {
        if (filter == null)
            return hasPrevious();
        _key.copyTo(_spareKey2);
        final boolean result = traverse(GT, filter, 0);
        _spareKey2.copyTo(_key);
        return result;
    }

    /**
     * Determines whether the current key has an associated value - that is,
     * whether a {@link #fetch} operation would return a defined value - without
     * actually changing the state of either the <code>Key</code> or the
     * <code>Value</code>.
     * 
     * @return <code>true</code> if the key has an associated value
     * 
     * @throws PersistitException
     */
    @Override
    public boolean isValueDefined() throws PersistitException {
        return traverse(EQ, true, -1);
    }

    /**
     * Insert the current <code>Key</code> and <code>Value</code> pair into this
     * <code>Exchange</code>'s <code>Tree</code>. If there already is a value
     * associated with the current key, then replace it.
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange store() throws PersistitException {
        return store(_key, _value);
    }

    /**
     * Fetches the value associated with the <code>Key</code>, then inserts or
     * updates the value. Effectively this swaps the content of
     * <code>Value</code> with the database record associated with the current
     * <code>key</code>. It is equivalent to the code: <blockquote>
     * 
     * <pre>
     *  Value tempValue = new Value();
     *  <i>exchange</i>.fetch(tempValue);
     *  <i>exchange</i>.store();
     *  tempValue.copyTo(exchange.getValue());
     *  return <i>exchange</i>;
     * </pre>
     * 
     * </blockquote> except that this operation is performed atomically, without
     * need for external synchronization.
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetchAndStore() throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        _persistit.checkSuspended();
        _key.testValidForStoreAndFetch(_volume.getPageSize());
        int options = StoreOptions.WAIT | StoreOptions.FETCH;
        options |= (!_ignoreTransactions && _transaction.isActive()) ? StoreOptions.MVCC : 0;
        storeInternal(_key, _value, 0, options);
        _spareValue.copyTo(_value);
        return this;
    }

    /**
     * Fetches the value associated with the current <code>Key</code> into the
     * <code>Exchange</code>'s <code>Value</code>. The <code>Value</code> object
     * reflects the fetched state. If there is no value associated with the key
     * then {@link Value#isDefined} is false. Otherwise the value may be
     * retrieved using {@link Value#get} and other methods of <code>Value</code>
     * .
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch() throws PersistitException {
        return fetch(_value, Integer.MAX_VALUE);
    }

    /**
     * <p>
     * Fetches or partially fetches the value associated with the current
     * <code>Key</code> into the <code>Exchange</code>'s <code>Value</code>. The
     * <code>Value</code> object reflects the fetched state. If there is no
     * value associated with the key then {@link Value#isDefined} is false.
     * Otherwise the value may be retrieved using {@link Value#get} and other
     * methods of <code>Value</code>.
     * </p>
     * <p>
     * This method sets a lower bound on the number of bytes to be fetched. In
     * particular, it may be useful to retrieve only a small fraction of a very
     * long record such as the serialization of an image. Upon successful
     * completion of this method, at least <code>minimumBytes</code> of the
     * <code>Value</code> object will accurately reflect the value stored in the
     * database. This might allow an application to determine whether to
     * retrieve the rest of the value.
     * </p>
     * 
     * @param minimumBytes
     *            specifies a length at which Persistit will truncate the
     *            returned value.
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(final int minimumBytes) throws PersistitException {
        return fetch(_value, minimumBytes);
    }

    /**
     * Fetches the value associated with the current <code>Key</code> into the
     * supplied <code>Value</code> object (instead of the <code>Exchange</code>
     * 's assigned <code>Value</code>). The <code>Value</code> object reflects
     * the fetched state. If there is no value associated with the key then
     * {@link Value#isDefined} is false. Otherwise the value may be retrieved
     * using {@link Value#get} and other methods of <code>Value</code>.
     * 
     * @param value
     *            the <code>Value</code> into which the database value should be
     *            fetched.
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(final Value value) throws PersistitException {
        return fetch(value, Integer.MAX_VALUE);
    }

    /**
     * Fetch a single version of a value from a <code>Buffer</code> that is
     * assumed, but not required, to be an MVV. The correct version is
     * determined by the current transactions start timestamp. If no transaction
     * is active, the highest committed version is returned.
     * 
     * <p>
     * <b>Note</b>: This method only determines the visible version and copies
     * it into <code>value</code>, or clears it if there isn't one. It may still
     * be a LONG_RECORD or AntiValue
     * </p>
     * .
     * 
     * @param value
     *            The <code>Value</code> into which the value should be fetched.
     * @param minimumBytes
     *            The minimum number of bytes to copy into <code>value</code>.
     *            Note this only affects the final contents, not the amount of
     *            the internal MVV that was copied.
     * @return <code>true</code> if a version was visible, <code>false</code>
     *         otherwise.
     * @throws PersistitException
     *             for any internal error
     */
    private boolean mvccFetch(final Value value, final int minimumBytes) throws PersistitException {
        final TransactionStatus status;
        final int step;
        if (_transaction.isActive()) {
            status = _transaction.getTransactionStatus();
            step = _transaction.getStep();
        } else {
            status = null;
            step = 0;
        }
        _mvvVisitor.initInternal(status, step, MvvVisitor.Usage.FETCH);

        final int valueSize = value.getEncodedSize();
        final byte[] valueBytes = value.getEncodedBytes();
        MVV.visitAllVersions(_mvvVisitor, valueBytes, 0, valueSize);

        if (_mvvVisitor.foundVersion()) {
            final int finalSize = MVV.fetchVersionByOffset(valueBytes, valueSize, _mvvVisitor.getOffset(), valueBytes);
            value.setEncodedSize(finalSize);
            return true;
        } else {
            if (minimumBytes > 0) {
                value.clear();
            }
            return false;
        }
    }

    /**
     * <p>
     * Fetches or partially fetches the value associated with the current
     * <code>Key</code> into the supplied <code>Value</code> object (instead of
     * the <code>Exchange</code>'s assigned <code>Value</code>). The
     * <code>Value</code> object reflects the fetched state. If there is no
     * value associated with the key then {@link Value#isDefined} is false.
     * Otherwise the value may be retrieved using {@link Value#get} and other
     * methods of <code>Value</code>.
     * </p>
     * <p>
     * This method sets an lower bound on the number of bytes to be fetched. In
     * particular, it may be useful to retrieve only a small fraction of a very
     * long record such as the serialization of an image. Upon successful
     * completion of this method, at least <code>minimumBytes</code> of the
     * <code>Value</code> object will accurately reflect the value stored in the
     * database. This might allow an application to determine whether to
     * retrieve the rest of the value using the {@link #fetch()} operation.
     * </p>
     * 
     * @param value
     *            the <code>Value</code> into which the database value should be
     *            fetched.
     * @param minimumBytes
     *            specifies a length at which Persistit will truncate the
     *            returned value.
     * 
     * 
     * @return This <code>Exchange</code> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(final Value value, int minimumBytes) throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();

        _key.testValidForStoreAndFetch(_volume.getPageSize());
        if (minimumBytes < 0) {
            minimumBytes = 0;
        }
        searchAndFetchInternal(value, minimumBytes);
        return this;
    }

    /**
     * Helper for fully pulling a value out of a Buffer. That is, if the value
     * is a LONG_RECORD it will also be fetched.
     * 
     * @param buffer
     *            Buffer to read from.
     * @param value
     *            Value to write to.
     * @param foundAt
     *            Location within <code>buffer</code>.
     * @param minimumBytes
     *            Minimum amount of LONG_RECORD to fetch. If &lt;0, the
     *            <code>value</code> will contain just the descriptor portion.
     * @throws PersistitException
     *             As thrown from any internal method.
     * @return <code>true</code> if the value was visible.
     */
    private boolean fetchFromBufferInternal(final Buffer buffer, final Value value, final int foundAt,
            final int minimumBytes) throws PersistitException {
        buffer.fetch(foundAt, value);
        return fetchFromValueInternal(value, minimumBytes, buffer);
    }

    /**
     * Helper for finalizing the value to return from a, potentially, MVV
     * contained in the given Value.
     * 
     * @param value
     *            Value to finalize.
     * @param minimumBytes
     *            Minimum amount of LONG_RECORD to fetch. If &lt;0, the
     *            <code>value</code> will contain just the descriptor portion.
     * @param bufferForPruning
     *            If not <code>null</code> and <code>Value</code> did contain an
     *            MVV, call {@link Buffer#enqueuePruningAction(int)}.
     * @throws PersistitException
     *             As thrown from any internal method.
     * @return <code>true</code> if the value was visible.
     */
    private boolean fetchFromValueInternal(final Value value, final int minimumBytes, final Buffer bufferForPruning)
            throws PersistitException {
        boolean visible = true;
        /*
         * We must fetch the full LONG_RECORD, if needed, while buffer is
         * claimed from calling code so that it can't be de-allocated as we are
         * reading it.
         */
        if (!_ignoreMVCCFetch) {
            /*
             * Must fetch entire record as it *could* be an MVV, and reading
             * partial MVV is not supported (need all for correct version)
             */
            fetchFixupForLongRecords(value, Integer.MAX_VALUE);
            if (MVV.isArrayMVV(value.getEncodedBytes(), 0, value.getEncodedSize())) {
                if (bufferForPruning != null) {
                    final int treeHandle = _tree.getHandle();
                    assert treeHandle != 0 : "MVV found in a temporary tree " + _tree;
                    bufferForPruning.enqueuePruningAction(treeHandle);
                }
                visible = mvccFetch(value, minimumBytes);
                fetchFixupForLongRecords(value, minimumBytes);
            }
            if (value.isDefined() && value.isAntiValue()) {
                value.clear();
                visible = false;
            }
        } else {
            fetchFixupForLongRecords(value, minimumBytes);
        }
        return visible;
    }

    /**
     * Looks the current key, {@link #_key}, up in the tree and fetches the
     * value from the page. The value is left as found. Specifically, that means
     * it can be a <b>user value, LONG_RECORD, or MVV</b>.
     * 
     * @param value
     *            The value as found on the page.
     * @param minimumBytes
     *            If >= 0 and stored value is a LONG_RECORD, fetch at least this
     *            many bytes.
     * @throws PersistitException
     *             As thrown from {@link #search(Key, boolean)}
     */
    private void searchAndFetchInternal(final Value value, final int minimumBytes) throws PersistitException {
        Buffer buffer = null;
        try {
            final int foundAt = search(_key, false);
            final LevelCache lc = _levelCache[0];
            buffer = lc._buffer;
            fetchFromBufferInternal(buffer, value, foundAt, minimumBytes);
            _volume.getStatistics().bumpFetchCounter();
            _tree.getStatistics().bumpFetchCounter();
        } finally {
            if (buffer != null) {
                buffer.releaseTouched();
            }
            _treeHolder.verifyReleased();
        }
    }

    boolean isLongRecord(final Value value) {
        return value.isDefined() && Buffer.isLongRecord(value.getEncodedBytes(), 0, value.getEncodedSize());
    }

    boolean isLongMVV(final Value value) {
        return value.isDefined() && Buffer.isLongMVV(value.getEncodedBytes(), 0, value.getEncodedSize());
    }

    void fetchFixupForLongRecords(final Value value, final int minimumBytes) throws PersistitException {
        if (minimumBytes >= 0 && isLongRecord(value)) {
            //
            // This will potential require numerous pages: the buffer
            // claim is held for the duration to prevent a non-atomic
            // update.
            //
            getLongRecordHelper().fetchLongRecord(value, minimumBytes);
        }
    }

    /**
     * Return true if there is at least one key stored in this
     * <code>Exchange</code> 's <code>Tree</code> that is a logical child of the
     * current <code>Key</code>. A logical child is a key that can be formed by
     * appending a value to the parent. (See <a
     * href="Key.html#_keyChildren">Logical Key Children and Siblings</a>).
     * 
     * @return <code>true</code> if the current <code>Key</code> has logical
     *         children
     * @throws PersistitException
     */
    @Override
    public boolean hasChildren() throws PersistitException {
        assertCorrectThread(true);
        _key.copyTo(_spareKey2);
        final int size = _key.getEncodedSize();
        final boolean result = traverse(GT, true, 0, _key.getDepth() + 1, size, null);
        _spareKey2.copyTo(_key);
        return result;
    }

    /**
     * Remove a single key/value pair from this <code>Exchange</code>'s
     * <code>Tree</code> and return the removed value in the
     * <code>Exchange</code>'s <code>Value</code>. This method atomically
     * fetches the former value then deletes it. If there was no value formerly
     * associated with the key then <code>Value</code> becomes undefined - that
     * is, the value of {@link Value#isDefined} becomes <code>false</code>.
     * 
     * @return <code>true</code> if there was a key/value pair to remove
     * @throws PersistitException
     */
    public boolean fetchAndRemove() throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();
        _persistit.checkSuspended();
        _spareValue.clear();
        final boolean result = removeInternal(EQ, true);
        _spareValue.copyTo(_value);
        Debug.$assert0.t(_value.isDefined() == result);
        return result;
    }

    /**
     * Remove the entire <code>Tree</code> that this <code>Exchange</code> is
     * based on. Subsequent to successful completion of this method, the
     * <code>Exchange</code> will no longer be usable. Attempts to perform
     * operations on it will result in an <code>IllegalStateException</code>.
     * 
     * @throws PersistitException
     */
    public void removeTree() throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();

        final long timestamp = _persistit.getCurrentTimestamp();
        for (int i = 0; i < 100; i++) {
            _persistit.checkClosed();
            _persistit.checkSuspended();
            _persistit.getJournalManager().pruneObsoleteTransactions();
            if (_persistit.getJournalManager().getEarliestAbortedTransactionTimestamp() > timestamp) {
                break;
            }
            Util.sleep(1000);
        }
        if (!_ignoreTransactions) {
            _transaction.removeTree(this);
        }

        clear();

        _value.clear();
        /*
         * Remove from directory tree.
         */
        _volume.getStructure().removeTree(_tree);

        initCache();
    }

    /**
     * Remove a single key/value pair from the this <code>Exchange</code>'s
     * <code>Tree</code>.
     * 
     * @return <code>true</code> if there was a key/value pair to remove
     * @throws PersistitException
     */
    public boolean remove() throws PersistitException {
        return removeInternal(EQ, false);
    }

    /**
     * Remove all keys in this <code>Exchange</code>'s <code>Tree</code>.
     * 
     * @return <code>true</code> if there were key/value pairs removed
     * @throws PersistitException
     */
    public boolean removeAll() throws PersistitException {
        clear();
        return removeInternal(GTEQ, false);
    }

    /**
     * <p>
     * Depending on the value of the selection parameter, remove the record
     * associated with the current key, its logical children, or both.
     * </p>
     * <p>
     * Following are valid values for selection: <br />
     * <dl>
     * <dt>Key.EQ</dt>
     * <dd>Remove the record associated with the current key if it exists.</dd>
     * <dt>Key.GT</dt>
     * <dd>Remove the records associated with logical children of the current
     * key.</dd>
     * <dt>Key.GTEQ</dt>
     * <dd>Remove the record associated with the current key AND its logical
     * children.</dd>
     * </dl>
     * 
     * @param direction
     *            One of Key.EQ, Key.GT, Key.GTEQ
     * @return <code>true</code> if one or more records were actually removed,
     *         else </i>false</i>.
     * @throws PersistitException
     */
    public boolean remove(final Direction direction) throws PersistitException {
        return removeInternal(direction, false);
    }

    private boolean removeInternal(final Direction selection, final boolean fetchFirst) throws PersistitException {
        if (selection != EQ && selection != GTEQ && selection != GT) {
            throw new IllegalArgumentException("Invalid mode " + selection);
        }
        final int keySize = _key.getEncodedSize();

        _key.copyTo(_spareKey3);
        _key.copyTo(_spareKey4);

        // Special case for empty key
        if (keySize == 0) {
            if (selection == EQ) {
                assertCorrectThread(true);
                return false;
            }
            _spareKey3.append(BEFORE);
            _spareKey4.append(AFTER);
        } else {
            if (selection == EQ) {
                _spareKey4.nudgeDeeper();
            } else if (selection == GT) {
                _spareKey3.nudgeDeeper();
                _spareKey4.nudgeRight();
            } else if (selection == GTEQ) {
                _spareKey4.nudgeRight();
            }
        }

        final boolean result = removeKeyRangeInternal(_spareKey3, _spareKey4, fetchFirst);
        _treeHolder.verifyReleased();
        return result;
    }

    /**
     * Removes all records with keys falling between <code>key1</code> and
     * </code>key2</code>, left-inclusive.
     * 
     * @param key1
     *            Start of the deletion range. No record with a key smaller than
     *            key1 will be removed. key1 may be empty, in which case all
     *            records having keys less than key2 will be removed.
     * 
     * @param key2
     *            End of the deletion range. No record with a key greater than
     *            or equal to key2 will be removed. key2 may be empty, in which
     *            case all records having keys equal to or greater than key1
     *            will be removed.
     * 
     * @return <code>true</code> if one or more records were actually removed,
     *         else </i>false</i>.
     * 
     * @throws PersistitException
     *             if there are any internal errors
     * @throws IllegalArgumentException
     *             if key1 is equal to or greater than key2
     */
    public boolean removeKeyRange(final Key key1, final Key key2) throws PersistitException {
        key1.copyTo(_spareKey3);
        key2.copyTo(_spareKey4);

        // Special case for empty key
        if (key1.getEncodedSize() == 0) {
            _spareKey3.append(BEFORE);
        }

        if (key2.getEncodedSize() == 0) {
            _spareKey4.append(AFTER);
        }

        if (_spareKey3.compareTo(_spareKey4) >= 0) {
            throw new IllegalArgumentException("Second key must be greater than the first");
        }

        final boolean result = removeKeyRangeInternal(_spareKey3, _spareKey4, false);
        _treeHolder.verifyReleased();
        return result;
    }

    /**
     * Removes all records with keys falling between <code>key1</code> and
     * </code>key2</code>, left-inclusive. Validity checks and Key value
     * adjustments have been done by caller - this method does the work.
     * 
     * @param key1
     *            Key that is less than or equal to the leftmost to be removed
     * @param key2
     *            Key that is greater than the rightmost to be removed
     * @param fetchFirst
     *            Control whether to copy the existing value for the first key
     *            into _spareValue before deleting the record.
     * @return <code>true</code> if any records were removed.
     * @throws PersistitException
     */
    private boolean removeKeyRangeInternal(final Key key1, final Key key2, final boolean fetchFirst)
            throws PersistitException {
        Debug.$assert0.t(key1.getEncodedSize() > 0);
        Debug.$assert0.t(key2.getEncodedSize() > 0);
        Debug.$assert0.t(key1.compareTo(key2) < 0);

        assertCorrectThread(true);
        _persistit.checkClosed();

        if (!isDirectoryExchange()) {
            _persistit.checkSuspended();
        }
        if (!_ignoreTransactions && !_transaction.isActive()) {
            _persistit.getJournalManager().throttle();
        }

        if (_ignoreTransactions || !_transaction.isActive()) {
            return raw_removeKeyRangeInternal(key1, key2, fetchFirst, false);
        }

        // Record the delete operation on the journal

        _transaction.remove(this, key1, key2);

        checkLevelCache();

        _value.clear().putAntiValueMVV();
        final int storeOptions = StoreOptions.MVCC | StoreOptions.WAIT | StoreOptions.ONLY_IF_VISIBLE
                | StoreOptions.DONT_JOURNAL | (fetchFirst ? StoreOptions.FETCH : 0);

        boolean anyRemoved = false;
        boolean keyIsLessThan = true;
        final Key nextKey = new Key(key1);

        while (keyIsLessThan && !key1.isRightEdge()) {
            Buffer buffer = null;
            try {
                int foundAt = search(key1, true);
                buffer = _levelCache[0]._buffer;

                while (!buffer.isAfterRightEdge(foundAt)) {
                    keyIsLessThan = key1.compareTo(key2) < 0;
                    if (!keyIsLessThan) {
                        break;
                    }
                    foundAt = buffer.nextKey(nextKey, foundAt);
                    buffer.releaseTouched();
                    buffer = null;
                    anyRemoved |= storeInternal(key1, _value, 0, storeOptions);
                    nextKey.copyTo(key1);
                    break;
                }
            } finally {
                if (buffer != null) {
                    buffer.releaseTouched();
                    buffer = null;
                }
            }
        }

        _value.clear();
        return anyRemoved;
    }

    /**
     * Removes all records with keys falling between <code>key1</code> and
     * </code>key2</code>, lefty-inclusive. Validity checks and Key value
     * adjustments have been done by caller - this method does the work.
     * 
     * @param key1
     *            Key that is less than or equal to the leftmost to be removed
     * @param key2
     *            Key that is greater than the rightmost to be removed
     * @param fetchFirst
     *            Control whether to copy the existing value for the first key
     *            into _spareValue before deleting the record.
     * @param removeOnlyAntiValue
     *            Control whether to remove normal records or only an AntiValue.
     *            If true then this method tests whether there is one record
     *            being identified and removes it only if it is a primordial
     *            AntiValue.
     * @return <code>true</code> if any records were removed.
     * @throws PersistitException
     */
    boolean raw_removeKeyRangeInternal(final Key key1, final Key key2, final boolean fetchFirst,
            final boolean removeOnlyAntiValue) throws PersistitException {
        /*
         * _spareKey1 and _spareKey2 are mutated within the method and are then
         * wrong in the event of a retry loop.
         */

        assert key1 != _spareKey1 && key2 != _spareKey1 && key1 != _spareKey2 && key2 != _spareKey2;

        _persistit.checkClosed();
        _persistit.checkSuspended();

        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        if (Debug.ENABLED) {
            Debug.suspend();
        }
        boolean treeClaimAcquired = false;
        boolean treeWriterClaimRequired = false;
        boolean result = false;

        boolean deallocationRequired = true; // assume until proven false
        boolean tryQuickDelete = true;

        if (!_ignoreTransactions) {
            _transaction.remove(this, key1, key2);
        }

        try {
            //
            // Retry here to get an exclusive Tree latch in the occasional case
            // where pages are being joined.
            //
            for (;;) {

                checkLevelCache();
                int depth = _cacheDepth; // The depth to which we have
                // populated the level cache.

                try {
                    //
                    // First try for a quick delete from a single data page.
                    //
                    if (tryQuickDelete) {
                        final List<Chain> chains = new ArrayList<Chain>();
                        Buffer buffer = null;
                        try {
                            final int foundAt1 = search(key1, true) & P_MASK;
                            buffer = _levelCache[0]._buffer;
                            //
                            // Re-check tree generation because a structure
                            // delete could have changed
                            // search results.
                            //
                            if (_tree.getGeneration() == _cachedTreeGeneration && foundAt1 > buffer.getKeyBlockStart()
                                    && foundAt1 < buffer.getKeyBlockEnd()) {
                                int foundAt2 = buffer.findKey(key2) & P_MASK;
                                if (!buffer.isBeforeLeftEdge(foundAt2) && !buffer.isAfterRightEdge(foundAt2)) {
                                    foundAt2 &= P_MASK;
                                    if (foundAt2 < buffer.getKeyBlockEnd()) {
                                        Debug.$assert0.t(foundAt2 >= foundAt1);

                                        if (removeOnlyAntiValue) {
                                            for (int p = foundAt1; p < foundAt2; p += KEYBLOCK_LENGTH) {
                                                if (!buffer.isPrimordialAntiValue(p)) {
                                                    return false;
                                                }
                                            }
                                        }
                                        if (fetchFirst) {
                                            removeFetchFirst(buffer, foundAt1, buffer, foundAt2);
                                        }

                                        final long timestamp = timestamp();
                                        buffer.writePageOnCheckpoint(timestamp);
                                        _volume.getStructure().harvestLongRecords(buffer, foundAt1, foundAt2, chains);

                                        final boolean removed = buffer.removeKeys(foundAt1, foundAt2, _spareKey1);
                                        if (removed) {
                                            _tree.bumpChangeCount();
                                            buffer.setDirtyAtTimestamp(timestamp);
                                        }
                                        result = removed;
                                        break;
                                    }
                                }
                            }
                            // If we didn't meet the criteria for quick delete,
                            // then don't try it again on a RetryException.
                            tryQuickDelete = false;
                        } finally {
                            if (buffer != null) {
                                buffer.releaseTouched();
                                buffer = null;
                            }
                        }
                        _volume.getStructure().deallocateGarbageChain(chains);
                    }

                    /*
                     * This deletion is more complicated and involves an index
                     * search. The tree must be latched.
                     */
                    if (!treeClaimAcquired) {
                        if (!_treeHolder.claim(treeWriterClaimRequired)) {
                            Debug.$assert0.t(false);
                            throw new InUseException("Thread " + Thread.currentThread().getName()
                                    + " failed to get writer claim on " + _tree);
                        }
                        treeClaimAcquired = true;
                    }
                    //
                    // Need to redo this check now that we have a
                    // claim on the Tree.
                    //
                    checkLevelCache();

                    long pageAddr1 = _tree.getRootPageAddr();
                    long pageAddr2 = pageAddr1;

                    for (int level = _cacheDepth; --level >= 0;) {
                        final LevelCache lc = _levelCache[level];
                        lc.initRemoveFields();
                        depth = level;

                        final int foundAt1 = searchLevel(key1, true, pageAddr1, level, true);
                        int foundAt2 = -1;

                        //
                        // Note: this buffer now has a writer claim on it.
                        //
                        Buffer buffer = lc._buffer;
                        lc._flags |= LEFT_CLAIMED;

                        lc._leftBuffer = buffer;
                        lc._leftFoundAt = foundAt1;
                        boolean samePage = pageAddr2 == pageAddr1;

                        if (samePage) {
                            foundAt2 = buffer.findKey(key2);

                            if (!buffer.isAfterRightEdge(foundAt2)) {
                                lc._rightBuffer = buffer;
                                lc._rightFoundAt = foundAt2;
                            } else {
                                pageAddr2 = buffer.getRightSibling();
                                samePage = false;
                            }
                        }
                        if (!samePage) {
                            //
                            // Since we are spanning pages we need an
                            // exclusive claim on the tree to prevent
                            // an insertion from propagating upward through
                            // the deletion range.
                            //
                            if (!treeWriterClaimRequired) {
                                treeWriterClaimRequired = true;
                                if (!_treeHolder.upgradeClaim()) {
                                    throw RetryException.SINGLE;
                                }
                            }

                            foundAt2 = searchLevel(key2, false, pageAddr2, level, true);

                            buffer = lc._buffer;
                            lc._flags |= RIGHT_CLAIMED;
                            lc._rightBuffer = buffer;
                            lc._rightFoundAt = foundAt2;
                            pageAddr2 = buffer.getPageAddress();
                        }

                        if (lc._leftBuffer.isIndexPage()) {
                            Debug.$assert0.t(lc._rightBuffer.isIndexPage() && depth > 0);
                            //
                            // Come down to the left of the key.
                            //
                            final int p1 = lc._leftBuffer.previousKeyBlock(foundAt1);
                            final int p2 = lc._rightBuffer.previousKeyBlock(foundAt2);

                            Debug.$assert0.t(p1 != -1 && p2 != -1);
                            pageAddr1 = lc._leftBuffer.getPointer(p1);
                            pageAddr2 = lc._rightBuffer.getPointer(p2);

                        } else {
                            Debug.$assert0.t(depth == 0);
                            break;
                        }
                    }

                    LevelCache lc = _levelCache[0];
                    if (removeOnlyAntiValue
                            & !isKeyRangeAntiValue(lc._leftBuffer, lc._leftFoundAt, lc._rightBuffer, lc._rightFoundAt)) {
                        result = false;
                        break;
                    }

                    if (fetchFirst) {
                        removeFetchFirst(lc._leftBuffer, lc._leftFoundAt, lc._rightBuffer, lc._rightFoundAt);
                    }
                    //
                    // We have fully delineated the subtree that
                    // needs to be removed. Now walk down the tree,
                    // stitching together the pages where necessary.
                    //
                    _tree.bumpGeneration();

                    final long timestamp = timestamp();
                    for (int level = _cacheDepth; --level >= 0;) {
                        lc = _levelCache[level];
                        final Buffer buffer1 = lc._leftBuffer;
                        final Buffer buffer2 = lc._rightBuffer;
                        int foundAt1 = lc._leftFoundAt;
                        int foundAt2 = lc._rightFoundAt;
                        foundAt1 &= P_MASK;
                        foundAt2 &= P_MASK;

                        boolean needsReindex = false;
                        buffer1.writePageOnCheckpoint(timestamp);
                        if (buffer1 != buffer2) {
                            buffer2.writePageOnCheckpoint(timestamp);
                            //
                            // Deletion spans multiple pages at this level.
                            // We will need to join or rebalance the pages.
                            //
                            final long leftGarbagePage = buffer1.getRightSibling();
                            _key.copyTo(_spareKey1);

                            // Before we remove the records in this range, we
                            // need to recover any LONG_RECORD pointers that
                            // are associated with keys in this range.
                            _volume.getStructure().harvestLongRecords(buffer1, foundAt1, Integer.MAX_VALUE);
                            _volume.getStructure().harvestLongRecords(buffer2, 0, foundAt2);

                            Debug.$assert0.t(_tree.isOwnedAsWriterByMe() && buffer1.isOwnedAsWriterByMe()
                                    && buffer2.isOwnedAsWriterByMe());
                            boolean rebalanced = false;
                            try {
                                rebalanced = buffer1.join(buffer2, foundAt1, foundAt2, _spareKey1, _spareKey2,
                                        _joinPolicy);
                            } catch (final RebalanceException rbe) {
                                rebalanceSplit(lc);
                                level++;
                                continue;
                            }
                            if (buffer1.isDataPage()) {
                                _tree.bumpChangeCount();
                            }

                            buffer1.setDirtyAtTimestamp(timestamp);
                            buffer2.setDirtyAtTimestamp(timestamp);

                            final long rightGarbagePage = buffer1.getRightSibling();

                            if (rightGarbagePage != leftGarbagePage) {
                                // here we just remember the page boundaries
                                // that will need to be deallocated.
                                lc._deallocLeftPage = leftGarbagePage;
                                lc._deallocRightPage = rightGarbagePage;
                                deallocationRequired = true;
                            }

                            if (rebalanced) {
                                //
                                // If the join operation was not able to
                                // coalesce the two pages into one, then we need
                                // to re-index the new first key of the second
                                // page.
                                //
                                // We have either a quick way to do this or a
                                // more complex way. If there is a single parent
                                // page in the index for the two re-balanced
                                // pages, and if the key to be reinserted fits
                                // in that parent page, then all we need to do
                                // is insert it. Otherwise, we will need to
                                // split the page above us, and that will
                                // potentially result in additional buffer
                                // reservations. Because that could force a
                                // retry at a bad time, in that case we defer
                                // the re-insertion of the index key until
                                // after all the current claims are released.
                                //
                                needsReindex = true;
                                if (level < _cacheDepth - 1) {
                                    final LevelCache parentLc = _levelCache[level + 1];
                                    final Buffer buffer = parentLc._leftBuffer;

                                    Debug.$assert0.t(buffer != null);
                                    if (parentLc._rightBuffer == buffer) {
                                        final int foundAt = buffer.findKey(_spareKey1);
                                        Debug.$assert0.t((foundAt & EXACT_MASK) == 0);
                                        // Try it the simple way
                                        _value.setPointerValue(buffer2.getPageAddress());
                                        _value.setPointerPageType(buffer2.getPageType());
                                        _rawValueWriter.init(_value);
                                        final int fit = buffer.putValue(_spareKey1, _rawValueWriter, foundAt, false);

                                        // If it worked then we're done.
                                        if (fit != -1) {
                                            needsReindex = false;
                                            buffer.setDirtyAtTimestamp(timestamp);
                                        }
                                    }
                                }
                                if (needsReindex) {
                                    _spareKey1.copyTo(_spareKey2);
                                    _value.setPointerValue(buffer2.getPageAddress());
                                    _value.setPointerPageType(buffer2.getPageType());

                                    storeInternal(_spareKey2, _value, level + 1, StoreOptions.NONE);
                                    needsReindex = false;
                                }
                            }

                            result = true;
                        } else if (foundAt1 != foundAt2) {
                            Debug.$assert0.t(foundAt2 > foundAt1);
                            _key.copyTo(_spareKey1);
                            //
                            // Before we remove these records, we need to
                            // recover any LONG_RECORD pointers that may be
                            // associated with keys in this range.
                            //
                            _volume.getStructure().harvestLongRecords(buffer1, foundAt1, foundAt2);
                            result |= buffer1.removeKeys(foundAt1, foundAt2, _spareKey1);
                            if (buffer1.isDataPage() && result) {
                                _tree.bumpChangeCount();
                            }
                            buffer1.setDirtyAtTimestamp(timestamp);
                        }

                        if (level < _cacheDepth - 1) {
                            removeKeyRangeReleaseLevel(level + 1);
                        }
                    }
                    break;
                } catch (final RetryException re) {
                    // handled below by releasing claims and retrying
                } finally {
                    //
                    // Release all buffers.
                    //
                    for (int level = _cacheDepth; --level >= depth;) {
                        removeKeyRangeReleaseLevel(level);
                    }

                    if (treeClaimAcquired) {
                        _treeHolder.release();
                        treeClaimAcquired = false;
                    }
                }
                /*
                 * Having released all prior claims, now acquire an exclusive
                 * claim on the Tree.
                 */
                if (treeWriterClaimRequired) {
                    if (!_treeHolder.claim(true)) {
                        Debug.$assert0.t(false);
                        throw new InUseException("Thread " + Thread.currentThread().getName()
                                + " failed to get reader claim on " + _tree);
                    }
                    treeClaimAcquired = true;
                }
            }

            while (deallocationRequired) {
                long left = -1;
                long right = -1;
                for (int level = _cacheDepth; --level >= 0;) {
                    final LevelCache lc = _levelCache[level];
                    left = lc._deallocLeftPage;
                    right = lc._deallocRightPage;
                    if (left != 0) {
                        sequence(DEALLOCATE_CHAIN_A);
                        _volume.getStructure().deallocateGarbageChain(left, right);
                        lc._deallocLeftPage = 0;
                        lc._deallocRightPage = 0;
                    }
                }
                // If we successfully finish the loop then we're done
                deallocationRequired = false;
                break;
            }
        } finally {
            if (treeClaimAcquired) {
                if (treeWriterClaimRequired) {
                    _tree.bumpGeneration();
                }
                _treeHolder.release();
                treeClaimAcquired = false;
            }
        }

        _volume.getStatistics().bumpRemoveCounter();
        _tree.getStatistics().bumpRemoveCounter();
        if (fetchFirst) {
            _volume.getStatistics().bumpFetchCounter();
            _tree.getStatistics().bumpFetchCounter();
        }
        return result;
    }

    /**
     * Handle the extremely rare case where removing a key from a pair of
     * adjacent pages requires the left page to be split. To split the page this
     * method inserts an empty record with key being deleted, allowing the
     * {@link Buffer#split(Buffer, Key, ValueHelper, int, Key, Sequence, SplitPolicy)}
     * method to be used.
     * 
     * @param lc
     *            LevelCache set up by raw_removeKeyRangeInternal
     * @throws PersistitException
     */
    private void rebalanceSplit(final LevelCache lc) throws PersistitException {
        //
        // Allocate a new page
        //
        final int level = lc._level;
        final int foundAt = lc._leftFoundAt;
        final Buffer left = lc._leftBuffer;
        final Buffer inserted = _volume.getStructure().allocPage();
        try {
            final long timestamp = timestamp();
            left.writePageOnCheckpoint(timestamp);
            inserted.writePageOnCheckpoint(timestamp);

            Debug.$assert0.t(inserted.getPageAddress() != 0);
            Debug.$assert0.t(inserted != left);

            inserted.init(left.getPageType());

            final Value value = _persistit.getThreadLocalValue();
            value.clear();
            _rawValueWriter.init(value);
            final Key key = _persistit.getThreadLocalKey();
            lc._rightBuffer.nextKey(key, Buffer.HEADER_SIZE);

            left.split(inserted, key, _rawValueWriter, foundAt | EXACT_MASK, _spareKey1, Sequence.NONE,
                    SplitPolicy.EVEN_BIAS);

            inserted.setRightSibling(left.getRightSibling());
            left.setRightSibling(inserted.getPageAddress());
            left.setDirtyAtTimestamp(timestamp);
            inserted.setDirtyAtTimestamp(timestamp);
            lc._leftBuffer = inserted;
            lc._leftFoundAt = inserted.findKey(key);

            _persistit.getCleanupManager().offer(
                    new CleanupManager.CleanupIndexHole(_tree.getHandle(), inserted.getPageAddress(), level));
        } finally {
            left.releaseTouched();
        }
    }

    private void removeKeyRangeReleaseLevel(final int level) {

        final LevelCache lc = _levelCache[level];
        final Buffer buffer1 = lc._leftBuffer;
        final Buffer buffer2 = lc._rightBuffer;

        if (buffer2 != null && (lc._flags & RIGHT_CLAIMED) != 0) {
            buffer2.releaseTouched();
        }
        if (buffer1 != null && (lc._flags & LEFT_CLAIMED) != 0) {
            buffer1.releaseTouched();
        }

        lc._leftBuffer = null;
        lc._rightBuffer = null;
        lc._flags = 0;
    }

    private void removeFetchFirst(final Buffer buffer1, int foundAt1, final Buffer buffer2, final int foundAt2)
            throws PersistitException {
        if (buffer1 == buffer2) {
            if (buffer1.nextKeyBlock(foundAt1) == (foundAt2 & P_MASK)) {
                buffer1.fetch(foundAt1 | EXACT_MASK, _spareValue);
            }
        } else {
            if (buffer1.getRightSibling() == buffer2.getPageAddress() && buffer1.nextKeyBlock(foundAt1) == -1) {
                foundAt1 = buffer2.toKeyBlock(0);
                if (buffer2.nextKeyBlock(foundAt1) == (foundAt2 & P_MASK)) {
                    buffer2.fetch(foundAt1 | EXACT_MASK, _spareValue);
                }
            }
        }
        if (_spareValue.isDefined()) {
            fetchFixupForLongRecords(_spareValue, Integer.MAX_VALUE);
        }
    }

    private boolean isKeyRangeAntiValue(final Buffer buffer1, final int foundAt1, final Buffer buffer2,
            final int foundAt2) {
        if (buffer1.getKeyBlockEnd() != (foundAt1 & P_MASK) + KEYBLOCK_LENGTH) {
            return false;
        }
        if (buffer2.getKeyBlockStart() != (foundAt2 & P_MASK) - KEYBLOCK_LENGTH) {
            return false;
        }
        if (buffer1.getRightSibling() != buffer2.getPageAddress()) {
            return false;
        }
        return buffer2.isPrimordialAntiValue(Buffer.KEY_BLOCK_START);
    }

    void prune() throws PersistitException {
        prune(_key);
    }

    boolean prune(final Key key) throws PersistitException {
        Buffer buffer = null;
        Debug.$assert1.t(_tree.isValid());
        try {
            search(key, true);
            buffer = _levelCache[0]._buffer;
            if (buffer != null) {
                return buffer.pruneMvvValues(_tree, true);
            } else {
                return false;
            }
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    boolean prune(final Key key1, final Key key2) throws PersistitException {
        Buffer buffer = null;
        boolean pruned = false;

        Debug.$assert1.t(_tree.isValid());
        try {
            search(key1, true);
            buffer = _levelCache[0]._buffer;

            while (buffer != null) {
                checkPageType(buffer, Buffer.PAGE_TYPE_DATA, false);
                pruned |= buffer.pruneMvvValues(_tree, true);
                final int foundAt = buffer.findKey(key2);
                if (!buffer.isAfterRightEdge(foundAt)) {
                    break;
                }
                final Buffer oldBuffer = buffer;
                final long rightPageAddress = buffer.getRightSibling();
                if (rightPageAddress == 0) {
                    break;
                }
                buffer = _pool.get(_volume, buffer.getRightSibling(), true, true);
                oldBuffer.release();
            }

        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
        return pruned;
    }

    boolean prune(final long page) throws PersistitException {
        Buffer buffer = null;
        try {
            buffer = _pool.get(_volume, page, true, true);
            return buffer.pruneMvvValues(_tree, true);
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    boolean pruneLeftEdgeValue(final long page) throws PersistitException {
        _ignoreTransactions = true;
        Buffer buffer = null;
        try {
            buffer = _pool.get(_volume, page, false, true);
            buffer.clearEnqueuedForPruning();
            final long at = buffer.at(Buffer.KEY_BLOCK_START);
            if (at > 0) {
                final int offset = (int) (at >>> 32);
                final int size = (int) at;
                if (size == 1 && buffer.getBytes()[offset] == MVV.TYPE_ANTIVALUE) {
                    buffer.nextKey(_spareKey3, Buffer.KEY_BLOCK_START);
                    buffer.release();
                    buffer = null;
                    _spareKey3.copyTo(_spareKey4);
                    _spareKey4.nudgeDeeper();
                    raw_removeKeyRangeInternal(_spareKey3, _spareKey4, false, true);
                    return true;
                }
            }
            return false;
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    boolean fixIndexHole(final long page, final int level) throws PersistitException {
        _ignoreTransactions = true;
        Buffer buffer = null;
        if (!_treeHolder.claim(false, Persistit.SHORT_DELAY)) {
            return false;
        }
        try {
            buffer = _pool.get(_volume, page, false, true);
            buffer.nextKey(_spareKey2, buffer.toKeyBlock(0));
            _value.setPointerValue(page);
            _value.setPointerPageType(buffer.getPageType());
            buffer.release();
            buffer = null;
            storeInternal(_spareKey2, _value, level + 1, StoreOptions.NONE);
            return true;
        } finally {
            _treeHolder.release();
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    private void checkPageType(final Buffer buffer, final int expectedType, final boolean releaseOnFailure)
            throws PersistitException {
        assert !buffer.isOwnedAsWriterByOther();
        final int type = buffer.getPageType();
        if (type != expectedType) {
            if (releaseOnFailure) {
                buffer.releaseTouched();
            }
            corrupt("Volume " + _volume + " page " + buffer.getPageAddress() + " invalid page type " + type
                    + ": should be " + expectedType);
        }
    }

    /**
     * Assert that the current thread matches the "owner" of the Exchange. The
     * owner is set when the Exchange is created or first used. To enable
     * pooling, the {@link #removeState(boolean)} method clears it.
     * 
     * @param set
     *            Whether to set or clear the thread field for subsequent
     *            checks.
     */
    private void assertCorrectThread(final boolean set) {
        assert checkThread(set) : "Thread " + Thread.currentThread() + " must not use " + this + " owned by " + _thread;
    }

    private boolean checkThread(final boolean set) {
        final Thread t = Thread.currentThread();
        final boolean okay = _thread == null || _thread == t;
        if (okay) {
            _thread = set ? t : null;
        }
        return okay;
    }

    /**
     * The transaction context for this Exchange. By default, this is the
     * transaction context of the current thread, and by default, all
     * <code>Exchange</code>s created by a thread share the same transaction
     * context.
     * 
     * @return The <code>Transaction</code> context for this thread.
     */
    @Override
    public Transaction getTransaction() {
        assertCorrectThread(true);
        return _transaction;
    }

    LongRecordHelper getLongRecordHelper() {
        if (_longRecordHelper == null) {
            _longRecordHelper = new LongRecordHelper(_persistit, this);
        }
        return _longRecordHelper;
    }

    /**
     * Allows for all MVV contents to be returned through the Value object
     * during fetch. This can then be displayed conveniently through
     * {@link Value#toString()} or as an array from {@link Value#get()}.
     * 
     * @param doIgnore
     *            If <code>true</code> return MVVs as described otherwise return
     *            the appropriate single version.
     */
    void ignoreMVCCFetch(final boolean doIgnore) {
        _ignoreMVCCFetch = doIgnore;
    }

    void ignoreTransactions() {
        _ignoreTransactions = true;
    }

    /**
     * Package-private method indicates whether this <code>Exchange</code>
     * refers to the directory tree.
     * 
     * @return <code>true</code> if this is a directory exchange, else
     *         <code>false</code>.
     */
    boolean isDirectoryExchange() {
        assertCorrectThread(true);
        return _isDirectoryExchange;
    }

    public void setSplitPolicy(final SplitPolicy policy) {
        assertCorrectThread(true);
        _splitPolicy = policy;
    }

    public void setJoinPolicy(final JoinPolicy policy) {
        assertCorrectThread(true);
        _joinPolicy = policy;
    }

    public KeyHistogram computeHistogram(final Key start, final Key end, final int sampleSize, final int keyDepth,
            final KeyFilter keyFilter, final int requestedTreeDepth) throws PersistitException {
        assertCorrectThread(true);
        _persistit.checkClosed();

        checkLevelCache();
        final int treeDepth = requestedTreeDepth > _tree.getDepth() ? _tree.getDepth() : requestedTreeDepth;
        if (treeDepth < 0) {
            throw new IllegalArgumentException("treeDepth out of bounds: " + treeDepth);
        }
        final KeyHistogram histogram = new KeyHistogram(getTree(), start, end, sampleSize, keyDepth, treeDepth);
        Buffer previousBuffer = null;
        LevelCache lc = null;
        Buffer buffer = null;
        Direction direction = GTEQ;
        if (start != null) {
            start.copyTo(_key);
        } else {
            LEFT_GUARD_KEY.copyTo(_key);
            direction = GT;
        }

        int foundAt = searchTree(_key, treeDepth, false);
        try {
            lc = _levelCache[treeDepth];
            buffer = lc._buffer;
            if (buffer != null) {
                checkPageType(buffer, treeDepth + 1, false);
            }

            while (foundAt != -1) {
                foundAt = buffer.traverse(_key, direction, foundAt);
                direction = GT;
                if (buffer.isAfterRightEdge(foundAt)) {
                    final long rightSiblingPage = buffer.getRightSibling();
                    if (rightSiblingPage > 0) {
                        final Buffer rightSibling = _pool.get(_volume, rightSiblingPage, false, true);
                        buffer.releaseTouched();
                        //
                        // Reset foundAtNext to point to the first key block
                        // of the right sibling page.
                        //
                        buffer = rightSibling;
                        checkPageType(buffer, treeDepth + 1, false);
                        foundAt = buffer.traverse(_key, GT, buffer.toKeyBlock(0));
                    } else {
                        foundAt = -1;
                        break;
                    }
                }
                if (end != null && end.compareTo(_key) < 0) {
                    break;
                }
                if (!_key.isLeftEdge()) {
                    if (buffer != previousBuffer) {
                        histogram.addPage(buffer.getBufferSize(), buffer.getBufferSize() - buffer.getAvailableSize());
                        previousBuffer = buffer;
                    }
                    if (keyFilter == null || keyFilter.selected(_key)) {
                        histogram.addKeyCopy(_key);
                    }
                }
            }
        } finally {
            if (buffer != null) {
                buffer.releaseTouched();
            }
        }

        histogram.cull();
        return histogram;
    }

    void corrupt(final String error) throws CorruptVolumeException {
        Debug.$assert0.t(false);
        _persistit.getLogBase().corruptVolume.log(error + Util.NEW_LINE + toStringDetail());
        throw new CorruptVolumeException(error);
    }

    /**
     * Store an Object with this Exchange for the convenience of an application.
     * 
     * @param appCache
     *            the object to be cached for application convenience.
     */
    public void setAppCache(final Object appCache) {
        assertCorrectThread(true);
        _appCache = appCache;
    }

    /**
     * @return the object cached for application convenience
     */
    public Object getAppCache() {
        assertCorrectThread(true);
        return _appCache;
    }

    /**
     * Returns a copy of either the data page or a page on the index path to the
     * data page containing the current key. This method looks up the current
     * key, then copies and returns the page at the specified tree level in a
     * new Buffer. The resulting Buffer object is not part of the BufferPool and
     * can simply be discarded when the caller is finished with it.
     * 
     * @param level
     *            The tree level, starting at zero for the data page.
     * @return copy of page on the key's index tree at that level.
     */
    public Buffer fetchBufferCopy(final int level) throws PersistitException {
        assertCorrectThread(true);
        if (level >= _tree.getDepth() || level <= -_tree.getDepth()) {
            throw new IllegalArgumentException("Tree depth is " + _tree.getDepth());
        }
        final int lvl = level >= 0 ? level : _tree.getDepth() + level;
        final int foundAt = searchTree(_key, lvl, false);
        final Buffer buffer = _levelCache[lvl]._buffer;
        try {
            if (foundAt == -1) {
                return null;
            } else {
                return new Buffer(buffer);
            }
        } finally {
            buffer.releaseTouched();
        }
    }

    String toStringDetail() {
        final StringBuilder sb = new StringBuilder(toString());
        for (int level = 0; level < MAX_TREE_DEPTH; level++) {
            final LevelCache lc = _levelCache[level];
            if (lc == null || lc._buffer == null) {
                break;
            } else {
                sb.append(Util.NEW_LINE);
                sb.append(level);
                sb.append(": ");
                sb.append(lc);
            }
        }
        return sb.toString();
    }

    /**
     * Intended to be a test method. Fetches the current _key and determines if
     * stored value is a LONG_RECORD. No other state, including the fetched
     * value, can be gotten from this method.
     * 
     * @return <code>true</code> if the value is a LONG_RECORD
     * @throws PersistitException
     *             Any error during fetch
     */
    boolean isValueLongRecord() throws PersistitException {
        final boolean savedIgnore = _ignoreMVCCFetch;
        try {
            _ignoreMVCCFetch = true;
            searchAndFetchInternal(_spareValue, -1);
            final boolean wasLong = isLongRecord(_spareValue);
            _spareValue.clear();
            return wasLong;
        } finally {
            _ignoreMVCCFetch = savedIgnore;
        }
    }

    /**
     * Intended to be a test method. Fetches the current _key and determines if
     * stored value is a long MVV. No other state, including the fetched value,
     * can be gotten from this method.
     * 
     * @return <code>true</code> if the value is a long MVV
     * @throws PersistitException
     *             Any error during fetch
     */
    boolean isValueLongMVV() throws PersistitException {
        final boolean savedIgnore = _ignoreMVCCFetch;
        try {
            _ignoreMVCCFetch = true;
            searchAndFetchInternal(_spareValue, -1);
            final boolean wasLong = isLongMVV(_spareValue);
            _spareValue.clear();
            return wasLong;
        } finally {
            _ignoreMVCCFetch = savedIgnore;
        }
    }
}
