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

import java.lang.ref.WeakReference;

import com.persistit.Key.Direction;
import com.persistit.LockManager.ResourceTracker;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TreeNotFoundException;

/**
 * <p>
 * The main facade for fetching, storing and removing records from a
 * Persistit&trade; database.
 * </p>
 * <p>
 * Applications interact with Persistit through instances of this class. A
 * <tt>Exchange</tt> has two important associated member objects, a
 * {@link com.persistit.Key} and a {@link com.persistit.Value}. A <tt>Key</tt>
 * is a mutable representation of a key, and a <tt>Value</tt> is a mutable
 * representation of a value. Applications manipulate these objects and interact
 * with the database through one of the following four general patterns:
 * <ol>
 * <li>
 * Modify the <tt>Key</tt>, perform a {@link com.persistit.Exchange#fetch fetch}
 * operation, and query the <tt>Value</tt>.</li>
 * <li>
 * Modify the <tt>Key</tt>, modify the <tt>Value</tt>, and then perform a
 * {@link com.persistit.Exchange#store store} operation to insert or replace
 * data in the database.</li>
 * <li>
 * Modify the <tt>Key</tt>, and then perform a
 * {@link com.persistit.Exchange#remove remove} to remove one or more key/value
 * pairs.</li>
 * <li>
 * Optionally modify the <tt>Key</tt>, perform a
 * {@link com.persistit.Exchange#traverse traverse} operation, then query the
 * resulting state of <tt>Key</tt> and/or <tt>Value</tt> to enumerate key/value
 * pairs currently stored in the database.</li>
 * </ol>
 * <p>
 * Additional methods of <tt>Exchange</tt> include {@link #fetchAndStore
 * fetchAndStore} and {@link #fetchAndRemove fetchAndRemove} which atomically
 * modify the database and return the former value associated with the current
 * <tt>Key</tt>, and {@link #incrementValue} which atomically increments an
 * integer value associated with the current <tt>Key</tt>.
 * </p>
 * <p>
 * <h3>Exchange is Not Threadsafe</h3>
 * <em>Important:</em> an <tt>Exchange</tt> and its associated <tt>Key</tt> and
 * <tt>Value</tt> instances are <i>not</i> thread-safe. Generally each
 * <tt>Thread</tt> should allocate and use its own <tt>Exchange</tt> instances.
 * Were it to occur, modification of the <tt>Key</tt> or <tt>Value</tt> objects
 * associated with an <tt>Exchange</tt> by another thread could cause severe and
 * unpredictable errors, including possible corruption of the underlying data
 * storage. For this reason, each <tt>Exchange</tt> instance is associated with
 * the first <tt>Thread</tt> that performs any method accessing or modifying
 * either its key or value. Any subsequent attempt to access or modify either of
 * these fields by a different <tt>Thread</tt> results in a
 * {@link com.persistit.exception.WrongThreadException WrongThreadException}.
 * Also see {@link #clearOwnerThread} and {@link #getOwner()}.
 * </p>
 * <p>
 * Despite the fact that the methods of <tt>Exchange</tt> are not threadsafe,
 * Persistit is designed to allow multiple threads, using <em>multiple</em>
 * <tt>Exchange</tt> instances, to access and update the underlying database in
 * a highly concurrent fashion.
 * </p>
 * <p>
 * <h3>Exchange Pools</h3>
 * Normally each thread should allocate its own <tt>Exchange</tt> instances.
 * However, depending on the garbage collection performance characteristics of a
 * particular JVM it may be desirable to maintain a pool of <tt>Exchange</tt>'s
 * available for reuse, thereby reducing the frequency with which
 * <tt>Exchange</tt>s need to be constructed and then garbage collected. An
 * application may get an Exchange using
 * {@link Persistit#getExchange(String, String, boolean)} or
 * {@link Persistit#getExchange(Volume, String, boolean)}. These methods reuse a
 * previously constructed <tt>Exchange</tt> if one is available in a pool;
 * otherwise they construct methods construct a new one. Applications using the
 * Exchange pool should call
 * {@link Persistit#releaseExchange(Exchange, boolean)} to relinquish an
 * <tt>Exchange</tt> once it is no longer needed, thereby placing it in the pool
 * for subsequent reuse.
 * </p>
 * 
 * @version 1.0
 */
public class Exchange {
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

    /**
     * Upper bound on long record chains.
     */
    final static int MAX_LONG_RECORD_CHAIN = 5000;

    private final static int LEFT_CLAIMED = 1;

    private final static int RIGHT_CLAIMED = 2;

    private Persistit _persistit;

    private long _timeout;

    private final Key _key;
    private final Value _value;

    private final LevelCache[] _levelCache = new LevelCache[MAX_TREE_DEPTH];

    private BufferPool _pool;
    private long _treeGeneration = -1;
    private Volume _volume;
    private Tree _tree;

    private int _cacheDepth = 0;
    private int _treeDepth = 0;
    private long _rootPage = 0;

    private boolean _exclusive;
    private Key _spareKey1;
    private Key _spareKey2;

    private Value _spareValue;

    private SplitPolicy _splitPolicy = SplitPolicy.NICE_BIAS;
    private JoinPolicy _joinPolicy = JoinPolicy.EVEN_BIAS;

    private boolean _isDirectoryExchange = false;
    private boolean _hasDeferredDeallocations = false;
    private boolean _hasDeferredTreeUpdate = false;

    private Transaction _transaction;

    private boolean _ignoreTransactions;

    private long _longRecordPageAddress;

    private WeakReference<Thread> _currentThread;

    private boolean _secure;

    /**
     * <p>
     * Construct a new <tt>Exchange</tt> object to create and/or access the
     * {@link Tree} specified by treeName within the {@link Volume} specified by
     * <tt>volumeName</tt>. This constructor optionally creates a new
     * <tt>Tree</tt>. If the <tt>create</tt> parameter is false and a
     * <tt>Tree</tt> by the specified name does not exist, this constructor
     * throws a {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * <p>
     * The <tt>volumeName</tt< you supply must match exactly one open 
     * <tt>Volume</tt>. The name matches if either (a) the <tt>Volume</tt> has
     * an optional alias that is equal to the supplied name, or (b) if the
     * supplied name matches a substring of the <tt>Volume</tt>'s pathname. If
     * there is not unique match for the name you supply, this method throws a
     * {@link com.persistit.exception.VolumeNotFoundException}.
     * </p>
     * 
     * @param volumeName
     *            The volume name that either matches the alias or a partially
     *            matches the pathname of exactly one open <tt>Volume</tt>.
     * 
     * @param treeName
     *            The tree name
     * 
     * @param create
     *            <tt>true</tt> to create a new Tree if one by the specified
     *            name does not already exist.
     * 
     * @throws PersistitException
     */
    public Exchange(Persistit persistit, String volumeName, String treeName,
            boolean create) throws PersistitException {
        this(persistit, persistit.getVolume(volumeName), treeName, create);
    }

    /**
     * <p>
     * Construct a new <tt>Exchange</tt> object to create and/or access the
     * {@link Tree} specified by treeName within the specified {@link Volume}.
     * This constructor optionally creates a new <tt>Tree</tt>. If the
     * <tt>create</tt> parameter is false and a <tt>Tree</tt> by the specified
     * name does not exist, this constructor throws a
     * {@link com.persistit.exception.TreeNotFoundException}.
     * </p>
     * 
     * @param volume
     *            The Volume
     * @param treeName
     *            The tree name
     * @param create
     *            <tt>true</tt> to create a new Tree if one by the specified
     *            name does not already exist.
     * @throws PersistitException
     */
    public Exchange(Persistit persistit, Volume volume, String treeName,
            boolean create) throws PersistitException {
        _persistit = persistit;
        _key = new Key(persistit);
        _spareKey1 = new Key(persistit);
        _spareKey2 = new Key(persistit);
        _value = new Value(persistit);
        _spareValue = new Value(persistit);
        if (volume == null) {
            throw new NullPointerException();
        }
        init(volume, treeName, create);
    }

    /**
     * Construct a new <tt>Exchange</tt> to access the same {@link Volume} and
     * {@link Tree} as the supplied <tt>Exchange</tt>. The states of the
     * supplied <tt>Exchange</tt>'s {@link Key} and {@link Value} objects are
     * copied to new the <tt>Key</tt> and new <tt>Value</tt> associated with
     * this <tt>Exchange</tt> so that operations on the two <tt>Exchange</tt>s
     * initially behave identically.
     * 
     * @param exchange
     *            The <tt>Exchange</tt> to copy from.
     */
    public Exchange(Exchange exchange) {
        _persistit = exchange._persistit;
        _key = new Key(_persistit);
        _spareKey1 = new Key(_persistit);
        _spareKey2 = new Key(_persistit);
        _value = new Value(_persistit);
        _spareValue = new Value(_persistit);
        init(exchange);
    }

    /**
     * Construct a new <tt>Exchange</tt> to access the specified {@link Tree}.
     * 
     * @param tree
     *            The <tt>Tree</tt> to access.
     */
    public Exchange(Tree tree) {
        _persistit = tree._persistit;
        _key = new Key(_persistit);
        _spareKey1 = new Key(_persistit);
        _spareKey2 = new Key(_persistit);
        _value = new Value(_persistit);
        init(tree);
        _spareValue = new Value(_persistit);
        _volume = tree.getVolume();
        _isDirectoryExchange = tree == _volume.getDirectoryTree();
        initCache();
    }

    void init(Volume volume, String treeName, boolean create)
            throws PersistitException {
        if (volume == null) {
            throw new NullPointerException();
        }

        Tree tree = volume.getTree(treeName, create);
        if (tree == null) {
            throw new TreeNotFoundException(treeName);
        }
        init(tree);
    }

    void init(final Tree tree) {
        final Volume volume = tree.getVolume();
        _pool = _persistit.getBufferPool(volume.getPageSize());
        _transaction = _persistit.getTransaction();
        _timeout = _persistit.getDefaultTimeout();
        _key.clear();
        _value.clear();

        if (_volume != volume || _tree != tree) {
            _volume = volume;
            _tree = tree;
            _treeGeneration = -1;
            initCache();
        }
    }

    void init(Exchange exchange) {
        _persistit = exchange._persistit;
        _volume = exchange._volume;
        _tree = exchange._tree;
        _pool = exchange._pool;

        _rootPage = exchange._rootPage;
        _treeDepth = exchange._treeDepth;
        _treeGeneration = -1;
        _transaction = _persistit.getTransaction();
        _cacheDepth = exchange._cacheDepth;
        _timeout = exchange._timeout;

        initCache();

        for (int index = 0; index < _cacheDepth; index++) {
            exchange._levelCache[index].copyTo(_levelCache[index]);
        }

        exchange._key.copyTo(_key);
        exchange._value.copyTo(_value);
    }

    void removeState(boolean secure) {
        _key.clear(secure);
        _value.clear(secure);
        _spareKey1.clear(secure);
        _spareKey2.clear(secure);
        _spareValue.clear(secure);
        _transaction = null;
        _ignoreTransactions = false;
    }

    private void initCache() {
        for (int level = 0; level < MAX_TREE_DEPTH; level++) {
            if (_levelCache[level] != null)
                _levelCache[level].invalidate();
            else
                _levelCache[level] = new LevelCache(level);
        }
    }

    private void checkLevelCache() {
        if (_treeGeneration != _tree.getGeneration()) {
            _tree.loadRootLevelInfo(this);
            _cacheDepth = _treeDepth;
            // for (int index = 0; index < _cacheDepth; index++)
            for (int index = 0; index < MAX_TREE_DEPTH; index++) {
                LevelCache lc = _levelCache[index];
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
        //
        // The remaining fields are used only by removeKeyRangeInternal and
        // its helpers.
        //
        Buffer _leftBuffer;
        Buffer _rightBuffer;
        int _leftFoundAt;
        int _rightFoundAt;
        int _flags;
        long _deallocLeftPage;
        long _deallocRightPage;
        long _deferredReindexPage;
        long _deferredReindexChangeCount;

        private LevelCache(int level) {
            _level = level;
        }

        @Override
        public String toString() {
            if (_buffer == null)
                return "<empty>";

            return "Buffer=<" + _buffer + ">" + ", keyGeneration="
                    + _keyGeneration + ", bufferGeneration="
                    + _bufferGeneration + ", foundAt="
                    + _buffer.foundAtString(_foundAt) + ">";
        }

        private void copyTo(LevelCache to) {
            if (Debug.ENABLED) {
                Debug.$assert(to._level == _level || to._level == -1);
            }
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

        private void update(Buffer buffer, Key key, int foundAt) {
            if (Debug.ENABLED) {
                Debug.$assert(_level + Buffer.PAGE_TYPE_DATA == buffer
                        .getPageType());

                if (foundAt != -1 && (foundAt & Buffer.EXACT_MASK) != 0) {
                    int depth = Buffer.decodeDepth(foundAt);
                    int klength = key.getEncodedSize();
                    if (Debug.ENABLED) {
                        Debug.$assert(depth == klength);
                    }
                }
            }

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

        private void initRemoveFields() {
            _leftBuffer = null;
            _rightBuffer = null;
            _leftFoundAt = -1;
            _rightFoundAt = -1;
            _flags = 0;
        }
    }

    // -----------------------------------------------------------------
    /**
     * Delegates to {@link Key#reset} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange reset() {
        getKey().reset();
        return this;
    }

    /**
     * Delegates to {@link Key#clear} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange clear() {
        getKey().clear();
        return this;
    }

    /**
     * Delegates to {@link Key#setDepth} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange setDepth(int depth) {
        getKey().setDepth(depth);
        return this;
    }

    /**
     * Delegates to {@link Key#cut(int)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange cut(int level) {
        getKey().cut(level);
        return this;
    }

    /**
     * Delegates to {@link Key#cut()} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange cut() {
        getKey().cut();
        return this;
    }

    /**
     * Delegates to {@link Key#append(boolean)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(boolean item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(byte)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(byte item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(short)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(short item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(char)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(char item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(int)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(int item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(long)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(long item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(float)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(float item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(double)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(double item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#append(Object)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange append(Object item) {
        getKey().append(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(boolean)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(boolean item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(byte)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(byte item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(short)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(short item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(char)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(char item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(int)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(int item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(long)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(long item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(float)} on the associated <tt>Key</tt> object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(float item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(double)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(double item) {
        getKey().to(item);
        return this;
    }

    /**
     * Delegates to {@link Key#to(Object)} on the associated <tt>Key</tt>
     * object.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining.
     */
    public Exchange to(Object item) {
        getKey().to(item);
        return this;
    }

    /**
     * Returns the {@link Key} associated with this <tt>Exchange</tt>.
     * 
     * @return This <tt>Key</tt>.
     */
    public Key getKey() {
        return _key;
    }

    /**
     * Returns the {@link Value} associated with this <tt>Exchange</tt>.
     * 
     * @return The <tt>Value</tt>.
     */
    public Value getValue() {
        return _value;
    }

    BufferPool getBufferPool() {
        return _pool;
    }

    /**
     * Returns the {@link Volume} containing the data accessed by this
     * <tt>Exchange</tt>.
     * 
     * @return The <tt>Volume</tt>.
     */
    public Volume getVolume() {
        return _volume;
    }

    /**
     * Returns the {@link Tree} on which this <tt>Exchange</tt> operates.
     * 
     * @return The <tt>Tree</tt>
     */
    public Tree getTree() {
        return _tree;
    }

    /**
     * Returns the Persistit instance from which this Exchange was created.
     * 
     * @return The <tt>Persistit<tt> instance.
     */
    public Persistit getPersistitInstance() {
        return _persistit;
    }

    /**
     * Returns the count of structural changes committed to the {@link Tree} on
     * which this <tt>Exchange</tt> operates. This count includes changes
     * committed by all Threads, including the current one. A structural change
     * is one in which at least one key is inserted or deleted. Replacement of
     * an existing value is not counted.
     * 
     * @return The change count
     */
    public long getChangeCount() {
        return _tree.getChangeCount();
    }

    /**
     * Returns the maximum time, in milliseconds, that operations on this
     * <tt>Exchange</tt> will wait for completion. In particular, if there is
     * heavy disk I/O an operation may require a significant amount of time to
     * complete. This is the approximate upper bound on that time.
     * 
     * @return The current timeout, in milliseconds.
     */
    public long getTimeout() {
        return _timeout;
    }

    /**
     * Change the maximum time (in milliseconds) that operations on this
     * <tt>Exchange</tt> will wait for completion. In particular, if there is
     * heavy disk I/O an operation may require a significant amount of time to
     * complete. This sets the approximate upper bound on that time.
     * 
     * @param timeout
     *            Maximum time, in milliseconds
     */
    public void setTimeout(long timeout) {
        if (timeout < 0 || timeout > Persistit.MAXIMUM_TIMEOUT_VALUE) {
            throw new IllegalArgumentException("must be between 0 and "
                    + Persistit.MAXIMUM_TIMEOUT_VALUE);
        }
        _timeout = timeout;
    }

    /**
     * To be called only by loadRootLevelInfo in Tree
     */
    void setRootLevelInfo(long root, int depth, long generation) {
        _rootPage = root;
        _treeDepth = depth;
        _treeGeneration = generation;
    }

    /**
     * This method is for the convenience of Transaction, PersistitMap and
     * Journal.
     * 
     * @return spareKey1
     */
    Key getAuxiliaryKey1() {
        return _spareKey1;
    }

    /**
     * This method is for the convenience of Transaction and Journal.
     * 
     * @return spareKey2
     */
    Key getAuxiliaryKey2() {
        return _spareKey2;
    }

    /**
     * This method is fo the convenience of Transaction.
     * 
     * @return spareValue
     */
    Value getAuxiliaryValue() {
        return _spareValue;
    }

    /**
     * Returns a displayable String containing the volume name, tree name and
     * current key state for this <tt>Exchange</tt>.
     * 
     * @return The displayable String
     */
    @Override
    public String toString() {
        return "Exchange(Volume=" + _volume.getPath() + ",Tree="
                + _tree.getName() + "," + ",Key=<" + _key.toString() + ">)";

    }

    /**
     * Search for a data record by key. Uses and maintains level cache.
     * 
     * @return Encoded key location within the data page. The page itself is
     *         valid in the level cache.
     * @throws PMapException
     */
    private int search(Key key) throws PersistitException {
        Buffer buffer = null;
        checkLevelCache();
        LevelCache lc = _levelCache[0];
        buffer = reclaimQuickBuffer(lc);

        if (buffer == null) {
            return searchTree(key, 0);
        }

        try {
            checkPageType(buffer, Buffer.PAGE_TYPE_DATA);
        } catch (CorruptVolumeException e) {
            buffer.release(); // Don't make Most-Recently-Used
            throw e;
        }

        int foundAt = findKey(buffer, key, lc);

        if (buffer.isBeforeLeftEdge(foundAt)
                || buffer.isAfterRightEdge(foundAt)) {
            buffer.release(); // Don't make Most-Recently-Used
            buffer = null;
            return searchTree(key, 0);
        }
        return foundAt;
    }

    private int findKey(Buffer buffer, Key key, LevelCache lc) {
        int foundAt = lc._foundAt;
        boolean done = false;
        // Possibly we can accelerate.
        //
        if (foundAt != -1 && buffer.getGeneration() == lc._bufferGeneration
                && key == _key) {
            if (key.getGeneration() == lc._keyGeneration) {
                // return foundAt;
                done = true;
            }
        }
        if (Debug.ENABLED && done) {
            int foundAt2 = buffer.findKey(key);
            Debug.$assert(foundAt2 == foundAt);
        }
        //
        // If no longer valid, then look it up again.
        //
        if (!done) {
            foundAt = buffer.findKey(key);
            lc.update(buffer, key, foundAt);
        }
        return foundAt;
    }

    /**
     * Searches for the current key from top down and populates the level cache
     * while doing so.
     * 
     * @return Encoded key location within the level. The page itself is valid
     *         within the level cache.
     * @throws PMapException
     */
    private int searchTree(Key key, int toLevel) throws PersistitException {
        Buffer oldBuffer = null;
        int currentLevel;
        int foundAt = -1;
        boolean found = false;

        if (!_tree.claim(false)) {
            if (Debug.ENABLED) {
                Debug.debug1(true);
            }
            throw new InUseException("Thread "
                    + Thread.currentThread().getName()
                    + " failed to get reader claim on " + _tree);
        }
        checkLevelCache();

        long pageAddress = _rootPage;
        long oldPageAddress = pageAddress;
        if (Debug.ENABLED) {
            Debug.$assert(pageAddress != 0);
        }

        try {
            for (currentLevel = _cacheDepth; --currentLevel >= toLevel;) {
                if (pageAddress <= 0) {
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }

                    throw new CorruptVolumeException("Volume " + _volume
                            + " level=" + currentLevel + " page=" + pageAddress
                            + " oldPage=" + oldPageAddress + " key=<"
                            + key.toString() + "> " + " invalid page address");
                }

                foundAt = searchLevel(key, pageAddress, currentLevel);
                if (oldBuffer != null) {
                    _persistit.getLockManager().setOffset();
                    _pool.release(oldBuffer);
                    oldBuffer = null;
                }

                LevelCache lc = _levelCache[currentLevel];
                Buffer buffer = lc._buffer;

                if (buffer == null || buffer.isBeforeLeftEdge(foundAt)) {
                    oldBuffer = buffer; // So it will be released
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }

                    throw new CorruptVolumeException("Volume " + _volume
                            + " level=" + currentLevel + " page=" + pageAddress
                            + " key=<" + key.toString() + "> "
                            + " is before left edge");
                }

                checkPageType(buffer, currentLevel + Buffer.PAGE_TYPE_DATA);

                if (currentLevel == toLevel) {
                    for (int level = currentLevel; --level > 0;) {
                        _levelCache[level].invalidate();
                    }
                    found = true;
                    return foundAt;
                } else if (buffer.isIndexPage()) {
                    int p = foundAt & Buffer.P_MASK;
                    if ((foundAt & Buffer.EXACT_MASK) == 0) {
                        p -= Buffer.KEYBLOCK_LENGTH;
                    }
                    oldBuffer = buffer; // So it will be released
                    oldPageAddress = pageAddress;
                    pageAddress = buffer.getPointer(p);

                    if (Debug.ENABLED) {
                        Debug.$assert(pageAddress > 0
                                && pageAddress < Buffer.MAX_VALID_PAGE_ADDR);
                    }
                } else {
                    oldBuffer = buffer; // So it will be released
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }

                    throw new CorruptVolumeException("Volume " + _volume
                            + " level=" + currentLevel + " page=" + pageAddress
                            + " key=<" + key.toString() + ">" + " page type="
                            + buffer.getPageType() + " is invalid");
                }
            }
            // Should never get here.
            return -1;

        } finally {
            if (oldBuffer != null) {
                // _persistit.getLockManager().setOffset();
                _pool.release(oldBuffer);
                oldBuffer = null;
            }
            if (found) {
                _persistit.getLockManager().setOffset();
            }
            _tree.release();
        }
    }

    /**
     * Search for the key in the specified page (data or index).
     * 
     * @param pageAddress
     *            The address of the page to search
     * @return Encoded key location within the page.
     * @throws PMapException
     */
    private int searchLevel(Key key, long pageAddress, int currentLevel)
            throws PersistitException {
        Buffer oldBuffer = null;
        try {
            long initialPageAddress = pageAddress; // DEBUG - debugging only
            long oldPageAddress = pageAddress;
            for (int rightWalk = MAX_WALK_RIGHT; rightWalk-- > 0;) {
                Buffer buffer = null;
                if (pageAddress <= 0) {
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }
                    throw new CorruptVolumeException("Volume " + _volume
                            + " level=" + currentLevel + " page=" + pageAddress
                            + " previousPage=" + oldPageAddress
                            + " initialPage=" + initialPageAddress + " key=<"
                            + key.toString() + ">" + " oldBuffer=<" + oldBuffer
                            + ">" + " invalid page address");
                }
                LevelCache lc = _levelCache[currentLevel];

                if (lc._page == pageAddress) {
                    buffer = reclaimQuickBuffer(lc);
                }

                if (buffer == null) {
                    buffer = _pool.get(_volume, pageAddress, _exclusive, true);
                }
                checkPageType(buffer, currentLevel + Buffer.PAGE_TYPE_DATA);

                //
                // Release previous buffer after claiming this one. This
                // prevents another Thread from inserting pages to the left
                // of our new buffer.
                //
                if (oldBuffer != null) {
                    _persistit.getLockManager().setOffset();
                    _pool.release(oldBuffer);
                    oldBuffer = null;
                }

                int foundAt = -1;

                if (pageAddress == lc._page && key == _key
                        && key.getGeneration() == lc._keyGeneration
                        && buffer.getGeneration() == lc._bufferGeneration) {
                    foundAt = lc._foundAt;
                }

                if (foundAt == -1) {
                    foundAt = buffer.findKey(key);
                }
                // DEBUG - debug - get rid of else clause
                else if (Debug.ENABLED) {
                    int foundAt2 = buffer.findKey(key);
                    Debug.$assert(foundAt2 == foundAt);
                }

                if (!buffer.isAfterRightEdge(foundAt)) {
                    lc.update(buffer, key, foundAt);
                    return foundAt;
                } else {
                    // lc.update(buffer, key, -1);
                }

                oldPageAddress = pageAddress;
                pageAddress = buffer.getRightSibling();

                if (Debug.ENABLED) {
                    Debug.$assert(pageAddress > 0
                            && pageAddress < Buffer.MAX_VALID_PAGE_ADDR);
                }
                oldBuffer = buffer;
            }
            if (Debug.ENABLED) {
                Debug.debug1(true);
            }
            throw new CorruptVolumeException("Volume " + _volume + " level="
                    + currentLevel + " page=" + oldPageAddress
                    + " initialPage=" + initialPageAddress + " key=<"
                    + key.toString() + ">" + " walked right more than "
                    + MAX_WALK_RIGHT + " pages" + " last page visited="
                    + pageAddress);
        } finally {
            if (oldBuffer != null) {
                _pool.release(oldBuffer);
            }
        }
    }

    /**
     * Inserts or replaces a data value in the database.
     * 
     * @return
     */
    Exchange store(Key key, Value value) throws PersistitException {
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        key.testValidForStoreAndFetch(_volume.getPageSize());
        _persistit.checkClosed();
        _persistit.checkSuspended();
        final int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();
        _transaction.assignTimestamp();
        storeInternal(key, value, 0, false, false);
        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);
        return this;
    }

    /**
     * Inserts or replaces a data value in the database starting at a specified
     * level and working up toward the root of the tree.
     * 
     * @return this Exchange
     */
    void storeInternal(Key key, Value value, int level, boolean fetchFirst,
            boolean dontWait) throws PersistitException {
        boolean treeClaimRequired = false;
        boolean treeClaimAcquired = false;
        boolean treeWriterClaimRequired = false;
        boolean committed = false;
        int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();

        final boolean inTxn = _transaction.isActive() && !_ignoreTransactions;

        int maxSimpleSize = _volume.getPageSize() - Buffer.OVERHEAD
                - maxStorableKeySize(_volume.getPageSize()) * 2;

        //
        // First insert the record in the data page
        //
        _exclusive = true;
        Buffer buffer = null;

        // long originalRootPageAddr = _tree.getRootPageAddr();
        //
        // The LONG_RECORD pointer that was present before the update, if
        // there is a long record being replaced.
        //
        long oldLongRecordPointer = 0;
        //
        // The LONG_RECORD pointer for a new long record value, if the
        // the new value is long.
        //
        long newLongRecordPointer = 0;

        boolean overlength = value.getEncodedSize() > maxSimpleSize
                && value.getEncodedSize() > maxUnitRecordSize();

        try {
            if (overlength) {
                //
                // This method may delay significantly for retries, and may
                // throw a TimeoutException. It must be called when there are
                // no other claimed resources.
                //
                newLongRecordPointer = storeOverlengthRecord(value, 0);
            }

            for (;;) {
                if (Debug.ENABLED) {
                    Debug.$assert(buffer == null);
                }
                _persistit.getLockManager().verifyLockedResourceCount(
                        lockedResourceCount);
                if (Debug.ENABLED) {
                    Debug.suspend();
                }

                if (treeClaimRequired && !treeClaimAcquired) {
                    if (!_tree.claim(treeWriterClaimRequired)) {
                        if (Debug.ENABLED) {
                            Debug.debug1(true);
                        }
                        throw new InUseException("Thread "
                                + Thread.currentThread().getName()
                                + " failed to get reader claim on " + _tree);
                    }
                    treeClaimAcquired = true;
                    lockedResourceCount++;
                }

                checkLevelCache();

                try {
                    _persistit.getLockManager().verifyLockedResourceCount(
                            lockedResourceCount);
                    if (inTxn) {
                        if (value.isAtomicIncrementArmed()) {
                            long from = value.getLong();
                            if (_transaction.fetch(this, value,
                                    Integer.MAX_VALUE) == null) {
                                fetch(value);
                            }
                            if (!value.isDefined()) {
                                value.put(from);
                            } else {
                                value.performAtomicIncrement();
                            }
                            fetchFirst = false;
                        } else if (fetchFirst) {
                            if (_transaction.fetch(this, _spareValue,
                                    Integer.MAX_VALUE) == null) {
                                fetch(_spareValue);
                            }
                        }
                        if (value.getEncodedSize() > maxSimpleSize) {
                            newLongRecordPointer = storeOverlengthRecord(value,
                                    0);
                        }
                        _transaction.store(this, key, value);
                        committed = true;
                        break;
                    }

                    if (level >= _cacheDepth) {
                        if (Debug.ENABLED) {
                            Debug.$assert(level == _cacheDepth);
                        }
                        //
                        // Need to lock the tree because we may need to change
                        // its root.
                        //
                        if (!treeClaimAcquired || !_tree.upgradeClaim()) {
                            treeClaimRequired = true;
                            treeWriterClaimRequired = true;
                            throw RetryException.SINGLE;
                        }

                        if (Debug.ENABLED) {
                            Debug.$assert(value.getPointerValue() > 0);
                        }
                        insertIndexLevel(key, value);
                        break;
                    }

                    if (Debug.ENABLED) {
                        Debug.$assert(buffer == null);
                    }
                    int foundAt = -1;
                    LevelCache lc = _levelCache[level];
                    buffer = reclaimQuickBuffer(lc);

                    if (buffer != null) {
                        //
                        // Start by assuming cached value is okay
                        //
                        foundAt = findKey(buffer, key, lc);

                        if (buffer.isBeforeLeftEdge(foundAt)
                                || buffer.isAfterRightEdge(foundAt)) {
                            _pool.release(buffer);
                            buffer = null;
                        }
                    }

                    if (buffer == null) {
                        foundAt = searchTree(key, level);
                        buffer = lc._buffer;
                    }

                    if (Debug.ENABLED) {
                        Debug.$assert(buffer != null
                                && (buffer._status & SharedResource.WRITER_MASK) != 0
                                && (buffer._status & SharedResource.CLAIMED_MASK) != 0);
                    }
                    if ((foundAt & Buffer.EXACT_MASK) != 0) {
                        oldLongRecordPointer = buffer
                                .fetchLongRecordPointer(foundAt);
                    }

                    if (fetchFirst && buffer.isDataPage()) {
                        buffer.fetch(foundAt, _spareValue);
                        fetchFixupForLongRecords(_spareValue, Integer.MAX_VALUE);
                    }

                    if (value.getEncodedSize() > maxSimpleSize && !overlength) {
                        newLongRecordPointer = storeLongRecord(value,
                                oldLongRecordPointer, 0);
                    } else {
                        _longRecordPageAddress = 0;
                    }
                    final int lockedResourceCount2 = _persistit
                            .getLockManager().getLockedResourceCount();
                    //
                    // Here we have a buffer with a writer claim and
                    // a correct foundAt value
                    //
                    boolean splitPerformed = putLevel(lc, key, value, buffer,
                            foundAt, treeClaimAcquired);

                    _persistit.getLockManager().verifyLockedResourceCount(
                            lockedResourceCount2);

                    if (Debug.ENABLED) {
                        Debug.$assert((buffer._status & SharedResource.WRITER_MASK) != 0
                                && (buffer._status & SharedResource.CLAIMED_MASK) != 0);
                    }
                    //
                    // If a split is required then putLevel did not change
                    // anything. We need to repeat this after acquiring a
                    // tree claim. Not that if treeClaimAcquired is false
                    // then the putLevel method did not actually split the
                    // page. It just backed out so we could repeat after
                    // acquiring the claim.
                    //
                    if (splitPerformed && !treeClaimAcquired) {
                        treeClaimRequired = true;
                        _pool.release(buffer);
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
                        if ((foundAt & Buffer.EXACT_MASK) == 0) {
                            _tree.bumpChangeCount();
                        }
                        committed = true;
                    }

                    _pool.release(buffer);
                    buffer = null;

                    if (!splitPerformed) {
                        // No split means we're totally done.
                        //
                        break;
                    } else {
                        // Otherwise we need to index the new right
                        // sibling at the next higher index level.
                        if (Debug.ENABLED) {
                            Debug.$assert(value.getPointerValue() > 0);
                        }
                        key = _spareKey1;
                        key.bumpGeneration(); // because otherwise we may
                        // use the wrong cached
                        // foundAt value

                        // // PDB 20050715
                        // _spareKey2.copyTo(_spareKey1);
                        // key.copyTo(_spareKey2);
                        _spareKey1 = _spareKey2;
                        _spareKey2 = key;
                        level++;
                        continue;
                    }

                } catch (RetryException re) {
                    newLongRecordPointer = 0;
                    oldLongRecordPointer = 0;
                    if (buffer != null) {
                        _pool.release(buffer);
                        buffer = null;
                    }

                    if (treeClaimAcquired) {
                        _tree.release();
                        treeClaimAcquired = false;
                        lockedResourceCount--;
                    }
                    treeClaimAcquired = _tree.claim(true, dontWait ? 0
                            : Tree.DEFAULT_MAX_WAIT_TIME);
                    if (treeClaimAcquired) {
                        lockedResourceCount++;
                    } else {
                        if (dontWait) {
                            throw re;
                        } else {
                            throw new InUseException("Thread "
                                    + Thread.currentThread().getName()
                                    + " failed to get reader claim on " + _tree);
                        }
                    }
                } finally {
                    if (buffer != null) {
                        _pool.release(buffer);
                        buffer = null;
                    }
                    _persistit.getLockManager().verifyLockedResourceCount(
                            lockedResourceCount);
                }
            }
        } finally {
            _exclusive = false;

            if (treeClaimAcquired) {
                _tree.release();
                treeClaimAcquired = false;
                lockedResourceCount--;
            }

            value.changeLongRecordMode(false);
            if (!committed) {
                //
                // We failed to write the new LONG_RECORD. If there was
                // previously no LONG_RECORD, then deallocate the newly
                // allocated LONG_RECORD chain if we had successfully
                // allocated one.
                //
                if (newLongRecordPointer != oldLongRecordPointer
                        && newLongRecordPointer != 0) {
                    _volume.deallocateGarbageChainDeferred(
                            newLongRecordPointer, 0);
                    _hasDeferredDeallocations = true;
                }
            } else if (oldLongRecordPointer != newLongRecordPointer
                    && oldLongRecordPointer != 0) {
                _volume.deallocateGarbageChainDeferred(oldLongRecordPointer, 0);
                _hasDeferredDeallocations = true;
            }
        }
        if (_hasDeferredDeallocations || _hasDeferredTreeUpdate) {
            commitAllDeferredUpdates();
        }
        // if (journalId != -1)
        // journal().completed(journalId);
        _volume.bumpStoreCounter();
        if (fetchFirst)
            _volume.bumpFetchCounter();
    }

    private void commitAllDeferredUpdates() throws PersistitException {

        if (Debug.ENABLED) {
            Debug.suspend();
        }
        final int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();
        for (;;) {
            if (_hasDeferredDeallocations) {
                _volume.commitAllDeferredDeallocations();
                _hasDeferredDeallocations = false;
            }
            if (_hasDeferredTreeUpdate) {
                _tree.commit();
                _volume.getDirectoryTree().commit();
                _hasDeferredTreeUpdate = false;
            }
            break;
        }
        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);
    }

    private void insertIndexLevel(Key key, Value value)
            throws PersistitException {

        Buffer buffer = null;
        try {
            buffer = _volume.allocPage();

            buffer.init(Buffer.PAGE_TYPE_INDEX_MIN + _treeDepth - 1);

            long newTopPage = buffer.getPageAddress();
            long leftSiblingPointer = _rootPage;

            if (Debug.ENABLED) {
                Debug.$assert(leftSiblingPointer == _tree.getRootPageAddr());
            }
            long rightSiblingPointer = value.getPointerValue();
            //
            // Note: left and right sibling are of the same level and therefore
            // it is not necessary to invoke value.setPointerPageType() here.
            //
            value.setPointerValue(leftSiblingPointer);
            buffer.putValue(LEFT_GUARD_KEY, value);

            value.setPointerValue(rightSiblingPointer);
            buffer.putValue(key, value);

            value.setPointerValue(-1);
            buffer.putValue(RIGHT_GUARD_KEY, Value.EMPTY_VALUE);

            buffer.setDirtyStructure();

            if (_isDirectoryExchange) {
                Tree tree = _volume.getDirectoryTree();
                tree.changeRootPageAddr(newTopPage, 1);
                tree.bumpGeneration();
            } else {
                _tree.changeRootPageAddr(newTopPage, 1);
                _tree.bumpGeneration();
            }

            _hasDeferredTreeUpdate = true;

        } finally {
            if (buffer != null) {
                _pool.release(buffer);
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
     * @return <tt>true</tt> if it necessary to insert a key into the ancestor
     *         index page.
     * @throws PMapException
     */
    private boolean putLevel(LevelCache lc, Key key, Value value,
            Buffer buffer, int foundAt, boolean okToSplit)
            throws PersistitException {
        if (Debug.ENABLED) {
            Debug.$assert(_exclusive);
            Debug.$assert((buffer._status & SharedResource.WRITER_MASK) != 0
                    && (buffer._status & SharedResource.CLAIMED_MASK) != 0);
        }

        int result = buffer.putValue(key, value, foundAt, false);
        if (result != -1) {
            buffer.setDirty();
            // lc.update(buffer, key, exact ? foundAt : -1);
            lc.update(buffer, key, result);
            return false;
        } else {
            if (Debug.ENABLED) {
                Debug.$assert(buffer.getPageAddress() != _volume
                        .getGarbageRoot());
            }
            Buffer rightSibling = null;

            try {
                // We can't perform the split because we don't have a claim
                // on the Tree. We will just return, and the caller will
                // call again with that claim.
                //
                if (!okToSplit) {
                    return true;
                }
                //
                // Allocate a new page
                //
                rightSibling = _volume.allocPage();

                if (Debug.ENABLED) {
                    Debug.$assert(rightSibling.getPageAddress() != 0);
                    Debug.$assert(rightSibling != buffer);
                }

                rightSibling.init(buffer.getPageType());
                // debug
                //
                // Split the page. As a side-effect, this will bump the
                // generation counters of both buffers, and therefore the
                // level cache for this level will become
                // (appropriately) invalid.
                //
                buffer.split(rightSibling, key, value, foundAt, _spareKey1,
                        _splitPolicy);
                lc.update(buffer, key, -1);

                long oldRightSibling = buffer.getRightSibling();
                long newRightSibling = rightSibling.getPageAddress();

                if (Debug.ENABLED) {
                    Debug.$assert(newRightSibling > 0
                            && oldRightSibling != newRightSibling);
                    Debug.$assert(rightSibling.getPageType() == buffer
                            .getPageType());
                }

                rightSibling.setRightSibling(oldRightSibling);
                buffer.setRightSibling(newRightSibling);

                value.setPointerValue(newRightSibling);
                value.setPointerPageType(rightSibling.getPageType());

                rightSibling.setDirtyStructure();
                buffer.setDirtyStructure();

                return true;

            } finally {
                if (rightSibling != null) {
                    _pool.release(rightSibling);
                }
            }
        }
    }

    private Buffer reclaimQuickBuffer(LevelCache lc) throws PersistitException {
        Buffer buffer = lc._buffer;
        if (buffer == null)
            return null;

        boolean available = buffer.claim(_exclusive, 0);
        if (available) {
            // Have to retest all this now that we've gotten a claim
            if (buffer.getPageAddress() == lc._page
                    && buffer.getVolume() == _volume
                    && _treeGeneration == _tree.getGeneration()
                    && buffer.getGeneration() == lc._bufferGeneration
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
     * The direction value must be one of:
     * <dl>
     * <dt>Key.GT:</dt>
     * <dd>Find the next key that is strictly greater than the supplied key. If
     * there is none, return false.</dd>
     * <dt>Key.GTEQ:</dt>
     * <dd>If the supplied key exists in the database, return that key;
     * otherwise find the next greater key and return it.</dd>
     * <dt>Key.EQ:</dt>
     * <dd>Return <tt>true</tt> iff the specified key exists in the database.
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
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <tt>Tree</tt> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean traverse(Direction direction, boolean deep)
            throws PersistitException {
        return traverse(direction, deep, Integer.MAX_VALUE);
    }

    /**
     * <p>
     * Performs generalized tree traversal. The direction value indicates
     * whether to traverse forward or backward in collation sequence and whether
     * the key being sought must be strictly greater than or less than the
     * supplied key.
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
     * <dd>Return <tt>true</tt> iff the specified key exists in the database.
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
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <tt>Tree</tt> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @param minBytes
     *            The minimum number of bytes to fetch. See {@link #fetch(int)}.
     *            If minBytes is less than or equal to 0 then this method does
     *            not update the Key and Value fields of the Exchange.
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * 
     * @throws PersistitException
     */
    public boolean traverse(final Direction direction, final boolean deep,
            final int minBytes) throws PersistitException {
        _persistit.checkClosed();
        final ResourceTracker resourceTracker = _persistit.getLockManager()
                .getMyResourceTracker();

        boolean doFetch = minBytes > 0;
        boolean doModify = minBytes >= 0;
        boolean result;
        final int lockedResourceCount = resourceTracker
                .getLockedResourceCount();

        resourceTracker.verifyLockedResourceCount(lockedResourceCount);
        boolean inTxn = _transaction.isActive() && !_ignoreTransactions;
        _transaction.assignTimestamp();
        Buffer buffer = null;

        if (doFetch) {
            _value.clear();
        }

        final boolean reverse = (direction == LT) || (direction == LTEQ);
        if (_key.getEncodedSize() == 0) {
            if (reverse) {
                _key.appendAfter();
            } else {
                _key.appendBefore();
            }
        }

        _key.testValidForTraverse();

        boolean edge = direction == EQ || direction == GTEQ
                || direction == LTEQ;
        boolean nudged = false;

        checkLevelCache();

        try {
            if (inTxn && edge && !_key.isSpecial()) {
                Boolean txnResult = _transaction.fetch(this, this.getValue(),
                        minBytes);
                //
                // A pending STORE transaction record overrides
                // the base record.
                //
                if (txnResult == Boolean.TRUE) {
                    return true;

                } else if (txnResult == Boolean.FALSE) {
                    //
                    // A pending DELETE transaction record overrides the
                    // base record.
                    //
                    if (direction == EQ) {
                        return false;
                    } else {
                        edge = false;
                    }
                }
            }

            //
            // Now we are committed to computing a new key value. Save the
            // original key value for comparison.
            //
            _key.copyTo(_spareKey1);
            int index = _key.getEncodedSize();

            if (index == 0) {
                if (reverse) {
                    _key.appendAfter();
                } else {
                    _key.appendBefore();
                }
                nudged = true;
            }

            int foundAt = 0;
            LevelCache lc;
            boolean fetchFromPendingTxn = false;

            for (;;) {

                lc = _levelCache[0];
                //
                // Optimal path - pick up the buffer and location left
                // by previous operation.
                //
                if (lc._keyGeneration == _key.getGeneration()) {
                    buffer = reclaimQuickBuffer(lc);
                    foundAt = lc._foundAt;
                }
                //
                // But if direction is leftward and the position is at the left
                // edge of the buffer, re-do with a key search - there is no
                // other way to find the left sibling page.
                //
                if (reverse && buffer != null
                        && foundAt <= buffer.getKeyBlockStart()) {
                    // Going left from first record in the page requires a
                    // key search.
                    buffer.release();
                    buffer = null;
                }
                //
                // If the operations above failed to get the key, then
                // look it up with search.
                //
                if (buffer == null) {
                    if (!edge && !nudged) {
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
                    }
                    foundAt = search(_key);
                    buffer = lc._buffer;
                }

                if (edge && (foundAt & Buffer.EXACT_MASK) != 0) {
                    break;
                } else {
                    edge = false;
                    foundAt = buffer.traverse(_key, direction, foundAt);
                    if (buffer.isAfterRightEdge(foundAt)) {
                        long rightSiblingPage = buffer.getRightSibling();

                        if (Debug.ENABLED) {
                            Debug.$assert(rightSiblingPage >= 0
                                    && rightSiblingPage <= Buffer.MAX_VALID_PAGE_ADDR);
                        }
                        if (rightSiblingPage > 0) {
                            Buffer rightSibling = _pool.get(_volume,
                                    rightSiblingPage, _exclusive, true);
                            if (inTxn) {
                                _transaction.touchedPage(this, buffer);
                            }
                            resourceTracker.setOffset();
                            _pool.release(buffer);
                            //
                            // Reset foundAtNext to point to the first key block
                            // of the right sibling page.
                            //
                            buffer = rightSibling;
                            checkPageType(buffer, Buffer.PAGE_TYPE_DATA);
                            foundAt = buffer.traverse(_key, direction,
                                    buffer.toKeyBlock(0));

                        }
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

                    if (!nudged
                            && !deep
                            && _key.compareKeyFragment(_spareKey1, 0,
                                    _spareKey1.getEncodedSize()) == 0) {
                        _key.setEncodedSize(_spareKey1.getEncodedSize());
                        lc._keyGeneration = -1;
                        buffer.release();
                        buffer = null;
                        continue;
                    }
                }

                if (inTxn) {
                    //
                    // Here we need to test whether pending updates in a
                    // transaction, if there is one, will modify the result.
                    // The candidate key is in _key. This code
                    // determines whether (a) that key has been deleted by
                    // the pending transaction, or (b) there's a nearer key
                    // that has been added by the pending transaction.
                    // This is split into two calls. The first looks for
                    // a pending remove operation that affects the result.
                    //
                    Boolean txnResult = _transaction.traverse(_tree,
                            _spareKey1, _key, direction, deep, minBytes);

                    if (txnResult != null) {
                        _transaction.touchedPage(this, buffer);
                        _pool.release(buffer);
                        buffer = null;

                        if (txnResult == Boolean.TRUE) {
                            // There's a pending new record that
                            // yields a closer key. The key
                            // was updated to reflect its content, and
                            // if the key is selected by criteria below
                            // then the pending value is fetched.
                            //
                            fetchFromPendingTxn = true;
                        } else {
                            //
                            // There's a pending remove operation that
                            // covers the candidate key. The key has been
                            // modified to the beginning or end, depending
                            // on direction, of the remove range, and then
                            // we must go back and traverse again.
                            //
                            if (direction == LTEQ && !_key.isSpecial()) {
                                _key.nudgeLeft();
                                nudged = true;
                            }
                            continue;
                        }
                    }
                }
                break;
            }

            boolean matches;

            if (reverse && _key.isLeftEdge() || !reverse && _key.isRightEdge()) {
                matches = false;
            } else {
                if (deep) {
                    matches = true;
                    index = _key.getEncodedSize();

                    if (doFetch) {
                        if (fetchFromPendingTxn) {
                            _transaction.fetchFromLastTraverse(this, minBytes);
                        } else {
                            buffer.fetch(foundAt, _value);
                            fetchFixupForLongRecords(_value, minBytes);
                        }
                    }
                } else {
                    int parentIndex = _spareKey1.previousElementIndex(index);
                    if (parentIndex < 0)
                        parentIndex = 0;

                    matches = (_spareKey1.compareKeyFragment(_key, 0,
                            parentIndex) == 0);

                    if (matches) {
                        index = _key.nextElementIndex(parentIndex);
                        if (index > 0) {
                            if (index == _key.getEncodedSize()) {
                                if (doFetch) {
                                    if (fetchFromPendingTxn) {
                                        _transaction.fetchFromLastTraverse(
                                                this, minBytes);
                                    } else {
                                        buffer.fetch(foundAt, _value);
                                        fetchFixupForLongRecords(_value,
                                                minBytes);
                                    }
                                }
                            } else {
                                //
                                // The physical traversal went to a child
                                // of the next sibling (i.e. a niece or
                                // nephew), so therefore there must not be
                                // a record associated with the key value
                                // we are going to return. This makes
                                // the Value for this Exchange undefined.
                                //
                                if (doFetch) {
                                    _value.clear();
                                }
                                foundAt &= ~Buffer.EXACT_MASK;
                            }
                        } else
                            matches = false;
                    }
                }
            }

            if (doModify) {
                if (matches) {
                    _key.setEncodedSize(index);
                    if (!fetchFromPendingTxn) {
                        lc.update(buffer, _key, foundAt);
                    }
                } else {
                    if (deep) {
                        _key.setEncodedSize(0);
                    } else {
                        _spareKey1.copyTo(_key);
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
                _spareKey1.copyTo(_key);
            }
            result = matches;

        } finally {
            if (buffer != null) {
                if (inTxn)
                    _transaction.touchedPage(this, buffer);
                _pool.release(buffer);
                buffer = null;
            }
            resourceTracker.verifyLockedResourceCount(lockedResourceCount);
        }
        _volume.bumpTraverseCounter();
        return result;
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
     * <dd>Return <tt>true</tt> iff the specified key exists in the database.
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
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * 
     * @throws PersistitException
     */
    public boolean traverse(final Direction direction,
            final KeyFilter keyFilter, int minBytes) throws PersistitException {
        if (keyFilter == null) {
            return traverse(direction, true, minBytes);
        }

        if (direction == EQ) {
            return keyFilter.selected(_key)
                    && traverse(direction, true, minBytes);
        }

        if (_key.getEncodedSize() == 0) {
            if (direction == GT || direction == GTEQ) {
                _key.appendBefore();
            } else {
                _key.appendAfter();
            }
        }

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
            if (!traverse(direction, true, minBytes)) {
                return false;
            }
            if (keyFilter.selected(_key)) {
                return true;
            }
        }
    }

    /**
     * Traverses to the next logical sibling key value. Equivalent to
     * <tt>traverse(Key.GT, false)</tt>.
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next() throws PersistitException {
        return traverse(GT, false);
    }

    /**
     * Traverses to the previous logical sibling key value. Equivalent to
     * <tt>traverse(Key.LT, false)</tt>.
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean previous() throws PersistitException {
        return traverse(LT, false);
    }

    /**
     * Traverses to the next key with control over depth. Equivalent to
     * <tt>traverse(Key.GT, deep)</tt>.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <tt>Tree</tt> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next(boolean deep) throws PersistitException {
        return traverse(GT, deep);
    }

    /**
     * Traverses to the previous key with control over depth. Equivalent to
     * <tt>traverse(Key.LT, deep)</tt>.
     * 
     * @param deep
     *            Determines whether the result should represent the next (or
     *            previous) physical key in the <tt>Tree</tt> or should be
     *            restricted to just the logical siblings of the current key.
     *            (See <a href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * 
     * @throws PersistitException
     */
    public boolean previous(boolean deep) throws PersistitException {
        return traverse(LT, deep);
    }

    /**
     * Traverses to the next key value within the subset of all keys defined by
     * the supplied KeyFilter. Whether logical children of the current key value
     * are included in the result is determined by the <tt>KeyFilter</tt>.
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean next(KeyFilter filter) throws PersistitException {
        return traverse(GT, filter, Integer.MAX_VALUE);
    }

    /**
     * Traverses to the previous key value within the subset of all keys defined
     * by the supplied KeyFilter. Whether logical children of the current key
     * value are included in the result is determined by the <tt>KeyFilter</tt>.
     * 
     * @return <tt>true</tt> if there is a key to traverse to, else null.
     * @throws PersistitException
     */
    public boolean previous(KeyFilter filter) throws PersistitException {
        return traverse(LT, filter, Integer.MAX_VALUE);
    }

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <tt>Key</tt> or <tt>Value</tt>. This method
     * is equivalent to {@link #next()} except that no state is changed.
     * 
     * @return <tt>true</tt> if the key has a successor
     * 
     * @throws PersistitException
     */
    public boolean hasNext() throws PersistitException {
        return traverse(GT, false, -1);
    }

    /**
     * Determines whether the current key has a successor within the subset of
     * all keys defined by a <tt>KeyFilter</tt>. This method does not change the
     * state of <tt>Key</tt> or <tt>Value</tt>.
     * 
     * @return <tt>true</tt> if the key has a successor
     * 
     * @throws PersistitException
     */
    public boolean hasNext(KeyFilter filter) throws PersistitException {
        if (filter == null)
            return hasNext();
        _key.copyTo(_spareKey2);
        boolean result = traverse(GT, filter, 0);
        _spareKey2.copyTo(_key);
        return result;
    }

    /**
     * Determines whether the current key has a logical sibling successor,
     * without changing the state of <tt>Key</tt> or <tt>Value</tt>. This method
     * is equivalent to {@link #next(boolean)} except that no state is changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<tt>true</tt>, or must be a restricted logical siblings (
     *            <tt>false</tt>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <tt>true</tt> if the key has a successor
     * 
     * @throws PersistitException
     */
    public boolean hasNext(boolean deep) throws PersistitException {
        return traverse(GT, deep, -1);
    }

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <tt>Key</tt> or <tt>Value</tt>. This method
     * is equivalent to {@link #previous()} except that no state is changed.
     * 
     * @return <tt>true</tt> if the key has a predecessor
     * @throws PersistitException
     */
    public boolean hasPrevious() throws PersistitException {
        return traverse(LT, false, -1);
    }

    /**
     * Determines whether the current key has a logical sibling predecessor,
     * without changing the state of <tt>Key</tt> or <tt>Value</tt>. This method
     * is equivalent to {@link #previous(boolean)} except that no state is
     * changed.
     * 
     * @param deep
     *            Determines whether the predecessor may be of any logical depth
     *            (<tt>true</tt>, or must be a restricted logical siblings (
     *            <tt>false</tt>) of the current key. (See <a
     *            href="Key.html#_keyChildren">Logical Key Children and
     *            Siblings</a>).
     * 
     * @return <tt>true</tt> if the key has a predecessor
     * 
     * @throws PersistitException
     */
    public boolean hasPrevious(boolean deep) throws PersistitException {
        return traverse(LT, deep, -1);
    }

    /**
     * Determines whether the current key has a predecessor within the subset of
     * all keys defined by a <tt>KeyFilter</tt>. This method does not change the
     * state of <tt>Key</tt> or <tt>Value</tt>.
     * 
     * @return <tt>true</tt> if the key has a successor
     * 
     * @throws PersistitException
     */
    public boolean hasPrevious(KeyFilter filter) throws PersistitException {
        if (filter == null)
            return hasPrevious();
        _key.copyTo(_spareKey2);
        boolean result = traverse(GT, filter, 0);
        _spareKey2.copyTo(_key);
        return result;
    }

    /**
     * Determines whether the current key has an associated value - that is,
     * whether a {@link #fetch} operation would return a defined value - without
     * actually changing the state of either the <tt>Key</tt> or the
     * <tt>Value</tt>.
     * 
     * @return <tt>true</tt> if the key has an associated value
     * 
     * @throws PersistitException
     */
    public boolean isValueDefined() throws PersistitException {
        return traverse(EQ, false, -1);
    }

    /**
     * Insert the current <tt>Key</tt> and <tt>Value</tt> pair into this
     * <tt>Exchange</tt>'s <tt>Tree</tt>. If there already is a value associated
     * with the current key, then replace it.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange store() throws PersistitException {
        return store(_key, _value);
    }

    /**
     * Fetches the value associated with the <tt>Key</tt>, then inserts or
     * updates the value. Effectively this swaps the content of <tt>Value</tt>
     * with the database record associated with the current <tt>key</tt>. It is
     * equivalent to the code: <blockquote>
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
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetchAndStore() throws PersistitException {
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        _persistit.checkClosed();
        _persistit.checkSuspended();
        _key.testValidForStoreAndFetch(_volume.getPageSize());
        int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();
        _transaction.assignTimestamp();
        storeInternal(_key, _value, 0, true, false);
        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);
        _spareValue.copyTo(_value);
        return this;
    }

    /**
     * Fetches the value associated with the current <tt>Key</tt> into the
     * <tt>Exchange</tt>'s <tt>Value</tt>. The <tt>Value</tt> object reflects
     * the fetched state. If there is no value associated with the key then
     * {@link Value#isDefined} is false. Otherwise the value may be retrieved
     * using {@link Value#get} and other methods of <tt>Value</tt>.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch() throws PersistitException {
        return fetch(_value, Integer.MAX_VALUE);
    }

    /**
     * <p>
     * Fetches or partially fetches the value associated with the current
     * <tt>Key</tt> into the <tt>Exchange</tt>'s <tt>Value</tt>. The
     * <tt>Value</tt> object reflects the fetched state. If there is no value
     * associated with the key then {@link Value#isDefined} is false. Otherwise
     * the value may be retrieved using {@link Value#get} and other methods of
     * <tt>Value</tt>.
     * </p>
     * <p>
     * This method sets a lower bound on the number of bytes to be fetched. In
     * particular, it may be useful to retrieve only a small fraction of a very
     * long record such as the serialization of an image. Upon successful
     * completion of this method, at least <tt>minimumBytes</tt> of the
     * <tt>Value</tt> object will accurately reflect the value stored in the
     * database. This might allow an application to determine whether to
     * retrieve the rest of the value.
     * </p>
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(int minimumBytes) throws PersistitException {
        return fetch(_value, minimumBytes);
    }

    /**
     * Fetches the value associated with the current <tt>Key</tt> into the
     * supplied <tt>Value</tt> object (instead of the <tt>Exchange</tt>'s
     * assigned <tt>Value</tt>). The <tt>Value</tt> object reflects the fetched
     * state. If there is no value associated with the key then
     * {@link Value#isDefined} is false. Otherwise the value may be retrieved
     * using {@link Value#get} and other methods of <tt>Value</tt>.
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(Value value) throws PersistitException {
        return fetch(value, Integer.MAX_VALUE);
    }

    /**
     * <p>
     * Fetches or partially fetches the value associated with the current
     * <tt>Key</tt> into the supplied <tt>Value</tt> object (instead of the
     * <tt>Exchange</tt>'s assigned <tt>Value</tt>). The <tt>Value</tt> object
     * reflects the fetched state. If there is no value associated with the key
     * then {@link Value#isDefined} is false. Otherwise the value may be
     * retrieved using {@link Value#get} and other methods of <tt>Value</tt>.
     * </p>
     * <p>
     * This method sets a lower bound on the number of bytes to be fetched. In
     * particular, it may be useful to retrieve only a small fraction of a very
     * long record such as the serialization of an image. Upon successful
     * completion of this method, at least <tt>minimumBytes</tt> of the
     * <tt>Value</tt> object will accurately reflect the value stored in the
     * database. This might allow an application to determine whether to
     * retrieve the rest of the value.
     * </p>
     * 
     * @return This <tt>Exchange</tt> to permit method call chaining
     * @throws PersistitException
     */
    public Exchange fetch(Value value, int minimumBytes)
            throws PersistitException {
        _persistit.checkClosed();
        _key.testValidForStoreAndFetch(_volume.getPageSize());
        if (minimumBytes < 0)
            minimumBytes = 0;
        final int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();

        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);
        boolean inTxn = _transaction.isActive() && !_ignoreTransactions;
        _transaction.assignTimestamp();

        if (inTxn && _transaction.fetch(this, value, minimumBytes) != null) {
            return this;
        }

        Buffer buffer = null;
        try {
            int foundAt = search(_key);
            LevelCache lc = _levelCache[0];
            buffer = lc._buffer;
            buffer.fetch(foundAt, value);
            fetchFixupForLongRecords(value, minimumBytes);
            _volume.bumpFetchCounter();
            return this;
        } finally {
            if (buffer != null) {
                if (inTxn) {
                    _transaction.touchedPage(this, buffer);
                }
                _pool.release(buffer);
            }
            _persistit.getLockManager().verifyLockedResourceCount(
                    lockedResourceCount);
        }
    }

    void fetchFixupForLongRecords(Value value, int minimumBytes)
            throws PersistitException {
        if (value.isDefined()
                && (value.getEncodedBytes()[0] & 0xFF) == Buffer.LONGREC_TYPE) {
            //
            // This will potential require numerous pages: the buffer
            // claim is held for the duration to prevent a non-atomic
            // update.
            //
            fetchLongRecord(value, minimumBytes);
        }
    }

    /**
     * Atomically increment a stored integer value and return the result. If the
     * key currently has no associated value, then initialize the value to zero.
     * Otherwise add 1 to whatever value is stored, store the result and return
     * it.
     * 
     * @return The incremented value.
     * @throws PersistitException
     */
    public long incrementValue() throws PersistitException {
        return incrementValue(1);
    }

    /**
     * Atomically increment a stored integer value and return the result. If the
     * key currently has no associated value, then initialize the value to zero.
     * Otherwise add <tt>by</tt> to whatever value is stored, store the result
     * and return it.
     * 
     * @param by
     *            The amount by which to increment the stored value
     * @return The incremented value.
     * @throws PersistitException
     */
    public long incrementValue(long by) throws PersistitException {
        return incrementValue(by, 0);
    }

    /**
     * Atomically increment a stored integer value and return the result. If the
     * key currently has no associated value, then initialize the value to
     * <tt>from</tt>. Otherwise add <tt>by</tt> to whatever value is stored,
     * store the result and return it.
     * 
     * @param by
     *            The amount by which to increment the stored value
     * @return The incremented value.
     * @throws PersistitException
     */
    public long incrementValue(long by, long from) throws PersistitException {
        _persistit.checkClosed();
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        _value.put(from);
        _value.armAtomicIncrement(by);
        store();
        return _value.getLong();
    }

    /**
     * Return true if there is at least one key stored in this <tt>Exchange</tt>
     * 's <tt>Tree</tt> that is a logical child of the current <tt>Key</tt>. A
     * logical child is a key that can be formed by appending a value to the
     * parent. (See <a href="Key.html#_keyChildren">Logical Key Children and
     * Siblings</a>).
     * 
     * @return <tt>true</tt> if the current <tt>Key</tt> has logical children
     * @throws PersistitException
     */
    public boolean hasChildren() throws PersistitException {
        _key.copyTo(_spareKey1);
        final int size = _key.getEncodedSize();
        boolean result = traverse(GT, true, 0);
        if (result && _key.getEncodedSize() < size
                || _spareKey1.compareKeyFragment(_key, 0, size) != 0) {
            result = false;
        }
        _spareKey1.copyTo(_key);
        return result;
    }

    /**
     * Remove a single key/value pair from this <tt>Exchange</tt>'s
     * <tt>Tree</tt> and return the removed value in the <tt>Exchange</tt>'s
     * <tt>Value</tt>. This method atomically fetches the former value then
     * deletes it. If there was no value formerly associated with the key then
     * <tt>Value</tt> becomes undefined - that is, the value of
     * {@link Value#isDefined} becomes <tt>false</tt>.
     * 
     * @return <tt>true</tt> if there was a key/value pair to remove
     * @throws PersistitException
     */
    public boolean fetchAndRemove() throws PersistitException {
        _persistit.checkClosed();
        _persistit.checkSuspended();
        _spareValue.clear();
        boolean result = remove(EQ, true);
        _spareValue.copyTo(_value);
        if (Debug.ENABLED) {
            Debug.$assert(_value.isDefined() == result);
        }
        return result;
    }

    /**
     * Remove the entire <tt>Tree</tt> that this <tt>Exchange</tt> is based on.
     * Subsequent to successful completion of this method, the <tt>Exchange</tt>
     * will no longer be usable. Attempts to perform operations on it will
     * result in an <tt>IllegalStateException</tt>.
     * 
     * @throws PersistitException
     */
    public void removeTree() throws PersistitException {
        _persistit.checkClosed();
        _persistit.checkSuspended();
        final int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();
        boolean inTxn = _transaction.isActive() && !_ignoreTransactions;
        _transaction.assignTimestamp();
        clear();
        _value.clear();
        if (inTxn) {
            _transaction.removeTree(this);
        } else {
            _volume.removeTree(_tree);
        }
        initCache();
        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);
    }

    /**
     * Remove a single key/value pair from the this <tt>Exchange</tt>'s
     * <tt>Tree</tt>.
     * 
     * @return <tt>true</tt> if there was a key/value pair to remove
     * @throws PersistitException
     */
    public boolean remove() throws PersistitException {
        return remove(EQ, false);
    }

    /**
     * Remove all keys in this <tt>Exchange</tt>'s <tt>Tree</tt>.
     * 
     * @return <tt>true</tt> if there were key/value pairs removed
     * @throws PersistitException
     */
    public boolean removeAll() throws PersistitException {
        clear();
        return remove(GTEQ);
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
     * @return <tt>true</tt> if one or more records were actually removed, else
     *         </i>false</i>.
     * @throws PersistitException
     */
    public boolean remove(Direction direction) throws PersistitException {
        return remove(direction, false);
    }

    private boolean remove(Direction selection, boolean fetchFirst)
            throws PersistitException {
        _persistit.checkClosed();

        if (selection != EQ && selection != GTEQ && selection != GT) {
            throw new IllegalArgumentException("Invalid mode " + selection);
        }

        int keySize = _key.getEncodedSize();

        _key.copyTo(_spareKey1);
        _key.copyTo(_spareKey2);

        // Special case for empty key
        if (keySize == 0) {
            if (selection == EQ) {
                return false;
            }
            _spareKey1.append(BEFORE);
            _spareKey2.append(AFTER);
        } else {
            if (selection == EQ || selection == GTEQ) {
                _spareKey1.nudgeLeft();
            } else {
                _spareKey1.nudgeDeeper();
            }

            if (selection == GTEQ || selection == GT) {
                _spareKey2.nudgeRight();
            }
        }

        return removeKeyRangeInternal(_spareKey1, _spareKey2, fetchFirst);
    }

    /**
     * Removes record(s) for a range of keys. The keys to be removed are all
     * those in the Tree that are Greater Than or Equal to key1 and less than
     * key2.
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
     * @return <tt>true</tt> if one or more records were actually removed, else
     *         </i>false</i>.
     * 
     * @throws PersistitException
     * @throws IllegalArgymentException
     *             if key1 is equal to or greater than key2
     */
    public boolean removeKeyRange(Key key1, Key key2) throws PersistitException {
        key1.copyTo(_spareKey1);
        key2.copyTo(_spareKey2);

        // Special case for empty key
        if (key1.getEncodedSize() == 0) {
            _spareKey1.append(BEFORE);
        } else
            _spareKey1.nudgeLeft();

        if (key2.getEncodedSize() == 0) {
            _spareKey2.append(AFTER);
        } else {
            _spareKey2.nudgeLeft();
        }

        if (_spareKey1.compareTo(_spareKey2) >= 0) {
            throw new IllegalArgumentException(
                    "Second key must be larger than first");
        }
        return removeKeyRangeInternal(_spareKey1, _spareKey2, false);
    }

    /**
     * Removes all records with keys falling between key1 and key2, exclusive.
     * Validity checks and Key value adjustments have been done by wrapper
     * methods - this method does the work.
     * 
     * @param key1
     *            Key that is less than the leftmost to be removed
     * @param key2
     *            Key that is greater than the rightmost to be removed
     * @return <tt>true</tt> if any records were removed.
     * @throws PersistitException
     */
    boolean removeKeyRangeInternal(Key key1, Key key2, boolean fetchFirst)
            throws PersistitException {
        if (_volume.isReadOnly()) {
            throw new ReadOnlyVolumeException(_volume.toString());
        }
        _persistit.checkClosed();
        _persistit.checkSuspended();

        if (Debug.ENABLED) {
            Debug.suspend();
        }
        boolean inTxn = _transaction.isActive() && !_ignoreTransactions;
        _transaction.assignTimestamp();
        boolean treeClaimAcquired = false;
        boolean treeWriterClaimRequired = false;
        boolean result = false;
        _exclusive = true;

        boolean deallocationRequired = true; // assume until proven false
        boolean deferredReindexRequired = false;
        boolean tryQuickDelete = true;
        int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();

        // long journalId = -1;
        // if (!inTxn) {
        // journalId = journal().beginRemove(_tree, key1, key2, fetchFirst);
        // }

        try {
            //
            // This is the main retry loop. If any attempt to reserve a page
            // in the inner logic fails, we will iterate across all this logic
            // again until the expiration time.
            //
            for (;;) {
                _persistit.getLockManager().verifyLockedResourceCount(
                        lockedResourceCount);

                checkLevelCache();
                int depth = _cacheDepth; // The depth to which we have
                // populated the level cache.

                try {
                    if (inTxn) {
                        result = _transaction.remove(this, key1, key2,
                                fetchFirst);
                        break;
                    }
                    //
                    // First try for a quick delete from a single data page.
                    //
                    if (tryQuickDelete) {
                        Buffer buffer = null;
                        try {
                            int foundAt1 = search(key1) & Buffer.P_MASK;
                            buffer = _levelCache[0]._buffer;

                            if (!buffer.isBeforeLeftEdge(foundAt1)
                                    && !buffer.isAfterRightEdge(foundAt1)) {
                                int foundAt2 = buffer.findKey(key2);
                                if (!buffer.isBeforeLeftEdge(foundAt2)
                                        && !buffer.isAfterRightEdge(foundAt2)) {
                                    if ((foundAt2 & Buffer.EXACT_MASK) != 0) {
                                        foundAt2 = buffer
                                                .nextKeyBlock(foundAt2);
                                    }
                                    foundAt2 &= Buffer.P_MASK;

                                    if (Debug.ENABLED) {
                                        Debug.$assert(foundAt2 >= foundAt1);
                                    }
                                    if (fetchFirst) {
                                        removeFetchFirst(buffer, foundAt1,
                                                buffer, foundAt2);
                                    }
                                    final boolean anyLongRecords = _volume
                                            .harvestLongRecords(buffer,
                                                    foundAt1, foundAt2);

                                    boolean removed = buffer.removeKeys(
                                            foundAt1, foundAt2, _spareKey1);
                                    if (removed) {
                                        _tree.bumpChangeCount();
                                    }

                                    buffer.setDirty();
                                    if (anyLongRecords) {
                                        buffer.setDirtyStructure();
                                    }
                                    result = foundAt2 > foundAt1;
                                    break;
                                }
                            }
                            // If we didn't meet the criteria for quick delete,
                            // then don't try it again on a RetryException.
                            tryQuickDelete = false;
                        } finally {
                            if (buffer != null)
                                _pool.release(buffer);
                            buffer = null;
                        }
                    }

                    if (!treeClaimAcquired) {
                        if (!_tree.claim(treeWriterClaimRequired)) {
                            if (Debug.ENABLED) {
                                Debug.debug1(true);
                            }
                            throw new InUseException("Thread "
                                    + Thread.currentThread().getName()
                                    + " failed to get writer claim on " + _tree);
                        }
                        treeClaimAcquired = true;
                        lockedResourceCount++;
                        _tree.bumpGeneration();
                        // Because we actually haven't changed anything yet.
                        _treeGeneration++;
                    }
                    //
                    // Need to redo this check now that we have a
                    // claim on the Tree.
                    //
                    checkLevelCache();

                    long pageAddr1 = _rootPage;
                    long pageAddr2 = pageAddr1;

                    for (int level = _cacheDepth; --level >= 0;) {
                        LevelCache lc = _levelCache[level];
                        lc.initRemoveFields();
                        depth = level;

                        int foundAt1 = searchLevel(key1, pageAddr1, level)
                                & Buffer.P_MASK;

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
                            if ((foundAt2 & Buffer.EXACT_MASK) != 0) {
                                foundAt2 = buffer.nextKeyBlock(foundAt2);
                            }
                            foundAt2 &= Buffer.P_MASK;

                            if (!buffer.isAfterRightEdge(foundAt2)) {
                                lc._rightBuffer = buffer;
                                lc._rightFoundAt = foundAt2;
                            } else {
                                //
                                // Since we are spanning pages we need an
                                // exclusive claim on the tree to prevent
                                // an insertion from propagating upward through
                                // the deletion range.
                                //
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
                                if (!_tree.upgradeClaim()) {
                                    throw RetryException.SINGLE;
                                }
                            }

                            foundAt2 = searchLevel(key2, pageAddr2, level);
                            if ((foundAt2 & Buffer.EXACT_MASK) != 0) {
                                foundAt2 = buffer.nextKeyBlock(foundAt2);
                                if (Debug.ENABLED) {
                                    Debug.$assert(foundAt2 != -1);
                                }
                            }
                            foundAt2 &= Buffer.P_MASK;

                            if (Debug.ENABLED) {
                                Debug.$assert(foundAt2 != Buffer.INITIAL_KEY_BLOCK_START_VALUE);
                            }
                            buffer = lc._buffer;
                            lc._flags |= RIGHT_CLAIMED;
                            lc._rightBuffer = buffer;
                            lc._rightFoundAt = foundAt2;
                            pageAddr2 = buffer.getPageAddress();
                        }

                        if (lc._leftBuffer.isIndexPage()) {
                            if (Debug.ENABLED) {
                                Debug.$assert(lc._rightBuffer.isIndexPage()
                                        && depth > 0);
                            }
                            int p1 = lc._leftBuffer.previousKeyBlock(foundAt1);
                            int p2 = lc._rightBuffer.previousKeyBlock(foundAt2);

                            if (Debug.ENABLED) {
                                Debug.$assert(p1 != -1 && p2 != -1);
                            }
                            pageAddr1 = lc._leftBuffer.getPointer(p1);
                            pageAddr2 = lc._rightBuffer.getPointer(p2);
                        } else {
                            if (Debug.ENABLED) {
                                Debug.$assert(depth == 0);
                            }
                            break;
                        }
                    }

                    if (fetchFirst) {
                        LevelCache lc = _levelCache[0];
                        removeFetchFirst(lc._leftBuffer, lc._leftFoundAt,
                                lc._rightBuffer, lc._rightFoundAt);
                    }
                    //
                    // We have fully delineated the subtree that
                    // needs to be removed. Now walk down the tree,
                    // stitching together the pages where necessary.
                    //
                    for (int level = _cacheDepth; --level >= 0;) {
                        LevelCache lc = _levelCache[level];
                        Buffer buffer1 = lc._leftBuffer;
                        Buffer buffer2 = lc._rightBuffer;
                        int foundAt1 = lc._leftFoundAt;
                        int foundAt2 = lc._rightFoundAt;
                        boolean needsReindex = false;

                        if (buffer1 != buffer2) {
                            //
                            // Deletion spans multiple pages at this level.
                            // We will need to join or rebalance the pages.
                            //
                            long leftGarbagePage = buffer1.getRightSibling();
                            _key.copyTo(_spareKey1);

                            // Before we remove the records in this range, we
                            // need to recover any LONG_RECORD pointers that
                            // are associated with keys in this range.
                            _volume.harvestLongRecords(buffer1, foundAt1,
                                    Integer.MAX_VALUE);

                            _volume.harvestLongRecords(buffer2, 0, foundAt2);

                            boolean rebalanced = buffer1.join(buffer2,
                                    foundAt1, foundAt2, _spareKey1, _spareKey2,
                                    _joinPolicy);
                            if (buffer1.isDataPage()) {
                                _tree.bumpChangeCount();
                            }
                            buffer1.setDirtyStructure();
                            buffer2.setDirtyStructure();

                            long rightGarbagePage = buffer1.getRightSibling();

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
                                    LevelCache parentLc = _levelCache[level + 1];
                                    Buffer buffer = parentLc._leftBuffer;

                                    if (Debug.ENABLED) {
                                        Debug.$assert(buffer != null);
                                    }
                                    if (parentLc._rightBuffer == buffer) {
                                        int foundAt = buffer
                                                .findKey(_spareKey1);
                                        if (Debug.ENABLED) {
                                            Debug.$assert((foundAt & Buffer.EXACT_MASK) == 0);
                                        }
                                        // Try it the simple way
                                        _value.setPointerValue(buffer2
                                                .getPageAddress());
                                        _value.setPointerPageType(buffer2
                                                .getPageType());
                                        int fit = buffer.putValue(_spareKey1,
                                                _value, foundAt, false);

                                        // If it worked then we're done.
                                        if (fit != -1) {
                                            needsReindex = false;
                                            buffer.setDirtyStructure();
                                        }
                                    }
                                }
                                if (needsReindex) {
                                    lc._deferredReindexPage = buffer2
                                            .getPageAddress();
                                    lc._deferredReindexChangeCount = buffer2
                                            .getGeneration();
                                    deferredReindexRequired = true;
                                    needsReindex = false;
                                }
                            }

                            result = true;
                        } else if (foundAt1 != foundAt2) {
                            if (Debug.ENABLED) {
                                Debug.$assert(foundAt2 >= foundAt1);
                            }
                            _key.copyTo(_spareKey1);
                            //
                            // Before we remove these records, we need to
                            // recover any LONG_RECORD pointers that may be
                            // associated with keys in this range.
                            //
                            final boolean anyLongRecords = _volume
                                    .harvestLongRecords(buffer1, foundAt1,
                                            foundAt2);

                            result |= buffer1.removeKeys(foundAt1, foundAt2,
                                    _spareKey1);

                            if (buffer1.isDataPage() && result) {
                                _tree.bumpChangeCount();
                            }
                            buffer1.setDirty();
                            if (anyLongRecords) {
                                buffer1.setDirtyStructure();
                            }
                        }

                        if (level < _cacheDepth - 1) {
                            removeKeyRangeReleaseLevel(level + 1);
                        }
                    }
                    break;
                } catch (RetryException re) {
                    // handled below
                } finally {
                    //
                    // Release all buffers.
                    //
                    for (int level = _cacheDepth; --level >= depth;) {
                        removeKeyRangeReleaseLevel(level);
                    }

                    if (treeClaimAcquired) {
                        _tree.release();
                        treeClaimAcquired = false;
                        lockedResourceCount--;
                    }
                }
                if (treeWriterClaimRequired) {
                    if (!_tree.claim(true)) {
                        if (Debug.ENABLED) {
                            Debug.debug1(true);
                        }
                        throw new InUseException("Thread "
                                + Thread.currentThread().getName()
                                + " failed to get reader claim on " + _tree);
                    }
                    treeClaimAcquired = true;
                    lockedResourceCount++;
                }
            }
            while (deallocationRequired) {
                long left = -1;
                long right = -1;
                for (int level = _cacheDepth; --level >= 0;) {
                    LevelCache lc = _levelCache[level];
                    left = lc._deallocLeftPage;
                    right = lc._deallocRightPage;
                    if (left != 0) {
                        _volume.deallocateGarbageChain(left, right);
                        lc._deallocLeftPage = 0;
                        lc._deallocRightPage = 0;
                    }
                }
                _volume.commitAllDeferredDeallocations();
                // If we successfully finish the loop then we're done
                deallocationRequired = false;
                break;
            }
            while (deferredReindexRequired) {
                Buffer buffer = null;
                try {
                    for (int level = _cacheDepth; --level >= 0;) {
                        LevelCache lc = _levelCache[level];
                        if (lc._deferredReindexPage != 0) {
                            if (!treeClaimAcquired) {
                                if (!_tree.claim(treeWriterClaimRequired)) {
                                    if (Debug.ENABLED) {
                                        Debug.debug1(true);
                                    }
                                    throw new InUseException("Thread "
                                            + Thread.currentThread().getName()
                                            + " failed to get writer claim on "
                                            + _tree);
                                }
                                treeClaimAcquired = true;
                                lockedResourceCount++;
                            }

                            long deferredPage = lc._deferredReindexPage;
                            buffer = _pool.get(_volume, deferredPage, false,
                                    true);
                            if (buffer.getGeneration() == lc._deferredReindexChangeCount) {
                                checkPageType(buffer, level
                                        + Buffer.PAGE_TYPE_DATA);
                                buffer.nextKey(_spareKey2, buffer.toKeyBlock(0));
                                _value.setPointerValue(buffer.getPageAddress());
                                _value.setPointerPageType(buffer.getPageType());
                                storeInternal(_spareKey2, _value, level + 1,
                                        false, true);
                            } else {
                                if (_persistit.getLogBase().isLoggable(
                                        LogBase.LOG_UNINDEXED_PAGE)) {
                                    _persistit.getLogBase().log(
                                            LogBase.LOG_UNINDEXED_PAGE,
                                            deferredPage, 0, 0, 0, 0,
                                            _tree.getName(), _volume.getPath());
                                }
                            }
                            lc._deferredReindexPage = 0;
                            _pool.release(buffer);
                            buffer = null;
                        }
                    }
                    deferredReindexRequired = false;
                } catch (RetryException re) {
                    if (buffer != null) {
                        _pool.release(buffer);
                        buffer = null;
                    }
                    waitForTreeExclusive();
                } finally {
                    if (buffer != null) {
                        _pool.release(buffer);
                        buffer = null;
                    }
                }
            }
        } finally {
            if (treeClaimAcquired) {
                _tree.bumpGeneration();
                _tree.release();
                lockedResourceCount--;
                treeClaimAcquired = false;
            }
            _exclusive = false;
            _persistit.getLockManager().verifyLockedResourceCount(
                    lockedResourceCount);
        }
        // if (journalId != -1)
        // journal().completed(journalId);
        _volume.bumpRemoveCounter();
        if (fetchFirst)
            _volume.bumpFetchCounter();
        return result;
    }

    private void removeKeyRangeReleaseLevel(final int level) {

        int offset = 0;
        for (int lvl = 0; lvl < level; lvl++) {
            final LevelCache lc = _levelCache[lvl];
            Buffer buffer1 = lc._leftBuffer;
            Buffer buffer2 = lc._rightBuffer;
            if (buffer2 != null && (lc._flags & RIGHT_CLAIMED) != 0) {
                offset++;
            }
            if (buffer1 != null && (lc._flags & LEFT_CLAIMED) != 0) {
                offset++;
            }
        }

        final LevelCache lc = _levelCache[level];
        Buffer buffer1 = lc._leftBuffer;
        Buffer buffer2 = lc._rightBuffer;

        if (buffer2 != null && (lc._flags & RIGHT_CLAIMED) != 0) {
            _persistit.getLockManager().setOffset(offset);
            _pool.release(buffer2);
        }
        if (buffer1 != null && (lc._flags & LEFT_CLAIMED) != 0) {
            _persistit.getLockManager().setOffset(offset);
            _pool.release(buffer1);
        }

        lc._leftBuffer = null;
        lc._rightBuffer = null;
        lc._flags = 0;
    }

    private void removeFetchFirst(Buffer buffer1, int foundAt1, Buffer buffer2,
            int foundAt2) throws PersistitException {
        if (buffer1 == buffer2) {
            if (buffer1.nextKeyBlock(foundAt1) == foundAt2) {
                buffer1.fetch(foundAt1 | Buffer.EXACT_MASK, _spareValue);
            }
        } else {
            if (buffer1.getRightSibling() == buffer2.getPageAddress()
                    && buffer1.nextKeyBlock(foundAt1) == -1) {
                foundAt1 = buffer2.toKeyBlock(0);
                if (buffer2.nextKeyBlock(foundAt1) == foundAt2) {
                    buffer2.fetch(foundAt1 | Buffer.EXACT_MASK, _spareValue);
                }
            }
        }
        if (_spareValue.isDefined()) {
            fetchFixupForLongRecords(_spareValue, Integer.MAX_VALUE);
        }
    }

    void removeAntiValue(AntiValue av) throws PersistitException {
        _key.copyTo(_spareKey1);
        _key.copyTo(_spareKey2);
        byte[] bytes = av.getBytes();
        int elisionCount = av.getElisionCount();
        System.arraycopy(bytes, 0, _spareKey2.getEncodedBytes(), elisionCount,
                bytes.length);
        _spareKey2.setEncodedSize(elisionCount + bytes.length);
        removeKeyRangeInternal(_spareKey1, _spareKey2, false);
    }

    /**
     * Decodes the LONG_RECORD pointer that has previously been fetched into the
     * Value. This will replace the byte array in that value with the actual
     * long value. Note that this is all done with a reader claim being held on
     * the data page containing the LONG_RECORD reference.
     * 
     * @param value
     * @throws PersistitException
     */
    private void fetchLongRecord(Value value, int minimumBytesFetched)
            throws PersistitException {

        Buffer buffer = null;
        boolean inTxn = _transaction.isActive() && !_ignoreTransactions;
        _transaction.assignTimestamp();

        try {
            byte[] rawBytes = value.getEncodedBytes();
            int rawSize = value.getEncodedSize();
            if (rawSize != Buffer.LONGREC_SIZE) {
                if (Debug.ENABLED) {
                    Debug.debug1(true);
                }
                throw new CorruptVolumeException(
                        "Invalid LONG_RECORD value size=" + rawSize
                                + " but should be " + Buffer.LONGREC_SIZE);
            }
            if ((rawBytes[0] & 0xFF) != Buffer.LONGREC_TYPE) {
                if (Debug.ENABLED) {
                    Debug.debug1(true);
                }
                throw new CorruptVolumeException(
                        "Invalid LONG_RECORD value type="
                                + (rawBytes[0] & 0xFF) + " but should be "
                                + Buffer.LONGREC_TYPE);
            }
            int longSize = Buffer.decodeLongRecordDescriptorSize(rawBytes, 0);
            long startAtPage = Buffer.decodeLongRecordDescriptorPointer(
                    rawBytes, 0);

            int remainingSize = Math.min(longSize, minimumBytesFetched);

            value.ensureFit(remainingSize);
            value.setEncodedSize(remainingSize);

            int offset = 0;
            //
            // This is a workaround for an egregious bug in the AIX JRE 1.4.0
            // and JRE 1.4.2 System.arraycopy implementation. Without this, the
            // arraycopy method corrupts the array.
            //
            Util.arraycopy(rawBytes, Buffer.LONGREC_PREFIX_OFFSET,
                    value.getEncodedBytes(), offset, Buffer.LONGREC_PREFIX_SIZE);

            offset += Buffer.LONGREC_PREFIX_SIZE;
            remainingSize -= Buffer.LONGREC_PREFIX_SIZE;
            long page = startAtPage;

            for (int count = 0; page != 0 && offset < minimumBytesFetched; count++) {
                if (remainingSize <= 0) {
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }
                    throw new CorruptVolumeException(
                            "Invalid LONG_RECORD remaining size="
                                    + remainingSize + " of " + rawSize
                                    + " in page " + page);
                }
                buffer = _pool.get(_volume, page, false, true);
                if (buffer.getPageType() != Buffer.PAGE_TYPE_LONG_RECORD) {
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }
                    throw new CorruptVolumeException(
                            "LONG_RECORD chain is invalid at page " + page
                                    + " - invalid page type: " + buffer);
                }
                int segmentSize = buffer.getBufferSize() - Buffer.HEADER_SIZE;
                if (segmentSize > remainingSize)
                    segmentSize = remainingSize;

                System.arraycopy(buffer.getBytes(), Buffer.HEADER_SIZE,
                        value.getEncodedBytes(), offset, segmentSize);

                offset += segmentSize;
                remainingSize -= segmentSize;
                // previousPage = page;
                page = buffer.getRightSibling();
                if (inTxn) {
                    _transaction.touchedPage(this, buffer);
                }
                _pool.release(buffer);
                buffer = null;

                if (count > MAX_LONG_RECORD_CHAIN) {
                    if (count > Exchange.MAX_LONG_RECORD_CHAIN) {
                        throw new CorruptVolumeException(
                                "LONG_RECORD chain starting at " + startAtPage
                                        + " is too long");
                    }

                }
            }
            value.setLongSize(rawSize);
            value.setEncodedSize(offset);
        } finally {
            if (buffer != null)
                _pool.release(buffer);
        }
    }

    /**
     * Maximum record size that will be attempted to be stored within a single
     * generation. If the record is longer than this, Persistit uses the
     * storeOverlengthRecord method.
     * 
     * @return
     */
    int maxUnitRecordSize() {
        int max = _pool.getBufferCount() * _pool.getBufferSize();
        return max / 2;
    }

    /**
     * <p>
     * Stores the raw bytes of a long record in Value into a LONG_RECORD chain,
     * then replaces the content of the raw bytes of the Value with a
     * LONG_RECORD descriptor.
     * </p>
     * <p>
     * If a non-zero page address is supplied, this is the address of the long
     * record chain that was previously associated with this record. In this
     * case, storeLongRecord will attempt to reuse those pages rather than
     * allocating new pages to hold the long record.
     * </p>
     * <p>
     * Called with a writer claim on the data page that will contain the
     * LONG_RECORD value, and that page is reserved. This method will claim and
     * reserve an indefinite number of additional pages. Upon completion, All of
     * those claims will be released, and if no change was made they will be
     * unreserved. Throws a RetryException if it was not possible to reserve all
     * the necessary pages.
     * </p>
     * 
     * @param value
     *            The Value object containing the long record. The Value must be
     *            in "long record mode".
     * 
     * @param page
     *            Address of first page of LONG_RECORD chain being overwritten,
     *            or 0 if there is none.
     * 
     * @returns the page address of the first page of the LONG_RECORD chain
     * @throws PersistitException
     */
    long storeLongRecord(Value value, long oldChain, long newChain)
            throws PersistitException {
        // Calculate how many LONG_RECORD pages we will need.
        //
        boolean completed = false;
        value.changeLongRecordMode(true);

        long page = oldChain;
        int longSize = value.getLongSize();
        int remainingSize = longSize;
        byte[] longBytes = value.getLongBytes();
        byte[] rawBytes = value.getEncodedBytes();
        int loosePageIndex = -1;
        int index = 0;
        long looseChain = 0;
        Buffer[] bufferArray = null;

        if (Debug.ENABLED) {
            Debug.$assert(value.isLongRecordMode());
            Debug.$assert(rawBytes.length == Buffer.LONGREC_SIZE);
            Debug.$assert(longSize < maxUnitRecordSize());
        }

        System.arraycopy(longBytes, 0, rawBytes, Buffer.LONGREC_PREFIX_OFFSET,
                Buffer.LONGREC_PREFIX_SIZE);

        remainingSize -= Buffer.LONGREC_PREFIX_SIZE;
        int maxSegmentSize = _pool.getBufferSize() - Buffer.HEADER_SIZE;
        int count = (remainingSize + (maxSegmentSize - 1)) / maxSegmentSize;

        try {
            bufferArray = new Buffer[count];
            for (; index < count && page != 0; index++) {
                Buffer buffer = _pool.get(_volume, page, true, true);
                if (Debug.ENABLED) {
                    Debug.$assert(buffer.isLongRecordPage());
                }
                bufferArray[index] = buffer;
                page = buffer.getRightSibling();

                // verify that there's no cycle
                for (int i = 0; i < index; i++) {
                    if (bufferArray[i].getPageAddress() == page) {
                        if (Debug.ENABLED) {
                            Debug.debug1(true);
                        }

                        throw new CorruptVolumeException(
                                "LONG_RECORD chain cycle at " + bufferArray[0]);
                    }
                }
            }

            if (index == count) {
                looseChain = page;
                loosePageIndex = index;
            }

            for (; index < count; index++) {
                Buffer buffer = _volume.allocPage();
                bufferArray[index] = buffer;
            }
            //
            // Now we're committed - the just-allocated pages are no longer
            // subject to being deallocated by a retry.
            //
            page = newChain;
            for (index = count; --index >= 0;) {
                int offset = Buffer.LONGREC_PREFIX_SIZE
                        + (index * maxSegmentSize);
                int segmentSize = longSize - offset;
                if (segmentSize > maxSegmentSize)
                    segmentSize = maxSegmentSize;
                Buffer buffer = bufferArray[index];
                buffer.init(Buffer.PAGE_TYPE_LONG_RECORD);
                buffer.setRightSibling(page);

                System.arraycopy(longBytes, offset, buffer.getBytes(),
                        Buffer.HEADER_SIZE, segmentSize);

                int end = Buffer.HEADER_SIZE + segmentSize;
                if (end < buffer.getBufferSize()) {
                    buffer.clearBytes(end, buffer.getBufferSize());
                }
                buffer.setDirtyStructure();
                bufferArray[index] = null;
                page = buffer.getPageAddress(); // current head of the chain
                _pool.release(buffer);
            }
            completed = true;
            Buffer.writeLongRecordDescriptor(value.getEncodedBytes(), longSize,
                    page);
            _longRecordPageAddress = page;
            return page;
        } finally {
            if (!completed) {
                if (bufferArray != null) {
                    for (index = count; --index >= 0;) {
                        Buffer buffer = bufferArray[index];
                        if (buffer != null) {
                            _pool.release(buffer);
                            if (loosePageIndex >= 0 && index >= loosePageIndex) {
                                _volume.deallocateGarbageChainDeferred(
                                        buffer.getPageAddress(), -1);
                                _hasDeferredDeallocations = true;
                            }
                        }
                    }
                }
                value.changeLongRecordMode(false);
            } else {
                if (looseChain != 0) {
                    _volume.deallocateGarbageChainDeferred(looseChain, 0);
                    _hasDeferredDeallocations = true;
                }
            }
        }
    }

    /**
     * Creates a new LONG_RECORD chain and stores the supplied byte array in the
     * pages of this chain. This method catches and retries on RetryExceptions,
     * therefore it should only be called with no resource claims.
     * 
     * @param value
     *            The value. Must be in "long record mode"
     * 
     * @param from
     *            Offset to first byte of the long record.
     * 
     * @return Page address of the beginning of the chain
     * 
     * @throws PersistitException
     */
    private long storeOverlengthRecord(Value value, int from)
            throws PersistitException {
        value.changeLongRecordMode(true);

        // Calculate how many LONG_RECORD pages we will need.
        //
        boolean completed = false;
        int longSize = value.getLongSize();
        byte[] longBytes = value.getLongBytes();
        byte[] rawBytes = value.getEncodedBytes();
        int maxSegmentSize = _pool.getBufferSize() - Buffer.HEADER_SIZE;

        if (Debug.ENABLED) {
            Debug.$assert(value.isLongRecordMode());
            Debug.$assert(rawBytes.length == Buffer.LONGREC_SIZE);
        }

        System.arraycopy(longBytes, 0, rawBytes, Buffer.LONGREC_PREFIX_OFFSET,
                Buffer.LONGREC_PREFIX_SIZE);

        long looseChain = 0;
        if (from < Buffer.LONGREC_PREFIX_SIZE)
            from = Buffer.LONGREC_PREFIX_SIZE;

        Buffer buffer = null;
        int offset = from
                + (((longSize - from - 1) / maxSegmentSize) * maxSegmentSize);

        try {
            for (;;) {
                while (offset >= from) {
                    buffer = _volume.allocPage();
                    buffer.init(Buffer.PAGE_TYPE_LONG_RECORD);

                    int segmentSize = longSize - offset;
                    if (segmentSize > maxSegmentSize)
                        segmentSize = maxSegmentSize;

                    if (Debug.ENABLED) {
                        Debug.$assert(segmentSize >= 0
                                && offset >= 0
                                && offset + segmentSize < longBytes.length
                                && Buffer.HEADER_SIZE + segmentSize <= buffer
                                        .getBytes().length);
                    }

                    System.arraycopy(longBytes, offset, buffer.getBytes(),
                            Buffer.HEADER_SIZE, segmentSize);

                    int end = Buffer.HEADER_SIZE + segmentSize;
                    if (end < buffer.getBufferSize()) {
                        buffer.clearBytes(end, buffer.getBufferSize());
                    }
                    buffer.setRightSibling(looseChain);
                    looseChain = buffer.getPageAddress();
                    buffer.setDirtyStructure();
                    _pool.release(buffer);
                    offset -= maxSegmentSize;
                    buffer = null;
                }

                long page = looseChain;
                looseChain = 0;
                Buffer.writeLongRecordDescriptor(value.getEncodedBytes(),
                        longSize, page);
                completed = true;
                _longRecordPageAddress = page;
                return page;
            }
        } finally {
            if (buffer != null)
                _pool.release(buffer);
            if (looseChain != 0) {
                _volume.deallocateGarbageChainDeferred(looseChain, 0);
                _hasDeferredDeallocations = true;
            }
            if (!completed)
                value.changeLongRecordMode(false);
        }
    }

    void writeLongRecordPagesToJournal() throws PersistitException {
        Buffer buffer = null;
        long page = _longRecordPageAddress;
        if (page == 0) {
            return;
        }
        try {
            for (int count = 0; page != 0; count++) {
                buffer = _volume.getPool().get(_volume, page, false, true);
                if (buffer.getPageType() != Buffer.PAGE_TYPE_LONG_RECORD) {
                    if (Debug.ENABLED) {
                        Debug.debug1(true);
                    }
                    throw new CorruptVolumeException(
                            "LONG_RECORD chain  starting at "
                                    + _longRecordPageAddress
                                    + " is invalid at page " + page
                                    + " - invalid page type: " + buffer);
                }
                if (buffer.isDirty()) {
                    buffer.writePage();
                }
                page = buffer.getRightSibling();
                _volume.getPool().release(buffer);
                buffer = null;
                if (count > Exchange.MAX_LONG_RECORD_CHAIN) {
                    throw new CorruptVolumeException(
                            "LONG_RECORD chain starting at "
                                    + _longRecordPageAddress + " is too long");
                }
            }
        } finally {
            if (buffer != null) {
                _volume.getPool().release(buffer);
            }
        }
    }

    private void checkPageType(Buffer buffer, int expectedType)
            throws PersistitException {
        int type = buffer.getPageType();
        if (type != expectedType) {
            if (Debug.ENABLED) {
                Debug.$assert(false);
            }
            throw new CorruptVolumeException("Volume " + _volume + " page "
                    + buffer.getPageAddress() + " invalid page type " + type
                    + ": should be " + expectedType);
        }
    }

    /**
     * The transaction context for this Exchange. By default, this is the
     * transaction context of the current thread, and by default, all
     * <tt>Exchange</tt>s created by a thread share the same transaction
     * context.
     * 
     * @return The <tt>Transaction</tt> context for this thread.
     */
    public Transaction getTransaction() {
        return _transaction;
    }

    void ignoreTransactions() {
        _ignoreTransactions = true;
    }

    /**
     * Called by Transaction to set up a context for committing updates.
     * 
     * @param volume
     * @param _treeName
     */
    void setTree(Tree tree) throws PersistitException {
        _persistit.checkClosed();
        if (tree.getVolume() != _volume) {
            _volume = tree.getVolume();
            _pool = _persistit.getBufferPool(_volume.getPageSize());
        }
        if (_tree != tree) {
            _tree = tree;
            _treeGeneration = -1;
            checkLevelCache();
        }
    }

    /**
     * Package-private method indicates whether this <tt>Exchange</tt> refers to
     * the directory tree.
     * 
     * @return <tt>true</tt> if this is a directory exchange, else
     *         <tt>false</tt>.
     */
    boolean isDirectoryExchange() {
        return _isDirectoryExchange;
    }

    public void setSplitPolicy(SplitPolicy policy) {
        _splitPolicy = policy;
    }

    public void setJoinPolicy(JoinPolicy policy) {
        _joinPolicy = policy;
    }

    /**
     * Called after RetryException due to an operation discovering too late it
     * needs exclusive access to a Tree
     */
    private void waitForTreeExclusive() throws PersistitException {
        _tree.claim(true);
        _tree.release();
    }

    public KeyHistogram computeHistogram(final Key start, final Key end,
            final int sampleSize, final int keyDepth,
            final KeyFilter keyFilter, final int requestedTreeDepth)
            throws PersistitException {

        _persistit.checkClosed();
        checkLevelCache();
        final int treeDepth = requestedTreeDepth > _treeDepth ? _treeDepth
                : requestedTreeDepth;
        if (treeDepth < 0) {
            throw new IllegalArgumentException("treeDepth out of bounds: "
                    + treeDepth);
        }
        final KeyHistogram histogram = new KeyHistogram(getTree(), start, end,
                sampleSize, keyDepth, treeDepth);
        _transaction.assignTimestamp();
        final int lockedResourceCount = _persistit.getLockManager()
                .getLockedResourceCount();
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

        int foundAt = searchTree(_key, treeDepth);
        try {
            lc = _levelCache[treeDepth];
            buffer = lc._buffer;
            if (buffer != null) {
                checkPageType(buffer, treeDepth + 1);
            }

            while (foundAt != -1) {
                foundAt = buffer.traverse(_key, direction, foundAt);
                direction = GT;
                if (buffer.isAfterRightEdge(foundAt)) {
                    long rightSiblingPage = buffer.getRightSibling();
                    if (rightSiblingPage > 0) {
                        Buffer rightSibling = _pool.get(_volume,
                                rightSiblingPage, _exclusive, true);
                        _persistit.getLockManager().setOffset();
                        _pool.release(buffer);
                        //
                        // Reset foundAtNext to point to the first key block
                        // of the right sibling page.
                        //
                        buffer = rightSibling;
                        checkPageType(buffer, treeDepth + 1);
                        foundAt = buffer.traverse(_key, GT,
                                buffer.toKeyBlock(0));
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
                        histogram.addPage(
                                buffer.getBufferSize(),
                                buffer.getBufferSize()
                                        - buffer.getAvailableSize());
                        previousBuffer = buffer;
                    }
                    if (keyFilter == null || keyFilter.selected(_key)) {
                        histogram.addKeyCopy(_key);
                    }
                }
            }
        } finally {
            if (buffer != null) {
                _pool.release(buffer);
            }
        }
        _persistit.getLockManager().verifyLockedResourceCount(
                lockedResourceCount);

        histogram.cull();
        return histogram;
    }
}
