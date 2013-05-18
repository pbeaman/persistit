/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.persistit.exception.BufferSizeUnavailableException;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Debug;

class VolumeStructure {
    /**
     * Designated Tree name for the special directory "tree of trees".
     */
    final static String DIRECTORY_TREE_NAME = "_directory";
    /**
     * Key segment name for index by directory tree name.
     */
    final static String TREE_ROOT = "root";
    final static String TREE_STATS = "stats";
    final static String TREE_ACCUMULATOR = "totals";

    final static long INVALID_PAGE_ADDRESS = -1;

    private final Persistit _persistit;
    private final Volume _volume;
    private final int _pageSize;
    private BufferPool _pool;

    private volatile long _directoryRootPage;
    private volatile long _garbageRoot;

    private final Map<String, WeakReference<Tree>> _treeNameHashMap = new HashMap<String, WeakReference<Tree>>();
    private Tree _directoryTree;

    static class Chain {
        final long _left;
        final long _right;

        private Chain(final long left, final long right) {
            _left = left;
            _right = right;
        }

        private long getLeft() {
            return _left;
        }

        private long getRight() {
            return _right;

        }

        @Override
        public String toString() {
            return String.format("%,d->%,d", _left, _right);
        }

    }

    VolumeStructure(final Persistit persistit, final Volume volume, final int pageSize) {
        _persistit = persistit;
        _volume = volume;
        _pageSize = pageSize;
        _pool = persistit.getBufferPool(_pageSize);
    }

    void init(final long directoryRootPage, final long garbageRootPage) throws PersistitException {
        _garbageRoot = garbageRootPage;
        _directoryRootPage = directoryRootPage;
        if (directoryRootPage != 0) {
            _directoryTree = new Tree(_persistit, _volume, DIRECTORY_TREE_NAME);
            _directoryTree.setRootPageAddress(directoryRootPage);
        } else {
            _directoryTree = new Tree(_persistit, _volume, DIRECTORY_TREE_NAME);
            final long rootPageAddr = createTreeRoot(_directoryTree);
            _directoryTree.setRootPageAddress(rootPageAddr);
            updateDirectoryTree(_directoryTree);
        }
        if (!_volume.isTemporary()) {
            _directoryTree.loadHandle();
        }
        _directoryTree.setValid();
    }

    void close() throws PersistitInterruptedException {
        truncate();
        _directoryRootPage = 0;
        _garbageRoot = 0;
        _directoryTree = null;
        _persistit.removeVolume(_volume);
        _pool = null;
    }

    synchronized void truncate() {
        final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
        for (final WeakReference<Tree> treeRef : _treeNameHashMap.values()) {
            final Tree tree = treeRef.get();
            if (tree != null) {
                tree.invalidate();
            }
        }
        _treeNameHashMap.clear();
        _persistit.getJournalManager().truncate(_volume, timestamp);
    }

    Exchange directoryExchange() throws BufferSizeUnavailableException {
        final Exchange ex = new Exchange(_directoryTree);
        return ex;
    }

    Exchange accumulatorExchange() throws BufferSizeUnavailableException {
        return new Exchange(_directoryTree);
    }

    /**
     * Create a new tree in this volume. A tree is represented by an index root
     * page and all the index and data pages pointed to by that root page.
     * 
     * @return newly create NewTree object
     * @throws PersistitException
     */
    private long createTreeRoot(final Tree tree) throws PersistitException {
        _persistit.checkSuspended();
        Buffer rootPageBuffer = null;

        rootPageBuffer = allocPage();

        final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
        rootPageBuffer.writePageOnCheckpoint(timestamp);

        final long rootPage = rootPageBuffer.getPageAddress();

        try {
            rootPageBuffer.init(Buffer.PAGE_TYPE_DATA);
            rootPageBuffer.putValue(Key.LEFT_GUARD_KEY, ValueHelper.EMPTY_VALUE_WRITER);
            rootPageBuffer.putValue(Key.RIGHT_GUARD_KEY, ValueHelper.EMPTY_VALUE_WRITER);
            rootPageBuffer.setDirtyAtTimestamp(timestamp);
        } finally {
            rootPageBuffer = releaseBuffer(rootPageBuffer);
        }
        return rootPage;
    }

    /**
     * Look up by name and returns a <code>Tree</code> within this
     * <code>Volume</code>. If no such tree exists, this method either creates a
     * new tree or returns null depending on whether the
     * <code>createIfNecessary</code> parameter is <code>true</code>.
     * 
     * @param name
     *            The tree name
     * 
     * @param createIfNecessary
     *            Determines whether this method will create a new tree if there
     *            is no tree having the specified name.
     * 
     * @return The <code>NewTree</code>, or <code>null</code> if
     *         <code>createIfNecessary</code> is false and there is no such tree
     *         in this <code>Volume</code>.
     * 
     * @throws PersistitException
     */
    public synchronized Tree getTree(final String name, final boolean createIfNecessary) throws PersistitException {
        if (DIRECTORY_TREE_NAME.equals(name)) {
            throw new IllegalArgumentException("Tree name is reserved: " + name);
        }
        Tree tree = null;
        final WeakReference<Tree> treeRef = _treeNameHashMap.get(name);
        if (treeRef != null) {
            tree = treeRef.get();
            if (tree != null) {
                if (tree.isLive()) {
                    return tree;
                } else {
                    if (!createIfNecessary) {
                        return null;
                    }
                }
            }
        }
        if (tree == null) {
            tree = new Tree(_persistit, _volume, name);
        }
        final Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(name);
        final Value value = ex.fetch().getValue();
        if (value.isDefined()) {
            value.get(tree);
            loadTreeStatistics(tree);
            tree.setPrimordial();
            tree.setValid();
        } else if (createIfNecessary) {
            final long rootPageAddr = createTreeRoot(tree);
            tree.setRootPageAddress(rootPageAddr);
            updateDirectoryTree(tree);
            storeTreeStatistics(tree);
            tree.setValid();
        } else {
            return null;
        }
        if (_volume.isTemporary() || _volume.isLockVolume()) {
            tree.setPrimordial();
        }
        if (!_volume.isTemporary()) {
            tree.loadHandle();
        }
        _treeNameHashMap.put(name, new WeakReference<Tree>(tree));

        return tree;
    }

    /**
     * Helper method used by pruning. Allows access to directory tree by name.
     * 
     * @param name
     * @return
     * @throws PersistitException
     */
    Tree getTreeInternal(final String name) throws PersistitException {
        if (DIRECTORY_TREE_NAME.equals(name)) {
            return _directoryTree;
        } else {
            return getTree(name, false);
        }
    }

    void updateDirectoryTree(final Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            _volume.getStorage().claimHeadBuffer();
            try {
                _directoryRootPage = tree.getRootPageAddr();
                _volume.getStorage().flushMetaData();
            } finally {
                _volume.getStorage().releaseHeadBuffer();
            }
        } else {
            final Exchange ex = directoryExchange();
            if (!tree.isTransactionPrivate(false) || _volume.isLockVolume()) {
                ex.ignoreTransactions();
            }
            ex.getValue().put(tree);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(tree.getName()).store();
        }
    }

    void storeTreeStatistics(final Tree tree) throws PersistitException {
        if (tree.isLive() && tree.getStatistics().isDirty() && tree != _directoryTree) {
            final Exchange ex = directoryExchange();
            if (!ex.getVolume().isReadOnly()) {
                ex.getValue().put(tree.getStatistics());
                ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).store();
                tree.getStatistics().setDirty(false);
            }
        }
    }

    void loadTreeStatistics(final Tree tree) throws PersistitException {
        final Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).fetch();
        if (ex.getValue().isDefined()) {
            ex.getValue().get(tree.getStatistics());
        }
    }

    void removeTree(final Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            throw new IllegalArgumentException("Can't delete the Directory tree");
        }

        if (!tree.claim(true)) {
            throw new InUseException("Unable to acquire writer claim on " + tree);
        }
        try {
            final Exchange ex = directoryExchange();
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(tree.getName()).remove(Key.GTEQ);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).remove(Key.GTEQ);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ACCUMULATOR).append(tree.getName()).remove(Key.GTEQ);
            tree.delete();
        } finally {
            tree.release();
        }
    }

    synchronized void removed(final Tree tree) {
        _treeNameHashMap.remove(tree.getName());
    }

    void deallocateTree(final long treeRootPage, final int treeDepth) throws PersistitException {
        int depth = treeDepth;
        long page = treeRootPage;
        while (page != -1) {
            Buffer buffer = null;
            long deallocate = -1;
            try {
                buffer = _pool.get(_volume, page, false, true);
                if (buffer.getPageType() != depth) {
                    throw new CorruptVolumeException(buffer + " type code=" + buffer.getPageType()
                            + " is not equal to expected value " + depth);
                }
                if (buffer.isIndexPage()) {
                    deallocate = page;
                    final int p = buffer.toKeyBlock(0);
                    if (p > 0) {
                        page = buffer.getPointer(p);
                    } else {
                        page = -1;
                    }
                    depth--;
                } else if (buffer.isDataPage()) {
                    deallocate = page;
                    page = -1;
                }
            } finally {
                buffer = releaseBuffer(buffer);
            }
            if (deallocate != -1) {
                deallocateGarbageChain(deallocate, 0);
            }
        }
    }

    /**
     * Called by Exchange to recreate a Tree after a volume has been truncated.
     * 
     * @param tree
     * @throws PersistitException
     */
    void recreateTree(final Tree tree) throws PersistitException {
        Debug.$assert1.t(tree.getDepth() == -1);
        final long rootPageAddr = createTreeRoot(tree);
        tree.setRootPageAddress(rootPageAddr);
        updateDirectoryTree(tree);
        tree.getStatistics().reset();
        storeTreeStatistics(tree);
    }

    /**
     * Flush dirty {@link TreeStatistics} instances. Called periodically on the
     * PAGE_WRITER thread from {@link Persistit#cleanup()}.
     * 
     * @throws PersistitException
     */
    void flushStatistics() throws PersistitException {
        final List<Tree> trees = new ArrayList<Tree>();
        synchronized (this) {
            for (final WeakReference<Tree> ref : _treeNameHashMap.values()) {
                final Tree tree = ref.get();
                if (tree != null && tree != _directoryTree) {
                    trees.add(tree);
                }
            }
        }

        for (final Tree tree : trees) {
            storeTreeStatistics(tree);
        }
    }

    /**
     * Returns an array of all currently defined <code>NewTree</code> names.
     * 
     * @return The array
     * 
     * @throws PersistitException
     */
    public String[] getTreeNames() throws PersistitException {
        final List<String> list = new ArrayList<String>();
        final Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append("");
        while (ex.next()) {
            final String treeName = ex.getKey().indexTo(-1).decodeString();
            list.add(treeName);
        }
        final String[] names = list.toArray(new String[list.size()]);
        return names;
    }

    /**
     * Return a TreeInfo structure for a tree by the specified name. If there is
     * no such tree, then return <i>null</i>.
     * 
     * @param tree
     *            name
     * @return an information structure for the Management interface.
     */
    Management.TreeInfo getTreeInfo(final String name) {
        try {
            final Tree tree = getTree(name, false);
            if (tree != null) {
                return new Management.TreeInfo(tree);
            } else {
                return null;
            }
        } catch (final PersistitException pe) {
            return null;
        }
    }

    synchronized List<Tree> referencedTrees() {
        final List<Tree> list = new ArrayList<Tree>();
        for (final WeakReference<Tree> ref : _treeNameHashMap.values()) {
            final Tree tree = ref.get();
            if (tree != null) {
                list.add(tree);
            }
        }
        return list;
    }

    /**
     * Allocates a previously unused page. Returns a Buffer containing that
     * page. Empties all previous content of that page and sets its type to
     * UNUSED.
     * 
     * @return a Buffer containing the newly allocated page. The returned buffer
     *         has a writer claim on it.
     */
    Buffer allocPage() throws PersistitException {
        Buffer buffer = null;
        _volume.getStorage().claimHeadBuffer();
        try {
            final List<Chain> chains = new ArrayList<Chain>();
            final long garbageRoot = getGarbageRoot();
            if (garbageRoot != 0) {
                Buffer garbageBuffer = _pool.get(_volume, garbageRoot, true, true);
                try {
                    final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                    garbageBuffer.writePageOnCheckpoint(timestamp);
                    assert garbageBuffer.isGarbagePage() : "Garbage root page wrong type: " + garbageBuffer;

                    final long page = garbageBuffer.getGarbageChainLeftPage();
                    final long rightPage = garbageBuffer.getGarbageChainRightPage();

                    assert page != 0 && page != garbageRoot : "Garbage chain in garbage page + " + garbageBuffer
                            + " has invalid left page address " + page;

                    if (page == -1) {
                        final long newGarbageRoot = garbageBuffer.getRightSibling();
                        _persistit.getLogBase().garbagePageExhausted.log(garbageRoot, newGarbageRoot,
                                garbageBufferInfo(garbageBuffer));
                        setGarbageRoot(newGarbageRoot);
                        buffer = garbageBuffer;
                        garbageBuffer = null;
                    } else {
                        _persistit.getLogBase().allocateFromGarbageChain.log(page, garbageBufferInfo(garbageBuffer));
                        assert rightPage != -1 : "Garbage chain in garbage page + " + garbageBuffer
                                + " has invalid right page address " + rightPage;
                        buffer = _pool.get(_volume, page, true, true);
                        buffer.writePageOnCheckpoint(timestamp);

                        final long nextGarbagePage = buffer.getRightSibling();

                        if (nextGarbagePage == rightPage || nextGarbagePage == 0) {
                            _persistit.getLogBase().garbageChainDone.log(garbageBufferInfo(garbageBuffer), rightPage);
                            garbageBuffer.removeGarbageChain();
                        } else {
                            _persistit.getLogBase().garbageChainUpdate.log(garbageBufferInfo(garbageBuffer),
                                    nextGarbagePage, rightPage);
                            assert nextGarbagePage > 0 : "Deallocated page has invalid right pointer "
                                    + nextGarbagePage + " in " + buffer;
                            garbageBuffer.setGarbageLeftPage(nextGarbagePage);
                        }
                        garbageBuffer.setDirtyAtTimestamp(timestamp);
                    }

                    Debug.$assert0
                            .t(buffer != null && buffer.getPageAddress() != 0
                                    && buffer.getPageAddress() != _garbageRoot
                                    && buffer.getPageAddress() != _directoryRootPage);

                    harvestLongRecords(buffer, 0, Integer.MAX_VALUE, chains);

                    buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
                    buffer.clear();
                    return buffer;
                } finally {
                    garbageBuffer = releaseBuffer(garbageBuffer);
                    if (!chains.isEmpty()) {
                        deallocateGarbageChain(chains);
                    }
                }
            }
        } finally {
            _volume.getStorage().releaseHeadBuffer();
        }
        /*
         * If there was no garbage chain above then we need to allocate a new
         * page from the volume.
         */
        final long page = _volume.getStorage().allocNewPage();
        buffer = _pool.get(_volume, page, true, false);
        buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
        Debug.$assert0.t(buffer.getPageAddress() != 0);
        return buffer;
    }

    void deallocateGarbageChain(final long left, final long right) throws PersistitException {
        final List<Chain> list = new ArrayList<Chain>();
        list.add(new Chain(left, right));
        deallocateGarbageChain(list);
    }

    void deallocateGarbageChain(final List<Chain> chains) throws PersistitException {
        _volume.getStorage().claimHeadBuffer();
        try {
            while (!chains.isEmpty()) {
                final Chain chain = chains.remove(chains.size() - 1);
                final long left = chain.getLeft();
                final long right = chain.getRight();

                assert left > 0 || right < 0 : "Attempt to deallocate invalid garbage chain " + chain;

                Buffer garbageBuffer = null;
                final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();

                try {
                    final long garbagePage = getGarbageRoot();
                    if (garbagePage != 0) {
                        if (left == garbagePage) {
                            throw new IllegalStateException("De-allocating page that is already garbage: " + "root="
                                    + garbagePage + " left=" + left + " right=" + right);
                        }

                        garbageBuffer = _pool.get(_volume, garbagePage, true, true);
                        garbageBuffer.writePageOnCheckpoint(timestamp);

                        final boolean fits = garbageBuffer.addGarbageChain(left, right, -1);

                        if (fits) {
                            _persistit.getLogBase().newGarbageChain.log(left, right, garbageBufferInfo(garbageBuffer));
                            garbageBuffer.setDirtyAtTimestamp(timestamp);
                            continue;
                        } else {
                            _persistit.getLogBase().garbagePageFull.log(left, right, garbageBufferInfo(garbageBuffer));
                            garbageBuffer = releaseBuffer(garbageBuffer);
                        }
                    }
                    garbageBuffer = _pool.get(_volume, left, true, true);
                    garbageBuffer.writePageOnCheckpoint(timestamp);

                    assert garbageBuffer.isDataPage() || garbageBuffer.isIndexPage()
                            || garbageBuffer.isLongRecordPage() : "Attempt to allocate invalid type of page: "
                            + garbageBuffer;

                    final long nextGarbagePage = garbageBuffer.getRightSibling();

                    assert nextGarbagePage > 0 || right == 0 : "Attempt to deallcoate broken chain " + chain
                            + " starting at left page " + garbageBuffer;
                    Debug.$assert0.t(nextGarbagePage > 0 || right == 0);

                    harvestLongRecords(garbageBuffer, 0, Integer.MAX_VALUE, chains);

                    garbageBuffer.init(Buffer.PAGE_TYPE_GARBAGE);

                    _persistit.getLogBase().newGarbageRoot.log(garbageBufferInfo(garbageBuffer));

                    if (nextGarbagePage != right) {
                        // Will always fit because this is a freshly initialized
                        // page
                        garbageBuffer.addGarbageChain(nextGarbagePage, right, -1);
                        _persistit.getLogBase().newGarbageChain.log(nextGarbagePage, right,
                                garbageBufferInfo(garbageBuffer));
                    }
                    garbageBuffer.setRightSibling(garbagePage);
                    garbageBuffer.setDirtyAtTimestamp(timestamp);
                    setGarbageRoot(garbageBuffer.getPageAddress());
                } finally {
                    if (garbageBuffer != null) {
                        garbageBuffer.releaseTouched();
                    }
                }
            }
        } finally {
            _volume.getStorage().releaseHeadBuffer();
        }
    }

    void harvestLongRecords(final Buffer buffer, final int start, final int end) throws PersistitException {
        final List<Chain> chains = new ArrayList<Chain>();
        harvestLongRecords(buffer, start, end, chains);
        deallocateGarbageChain(chains);
    }

    void harvestLongRecords(final Buffer buffer, final int start, final int end, final List<Chain> chains)
            throws PersistitException {
        assert buffer.isOwnedAsWriterByMe() : "Harvesting from page owned by another thread: " + buffer;
        if (buffer.isDataPage()) {
            final int p1 = buffer.toKeyBlock(start);
            final int p2 = buffer.toKeyBlock(end);
            for (int p = p1; p < p2 && p != -1; p = buffer.nextKeyBlock(p)) {
                final long pointer = buffer.fetchLongRecordPointer(p);
                assert pointer != INVALID_PAGE_ADDRESS : "Long record at keyblock " + p
                        + " was already harvested from " + buffer;
                if (pointer != 0) {
                    chains.add(new Chain(pointer, 0));
                    /*
                     * Detects whether and prevents same pointer from being read
                     * and deallocated twice.
                     */
                    buffer.neuterLongRecord(p);
                }
            }
        }
    }

    private Buffer releaseBuffer(final Buffer buffer) {
        if (buffer != null) {
            buffer.releaseTouched();
        }
        return null;
    }

    public long getDirectoryRoot() {
        return _directoryRootPage;
    }

    /**
     * @return The directory <code>Tree</code>
     */
    Tree getDirectoryTree() {
        return _directoryTree;
    }

    /**
     * Returns the page address of the garbage tree. This method is useful to
     * diagnostic utility programs.
     * 
     * @return The page address
     */
    public long getGarbageRoot() {
        return _garbageRoot;
    }

    List<Long> getGarbageList() throws PersistitException {
        final List<Long> garbageList = new ArrayList<Long>();
        _volume.getStorage().claimHeadBuffer();
        try {
            final long root = getGarbageRoot();
            if (root != 0) {
                garbageList.add(root);
                final Buffer buffer = _pool.get(_volume, root, true, true);
                try {
                    for (final Management.RecordInfo rec : buffer.getRecords()) {
                        if (rec._garbageLeftPage > 0) {
                            garbageList.add(rec._garbageLeftPage);
                        }
                        if (rec._garbageRightPage > 0) {
                            garbageList.add(rec._garbageRightPage);
                        }
                    }
                } finally {
                    buffer.release();
                }
            }
        } finally {
            _volume.getStorage().releaseHeadBuffer();
        }
        return garbageList;
    }

    private void setGarbageRoot(final long garbagePage) throws PersistitException {
        _garbageRoot = garbagePage;
        _volume.getStorage().flushMetaData();
    }

    /**
     * @return The size in bytes of one page in this <code>Volume</code>.
     */
    public int getPageSize() {
        return _pageSize;
    }

    /**
     * Returns the <code>BufferPool</code> in which this volume's pages are
     * cached.
     * 
     * @return This volume's </code>BufferPool</code>
     */
    BufferPool getPool() {
        return _pool;
    }

    private String garbageBufferInfo(final Buffer buffer) {
        if (buffer.getPageType() != Buffer.PAGE_TYPE_GARBAGE) {
            return "!!!" + buffer.getPageAddress() + " is not a garbage page!!!";
        }
        return "@<" + buffer.getPageAddress() + ":" + buffer.getAlloc() + ">";
    }

    synchronized boolean treeMapContainsName(final String treeName) {
        return _treeNameHashMap.containsKey(treeName);
    }
}
