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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final Persistit _persistit;
    private final Volume _volume;
    private final int _pageSize;
    private BufferPool _pool;

    private volatile long _directoryRootPage;
    private volatile long _garbageRoot;

    private Map<String, WeakReference<Tree>> _treeNameHashMap = new HashMap<String, WeakReference<Tree>>();
    private Tree _directoryTree;

    VolumeStructure(final Persistit persistit, final Volume volume, int pageSize) {
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
        for (final WeakReference<Tree> treeRef : _treeNameHashMap.values()) {
            final Tree tree = treeRef.get();
            if (tree != null) {
                tree.invalidate();
            }
        }
        _treeNameHashMap.clear();
    }

    Exchange directoryExchange() {
        Exchange ex = new Exchange(_directoryTree);
        ex.ignoreTransactions();
        return ex;
    }

    Exchange accumulatorExchange() {
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

        long rootPage = rootPageBuffer.getPageAddress();

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
    public synchronized Tree getTree(String name, boolean createIfNecessary) throws PersistitException {
        if (DIRECTORY_TREE_NAME.equals(name)) {
            throw new IllegalArgumentException("Tree name is reserved: " + name);
        }
        Tree tree;
        WeakReference<Tree> treeRef = _treeNameHashMap.get(name);
        if (treeRef != null) {
            tree = treeRef.get();
            if (tree != null && tree.isValid()) {
                return tree;
            }
        }
        final Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(name);
        Value value = ex.fetch().getValue();
        tree = new Tree(_persistit, _volume, name);
        _persistit.getJournalManager().handleForTree(tree);
        if (value.isDefined()) {
            value.get(tree);
            loadTreeStatistics(tree);
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
        _treeNameHashMap.put(name, new WeakReference<Tree>(tree));
        return tree;
    }
    
    /**
     * Helper method used by pruning.  Allows access to directory tree
     * by name.
     * @param name
     * @return
     * @throws PersistitException
     */
    Tree getTreeInternal(final String name) throws PersistitException {
        if (DIRECTORY_TREE_NAME.equals(name)) {
            return _directoryTree;
        } else {
            return getTree(name, true);
        }
    }

    void updateDirectoryTree(Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            _volume.getStorage().claimHeadBuffer();
            try {
                _directoryRootPage = tree.getRootPageAddr();
                _volume.getStorage().flushMetaData();
            } finally {
                _volume.getStorage().releaseHeadBuffer();
            }
        } else {
            Exchange ex = directoryExchange();
            ex.getValue().put(tree);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(tree.getName()).store();
        }
    }

    void storeTreeStatistics(Tree tree) throws PersistitException {
        if (tree.getStatistics().isDirty() && !DIRECTORY_TREE_NAME.equals(tree.getName())) {
            Exchange ex = directoryExchange();
            if (!ex.getVolume().isReadOnly()) {
                ex.getValue().put(tree.getStatistics());
                ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).store();
                tree.getStatistics().setDirty(false);
            }
        }
    }

    void loadTreeStatistics(Tree tree) throws PersistitException {
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).fetch();
        if (ex.getValue().isDefined()) {
            ex.getValue().get(tree.getStatistics());
        }
    }

    boolean removeTree(Tree tree) throws PersistitException {
        if (tree == _directoryTree) {
            throw new IllegalArgumentException("Can't delete the Directory tree");
        }
        _persistit.checkSuspended();

        int depth = -1;
        long page = -1;

        if (!tree.claim(true)) {
            throw new InUseException("Unable to acquire writer claim on " + tree);
        }

        synchronized (this) {
            _treeNameHashMap.remove(tree.getName());
            tree.bumpGeneration();
            tree.invalidate();
        }

        try {
            final long rootPage = tree.getRootPageAddr();
            tree.changeRootPageAddr(-1, 0);
            page = rootPage;
            depth = tree.getDepth();
            Exchange ex = directoryExchange();
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append(tree.getName()).remove(Key.GTEQ);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_STATS).append(tree.getName()).remove(Key.GTEQ);
            ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ACCUMULATOR).append(tree.getName()).remove(Key.GTEQ);
        } finally {
            tree.release();
        }
        // The Tree is now gone. The following deallocates the
        // pages formerly associated with it. If this fails we'll be
        // left with allocated pages that are not available on the garbage
        // chain for reuse.

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
                    int p = buffer.toKeyBlock(0);
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
        return true;
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
     */
    void flushStatistics() {
        try {
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
        } catch (Exception e) {
            _persistit.getLogBase().adminFlushException.log(e);
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
        List<String> list = new ArrayList<String>();
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(TREE_ROOT).append("");
        while (ex.next()) {
            String treeName = ex.getKey().indexTo(-1).decodeString();
            list.add(treeName);
        }
        String[] names = list.toArray(new String[list.size()]);
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
    Management.TreeInfo getTreeInfo(String name) {
        try {
            final Tree tree = getTree(name, false);
            if (tree != null) {
                return new Management.TreeInfo(tree);
            } else {
                return null;
            }
        } catch (PersistitException pe) {
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
            long garbageRoot = getGarbageRoot();
            if (garbageRoot != 0) {
                Buffer garbageBuffer = _pool.get(_volume, garbageRoot, true, true);
                try {
                    final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                    garbageBuffer.writePageOnCheckpoint(timestamp);
                    Debug.$assert0.t(garbageBuffer.isGarbagePage());
                    Debug.$assert0.t((garbageBuffer.getStatus() & Buffer.CLAIMED_MASK) == 1);

                    long page = garbageBuffer.getGarbageChainLeftPage();
                    long rightPage = garbageBuffer.getGarbageChainRightPage();

                    Debug.$assert0.t(page != 0);

                    if (page == -1) {
                        long newGarbageRoot = garbageBuffer.getRightSibling();
                        _persistit.getLogBase().garbagePageExhausted.log(garbageRoot, newGarbageRoot,
                                garbageBufferInfo(garbageBuffer));
                        setGarbageRoot(newGarbageRoot);
                        buffer = garbageBuffer;
                        garbageBuffer = null;
                    } else {
                        _persistit.getLogBase().allocateFromGarbageChain.log(page, garbageBufferInfo(garbageBuffer));
                        boolean solitaire = rightPage == -1;
                        buffer = _pool.get(_volume, page, true, !solitaire);

                        Debug.$assert0.t(buffer.getPageAddress() > 0);

                        long nextGarbagePage = solitaire ? -1 : buffer.getRightSibling();

                        if (nextGarbagePage == rightPage || nextGarbagePage == 0) {
                            _persistit.getLogBase().garbageChainDone.log(garbageBufferInfo(garbageBuffer), rightPage);
                            garbageBuffer.removeGarbageChain();
                        } else {
                            _persistit.getLogBase().garbageChainUpdate.log(garbageBufferInfo(garbageBuffer),
                                    nextGarbagePage, rightPage);
                            Debug.$assert0.t(nextGarbagePage > 0);
                            garbageBuffer.setGarbageLeftPage(nextGarbagePage);
                        }
                        garbageBuffer.setDirtyAtTimestamp(timestamp);
                    }

                    Debug.$assert0
                            .t(buffer != null && buffer.getPageAddress() != 0
                                    && buffer.getPageAddress() != _garbageRoot
                                    && buffer.getPageAddress() != _directoryRootPage);

                    harvestLongRecords(buffer, 0, Integer.MAX_VALUE);

                    buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
                    buffer.clear();
                    return buffer;
                } finally {
                    garbageBuffer = releaseBuffer(garbageBuffer);
                }
            }
        } finally {
            _volume.getStorage().releaseHeadBuffer();
        }
        /*
         * If there was no garbage chain above then we need to allocate a new page from the volume.
         */
        long page = _volume.getStorage().allocNewPage();
        buffer = _pool.get(_volume, page, true, false);
        buffer.init(Buffer.PAGE_TYPE_UNALLOCATED);
        Debug.$assert0.t(buffer.getPageAddress() != 0);
        return buffer;

    }

    void deallocateGarbageChain(long left, long right) throws PersistitException {
        Debug.$assert0.t(left > 0);

        _volume.getStorage().claimHeadBuffer();

        Buffer garbageBuffer = null;

        try {
            long garbagePage = getGarbageRoot();
            if (garbagePage != 0) {
                Debug.$assert0.t(left != garbagePage && right != garbagePage);

                garbageBuffer = _pool.get(_volume, garbagePage, true, true);

                final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                garbageBuffer.writePageOnCheckpoint(timestamp);

                boolean fits = garbageBuffer.addGarbageChain(left, right, -1);

                if (fits) {
                    _persistit.getLogBase().newGarbageChain.log(left, right, garbageBufferInfo(garbageBuffer));
                    garbageBuffer.setDirtyAtTimestamp(timestamp);
                    return;
                } else {
                    _persistit.getLogBase().garbagePageFull.log(left, right, garbageBufferInfo(garbageBuffer));
                    garbageBuffer = releaseBuffer(garbageBuffer);
                }
            }
            boolean solitaire = (right == -1);
            garbageBuffer = _pool.get(_volume, left, true, !solitaire);

            final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
            garbageBuffer.writePageOnCheckpoint(timestamp);

            Debug.$assert0.t((garbageBuffer.isDataPage() || garbageBuffer.isIndexPage())
                    || garbageBuffer.isLongRecordPage() || (solitaire && garbageBuffer.isUnallocatedPage()));

            long nextGarbagePage = solitaire ? 0 : garbageBuffer.getRightSibling();

            Debug.$assert0.t(nextGarbagePage > 0 || right == 0 || solitaire);

            harvestLongRecords(garbageBuffer, 0, Integer.MAX_VALUE);

            garbageBuffer.init(Buffer.PAGE_TYPE_GARBAGE);

            _persistit.getLogBase().newGarbageRoot.log(garbageBufferInfo(garbageBuffer));

            if (!solitaire && nextGarbagePage != right) {
                // Will always fit because this is a freshly initialized page
                garbageBuffer.addGarbageChain(nextGarbagePage, right, -1);
                _persistit.getLogBase().newGarbageChain.log(nextGarbagePage, right, garbageBufferInfo(garbageBuffer));
            }
            garbageBuffer.setRightSibling(garbagePage);
            garbageBuffer.setDirtyAtTimestamp(timestamp);
            setGarbageRoot(garbageBuffer.getPageAddress());
        } finally {
            if (garbageBuffer != null) {
                garbageBuffer.releaseTouched();
            }
            _volume.getStorage().releaseHeadBuffer();
        }
    }

    // TODO - no one needs the return value
    boolean harvestLongRecords(Buffer buffer, int start, int end) throws PersistitException {
        boolean anyLongRecords = false;
        if (buffer.isDataPage()) {
            int p1 = buffer.toKeyBlock(start);
            int p2 = buffer.toKeyBlock(end);
            for (int p = p1; p < p2 && p != -1; p = buffer.nextKeyBlock(p)) {
                long pointer = buffer.fetchLongRecordPointer(p);
                if (pointer != 0) {
                    deallocateGarbageChain(pointer, 0);
                    anyLongRecords |= true;
                }
            }
        }
        return anyLongRecords;
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

    private void setGarbageRoot(long garbagePage) throws PersistitException {
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

    private String garbageBufferInfo(Buffer buffer) {
        if (buffer.getPageType() != Buffer.PAGE_TYPE_GARBAGE) {
            return "!!!" + buffer.getPageAddress() + " is not a garbage page!!!";
        }
        return "@<" + buffer.getPageAddress() + ":" + buffer.getAlloc() + ">";
    }
}
