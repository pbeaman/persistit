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

import java.util.ArrayList;
import java.util.BitSet;

import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.TimeoutException;

/**
 * <p>
 * A simple integrity checker that traverses all pages within one or more
 * {@link Tree}s, verifies the internal structure of each page, and verifies the
 * relationships between the pages. Any inconsistency is noted as a
 * {@link Fault}.
 * </p>
 * <p>
 * An application creates an <code>IntegrityCheck</code>, invokes its
 * {@link #checkVolume} or {@link #checkTree} method to peform the integrity
 * check, and then reviews the <code>Fault</code>s available through the
 * {@link #getFaults} method.
 * </p>
 * <p>
 * In this version of Persistit, <code>IntegrityCheck</code> operates reliably
 * only on quiescent <code>Tree</code>s; if other threads are modifying a
 * <code>Tree</code> while <code>IntegrityCheck</code> is reviewing its
 * structure, spurious faults are likely to be detected.
 * </p>
 * 
 * @version 1.0
 */
public class IntegrityCheck extends Task {
    final static int MAX_FAULTS = 200;
    final static int MAX_WALK_RIGHT = 1000;
    
    private final static String[] ARG_TEMPLATE = {
        "trees|string:|Tree specification",
        "_flags|U|Freeze updates (default)",
        "_flags|H|Fix holes (default)",
        "_flags|u|Don't freeze updates",
        "_flags|h|Don't fix holes",
    };

    private Tree _currentTree;
    private LongBitSet _usedPageBits = new LongBitSet();
    private long _totalPages = 0;
    private long _pagesVisited = 0;

    private long _indexPageCount = 0;
    private long _dataPageCount = 0;
    private long _indexBytesInUse = 0;
    private long _dataBytesInUse = 0;
    private long _longRecordPageCount = 0;
    private long _longRecordBytesInUse = 0;

    private Buffer[] _edgeBuffers = new Buffer[Exchange.MAX_TREE_DEPTH];
    private int[] _edgePositions = new int[Exchange.MAX_TREE_DEPTH];
    private Key[] _edgeKeys = new Key[Exchange.MAX_TREE_DEPTH];
    private int _depth = -1;

    private Tree[] _trees;
    private boolean _freezeUpdates;
    private boolean _fixHoles;

    private ArrayList<Fault> _faults = new ArrayList<Fault>();
    private ArrayList<Hole> _holes = new ArrayList<Hole>();
    private int _holeCount;

    // Used in checking long values
    private Value _value = new Value((Persistit) null);

    /**
     * Package-private constructor for use in a {@link Task}.
     */
    IntegrityCheck() {
    }

    public IntegrityCheck(Persistit persistit) {
        super(persistit);
        _persistit = persistit;
    }

    @Override
    protected void setupTask(String[] args) throws PersistitException {
        _trees = parseTreeList(args[0]);
        _freezeUpdates = args.length > 1 && "true".equals(args[1]);
        _fixHoles = args.length > 2 && "true".equals(args[2]);
    }
    
    @Override
    protected String[] argTemplate() {
        return ARG_TEMPLATE;
    }

    @Override
    protected void runTask() {
        boolean freeze = !_persistit.isUpdateSuspended()
                && (_freezeUpdates || _fixHoles);
        boolean needsToDrain = false;
        if (freeze) {
            _persistit.setUpdateSuspended(true);
            needsToDrain = true;
        }
        try {
            ArrayList<Volume> volumes = new ArrayList<Volume>();
            long _totalPages = 0;
            for (int index = 0; index < _trees.length; index++) {
                Tree tree = _trees[index];
                Volume volume = tree.getVolume();
                if (!volumes.contains(volume)) {
                    volumes.add(volume);
                    _totalPages += volume.getMaximumPageInUse();
                }
            }
            Volume previousVolume = null;
            for (int index = 0; index < _trees.length; index++) {
                Tree tree = _trees[index];
                Volume volume = tree.getVolume();
                if (volume != previousVolume)
                    initialize(false);
                previousVolume = volume;
                try {
                    if (needsToDrain) {
                        needsToDrain = false;
                        //
                        // Delay for a few seconds to allow
                        // completion of any update operations
                        // currently in progress. We do this only if
                        // we are going to fix holes. This is pretty
                        // kludgey: TODO - better mechanism!
                        //
                        Thread.sleep(3000);
                    }
                    postMessage(
                            "Checking " + tree.getName() + " in "
                                    + volume.getPath(), LOG_VERBOSE);
                    boolean okay = checkTree(tree);
                    appendMessage(okay ? " - OKAY" : " - FAULTS", LOG_VERBOSE);
                } catch (PersistitException pe) {
                    postMessage(pe.toString(), LOG_NORMAL);
                } catch (InterruptedException ie) {
                    // Just repeat the operation...
                    index--;
                }
            }
            postMessage(toString(), LOG_NORMAL);
        } finally {
            if (freeze) {
                _persistit.setUpdateSuspended(false);
            }
        }
    }

    private void addFault(String description, long page, int level, int position) {
        Fault fault = new Fault(_currentTree.getName(), this, description,
                page, level, position);
        if (_faults.size() < MAX_FAULTS)
            _faults.add(fault);
        postMessage(fault.toString(), LOG_VERBOSE);
    }

    private void initTree(Tree tree) {
        _currentTree = tree;
        _holeCount = 0;
        _holes.clear();
        for (int index = Exchange.MAX_TREE_DEPTH; --index >= 0;) {
            _edgeBuffers[index] = null;
            _edgeKeys[index] = null;
            _edgePositions[index] = 0;
            _depth = -1;
        }
    }

    /**
     * Indicates whether fault have been detected
     * 
     * @return <i>true</i> if faults were detected
     */
    public boolean hasFaults() {
        return _faults.size() > 0;
    }

    /**
     * Returns the detected faults
     * 
     * @return An array of detected Faults
     */
    public Fault[] getFaults() {
        // Note: avoiding use of JDK 1.2 toArray method.
        Fault[] array = new Fault[_faults.size()];
        for (int index = 0; index < _faults.size(); index++) {
            array[index] = _faults.get(index);
        }
        return array;
    }

    /**
     * Returns the total count of index pages traversed during the integrity
     * checking process.
     * 
     * @return The count of pages
     */
    public long getIndexPageCount() {
        return _indexPageCount;
    }

    /**
     * Returns the total count of data pages traversed during the integrity
     * checking process.
     * 
     * @return The count of pages
     */
    public long getDataPageCount() {
        return _dataPageCount;
    }

    /**
     * Returns the total count of long record pages traversed during the
     * integrity checking process. Long record pages contain segments of long
     * record values.
     * 
     * @return The count of pages
     */
    public long getLongRecordPageCount() {
        return _longRecordPageCount;
    }

    /**
     * Returns the total count of bytes in use (not page structure overhead) in
     * index pages traversed during the integrity checking process.
     * 
     * @return The count of allocated bytes
     */
    public long getIndexByteCount() {
        return _indexBytesInUse;
    }

    /**
     * Returns the total count of bytes in use (not page structure overhead) in
     * data pages traversed during the integrity checking process.
     * 
     * @return The count of allocated bytes
     */
    public long getDataByteCount() {
        return _dataBytesInUse;
    }

    /**
     * Returns the total count of bytes in use (not page structure overhead) in
     * long record pages traversed during the integrity checking process.
     * 
     * @return The count of allocated bytes
     */
    public long getLongRecordByteCount() {
        return _longRecordBytesInUse;
    }

    /**
     * Returns an approximate indication of progress during the integrity
     * checking process, where 0.0 indicates that work has not started, and 1.0
     * represents completion.
     * 
     * @return The progress indicator on a scale of 0.0 to 1.0.
     */
    public double getProgress() {
        if (_totalPages == 0)
            return 1;
        else
            return ((double) _pagesVisited) / ((double) _totalPages);
    }

    /**
     * Returns a displayable String indicating the tree currently being checked
     * and the relative counts of pages that have been traversed.
     * 
     * @return The progress description
     */
    @Override
    public String getStatus() {
        if (_currentTree == null)
            return null;
        else
            return _pagesVisited + "/" + _totalPages + " ("
                    + _currentTree.getName() + ")";
    }

    /**
     * Returns a displayable summary of the state of the integrity check,
     * including the number of detected faults, and the page and byte counts.
     * 
     * @return The displayable string
     */
    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * Creates A displayable representation of the state of the integrity check,
     * including the number of detected faults, and the page and byte counts.
     * The returned String optionally includes a listing of all detected Faults.
     * 
     * @param details
     *            If <i>true</i> the returned representation will include a list
     *            of all detected Faults.
     * 
     * @return the String representation.
     */

    public String toString(boolean details) {
        StringBuilder sb = new StringBuilder();
        sb.append("IntegerityCheck ");
        if (hasFaults()) {
            sb.append(_faults.size());
            sb.append(" FAULTS");
        } else
            sb.append("is OKAY");
        sb.append(" Index: ");
        sb.append(_indexPageCount);
        sb.append(" pages / ");
        sb.append(_indexBytesInUse);
        sb.append(" bytes, Data:");
        sb.append(_dataPageCount);
        sb.append(" pages, ");
        sb.append(_dataBytesInUse);
        sb.append(" bytes, Long Record:");
        sb.append(_longRecordPageCount);
        sb.append(" pages, ");
        sb.append(_longRecordBytesInUse);
        sb.append(" bytes");

        if (details) {

            for (int index = 0; index < _faults.size(); index++) {
                sb.append(Util.NEW_LINE);
                sb.append("        ");
                sb.append(_faults.get(index));
            }
        }
        return sb.toString();
    }

    private static class Hole {
        Tree _tree;
        long _page;
        int _level;

        Hole(Tree tree, long page, int level) {
            _tree = tree;
            _page = page;
            _level = level;
        }
    }

    /**
     * A representation of an error or inconsistency within a {@link Tree}.
     */
    public static class Fault {

        String _treeName;
        String _description;
        long[] _path;
        int _level;
        int _depth;
        int _position;

        Fault(String treeName, IntegrityCheck work, String description,
                long page, int level, int position) {
            _treeName = treeName;
            _description = description;
            _path = new long[level + 1];
            for (int index = 0; index <= level; index++) {
                _path[index] = work._edgeBuffers[index].getPageAddress();
            }
            _path[level] = page;
            _level = level;
            _depth = work._depth;
            _position = position;
        }

        /**
         * Returns a displayable description of this Fault.
         * 
         * @return The description
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            {
                sb.append(_treeName);
                sb.append(": ");
                sb.append(_description);
                sb.append(" (path");
                for (int index = 0; index <= _level; index++) {
                    sb.append("->");
                    sb.append(_path[index]);
                }
                if (_position != 0) {
                    sb.append(":");
                    sb.append(_position & Buffer.P_MASK);
                }
                sb.append(")");
                if (_depth >= 0) {
                    sb.append(" depth=");
                    sb.append(_depth);
                }
            }
            return sb.toString();
        }

    }

    /**
     * Implements a bit set that operates on long- rather than int- valued
     * indexes.
     */
    private static class LongBitSet {
        //
        // Temporary implementation uses ints. This limits its use to
        // volumes with fewer than Integer.MAX_VALUE pages.
        //
        BitSet _bitSet = new BitSet();

        public void set(long index, boolean value) {
            if (index > Integer.MAX_VALUE) {
                throw new RuntimeException(
                        "Large page addresses not implemented yet.");
            }
            if (value)
                _bitSet.set((int) index);
            else
                _bitSet.clear((int) index);
        }

        public boolean get(long index) {
            if (index > Integer.MAX_VALUE) {
                throw new RuntimeException(
                        "Large page addresses not implemented yet.");
            }
            return _bitSet.get((int) index);
        }
    }

    /**
     * Resets this <code>IntegrityCheck</code> to its initial state by removing
     * all <code>Fault</code>s and optional resetting all counters.
     * 
     * @param initCounts
     *            <code>true</code> to reset all counters to zero.
     */
    public void initialize(boolean initCounts) {
        _currentTree = null;
        _usedPageBits = new LongBitSet();
        _totalPages = 0;
        _pagesVisited = 0;

        if (initCounts) {
            _indexPageCount = 0;
            _dataPageCount = 0;
            _indexBytesInUse = 0;
            _dataBytesInUse = 0;
        }
    }

    /**
     * Performs the integrity checking process on a {@link Volume}. Logs any
     * detected Faults for subsequent review.
     * 
     * @param volume
     *            The {@link Volume} to check.
     * @return <i>true</i> if the volume is clean (has no Faults).
     * @throws PersistitException
     */
    public boolean checkVolume(Volume volume) throws PersistitException {
        initialize(false);
        String[] treeNames = volume.getTreeNames();
        // This is just for the progress counter.
        _totalPages = volume.getMaximumPageInUse();
        Tree directoryTree = volume.getDirectoryTree();
        if (directoryTree != null)
            checkTree(directoryTree);
        for (int index = 0; index < treeNames.length; index++) {
            Tree tree = volume.getTree(treeNames[index], false);
            if (tree != null)
                checkTree(tree);
        }
        return !hasFaults();
    }

    /**
     * Performs the integrity checking process on a {@link Tree}. Logs any
     * detected Faults for subsequent review.
     * 
     * @param tree
     *            The <code>Tree</code> to check.
     * @return <i>true</i> if the volume is clean (has no Faults).
     * @throws PersistitException
     */
    public boolean checkTree(Tree tree) throws PersistitException {
        int initialFaultCount = _faults.size();
        boolean again = true;
        if (!tree.claim(true)) {
            throw new TimeoutException(tree + " is in use");
        }
        _persistit.getLockManager().setDisabled(true);
        try {
            while (again) {
                try {
                    again = false;
                    initTree(tree);
                    checkTree(new Key(_persistit), 0, tree.getRootPageAddr(),
                            0, tree);
                } finally {
                    //
                    // Release all the buffers.
                    //
                    for (int index = Exchange.MAX_TREE_DEPTH; --index >= 0;) {
                        Buffer buffer = _edgeBuffers[index];
                        if (buffer != null) {
                            releasePage(buffer);
                            _edgeBuffers[index] = null;
                        }
                    }
                    _currentTree = null;
                }
                if (_holeCount > 0) {
                    if (_fixHoles) {
                        postMessage("Fixing " + _holes.size()
                                + " unindexed page"
                                + (_holeCount > 1 ? "s" : "") + " in tree "
                                + tree.getName() + " in volume "
                                + tree.getVolume().getPath(), LOG_NORMAL);

                        fixIndexHoles();

                        if (_holeCount > _holes.size()) {
                            again = true;
                        }
                    } else {
                        postMessage("Tree " + tree.getName() + " in volume "
                                + tree.getVolume().getPath() + " has "
                                + _holeCount + " unindexed page"
                                + (_holeCount > 1 ? "s" : ""), LOG_NORMAL);
                    }
                }
            }
        } finally {
            _persistit.getLockManager().setDisabled(true);
            tree.release();
        }
        return _faults.size() == initialFaultCount;
    }

    /**
     * Verifies integrity the subtree rooted in the supplied page. If this page
     * has already been visited, then we have some kind of cycle error. If this
     * page is a data page, then we are at the bottom of the tree. Else we
     * recursively checkTree on each of the subordinate pages.
     * 
     * @param page
     * @param level
     * @param work
     * @throws PersistitException
     */
    private void checkTree(Key parentKey, long parent, long page, int level,
            Tree tree) throws PersistitException {
        if (level >= Exchange.MAX_TREE_DEPTH) {
            addFault("Tree is too deep", page, level, 0);
        }
        if (_usedPageBits.get(page)) {
            addFault("Page has more than one parent", page, level, 0);
        }

        if (page == 0) {
            addFault("Page 0 not allowed in tree structure", page, level, 0);
        }

        _usedPageBits.set(page, true);

        Buffer buffer = getPage(page);
        _pagesVisited++;

        try {
            Buffer leftSibling = null;
            Key key;

            if (_edgeBuffers[level] != null) {
                key = _edgeKeys[level];
                leftSibling = walkRight(level, page, key);
                int compare = key.compareTo(parentKey);
                if (compare != 0) {
                    addFault("left sibling final key is "
                            + (compare < 0 ? "less than" : "greater than")
                            + " parent key", page, level, 0);
                }
            } else {
                key = new Key(parentKey);
                _edgeKeys[level] = key;
            }

            if (Debug.ENABLED)
                Debug.$assert(leftSibling != buffer);

            _edgeBuffers[level] = buffer;
            if (leftSibling != null)
                releasePage(leftSibling);
            _edgeKeys[level] = key;

            if (checkPageType(buffer, level, tree)
                    && verifyPage(buffer, page, level, key)) {
                if (buffer.isDataPage()) {
                    if (_depth >= 0 & _depth != level) {
                        addFault("Data page at wrong level", page, level, 0);
                    }
                    _depth = level;
                    _dataPageCount++;
                    _dataBytesInUse += (buffer.getBufferSize()
                            - buffer.getAvailableSize() - Buffer.DATA_PAGE_OVERHEAD);

                    for (int p = Buffer.INITIAL_KEY_BLOCK_START_VALUE;; p += Buffer.KEYBLOCK_LENGTH) {
                        p = buffer.nextLongRecord(_value, p);
                        if (p == -1)
                            break;
                        verifyLongRecord(_value, page, p);
                    }
                } else if (buffer.isIndexPage()) {
                    _indexPageCount++;
                    _indexBytesInUse += (buffer.getBufferSize()
                            - buffer.getAvailableSize() - Buffer.INDEX_PAGE_OVERHEAD);
                    //
                    // Resetting the key because we are going to re-traverse
                    // the same page, now handling each downpointer.
                    //
                    key.clear();
                    //
                    // Here we work our way through the index page.
                    //
                    for (int p = Buffer.INITIAL_KEY_BLOCK_START_VALUE;; p += Buffer.KEYBLOCK_LENGTH) {
                        int foundAt = buffer.nextKey(key, p);
                        // If the exact bit is not set, then next() reached
                        // the end of keyblocks.
                        if ((foundAt & Buffer.EXACT_MASK) == 0)
                            break;
                        long child = buffer.getPointer(foundAt);
                        if (child == -1 && buffer.isAfterRightEdge(foundAt)) {
                            break;
                        }
                        if (child <= 0 || child > Buffer.MAX_VALID_PAGE_ADDR) {
                            addFault("Invalid index pointer value " + child,
                                    page, level, foundAt);
                        }

                        // Recursively check the subtree.
                        checkTree(key, page, child, level + 1, tree);
                    }
                } else {
                    throw new RuntimeException("should never happen!");
                }
            }
            //
            // because the buffer should be left claimed in the
            // _edgeBuffers array in the normal case.
            //
            buffer = null;

            if (Debug.ENABLED)
                Debug.debug0(_edgeBuffers[level].isAvailable(true));
        } finally {
            if (buffer != null) {
                _edgeBuffers[level] = null;
                releasePage(buffer);
            }

        }
    }

    private boolean checkPageType(Buffer buffer, int level, Tree tree) {
        int expectedDepth = tree.getDepth() - level - 1;
        int type = buffer.getPageType();

        if (type != Buffer.PAGE_TYPE_DATA + expectedDepth) {
            addFault("Unexpected page type " + type, buffer.getPageAddress(),
                    level, 0);
            return false;
        } else {
            return true;
        }
    }

    private Buffer walkRight(int level, long toPage, Key key)
            throws PersistitException {
        Buffer startingBuffer = _edgeBuffers[level];
        if (startingBuffer == null)
            return null;
        if (Debug.ENABLED)
            Debug.debug0(startingBuffer.isAvailable(true));
        Buffer buffer = startingBuffer;
        if (buffer.getPageAddress() == toPage) {
            addFault("Overlapping page", toPage, level, 0);
            return startingBuffer;
        }

        int walkCount = MAX_WALK_RIGHT;
        Buffer oldBuffer = null;
        try {
            while (buffer.getRightSibling() != toPage) {
                long page = buffer.getRightSibling();

                if (startingBuffer.getPageAddress() == page) {
                    addFault("Right pointer cycle", page, level, 0);
                    oldBuffer = buffer;
                    return startingBuffer;
                }

                _holeCount++;
                if (_holeCount < MAX_FAULTS) {
                    _holes.add(new Hole(_currentTree, page, level));
                }

                if (page <= 0 || page > Buffer.MAX_VALID_PAGE_ADDR) {
                    addFault("Invalid right sibling address",
                            buffer.getPageAddress(), level, 0);
                    key.clear();
                    oldBuffer = buffer;
                    return startingBuffer;
                }
                if (walkCount-- <= 0) {
                    addFault("More than " + Exchange.MAX_WALK_RIGHT
                            + " unindexed siblings", page, level, 0);
                    key.clear();
                    oldBuffer = buffer;
                    return startingBuffer;
                }
                oldBuffer = buffer;
                buffer = getPage(page);
                if (oldBuffer != startingBuffer) {
                    _persistit.getLockManager().setOffset(1);
                    releasePage(oldBuffer);
                    oldBuffer = null;
                }
                boolean ok = verifyPage(buffer, page, level, key);
                if (!ok) {
                    key.clear();
                    oldBuffer = buffer;
                    return startingBuffer;
                }
                _pagesVisited++;
            }
            if (startingBuffer != buffer)
                releasePage(startingBuffer);
            if (Debug.ENABLED)
                Debug.debug0(buffer.isAvailable(true));

            return buffer;
        } finally {
            if (oldBuffer != null && oldBuffer != startingBuffer) {
                releasePage(oldBuffer);
                oldBuffer = null;
            }
        }
    }

    private boolean verifyPage(Buffer buffer, long page, int level, Key key) {
        if (buffer.getPageAddress() != page) {
            addFault("Buffer contains wrong page " + buffer.getPageAddress(),
                    page, level, 0);
            return false;
        }

        if (buffer.isDataPage() || buffer.isIndexPage()) {
            InvalidPageStructureException ipse = buffer.verify(key);
            if (ipse != null) {
                addFault(ipse.getMessage(), page, level, 0);
                key.clear();
                return false;
            }
        }
        return true;
    }

    private boolean verifyLongRecord(Value value, long page, int foundAt)
            throws PersistitException {
        int size = value.getEncodedSize();
        byte[] bytes = value.getEncodedBytes();
        if (size != Buffer.LONGREC_SIZE) {
            addFault("Invalid long record stub size (" + size + ")", page,
                    _depth, foundAt);
            return false;
        }
        int longSize = Buffer.decodeLongRecordDescriptorSize(bytes, 0);
        long pointer = Buffer.decodeLongRecordDescriptorPointer(bytes, 0);

        if (longSize < Buffer.LONGREC_PREFIX_SIZE) {
            addFault("Invalid long record size (" + longSize + ")", page,
                    _depth, foundAt);
        }
        if (pointer <= 0 || pointer > Buffer.MAX_VALID_PAGE_ADDR) {
            addFault("Invalid long record pointer (" + pointer + ")", page,
                    _depth, foundAt);
        }

        long fromPage = page;
        longSize -= Buffer.LONGREC_PREFIX_SIZE;

        for (long longPage = pointer; longPage != 0;) {
            if (_usedPageBits.get(longPage)) {
                addFault(
                        "Long record page " + longPage + " is multiply linked",
                        page, _depth, foundAt);
                break;
            }
            if (longSize <= 0) {
                addFault("Long record chain too long at page " + longPage
                        + " pointed to by " + fromPage, page, _depth, foundAt);
                break;
            }
            Buffer longBuffer = null;
            try {
                longBuffer = getPage(longPage);
                if (!longBuffer.isLongRecordPage()) {
                    addFault("Invalid long record page " + longPage + ": type="
                            + longBuffer.getPageTypeName(), page, _depth,
                            foundAt);
                    break;
                }
                int segmentSize = longBuffer.getBufferSize()
                        - Buffer.HEADER_SIZE;
                if (segmentSize > longSize)
                    segmentSize = longSize;
                longSize -= segmentSize;

                _longRecordBytesInUse += segmentSize;
                _longRecordPageCount++;

                fromPage = longPage;
                longPage = longBuffer.getRightSibling();
            } finally {
                if (longBuffer != null)
                    releasePage(longBuffer);
            }
        }

        return true;
    }

    private void fixIndexHoles() throws PersistitException {
        Exchange exchange = null;
        Tree tree = null;
        Volume volume = null;
        for (int index = 0; index < _holes.size(); index++) {
            Hole hole = _holes.get(index);
            if (hole._tree != tree) {
                tree = hole._tree;
                volume = tree.getVolume();
                exchange = _persistit
                        .getExchange(volume, tree.getName(), false);
            }
            Key spareKey2 = exchange.getAuxiliaryKey2();
            long page = hole._page;
            int level = _depth - hole._level;
            Buffer buffer = null;

            try {
                buffer = volume.getPool().get(volume, page, false, true);
                buffer.nextKey(spareKey2, buffer.toKeyBlock(0));
                _value.setPointerValue(page);
                _value.setPointerPageType(buffer.getPageType());

                exchange.storeInternal(spareKey2, _value, level + 1, false,
                        true);
            } finally {
                if (buffer != null) {
                    volume.getPool().release(buffer);
                    buffer = null;
                }
            }
        }
    }

    private Buffer getPage(long page) throws PersistitException {
        poll();
        BufferPool pool = _currentTree.getVolume().getPool();
        try {
            Buffer buffer = pool.get(_currentTree.getVolume(), page, false,
                    true);
            return buffer;
        } catch (PersistitException de) {
            throw de;
        }
    }

    private void releasePage(Buffer buffer) {
        BufferPool pool = _currentTree.getVolume().getPool();
        pool.release(buffer);
    }
}
