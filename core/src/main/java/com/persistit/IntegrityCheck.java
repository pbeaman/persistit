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

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.TimeoutException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

import static com.persistit.Exchange.FetchOpt;
import static com.persistit.Exchange.MvccOpt;
import static com.persistit.Exchange.WaitOpt;

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

    private Volume _currentVolume;
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
    private long[] _edgePages = new long[Exchange.MAX_TREE_DEPTH];
    private int[] _edgePositions = new int[Exchange.MAX_TREE_DEPTH];
    private Key[] _edgeKeys = new Key[Exchange.MAX_TREE_DEPTH];
    private int _depth = -1;

    private TreeSelector _treeSelector;
    private boolean _suspendUpdates;
    private boolean _fixHoles;

    private ArrayList<Fault> _faults = new ArrayList<Fault>();
    private ArrayList<Hole> _holes = new ArrayList<Hole>();
    private int _holeCount;

    // Used in checking long values
    private Value _value = new Value((Persistit) null);

    @Cmd("icheck")
    static Task icheck(@Arg("trees|string|Tree selector: Volumes/Trees to check") String treeSelectorString,
            @Arg("_flag|r|Use regex expression") boolean regex,
            @Arg("_flag|u|Don't freeze updates (Default is to freeze updates)") boolean dontSuspendUpdates,
            @Arg("_flag|h|Fix index holes") boolean fixHoles, @Arg("_flag|v|Verbose results") boolean verbose)
            throws Exception {
        final IntegrityCheck task = new IntegrityCheck();
        task._treeSelector = TreeSelector.parseSelector(treeSelectorString, regex, '\\');
        task._fixHoles = fixHoles;
        task._suspendUpdates = !dontSuspendUpdates;
        task.setMessageLogVerbosity(verbose ? LOG_VERBOSE : LOG_NORMAL);
        return task;
    }

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
    protected void runTask() {
        boolean freeze = !_persistit.isUpdateSuspended() && (_suspendUpdates || _fixHoles);
        boolean needsToDrain = false;
        if (freeze) {
            _persistit.setUpdateSuspended(true);
            needsToDrain = true;
        }

        try {
            ArrayList<Volume> volumes = new ArrayList<Volume>();
            long _totalPages = 0;

            for (final Volume volume : _persistit.getVolumes()) {
                if (_treeSelector.isSelected(volume)) {
                    volumes.add(volume);
                    _totalPages += volume.getStorage().getNextAvailablePage();
                }
            }

            Volume previousVolume = null;
            for (final Tree tree : _persistit.getSelectedTrees(_treeSelector)) {
                Volume volume = tree.getVolume();
                boolean checkWholeVolume = false;
                if (volume != previousVolume) {
                    initialize(false);
                    if (tree == volume.getDirectoryTree()) {
                        checkWholeVolume = true;
                    }
                }
                previousVolume = volume;
                try {
                    if (needsToDrain) {
                        needsToDrain = false;
                        //
                        // Delay for a few seconds to allow
                        // completion of any update operations
                        // currently in progress. We do this only if
                        // we are going to fix holes.
                        //
                        Thread.sleep(3000);
                    }
                    postMessage("Checking " + tree.getName() + " in " + volume.getPath(), LOG_VERBOSE);
                    long saveIndexPageCount = _indexPageCount;
                    long saveIndexBytesInUse = _indexBytesInUse;
                    long saveDataPageCount = _dataPageCount;
                    long saveDataBytesInUse = _dataBytesInUse;
                    long saveLongRecordPageCount = _longRecordPageCount;
                    long saveLongRecordBytesInUse = _longRecordBytesInUse;
                    boolean okay = checkWholeVolume ? checkVolume(volume) : checkTree(tree);
                    appendMessage(inUseInfo((_indexPageCount - saveIndexPageCount),
                            (_indexBytesInUse - saveIndexBytesInUse), (_dataPageCount - saveDataPageCount),
                            (_dataBytesInUse - saveDataBytesInUse), (_longRecordPageCount - saveLongRecordPageCount),
                            (_longRecordBytesInUse - saveLongRecordBytesInUse)), LOG_VERBOSE);

                    appendMessage(okay ? " - OKAY" : " - FAULTS", LOG_VERBOSE);
                } catch (PersistitException pe) {
                    postMessage(pe.toString(), LOG_NORMAL);
                } catch (InterruptedException ie) {
                    throw new PersistitInterruptedException(ie);
                }
            }
            _currentVolume = null;
            _currentTree = null;
            postMessage(toString(), LOG_NORMAL);
        } catch (PersistitException e) {
            postMessage(e.toString(), LOG_NORMAL);
        } finally {
            if (freeze) {
                _persistit.setUpdateSuspended(false);
            }
        }
    }

    private String resourceName() {
        return _currentTree == null ? _currentVolume.getName() : _currentVolume.getName() + "/"
                + _currentTree.getName();
    }

    private void addFault(String description, long page, int level, int position) {
        Fault fault = new Fault(resourceName(), this, description, page, level, position);
        if (_faults.size() < MAX_FAULTS)
            _faults.add(fault);
        postMessage(fault.toString(), LOG_VERBOSE);
    }

    private void initTree(Tree tree) {
        _currentVolume = tree.getVolume();
        _currentTree = tree;
        _holeCount = 0;
        _holes.clear();
        for (int index = Exchange.MAX_TREE_DEPTH; --index >= 0;) {
            _edgeBuffers[index] = null;
            _edgePages[index] = 0;
            _edgeKeys[index] = null;
            _edgePositions[index] = 0;
            _depth = -1;
        }
    }

    /**
     * Indicate mode of operation. If <code>true</code> then all threads
     * attempting to perform updates are blocked while the integrity check is in
     * progress.
     * 
     * @return <code>true</code> if updates are suspended
     */
    public boolean isSuspendUpdates() {
        return _suspendUpdates;
    }

    /**
     * Control mode of operation. If <code>true</code> then all threads
     * attempting to perform updates are blocked while the integrity check is in
     * progress.
     * 
     * @param suspendUpdates
     *            <code>true</code> to suspend updates
     */
    public void setSuspendUpdates(boolean suspendUpdates) {
        _suspendUpdates = suspendUpdates;
    }

    /**
     * Indicate whether missing index pages should be added when an index "hole"
     * is discovered.
     * 
     * @return <code>true</code> if index IntegrityCheck will attempt to fix
     *         holes.
     */
    public boolean isFixHoles() {
        return _fixHoles;
    }

    /**
     * Control whether missing index pages should be added when an index "hole"
     * is discovered.
     * 
     * @param fixHoles
     *            <code>true</code> to attempt to fix holes
     */
    public void setFixHoles(boolean fixHoles) {
        _fixHoles = fixHoles;
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
        return _faults.toArray(new Fault[_faults.size()]);
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
        if (_totalPages == 0) {
            return 1;
        } else {
            return ((double) _pagesVisited) / ((double) _totalPages);
        }
    }

    @Override
    public String getStatusDetail() {
        if (_faults.isEmpty()) {
            return getStatus() + " - no faults";
        }
        StringBuilder sb = new StringBuilder();
        for (final Fault fault : _faults) {
            sb.append(fault);
            sb.append(Util.NEW_LINE);
        }
        sb.append(getStatus());
        return sb.toString();
    }

    /**
     * Returns a displayable String indicating the tree currently being checked
     * and the relative counts of pages that have been traversed.
     * 
     * @return The progress description
     */
    @Override
    public String getStatus() {
        if (_currentVolume == null)
            return null;
        else
            return _pagesVisited + "/" + _totalPages + " (" + resourceName() + ")";
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
        sb.append(inUseInfo(_indexPageCount, _indexBytesInUse, _dataPageCount, _dataBytesInUse, _longRecordPageCount,
                _longRecordBytesInUse));

        if (details) {
            for (int index = 0; index < _faults.size(); index++) {
                sb.append(Util.NEW_LINE);
                sb.append("        ");
                sb.append(_faults.get(index));
            }
        }
        return sb.toString();
    }

    private String inUseInfo(long indexPageCount, long indexBytesInUse, long dataPageCount, long dataBytesInUse,
            long longRecordPageCount, long longRecordBytesInUse) {
        return String.format(" Index: %,d pages / %,d bytes, Data: %,d pages / %,d bytes,"
                + " Long Record: %,d pages / %,d bytes", indexPageCount, indexBytesInUse, dataPageCount,
                dataBytesInUse, longRecordPageCount, longRecordBytesInUse);
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

        Fault(String treeName, IntegrityCheck work, String description, long page, int level, int position) {
            _treeName = treeName;
            _description = description;
            _path = new long[level + 1];
            for (int index = 0; index <= level; index++) {
                if (index >= work._edgeBuffers.length) {
                    _path[index] = 0;
                } else {
                    _path[index] = work._edgePages[index];
                }
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
                throw new RuntimeException("Large page addresses not implemented yet.");
            }
            if (value)
                _bitSet.set((int) index);
            else
                _bitSet.clear((int) index);
        }

        public boolean get(long index) {
            if (index > Integer.MAX_VALUE) {
                throw new RuntimeException("Large page addresses not implemented yet.");
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
        _currentVolume = null;
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
        _currentVolume = volume;
        String[] treeNames = volume.getTreeNames();
        // This is just for the progress counter.
        _totalPages = volume.getStorage().getNextAvailablePage();
        Tree directoryTree = volume.getDirectoryTree();
        if (directoryTree != null) {
            checkTree(directoryTree);
        }
        for (int index = 0; index < treeNames.length; index++) {
            Tree tree = volume.getTree(treeNames[index], false);
            if (tree != null)
                checkTree(tree);
        }
        final long garbageRoot = volume.getStructure().getGarbageRoot();
        checkGarbage(garbageRoot);
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
        try {
            while (again) {
                try {
                    again = false;
                    initTree(tree);
                    checkTree(new Key(_persistit), 0, tree.getRootPageAddr(), 0, tree);
                } finally {
                    //
                    // Release all the buffers.
                    //
                    for (int index = Exchange.MAX_TREE_DEPTH; --index >= 0;) {
                        Buffer buffer = _edgeBuffers[index];
                        if (buffer != null) {
                            buffer.release();
                            _edgeBuffers[index] = null;
                            _edgePages[index] = 0;
                        }
                    }
                    _currentTree = null;
                }
                if (_holeCount > 0) {
                    if (_fixHoles) {
                        postMessage("Fixing " + _holes.size() + " unindexed page" + (_holeCount > 1 ? "s" : "")
                                + " in tree " + tree.getName() + " in volume " + tree.getVolume().getPath(), LOG_NORMAL);

                        fixIndexHoles();

                        if (_holeCount > _holes.size()) {
                            again = true;
                        }
                    } else {
                        postMessage("Tree " + tree.getName() + " in volume " + tree.getVolume().getPath() + " has "
                                + _holeCount + " unindexed page" + (_holeCount > 1 ? "s" : ""), LOG_NORMAL);
                    }
                }
            }
        } finally {
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
    private void checkTree(Key parentKey, long parent, long page, int level, Tree tree) throws PersistitException {
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
            if (parent == 0 && buffer.getRightSibling() != 0) {
                addFault("Tree root has a right sibling", page, 0, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Buffer leftSibling = null;
            Key key;

            if (_edgeBuffers[level] != null) {
                key = _edgeKeys[level];
                leftSibling = walkRight(level, page, key);
                int compare = key.compareTo(parentKey);
                if (compare != 0) {
                    addFault("left sibling final key is " + (compare < 0 ? "less than" : "greater than")
                            + " parent key", page, level, 0);
                }
            } else {
                key = new Key(parentKey);
                _edgeKeys[level] = key;
            }

            Debug.$assert0.t(leftSibling != buffer);

            _edgeBuffers[level] = buffer;
            _edgePages[level] = page;
            if (leftSibling != null) {
                leftSibling.release();
            }
            _edgeKeys[level] = key;

            if (checkPageType(buffer, level, tree) && verifyPage(buffer, page, level, key)) {
                if (buffer.isDataPage()) {
                    if (_depth >= 0 & _depth != level) {
                        addFault("Data page at wrong level", page, level, 0);
                    }
                    _depth = level;
                    _dataPageCount++;
                    _dataBytesInUse += (buffer.getBufferSize() - buffer.getAvailableSize() - Buffer.DATA_PAGE_OVERHEAD);

                    for (int p = Buffer.KEY_BLOCK_START;; p += Buffer.KEYBLOCK_LENGTH) {
                        p = buffer.nextLongRecord(_value, p);
                        if (p == -1)
                            break;
                        verifyLongRecord(_value, page, p);
                    }
                } else if (buffer.isIndexPage()) {
                    _indexPageCount++;
                    _indexBytesInUse += (buffer.getBufferSize() - buffer.getAvailableSize() - Buffer.INDEX_PAGE_OVERHEAD);
                    //
                    // Resetting the key because we are going to re-traverse
                    // the same page, now handling each downpointer.
                    //
                    key.clear();
                    //
                    // Here we work our way through the index page.
                    //
                    for (int p = Buffer.KEY_BLOCK_START;; p += Buffer.KEYBLOCK_LENGTH) {
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
                            addFault("Invalid index pointer value " + child, page, level, foundAt);
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

            Debug.$assert0.t(!_edgeBuffers[level].isAvailable(true));
        } finally {
            if (buffer != null) {
                _edgeBuffers[level] = null;
                _edgePages[level] = 0;
                buffer.release();
            }

        }
    }

    private void checkGarbage(final long garbageRootPage) throws PersistitException {
        long garbagePageAddress = garbageRootPage;
        boolean first = true;
        while (garbagePageAddress != 0) {
            Buffer garbageBuffer = getPage(garbagePageAddress);
            if (first) {
                _edgePages[0] = garbagePageAddress;
                first = false;
            }
            checkGarbagePage(garbageBuffer);
            _pagesVisited++;
            garbagePageAddress = garbageBuffer.getRightSibling();
            garbageBuffer.release();
        }
        _edgePages[0] = 0;
    }

    private void checkGarbagePage(Buffer garbageBuffer) throws PersistitException {
        long page = garbageBuffer.getPageAddress();
        if (!garbageBuffer.isGarbagePage()) {
            addFault("Unexpected page type " + garbageBuffer.getPageType() + " expected a garbage page", page, 1, 0);
            return;
        }
        if (_usedPageBits.get(page)) {
            addFault("Garbage page is referenced by multiple parents", page, 1, 0);
            return;
        }

        int next = garbageBuffer.getAlloc();
        int size = garbageBuffer.getBufferSize();
        int count = (size - next) / Buffer.GARBAGE_BLOCK_SIZE;
        if (count * Buffer.GARBAGE_BLOCK_SIZE != (size - next)) {
            addFault("Garbage page is malformed: _alloc=" + next + " is not at a multiple of "
                    + Buffer.GARBAGE_BLOCK_SIZE + " bytes", page, 1, 0);
        }
        _usedPageBits.set(page, true);
        _edgePages[1] = page;
        for (int p = garbageBuffer.getAlloc(); p < garbageBuffer.getBufferSize(); p += Buffer.GARBAGE_BLOCK_SIZE) {
            long left = garbageBuffer.getGarbageChainLeftPage(next);
            long right = garbageBuffer.getGarbageChainRightPage(next);
            _edgePositions[1] = p;
            checkGarbageChain(left, right);
        }
        _edgePages[1] = 0;
    }

    private void checkGarbageChain(long left, long right) throws PersistitException {
        long page = left;
        _edgePages[2] = page;
        while (page != 0 && page != right) {
            if (_usedPageBits.get(page)) {
                addFault("Page on garbage chain is referenced by multiple parents", page, 3, 0);
                return;
            }
            Buffer buffer = getPage(page);
            if (!buffer.isDataPage() && !buffer.isIndexPage() && !buffer.isLongRecordPage()) {
                addFault("Page of type " + buffer.getPageTypeName() + " found on garbage page", page, 3, 0);
            }
            _pagesVisited++;
            page = buffer.getRightSibling();
            buffer.release();
        }
        _edgePages[2] = 0;
    }

    private boolean checkPageType(Buffer buffer, int level, Tree tree) {
        int expectedDepth = tree.getDepth() - level - 1;
        int type = buffer.getPageType();

        if (type != Buffer.PAGE_TYPE_DATA + expectedDepth) {
            addFault("Unexpected page type " + type, buffer.getPageAddress(), level, 0);
            return false;
        } else {
            return true;
        }
    }

    private Buffer walkRight(int level, long toPage, Key key) throws PersistitException {
        Buffer startingBuffer = _edgeBuffers[level];
        if (startingBuffer == null)
            return null;
        Debug.$assert0.t(!startingBuffer.isAvailable(true));
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
                    addFault("Invalid right sibling address", buffer.getPageAddress(), level, 0);
                    key.clear();
                    oldBuffer = buffer;
                    return startingBuffer;
                }
                if (walkCount-- <= 0) {
                    addFault("More than " + Exchange.MAX_WALK_RIGHT + " unindexed siblings", page, level, 0);
                    key.clear();
                    oldBuffer = buffer;
                    return startingBuffer;
                }
                oldBuffer = buffer;
                buffer = getPage(page);
                if (oldBuffer != startingBuffer) {
                    oldBuffer.release();
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
            if (startingBuffer != buffer) {
                startingBuffer.release();
            }
            Debug.$assert0.t(!buffer.isAvailable(true));

            return buffer;
        } finally {
            if (oldBuffer != null && oldBuffer != startingBuffer) {
                oldBuffer.release();
                oldBuffer = null;
            }
        }
    }

    private boolean verifyPage(Buffer buffer, long page, int level, Key key) {
        if (buffer.getPageAddress() != page) {
            addFault("Buffer contains wrong page " + buffer.getPageAddress(), page, level, 0);
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

    private boolean verifyLongRecord(Value value, long page, int foundAt) throws PersistitException {
        int size = value.getEncodedSize();
        byte[] bytes = value.getEncodedBytes();
        if (size != Buffer.LONGREC_SIZE) {
            addFault("Invalid long record stub size (" + size + ")", page, _depth, foundAt);
            return false;
        }
        int longSize = Buffer.decodeLongRecordDescriptorSize(bytes, 0);
        long pointer = Buffer.decodeLongRecordDescriptorPointer(bytes, 0);

        if (longSize < Buffer.LONGREC_PREFIX_SIZE) {
            addFault("Invalid long record size (" + longSize + ")", page, _depth, foundAt);
        }
        if (pointer <= 0 || pointer > Buffer.MAX_VALID_PAGE_ADDR) {
            addFault("Invalid long record pointer (" + pointer + ")", page, _depth, foundAt);
        }

        long fromPage = page;
        longSize -= Buffer.LONGREC_PREFIX_SIZE;

        for (long longPage = pointer; longPage != 0;) {
            if (_usedPageBits.get(longPage)) {
                addFault("Long record page " + longPage + " is multiply linked", page, _depth, foundAt);
                break;
            }
            _usedPageBits.set(longPage, true);
            if (longSize <= 0) {
                addFault("Long record chain too long at page " + longPage + " pointed to by " + fromPage, page, _depth,
                        foundAt);
                break;
            }
            Buffer longBuffer = null;
            try {
                longBuffer = getPage(longPage);
                if (!longBuffer.isLongRecordPage()) {
                    addFault("Invalid long record page " + longPage + ": type=" + longBuffer.getPageTypeName(), page,
                            _depth, foundAt);
                    break;
                }
                int segmentSize = longBuffer.getBufferSize() - Buffer.HEADER_SIZE;
                if (segmentSize > longSize)
                    segmentSize = longSize;
                longSize -= segmentSize;

                _longRecordBytesInUse += segmentSize;
                _longRecordPageCount++;

                fromPage = longPage;
                longPage = longBuffer.getRightSibling();
            } finally {
                if (longBuffer != null)
                    longBuffer.release();
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
                exchange = _persistit.getExchange(volume, tree.getName(), false);
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

                exchange.storeInternal(spareKey2, _value, level + 1, FetchOpt.NO_FETCH, MvccOpt.NO_MVCC, WaitOpt.NO_WAIT, false);
            } finally {
                if (buffer != null) {
                    buffer.release();
                    buffer = null;
                }
            }
        }
    }

    private Buffer getPage(long page) throws PersistitException {
        poll();
        BufferPool pool = _currentVolume.getPool();
        try {
            Buffer buffer = pool.get(_currentVolume, page, false, true);
            return buffer;
        } catch (PersistitException de) {
            throw de;
        }
    }
}
