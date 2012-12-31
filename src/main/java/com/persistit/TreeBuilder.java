/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.DuplicateKeyException;
import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

public class TreeBuilder {
    private final static float DEFAULT_BUFFER_POOL_FRACTION = 0.5f;
    private final static long REPORT_REPORT_MULTIPLE = 1000000;
    private final static String DEFAULT_NAME = "TreeBuilder";

    private final String _name;
    private final Persistit _persistit;
    private final List<File> _directories = new ArrayList<File>();
    private final List<Volume> _sortVolumes = new ArrayList<Volume>();
    private final int _pageSize;
    private final int _pageLimit;
    private AtomicLong _keyCount = new AtomicLong();
    private volatile long _reportKeyCountMultiple = REPORT_REPORT_MULTIPLE;

    private final Set<Tree> _allTrees = new HashSet<Tree>();
    private final List<Tree> _sortedTrees = new ArrayList<Tree>();

    private final ThreadLocal<Map<Tree, Exchange>> _sortExchangeMapThreadLocal = new ThreadLocal<Map<Tree, Exchange>>() {
        @Override
        public Map<Tree, Exchange> initialValue() {
            return new HashMap<Tree, Exchange>();
        }
    };

    private Volume _currentSortVolume;
    private int _nextDirectoryIndex;
    private Comparator<Tree> _defaultTreeComparator = new Comparator<Tree>() {
        @Override
        public int compare(Tree a, Tree b) {
            if (a == b) {
                return 0;
            }
            if (a.getVolume() == b.getVolume()) {
                return a.getName().compareTo(b.getName());
            } else {
                return a.getVolume().getName().compareTo(b.getVolume().getName());
            }
        }

        public boolean equals(Object obj) {
            return this == obj;
        }
    };

    private class Node implements Comparable<Node> {
        final Volume _volume;
        int _treeListIndex = -1;
        Exchange _exchange = null;
        Tree _currentTree = null;
        Node _duplicate;

        private Node(final Volume volume) {
            _volume = volume;
        }

        @Override
        public int compareTo(final Node node) {
            if (_exchange == null) {
                return node._exchange == null ? 0 : -1;
            }
            int treeComparison = getTreeComparator().compare(_currentTree, node._currentTree);
            if (treeComparison != 0) {
                return treeComparison;
            }
            final Key k1 = _exchange.getKey();
            final Key k2 = node._exchange.getKey();
            return k1.compareTo(k2);
        }

        public boolean next() throws PersistitException {
            for (;;) {
                if (_exchange == null) {
                    _treeListIndex++;
                    if (_treeListIndex >= _sortedTrees.size()) {
                        return false;
                    }
                    final String tempTreeName = "_" + _sortedTrees.get(_treeListIndex).getHandle();
                    final Tree sortTree = _volume.getTree(tempTreeName, false);
                    if (sortTree == null) {
                        continue;
                    }
                    _exchange = new Exchange(sortTree);
                    _currentTree = _sortedTrees.get(_treeListIndex);
                }
                if (_exchange.next(true)) {
                    return true;
                }
                _exchange = null;
            }
        }

        @Override
        public String toString() {
            Node n = this;
            StringBuilder sb = new StringBuilder();
            while (n != null) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                if (n._exchange == null) {
                    sb.append("<null>");
                } else {
                    sb.append("<"
                            + (n._currentTree == null ? "?" : n._currentTree.getName() + n._exchange.getKey() + "="
                                    + n._exchange.getValue()) + ">");
                }
                n = n._duplicate;
            }
            return sb.toString();
        }
    }

    public TreeBuilder(final Persistit persistit) {
        this(persistit, DEFAULT_NAME, -1, DEFAULT_BUFFER_POOL_FRACTION);
    }

    public TreeBuilder(final Persistit persistit, final String name, final int pageSize, final float bufferPoolFraction) {
        _name = name;
        _persistit = persistit;
        _pageSize = pageSize == -1 ? computePageSize(persistit) : pageSize;
        int bufferCount = _persistit.getBufferPool(_pageSize).getBufferCount();
        _pageLimit = (int) (bufferCount * bufferPoolFraction);
    }

    private int computePageSize(final Persistit persistit) {
        int pageSize = persistit.getConfiguration().getTmpVolPageSize();
        if (pageSize == 0) {
            for (final int size : persistit.getBufferPoolHashMap().keySet()) {
                if (size > pageSize) {
                    pageSize = size;
                }
            }
        }
        return pageSize;
    }

    public void close() {
        _sortExchangeMapThreadLocal.get().clear();
    }

    public String getName() {
        return _name;
    }

    public void setReportKeyCountMultiple(final long multiple) {
        _reportKeyCountMultiple = Util.rangeCheck(multiple, 1, Long.MAX_VALUE);
    }

    public long getReportKeyCountMultiple() {
        return _reportKeyCountMultiple;
    }
    
    public synchronized int getSortVolumeCount() {
        return _sortVolumes.size();
    }

    public List<Tree> getTrees() {
        List<Tree> list = new ArrayList<Tree>();
        list.addAll(_allTrees);
        Collections.sort(list, getTreeComparator());
        return list;
    }

    public Comparator<Tree> getTreeComparator() {
        return _defaultTreeComparator;
    }

    public void setSortTreeDirectories(List<File> directories) throws Exception {
        if (directories == null || directories.isEmpty()) {
            synchronized (this) {
                _directories.clear();
            }
        } else {
            /*
             * Make sure all supplied items are directories
             */
            for (final File file : directories) {
                if (file.exists() && !file.isDirectory()) {
                    throw new IllegalArgumentException(file + " is not a directory");
                }
            }
            /*
             * Make sure all directories exist
             */
            for (final File file : directories) {
                if (!file.exists() && !file.mkdirs()) {
                    throw new IllegalArgumentException(file + " could not be created as a new directory");
                }
            }
            /*
             * Make sure all directories permit creation of a new file
             */
            for (final File file : directories) {
                final File temp = File.createTempFile(VolumeStorageT2.TEMP_FILE_PREFIX, null, file);
                temp.delete();
            }
            synchronized (this) {
                _directories.clear();
                _directories.addAll(directories);
                _nextDirectoryIndex = 0;
            }
        }
    }

    public List<File> getSortTreeDirectories() {
        return Collections.unmodifiableList(_directories);
    }

    public final void store(final Exchange exchange) throws Exception {
        store(exchange.getTree(), exchange.getKey(), exchange.getValue());
    }

    public final void store(final Tree tree, final Key key, final Value value) throws Exception {
        final Map<Tree, Exchange> map = _sortExchangeMapThreadLocal.get();
        Exchange ex = map.get(tree);
        if (ex == null || ex.getTree().getVolume().getNextAvailablePage() > _pageLimit) {
            final Volume newSortVolume = getSortVolume();
            String tempTreeName = "_" + _persistit.getJournalManager().handleForTree(tree);
            ex = _persistit.getExchange(newSortVolume, tempTreeName, true);
            map.put(tree, ex);
            synchronized (this) {
                _allTrees.add(tree);
            }
        }
        key.copyTo(ex.getKey());
        value.copyTo(ex.getValue());
        ex.fetchAndStore();
        if (ex.getValue().isDefined()) {
            if (!duplicateKeyDetected(tree, key, ex.getValue(), value)) {
                /*
                 * Restore original value if replacement is denied
                 */
                ex.store();
            }
        } else {
            final long count = _keyCount.incrementAndGet();
            if ((count % _reportKeyCountMultiple) == 0) {
                reportSorted(count);
            }
        }
    }

    private void insertNode(final Map<Node, Node> sorted, final Node node) throws Exception {
        final Node other = sorted.put(node, node);
        if (other != null) {
            if (!duplicateKeyDetected(node._currentTree, node._exchange.getKey(), other._exchange.getValue(),
                    node._exchange.getValue())) {
                sorted.put(node, other);
                Node p = other._duplicate;
                other._duplicate = node;
                node._duplicate = p;
            } else {
                node._duplicate = other;
            }
        }
    }

    public synchronized void merge() throws Exception {
        if ((_keyCount.get() % _reportKeyCountMultiple) != 0) {
            reportSorted(_keyCount.get());
        }
        _keyCount.set(0);
        _sortedTrees.clear();
        _sortedTrees.addAll(_allTrees);
        Tree currentTree = null;
        Exchange currentExchange = null;
        Collections.sort(_sortedTrees, getTreeComparator());
        final SortedMap<Node, Node> sorted = new TreeMap<Node, Node>();
        for (final Volume volume : _sortVolumes) {
            final Node node = new Node(volume);
            if (node.next()) {
                insertNode(sorted, node);
            }
        }

        for (;;) {
            if (sorted.isEmpty()) {
                break;
            }
            Node node = sorted.firstKey();
            node = sorted.remove(node);
            if (node._currentTree != currentTree) {
                currentExchange = new Exchange(node._currentTree);
                currentTree = node._currentTree;
            }

            node._exchange.getKey().copyTo(currentExchange.getKey());
            node._exchange.getValue().copyTo(currentExchange.getValue());
            if (beforeMergeKey(currentExchange)) {
                currentExchange.store();
                afterMergeKey(currentExchange);
                if ((_keyCount.incrementAndGet() % _reportKeyCountMultiple) == 0) {
                    reportMerged(_keyCount.get());
                }
            }
            while (node != null) {
                Node next = node._duplicate;
                node._duplicate = null;
                if (node.next()) {
                    insertNode(sorted, node);
                }
                node = next;
            }
        }
        if ((_keyCount.get() % _reportKeyCountMultiple) != 0) {
            reportMerged(_keyCount.get());
        }
    }

    private synchronized Volume getSortVolume() throws Exception {
        final boolean full = _currentSortVolume != null && _currentSortVolume.getNextAvailablePage() > _pageLimit;
        if (full) {
            sortVolumeFull(_currentSortVolume);
            _persistit.getBufferPool(_pageSize).evict(_currentSortVolume);
        }
        if (full || _currentSortVolume == null) {
            _currentSortVolume = createSortVolume();
            _sortVolumes.add(_currentSortVolume);
        }
        return _currentSortVolume;
    }

    private Volume createSortVolume() throws Exception {
        final File directory;
        if (_directories.isEmpty()) {
            final String directoryName = _persistit.getConfiguration().getTmpVolDir();
            directory = directoryName == null ? null : new File(directoryName);
        } else {
            directory = _directories.get(_nextDirectoryIndex % _directories.size());
            _nextDirectoryIndex++;
        }
        return Volume.createTemporaryVolume(_persistit, _pageSize, directory);
    }

    protected void sortVolumeFull(final Volume volume) throws Exception {

    }

    protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2)
            throws Exception {
        throw new DuplicateKeyException(String.format("Tree=%s Key=%s", tree, key));
    }

    protected boolean beforeMergeKey(final Exchange exchange) throws Exception {
        return true;
    }

    protected void afterMergeKey(final Exchange exchange) throws Exception {

    }

    protected void reportSorted(final long count) {

    }

    protected void reportMerged(final long count) {

    }

    void unitTestNextSortVolume() {
        _sortExchangeMapThreadLocal.get().clear();
        _currentSortVolume = null;
    }
}
