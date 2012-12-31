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

/**
 * <p>
 * A mechanism for optimizing the process of loading large sets of records with
 * non-sequential keys. This class speeds up the process of inserting records
 * into a set of Persistit <code>Tree</code>s by sorting them before inserting
 * them. The sort process uses multiple "sort trees" in multiple temporary
 * <code>Volume</code>s to hold copies of the data. These are then merged into
 * the final "destination trees." Each sort tree is constrained to be small
 * enough to fit in the {@link BufferPool}.
 * </p>
 * <h3>Background</h3>
 * <p>
 * In general, Persistit can store records very quickly, even when the keys of
 * those records arrive in random order, as long as all the pages of the
 * destination tree or trees are resident in the buffer pool. However, the
 * situation changes dramatically as soon as the the destination tree or trees
 * exceed the size of the buffer pool. Once that happens, insert performance
 * degrades because the ratio of records inserted per disk I/O operation
 * performed decreases. In a worst-case scenario, inserting each new key may
 * require two or more disk I/O operations. These may occur because Persistit
 * performs the following steps:
 * <ul>
 * <li>Look up the key requires reading the page containing that key from disk
 * into the BufferPool.</li>
 * <li>Reading the page requires a Buffer containing some other page to be
 * evicted.</li>
 * <li>The page being evicted is likely to be dirty and therefore Persistit must
 * write its contents to disk before reusing the Buffer.</li>
 * </ul>
 * Further, these disk I/O operations are are usually at unrelated file
 * positions and therefore may each require random seeks. As a result, inserting
 * one key can take orders of magnitude longer once the tree no longer fits in
 * the buffer pool.
 * </p>
 * <p>
 * <code>TreeBuilder</code> mitigates that degradation by sorting the keys
 * before inserting them into their final destination trees. To do so it builds
 * a collection of bounded-size sort trees in temporary volumes. Then it
 * performs a merge sort from those trees into the final destination tree or
 * trees. This mechanism eliminates the problem that every key insertion
 * requires two (or more) random disk I/O operations. However, it is still the
 * case that every sort tree page must be written and read once, and every
 * destination tree page must be written at least once. Therefore the I/O
 * associated with TreeBuilder is reduced but not eliminated.
 * </p>
 * <p>
 * TreeBuilder is effective if and only if (a) the keys arrive in random order,
 * and (b) the data is significantly larger than available memory in the buffer
 * pool. In general it is faster to insert the keys directly into the
 * destination trees unless both of these conditions are true.
 * </p>
 * <h3>Using TreeBuilder</h3>
 * <p>
 * The following example demonstrates the fundamental operation of
 * <code>TreeBuilder</code>: <code><pre>
 *   Exchange exchange = db.getExchange("myVolume", "myTree", true);
 *   TreeBuilder tb = new TreeBuilder(db);
 *   //
 *   // Insert the data into sort trees
 *   //
 *   while (<i>source has more data</i>) {
 *      exchange.to(<i>next key</i>).getValue().put(<i>next value</i>);
 *      tb.store(exchange);
 *   }
 *   //
 *   // Merge the data into myTree
 *   // 
 *   tb.merge();
 * </pre></code> Note that a TreeBuilder can pre-sort data for multiple
 * destination trees. For example, it is possible to load and merge records for
 * a table and its corresponding indexes in one pass using TreeBuilder. During
 * the merge operation the final destination <code>Tree</code> are built in
 * sequence. By default that sequence is by alphabetical order of tree name, but
 * it is possible to customize TreeBuilder to change that order.
 * </p>
 * <p>
 * Loading a large data set may take a long time under the best of
 * circumstances. Therefore this class is designed to be extended by
 * applications to support progress reporting, to control disk space allocation,
 * to handle attempts to insert conflicting records with duplicate keys, etc.
 * See the following methods which may be overridden to provide custom behavior:
 * <ul>
 * <li>{@link #reportSorted(long)} - report completion of N records inserts into
 * sort trees</li>
 * <li>{@link #reportMerged(long)} - report completion of N records merged</li>
 * <li>{@link #duplicateKeyDetected(Tree, Key, Value, Value)} - handle detection
 * of records inserted with duplicate keys</li>
 * <li>{@link #beforeMergeKey(Exchange)} - allowing filtering or custom handling
 * per record while merging</li>
 * <li>{@link #afterMergeKey(Exchange)} - behavior after merging one record</li>
 * <li>{@link #beforeSortVolumeEvicted(Volume)} - behavior before evicting a
 * sort volume when full</li>
 * <li>{@link #afterSortVolumeEvicted(Volume)} - behavior after evicting a sort
 * volume when full</li>
 * <li>{@link #getTreeComparator()} - return a custom Comparator to determine
 * sequence in which trees are populated within the {@link #merge()} method
 * </ul>
 * </p>
 * 
 * @author peter
 * 
 */
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

        private boolean next() throws PersistitException {
            for (;;) {
                if (_exchange == null) {
                    _treeListIndex++;
                    if (_treeListIndex >= _sortedTrees.size()) {
                        _volume.close();
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

    public final String getName() {
        return _name;
    }

    public final void setReportKeyCountMultiple(final long multiple) {
        _reportKeyCountMultiple = Util.rangeCheck(multiple, 1, Long.MAX_VALUE);
    }

    public final long getReportKeyCountMultiple() {
        return _reportKeyCountMultiple;
    }

    public final synchronized int getSortVolumeCount() {
        return _sortVolumes.size();
    }

    public final List<Tree> getTrees() {
        List<Tree> list = new ArrayList<Tree>();
        list.addAll(_allTrees);
        Collections.sort(list, getTreeComparator());
        return list;
    }

    public final void setSortTreeDirectories(List<File> directories) throws Exception {
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

    public final List<File> getSortTreeDirectories() {
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

    /**
     * Merge the record previously stored in sort volumes into their destination
     * <code>Tree</code>s.
     * 
     * @throws Exception
     */
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
        _keyCount.set(0);
        _sortExchangeMapThreadLocal.get().clear();
        _allTrees.clear();
        _sortedTrees.clear();
    }

    private synchronized Volume getSortVolume() throws Exception {
        final boolean full = _currentSortVolume != null && _currentSortVolume.getNextAvailablePage() > _pageLimit;
        if (full) {
            if (beforeSortVolumeEvicted(_currentSortVolume)) {
                _persistit.getBufferPool(_pageSize).evict(_currentSortVolume);
            }
            afterSortVolumeEvicted(_currentSortVolume);
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

    /**
     * This method may be extended to provide an application-specific ordering
     * on <code>Tree</code>s. This ordering determines the sequence in which
     * destination trees are built from the sort data. By default trees are
     * build in alphabetical order by volume and tree name. However, an
     * application may choose a different order to ensure invariants for
     * concurrent use.
     * 
     * @return a <code>java.util.Comparator</code> on <code>Tree</code>
     */
    protected Comparator<Tree> getTreeComparator() {
        return _defaultTreeComparator;
    }

    /**
     * This method may be extended to provide application-specific behavior when
     * a sort volume has been filled to capacity. The default implementation
     * return <code>true</code>. If this method returns <code>true</code>,
     * <code>TreeBuilder</code> evicts the <code>Volume</code> to avoid
     * over-running the <code>BufferPool</code> and then starts a new sort tree
     * if a new record is subsequently stored.
     * 
     * @param volume
     *            The temporary <code>Volume</code> that has been filled
     * @return <code>true</code> to cause the current sort volume to be evicted
     *         from the <code>BufferPool</code>
     * @throws Exception
     */
    protected boolean beforeSortVolumeEvicted(final Volume volume) throws Exception {
        return true;
    }

    /**
     * This method may be extended to provide application-specific reporting
     * functionality after a sort volume has been filled to capacity and has
     * been evicted. An application may also modify the temporary directory set
     * via {@link #setSortTreeDirectories(List)} within this method if necessary
     * to adjust disk space utilization, for example. The default behavior of
     * this method is to do nothing.
     * 
     * @param volume
     *            The temporary <code>Volume</code> that has been filled
     * @throws Exception
     */
    protected void afterSortVolumeEvicted(final Volume volume) throws Exception {

    }

    /**
     * This method may be extended to provide application-specific behavior when
     * an attempt is made to merge records with duplicate keys. The default
     * behavior is to throw a {@link DuplicateKeyException}.
     * 
     * @param tree
     * @param key
     * @param v1
     * @param v2
     * @return
     * @throws DuplicateKeyException
     *             if a key being inserted or merged matches a key already
     *             exists
     * @throws Exception
     */
    protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2)
            throws Exception {
        throw new DuplicateKeyException(String.format("Tree=%s Key=%s", tree, key));
    }

    /**
     * This method may be extended to provide alternative functionality. The
     * default implementation merely returns <code>true</code> which signifies
     * that the key-value pair represented in the <code>Exchange</code> should
     * be merged into the destination <code>Tree</code>. A custom implementation
     * could be used to filter out unwanted records or to emit records to a
     * different destination.
     * 
     * @param exchange
     *            represents the key-value pair proposed for merging
     * @return <code>true</code> to allow the record to be merged
     * @throws Exception
     */
    protected boolean beforeMergeKey(final Exchange exchange) throws Exception {
        return true;
    }

    /**
     * This method may be extended to provide custom behavior after merging one
     * record. The default implementation does nothing. This method is called
     * only if the corresponding call to {@link #beforeMergeKey(Exchange)}
     * returned <code>true</code>.
     * 
     * @param exchange
     *            represents the key-value pair that was merged.
     * @throws Exception
     */
    protected void afterMergeKey(final Exchange exchange) throws Exception {

    }

    /**
     * This method may be extended to provide application-specific progress
     * reports. By default it does nothing. This method is called after
     * inserting a number of records into sort trees. The method
     * {@link #setReportKeyCountMultiple(long)} determines the frequency at
     * which this method is called.
     * 
     * @param count
     *            The total number of recirds that has been merged so far.
     */
    protected void reportSorted(final long count) {

    }

    /**
     * This method may be extended to provide application-specific progress
     * reports. By default it does nothing. This method is called after merging
     * a number of records into destination trees. The method
     * {@link #setReportKeyCountMultiple(long)} determines the frequency at
     * which this method is called.
     * 
     * @param count
     *            The total number of recirds that has been merged so far.
     */
    protected void reportMerged(final long count) {

    }

    void unitTestNextSortVolume() {
        _sortExchangeMapThreadLocal.get().clear();
        _currentSortVolume = null;
    }
}
