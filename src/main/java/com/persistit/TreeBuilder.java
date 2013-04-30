/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <li>{@link #afterMergeKey(Exchange)} - customizable behavior after merging
 * one record</li>
 * <li>{@link #beforeSortVolumeClosed(Volume, File)} - customizable behavior
 * before closing a sort volume when full</li>
 * <li>{@link #afterSortVolumeClose(Volume, File)} - customizable behavior after
 * closing a sort volume when full</li>
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
    private final static String SDF = "yyyyMMddHHmm";
    private final static int STREAM_SIZE = 1024 * 1024;

    private final String _name;
    private final long _uniqueId;
    private final Persistit _persistit;
    private final List<File> _directories = new ArrayList<File>();
    private final int _pageSize;
    private final int _pageLimit;
    private final AtomicLong _sortedKeyCount = new AtomicLong();
    private final AtomicLong _mergedKeyCount = new AtomicLong();
    private volatile long _reportKeyCountMultiple = REPORT_REPORT_MULTIPLE;
    private Volume _sortVolume;
    private File _sortFile;

    private final List<Tree> _allTrees = new ArrayList<Tree>();
    private final Map<String, Tree> _sortTreeMap = new HashMap<String, Tree>();

    private int _sortFileIndex;
    private final List<Node> _sortNodes = new ArrayList<Node>();

    private final ThreadLocal<Map<Tree, Exchange>> _sortExchangeMapThreadLocal = new ThreadLocal<Map<Tree, Exchange>>() {
        @Override
        public Map<Tree, Exchange> initialValue() {
            return new HashMap<Tree, Exchange>();
        }
    };

    private final Comparator<Tree> _defaultTreeComparator = new Comparator<Tree>() {
        /**
         * Default implementation returns trees sorted in the order they were
         * added to the _allTrees list - in other words, sorting should leave
         * the list unchanged.
         * 
         * @param a
         * @param b
         * @return
         */
        @Override
        public int compare(final Tree a, final Tree b) {
            if (a == b) {
                return 0;
            }
            return _allTrees.indexOf(a) - _allTrees.indexOf(b);
        }

        @Override
        public boolean equals(final Object obj) {
            return this == obj;
        }
    };

    private class Node implements Comparable<Node> {

        private Tree _tree;
        private Key _key;
        private Value _value;
        private Node _duplicate;
        private final int _precedence;

        private final File _file;
        private StreamLoader _loader;
        private Handler _handler;
        private boolean _next;

        private class Handler extends StreamLoader.ImportHandler {

            private Handler(final Persistit persistit) {
                super(persistit);
            }

            @Override
            protected void handleDataRecord(final Key key, final Value value) throws PersistitException {
                Node.this._tree = super._tree;
                _key = key;
                _value = value;
                _next = true;
            }
        }

        private File getFile() {
            return _file;
        }

        private Node(final File file, final int index) {
            _file = file;
            _precedence = index;
        }

        @Override
        public int compareTo(final Node node) {
            if (_tree == null) {
                return node._tree == null ? 0 : 1;
            }
            if (node._tree == null) {
                return -1;
            }
            if (_tree != node._tree) {
                return _allTrees.indexOf(_tree) - _allTrees.indexOf(node._tree);
            } else
                return _key.compareTo(node._key);
        }

        @Override
        public String toString() {
            Node n = this;
            final StringBuilder sb = new StringBuilder();
            while (n != null) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                if (n._tree == null) {
                    sb.append("<end>");
                } else {
                    sb.append("<" + (n._tree.getName() + n._key + "=" + n._value) + ">");
                }
                n = n._duplicate;
            }
            return sb.toString();
        }

        private void createStreamLoader() throws Exception {
            _loader = new StreamLoader(_persistit, new DataInputStream(new BufferedInputStream(new FileInputStream(
                    _file), STREAM_SIZE)));
            _handler = new Handler(_persistit);
        }

        private boolean next() throws Exception {
            _next = false;
            while (_loader.next(_handler) && !_next)
                ;
            if (!_next) {
                _loader.close();
            }
            return _next;
        }

    }

    private class SortStreamSaver extends StreamSaver {

        Tree _sortTree = null;

        SortStreamSaver(final Persistit persistit, final DataOutputStream stream) {
            super(persistit, stream);
        }

        @Override
        protected void writeData(final Exchange exchange) throws IOException {
            if (exchange.getTree() != _sortTree) {
                final Tree source = _sortTreeMap.get(exchange.getTree().getName());
                if (_lastVolume != source.getVolume()) {
                    writeVolumeInfo(source.getVolume());
                    _lastVolume = source.getVolume();
                }
                if (_lastTree != source) {
                    writeTreeInfo(source);
                    _lastTree = source;
                }
            }
            writeData(exchange.getKey(), exchange.getValue());
            _recordCount++;
        }
    }

    public TreeBuilder(final Persistit persistit) {
        this(persistit, new SimpleDateFormat(SDF).format(new Date()), -1, DEFAULT_BUFFER_POOL_FRACTION);
    }

    public TreeBuilder(final Persistit persistit, final String name, final int pageSize, final float bufferPoolFraction) {
        _name = name;
        _uniqueId = persistit.unique();
        _persistit = persistit;
        _pageSize = pageSize == -1 ? computePageSize(persistit) : pageSize;
        final int bufferCount = _persistit.getBufferPool(_pageSize).getBufferCount();
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

    /**
     * @return Name provide when TreeBuilder was constructed. Default name is
     *         "TreeBuilder".
     */
    public final String getName() {
        return _name;
    }

    /**
     * Set the count of keys inserted or merged per call to
     * {@link #reportSorted(long)} or {@link #reportMerged(long)}.
     * 
     * @param multiple
     */
    public final void setReportKeyCountMultiple(final long multiple) {
        _reportKeyCountMultiple = Util.rangeCheck(multiple, 1, Long.MAX_VALUE);
    }

    /**
     * 
     * @return Count of keys inserted or merged per call to
     *         {@link #reportSorted(long)} or {@link #reportMerged(long)}
     */
    public final long getReportKeyCountMultiple() {
        return _reportKeyCountMultiple;
    }

    /**
     * @return Count of sort trees that have been created while sorting keys
     */
    public final synchronized int getSortFileCount() {
        return _sortFileIndex;
    }

    /**
     * @return Number of keys stored in sort trees
     */
    public long getSortedKeyCount() {
        return _sortedKeyCount.get();
    }

    /**
     * @return Number of keys merged into destination trees
     */
    public long getMergedKeyCount() {
        return _mergedKeyCount.get();
    }

    /**
     * @return List of destination
     *         <code>Tree<code> instances. This list is built as keys are stored.
     */
    public synchronized final List<Tree> getTrees() {
        final List<Tree> list = new ArrayList<Tree>(_allTrees);
        return list;
    }

    /**
     * <p>
     * Define a list of directories in which sort volumes will be created. This
     * method can be used to override the default value provided by
     * {@link Configuration#getTmpVolDir()} to control more closely where sort
     * trees will be stored. If the list is empty then the directory defined by
     * the <code>Configuration</code> will be used. If multiple directories are
     * declared then volumes will be allocated to them in round-robin fashion.
     * This technique can distribute large load sets over multiple volumes and
     * can allow for interleaved disk reads during the merge process.
     * </p>
     * <p>
     * If a <code>File</code> supplied to this method does not exist, an attempt
     * is made to create it as a directory. This method also attempts to create
     * and delete a file in each supplied directory to ensure that if there is a
     * file permission or other problem, it is detected immediately, rather than
     * much later during the sort process.
     * </p>
     * 
     * @param directories
     *            List of <code>File</code> instances, each of which must be a
     *            directory
     * @throws IllegalArgumentException
     *             if a supplied file exists and is not a directory or cannot be
     *             created as a new directory
     * @throws IOException
     *             if an attempt to create a file in one of the supplied
     *             directories fails
     */
    public final void setSortTreeDirectories(final List<File> directories) throws IOException {
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
                _sortFileIndex = 0;
            }
        }
    }

    /**
     * 
     * @return List of directories set via the
     *         {@link #setSortTreeDirectories(List)} method.
     */
    public final List<File> getSortFileDirectories() {
        return Collections.unmodifiableList(_directories);
    }

    /**
     * Store a key-value pair into a sort tree. The {@link Tree}, {@link Key}
     * and {@link Value} are specified by the supplied {@link Exchange}.
     * 
     * @param exchange
     *            The Exchange
     * @throws Exception
     */
    public final void store(final Exchange exchange) throws Exception {
        store(exchange.getTree(), exchange.getKey(), exchange.getValue());
    }

    /**
     * Store a key-value pair for a specified <code>Tree</code> into a sort
     * tree.
     * 
     * @param tree
     *            the Tree
     * @param key
     *            the Key
     * @param value
     *            the Value
     * @throws Exception
     */
    public final void store(final Tree tree, final Key key, final Value value) throws Exception {
        final Map<Tree, Exchange> map = _sortExchangeMapThreadLocal.get();
        Exchange ex = map.get(tree);
        if (ex == null || ex.getTree().getVolume().getNextAvailablePage() > _pageLimit) {
            final Volume newSortVolume = getSortVolume();
            final String tempTreeName = "_" + _persistit.getJournalManager().handleForTree(tree);
            ex = _persistit.getExchange(newSortVolume, tempTreeName, true);
            map.put(tree, ex);
            synchronized (this) {
                if (!_allTrees.contains(tree)) {
                    _allTrees.add(tree);
                    _sortTreeMap.put(tempTreeName, tree);
                }
            }
        }
        key.copyTo(ex.getKey());
        value.copyTo(ex.getValue());

        ex.fetchAndStore();
        boolean stored = true;
        if (ex.getValue().isDefined()) {
            if (!duplicateKeyDetected(ex.getTree(), ex.getKey(), ex.getValue(), value)) {
                stored = false;
                ex.store();
            }
        }
        if (stored) {
            final long count = _sortedKeyCount.incrementAndGet();
            if ((count % _reportKeyCountMultiple) == 0) {
                reportSorted(count);
            }
        }
    }

    private void insertNode(final Map<Node, Node> sorted, final Node node) throws Exception {
        final Node other = sorted.put(node, node);
        if (other != null) {
            final boolean reverse;
            if (node._precedence < other._precedence) {
                reverse = duplicateKeyDetected(node._tree, node._key, node._value, other._value);
            } else {
                reverse = !duplicateKeyDetected(node._tree, node._key, other._value, node._value);
            }
            if (reverse) {
                sorted.put(node, other);
                final Node p = other._duplicate;
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
        finishSortVolume();
        if ((_mergedKeyCount.get() % _reportKeyCountMultiple) != 0) {
            reportSorted(_mergedKeyCount.get());
        }
        Tree currentTree = null;
        Exchange ex = null;
        final SortedMap<Node, Node> sorted = new TreeMap<Node, Node>();

        for (final Node node : _sortNodes) {
            node.createStreamLoader();
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
            if (node._tree != currentTree) {
                ex = new Exchange(node._tree);
                currentTree = node._tree;
            }
            node._key.copyTo(ex.getKey());
            node._value.copyTo(ex.getValue());

            if (beforeMergeKey(ex)) {
                ex.fetchAndStore();
                boolean stored = true;
                if (ex.getValue().isDefined()) {
                    if (!duplicateKeyDetected(ex.getTree(), ex.getKey(), ex.getValue(), node._value)) {
                        ex.store();
                        stored = false;
                    }
                }
                if (stored) {
                    afterMergeKey(ex);
                    if ((_mergedKeyCount.incrementAndGet() % _reportKeyCountMultiple) == 0) {
                        reportMerged(_mergedKeyCount.get());
                    }
                }
            }
            while (node != null) {
                final Node next = node._duplicate;
                node._duplicate = null;
                if (node.next()) {
                    insertNode(sorted, node);
                }
                node = next;
            }
        }
        if ((_mergedKeyCount.get() % _reportKeyCountMultiple) != 0) {
            reportMerged(_mergedKeyCount.get());
        }
        reset();
    }

    private synchronized void reset() throws Exception {
        Exception exception = null;
        try {
            if (_sortVolume != null) {
                _sortVolume.close();
            }
        } catch (final PersistitException e) {
            if (exception == null) {
                exception = e;
            }
        }

        for (final Node node : _sortNodes) {
            try {
                if (node.getFile() != null) {
                    node.getFile().delete();
                }
            } catch (final Exception e) {
                if (exception == null) {
                    exception = e;
                }
            }
        }
        _allTrees.clear();
        _sortNodes.clear();
        _sortVolume = null;
        _sortFileIndex = 0;
        _sortExchangeMapThreadLocal.get().clear();
        if (exception != null) {
            throw exception;
        }
    }

    public void clear() throws Exception {
        _sortedKeyCount.set(0);
        _mergedKeyCount.set(0);
        reset();
    }

    private synchronized Volume getSortVolume() throws Exception {
        if (_sortVolume != null && _sortVolume.getNextAvailablePage() > _pageLimit) {
            finishSortVolume();
        }
        if (_sortVolume == null) {
            final File directory;
            if (_directories.isEmpty()) {
                String directoryName = _persistit.getConfiguration().getTmpVolDir();
                if (directoryName == null) {
                    directoryName = System.getProperty("java.io.tmpdir");
                }
                directory = new File(directoryName);
                if (!directory.exists()) {
                    directory.mkdirs();
                }
                _directories.add(directory);
            } else {
                directory = _directories.get(_sortFileIndex % _directories.size());
            }
            _sortVolume = Volume.createTemporaryVolume(_persistit, _pageSize, directory);
            _sortFile = new File(directory, String.format("%s_%d.%06d", _name, _uniqueId, _sortFileIndex));
            final Node node = new Node(_sortFile, _sortFileIndex);
            _sortNodes.add(node);
            _sortFileIndex++;
        }
        return _sortVolume;
    }

    private void finishSortVolume() throws Exception {
        if (_sortVolume != null) {
            beforeSortVolumeClosed(_sortVolume, _sortFile);
            saveSortVolume(_sortVolume, _sortFile);
            afterSortVolumeClose(_sortVolume, _sortFile);
            _sortVolume.close();
            _sortVolume = null;
        }
    }

    private void saveSortVolume(final Volume volume, final File file) throws Exception {
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
                STREAM_SIZE));
        final List<Tree> sorted = new ArrayList<Tree>(_allTrees);
        Collections.sort(sorted, getTreeComparator());
        final StreamSaver saver = new SortStreamSaver(_persistit, dos);
        for (final Tree tree : sorted) {
            final String sortTreeName = "_" + tree.getHandle();
            final Tree sortTree = volume.getTree(sortTreeName, false);
            if (sortTree != null) {
                final Exchange exchange = new Exchange(sortTree);
                saver.save(exchange, null);
            }
        }
        file.deleteOnExit();
        dos.close();
    }

    /**
     * This method may be extended to provide application-specific behavior when
     * a sort volume has been filled to capacity. Subsequent to this call, the
     * sort volume is streamed to a sort file and then its pages in the
     * <code>BufferPool</code> are invalidated to allow their immediate reuse.
     * 
     * @param volume
     *            The temporary <code>Volume</code> that has been filled
     * @param file
     *            the file to which the sorted key-value pairs will be written
     * @throws Exception
     */
    protected void beforeSortVolumeClosed(final Volume volume, final File file) throws Exception {

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
     * @param file
     *            the file to which the sorted key-value pairs have been written
     * @throws Exception
     */
    protected void afterSortVolumeClose(final Volume volume, final File file) throws Exception {

    }

    /**
     * <p>
     * This method may be extended to provide application-specific behavior when
     * an attempt is made to merge records with duplicate keys. The two
     * <code>Value</code>s v1 and v2 are provided in the order they were
     * inserted into the <code>TreeBuilder</code>. behavior is to write a
     * warning to the log and retain the first value..
     * </p>
     * 
     * @param tree
     *            the <code>Tree</code> to which a key is being merged
     * @param key
     *            the <code>Key</code>
     * @param v1
     *            the <code>Value</code> previously inserted
     * @param v2
     *            the conflicting <code>Value</code>
     * @return <code>true</code> to replace the value previously stored,
     *         <code>false</code> to leave the value first inserted and ignore
     *         the new value.
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

    void unitTestNextSortFile() throws Exception {
        finishSortVolume();
        _sortExchangeMapThreadLocal.get().clear();
        _sortVolume = null;
    }
}
