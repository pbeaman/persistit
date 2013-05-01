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

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Accumulate and hold information about the distribution of {@link Key} objects
 * in a Persistit {@link Tree}. This class is used by the
 * {@link Exchange#computeHistogram(Key, Key, int, int, KeyFilter, int)} method
 * to accumulate and return the result of scanning all the keys at a fixed depth
 * within a Tree.
 * <p>
 * </p>
 * The result is represented by a List of {@link KeyCount} objects, each
 * representing a key and a count. The count represents the number of smaller
 * keys in the tree level. With this information client applications can
 * estimate the number of elements between any two keys in the Tree.
 * <p>
 * </p>
 * Application code can specify a <code>keyDepth</code> at which sibling keys
 * are grouped together. For example, suppose a Tree contains keys such as
 * 
 * <pre>
 *   {"BLUE",1}
 *   {"BLUE",2}
 *   {"BLUE",3}
 *   {"RED",1}
 *   {"RED",2}
 * </pre>
 * 
 * If <code>keyDepth=2</code> the resulting histogram will have 5 buckets, each
 * with a <code>count<code> value of 1.  But if <code>keyDepth=1</code> the
 * result will have two buckets, one each for "BLUE" and "RED". Specifying
 * <code>keyDepth=0</code> turns off aggregation by partial key depth.
 * <p>
 * </p>
 * During the aggregation process the {@link Exchange#computeHistogram} method
 * invokes the {@link #addKeyCopy} method for each Key it traverses. Each key is
 * analyzed to determine whether the first <code>keyDepth</code> segments are
 * the same as the previously added Key. If so then the previous count is
 * incremented; otherwise a new KeyCount entry is added to the sample list.
 * <p>
 * </p>
 * 
 * 
 * @author peter
 * 
 */
public class KeyHistogram {

    private final Tree _tree;

    private final Key _startKey;

    private final Key _endKey;

    private final int _requestedSampleSize;

    private final int _treeDepth;

    private final int _keyDepth;

    private final List<KeyCount> _keys = new ArrayList<KeyCount>();

    private int _modulus = 1;

    private int _keyCount = 0;

    private int _pageCount = 0;

    private long _pageBytesTotal = 0;

    private long _pageBytesInUse = 0;

    /**
     * Element in a <code>KeyHistogram</code> that denotes the estimated number
     * of keys in a histogram bucket. This class has two fields representing a
     * key and a count. The count indicates the estimated number of other keys
     * less than this one in the tree level.
     */
    public static class KeyCount {

        final byte[] _bytes;

        int _count;

        private KeyCount(final byte[] bytes, final int count) {
            _bytes = bytes;
            _count = count;
        }

        /**
         * Get the key bytes
         * 
         * @return the bytes of the key
         */
        public byte[] getBytes() {
            return _bytes;
        }

        /**
         * Get the count
         * 
         * @return the count
         */
        public int getCount() {
            return _count;
        }

        private void setCount(final int count) {
            this._count = count;
        }

        @Override
        public String toString() {
            final Key key = new Key((Persistit) null);
            System.arraycopy(_bytes, 0, key.getEncodedBytes(), 0, _bytes.length);
            key.setEncodedSize(_bytes.length);
            return String.format("%,10d %s", _count, key);
        }
    }

    public KeyHistogram(final Tree tree, final Key start, final Key end, final int sampleSize, final int keyDepth,
            final int treeDepth) {
        _tree = tree;
        _startKey = start;
        _endKey = end;
        _requestedSampleSize = sampleSize;
        _keyDepth = keyDepth;
        _treeDepth = treeDepth;
    }

    public Tree getTree() {
        return _tree;
    }

    public Key getStartKey() {
        return _startKey;
    }

    public Key getEndKey() {
        return _endKey;
    }

    public int getKeyCount() {
        return _keyCount;
    }

    public int getRequestedSampleSize() {
        return _requestedSampleSize;
    }

    public int getSampleSize() {
        return _keys.size();
    }

    public List<KeyCount> getSamples() {
        return _keys;
    }

    public int getTreeDepth() {
        return _treeDepth;
    }

    public int getKeyDepth() {
        return _keyDepth;
    }

    public int getPageCount() {
        return _pageCount;
    }

    public long getPageBytesTotal() {
        return _pageBytesTotal;
    }

    public long getPageBytesInUse() {
        return _pageBytesInUse;
    }

    /**
     * Add a key. Keys must be added in key-sort order. If the supplied key is
     * the same as the previously added key up to the segment specified by the
     * keyDepth property, then accumulate to the same KeyCount bucket. Otherwise
     * add a new KeyCount bucket. When the sample list becomes too long, this
     * method removes every other sample and aggregates the count values. By so
     * doing, this method keeps the number of retained samples relatively small.
     * 
     * @param key
     *            The Key to add to the sample set
     */
    void addKeyCopy(final Key key) {
        _keyCount++;
        if (_keyCount % _modulus == 0) {
            final int length = _keyDepth == 0 ? key.getEncodedSize() : key.indexTo(_keyDepth).getIndex();
            final int end = _keys.size() - 1;
            boolean same = false;
            if (end >= 0) {
                final byte[] last = _keys.get(end).getBytes();
                same = last.length == length;
                for (int index = 0; same && index < length; index++) {
                    same &= last[index] == key.getEncodedBytes()[index];
                }
            }
            if (same) {
                _keys.get(end).setCount(_keyCount);
            } else {
                final byte[] bytes = new byte[length];
                System.arraycopy(key.getEncodedBytes(), 0, bytes, 0, length);
                _keys.add(new KeyCount(bytes, _keyCount));
            }
        }
        if (_keys.size() >= _requestedSampleSize * 16) {
            for (int index = (_keys.size() & 0x7FFFFFFE); (index -= 2) >= 0;) {
                _keys.remove(index);
            }
            _modulus *= 2;
        }
    }

    /**
     * Accumulates total number of pages, bytes and bytes-in-use traversed.
     * 
     * @param size
     *            Size of the page
     * @param used
     *            Number of bytes in use in the page.
     */
    void addPage(final int size, final int used) {
        _pageCount++;
        _pageBytesTotal += size;
        _pageBytesInUse += used;
    }

    /**
     * Culls the List of keys down to the requested sample size
     */
    void cull() {
        final int have = _keys.size();
        final int want = _requestedSampleSize;
        int counter = have;
        for (int index = have; --index >= 0;) {
            counter += want;
            if (counter <= have) {
                _keys.remove(index);
            } else {
                counter -= have;
            }
        }
        if (_keys.size() > want) {
            _keys.remove(0);
        }
    }
}
