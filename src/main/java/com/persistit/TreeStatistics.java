/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.util.Util;

/**
 * <p>
 * Collection of <code>AtomicLong</code> counters representing interesting
 * statistics in a {@link Tree}. The {@link #store} and {@link #load} methods
 * are designed to serialize and deserialize evolving versions of this class,
 * therefore it is possible to add and remove new counters while remaining
 * compatible with prior versions.
 * </p>
 * <p>
 * Currently implemented counters include:
 * <ul>
 * <li>Fetch</li>
 * <li>Traverse</li>
 * <li>Store</li>
 * <li>Remove</li>
 * </ul>
 * </p>
 * 
 * @author peter
 */
public class TreeStatistics {
    final static int MAX_SERIALIZED_SIZE = 54;

    private final AtomicLong _fetchCounter = new AtomicLong();
    private final AtomicLong _traverseCounter = new AtomicLong();
    private final AtomicLong _storeCounter = new AtomicLong();
    private final AtomicLong _removeCounter = new AtomicLong();

    private final AtomicBoolean _dirty = new AtomicBoolean();
    /*
     * Array of AtomicLong instances currently used in serializing and
     * deserializing statistics. To ensure correct evolution of serialized
     * values, (a) the length of this array is limited to 32, and (b) if a field
     * becomes obsolete, replace it in this array by null rather than reusing
     * its position.
     */
    private final AtomicLong[] _statsArray = new AtomicLong[] { _fetchCounter, _traverseCounter, _storeCounter,
            _removeCounter };

    private final static String[] _statsArrayNames = new String[] { "fetchCounter", "traverseCounter", "storeCounter",
            "removeCounter" };

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int index = 0; index < _statsArrayNames.length; index++) {
            final String name = _statsArrayNames[index];
            final AtomicLong value = _statsArray[index];
            if (name != null) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(name);
                sb.append("=");
                sb.append(value == null ? "" : String.format("%,d", value.get()));
            }
        }
        return sb.toString();
    }

    /**
     * @return the count of {@link Exchange#fetch} operations, including
     *         {@link Exchange#fetchAndStore} and
     *         {@link Exchange#fetchAndRemove} operations
     */
    public long getFetchCounter() {
        return _fetchCounter.get();
    }

    /**
     * @return the count of {@link Exchange#traverse} operations including
     *         {@link Exchange#next} and {@link Exchange#previous}
     */
    public long getTraverseCounter() {
        return _traverseCounter.get();
    }

    /**
     * @return the count of {@link Exchange#store} operations, including
     *         {@link Exchange#fetchAndStore}
     */
    public long getStoreCounter() {
        return _storeCounter.get();
    }

    /**
     * @return the count of {@link Exchange#remove} operations, including
     *         {@link Exchange#fetchAndRemove} operations
     */
    public long getRemoveCounter() {
        return _removeCounter.get();
    }

    boolean isDirty() {
        return _dirty.get();
    }

    void setDirty(final boolean dirty) {
        _dirty.set(dirty);
    }

    /**
     * @return Approximate size in bytes of the overhead space consumed in this
     *         <code>Tree</code> by multi-version-values
     */

    void reset() {
        _fetchCounter.set(0);
        _traverseCounter.set(0);
        _storeCounter.set(0);
        _removeCounter.set(0);
        setDirty(true);
    }

    void bumpFetchCounter() {
        _fetchCounter.incrementAndGet();
        setDirty(true);
    }

    void bumpTraverseCounter() {
        _traverseCounter.incrementAndGet();
        setDirty(true);
    }

    void bumpStoreCounter() {
        _storeCounter.incrementAndGet();
        setDirty(true);
    }

    void bumpRemoveCounter() {
        _removeCounter.incrementAndGet();
        setDirty(true);
    }

    /**
     * <p>
     * Serialize the statistics value in a variable-length byte array. The
     * format is designed to allow more fields to be added and existing fields
     * to be removed. Up to 64 field could eventually be allocated. The first
     * eight bytes of the serialized form include a bit map; a bit set in the
     * bit map indicates that the corresponding field is present in the
     * remaining bytes of the serialized form. Each field is stored as an 8-byte
     * long value.
     * </p>
     * 
     * @param bytes
     *            byte array into which statistics are serialized
     * @param index
     *            at which serialization starts in byte array
     * @return length of serialized statistics
     */
    int store(final byte[] bytes, int index) {
        long bits = 0;
        int offset = index + 8;
        int field = 0;
        for (AtomicLong a : _statsArray) {
            if (a != null) {
                long v = a.get();
                bits |= 1 << field;
                Util.putLong(bytes, offset, v);
                offset += 8;
            }
            field++;
        }
        Util.putLong(bytes, index, bits);
        return offset - index;
    }

    /**
     * Deserialize stored statistics values from the supplied byte array
     * starting at <code>index</code>. The first eight bytes contain a bit map
     * described in {@link #save(byte[], int)}.
     * 
     * @param bytes
     *            serialized statistics
     * @param index
     *            at which serialized statistics start in the byte array
     */
    int load(final byte[] bytes, final int index, final int length) {
        int offset = index + 8;
        final int end = index + length;
        long bits = Util.getLong(bytes, index);
        for (int field = 0; field < 64; field++) {
            AtomicLong a = field < _statsArray.length ? _statsArray[field] : null;
            if ((bits & (1 << field)) != 0) {
                if (a != null) {
                    checkEnd(offset + 8, end);
                    a.set(Util.getLong(bytes, offset));
                }
                offset += 8;
            }
        }
        return length;
    }

    private void checkEnd(int index, int end) {
        if (index > end) {
            throw new IllegalStateException("TreeStatistics record is too short at offset " + index);
        }
    }
}
