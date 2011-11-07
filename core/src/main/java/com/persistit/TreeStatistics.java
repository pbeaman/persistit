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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.util.Util;

/**
 * Collection of <code>AtomicLong</code> counters representing for interesting
 * statistics in a {@link Tree}. The {@link #save} and {@link #load} methods are
 * designed to serialize and deserialize evolving versions of this class.
 * Therefore it should be relatively to add and remove in new versions of
 * Persistit.
 * 
 * @author peter
 */
class TreeStatistics {

    private final AtomicLong _fetchCounter = new AtomicLong();
    private final AtomicLong _traverseCounter = new AtomicLong();
    private final AtomicLong _storeCounter = new AtomicLong();
    private final AtomicLong _removeCounter = new AtomicLong();
    private final AtomicLong _mvvCounter = new AtomicLong();
    private final AtomicLong _mvvOverhead = new AtomicLong();

    private final AtomicBoolean _dirty = new AtomicBoolean();
    /*
     * Array of AtomicLong instances currently used in serializing and
     * deserializing statistics. To ensure correct evolution of serialized
     * values, (a) the length of this array is limited to 32, and (b) if a field
     * becomes obsolete, replace it in this array by null rather than reusing
     * its position.
     */
    private final AtomicLong[] _statsArray = new AtomicLong[] { _fetchCounter, _traverseCounter, _storeCounter,
            _removeCounter, _mvvCounter, _mvvOverhead };

    private final static String[] _statsArrayNames = new String[] { "fetchCounter", "traverseCounter", "storeCounter",
            "removeCounter", "mvvCounter", "mvvOverhead" };

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
     *         {@link Exchange#fetchAndRemove} operations.
     */
    public long getFetchCounter() {
        return _fetchCounter.get();
    }

    /**
     * @return the count of {@link Exchange#traverse} operations including
     *         {@link Exchange#next} and {@link Exchange#_previous}.
     */
    public long getTraverseCounter() {
        return _traverseCounter.get();
    }

    /**
     * @return the count of {@link Exchange#store} operations, including
     *         {@link Exchange#fetchAndStore} and
     *         {@link Exchange#incrementValue} operations.
     */
    public long getStoreCounter() {
        return _storeCounter.get();
    }

    /**
     * @return the count of {@link Exchange#remove} operations, including
     *         {@link Exchange#fetchAndRemove} operations.
     */
    public long getRemoveCounter() {
        return _removeCounter.get();
    }

    /**
     * @return Count of records in this <code>Tree</code> having multi-version
     *         values.
     */
    public long getMvvCounter() {
        return _mvvCounter.get();
    }

    boolean isDirty() {
        return _dirty.get();
    }

    void dirty() {
        _dirty.set(true);
    }

    /**
     * @return Approximate size in bytes of the overhead space consumed in this
     *         <code>Tree</code> by multi-version-values.
     */

    void reset() {
        _fetchCounter.set(0);
        _traverseCounter.set(0);
        _storeCounter.set(0);
        _removeCounter.set(0);
        _mvvCounter.set(0);
        _mvvOverhead.set(0);
        dirty();
    }

    void bumpFetchCounter() {
        _fetchCounter.incrementAndGet();
        dirty();
    }

    void bumpTraverseCounter() {
        _traverseCounter.incrementAndGet();
        dirty();
    }

    void bumpStoreCounter() {
        _storeCounter.incrementAndGet();
        dirty();
    }

    void bumpRemoveCounter() {
        _removeCounter.incrementAndGet();
        dirty();
    }

    void bumpMvvCounter() {
        _mvvCounter.incrementAndGet();
        dirty();
    }

    void bumpMvvOverhead(final int delta) {
        _mvvOverhead.addAndGet(delta);
        dirty();
    }

    /**
     * <p>
     * Serialize the statistics value in a variable-length byte array. The
     * format is designed to allow more fields to be added and existing fields
     * to be removed. Up to 32 field could eventually be allocated.
     * </p>
     * <p>
     * The value of each field is represented in 2, 4 or 8 bytes depending on
     * the current value of the field. The first 8 bytes of the serialized form
     * contain a bit map. Each statistics field corresponds to a two-bit field
     * in the bit map; these two bits indicate the size of the stored statistics
     * field:
     * <ul>
     * <li>0: 0 bytes - the field is not presently implemented in this class</li>
     * <li>1: 2 bytes</li>
     * <li>2: 4 bytes</li>
     * <li>3: 8 bytes</li>
     * </ul>
     * 
     * @param bytes
     *            byte array into which statistics are serialized
     * @param index
     *            at which serialization starts in byte array
     * @return length of serialized statistics
     */
    int save(final byte[] bytes, int index) {
        long bits = 0;
        int offset = index + 8;
        int field = 0;
        for (AtomicLong a : _statsArray) {
            if (a != null) {
                long v = a.get();
                if (v < Character.MAX_VALUE) {
                    bits |= 1L << (field * 2);
                    Util.putChar(bytes, offset, (char) v);
                    offset += 2;
                } else if (v < Integer.MAX_VALUE) {
                    bits |= 2L << (field * 2);
                    Util.putInt(bytes, offset, (int) v);
                    offset += 4;
                } else {
                    bits |= 3L << (field * 2);
                    Util.putLong(bytes, offset, v);
                    offset += 8;
                }
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
    void load(final byte[] bytes, final int index, final int length) {
        int offset = index + 8;
        final int end = index + length;
        long bits = Util.getLong(bytes, index);
        for (int field = 0; field < 32; field++) {
            int code = (int) (bits >>> (field * 2)) & 3;
            AtomicLong a = field < _statsArray.length ? _statsArray[field] : null;
            switch (code) {
            case 0:
                break;
            case 1:
                if (a != null) {
                    checkEnd(offset + 2, end);
                    a.set(Util.getChar(bytes, offset));
                }
                offset += 2;
                break;
            case 2:
                if (a != null) {
                    checkEnd(offset + 4, end);
                    a.set(Util.getInt(bytes, offset));
                }
                offset += 4;
                break;
            case 3:
                if (a != null) {
                    checkEnd(offset + 8, end);
                    a.set(Util.getLong(bytes, offset));
                }
                offset += 8;
                break;
            }
        }
    }

    private void checkEnd(int index, int end) {
        if (index > end) {
            throw new IllegalStateException("TreeStatistics record is too short at offset " + index);
        }
    }
}
