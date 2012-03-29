/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
public class TreeStatistics {
    final static int MAX_SERIALIZED_SIZE = 54;

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

    /**
     * @return Overhead bytes consumed by multi-version values that will be
     *         removed by pruning.
     */
    public long getMvvOverhead() {
        return _mvvOverhead.get();
    }

    boolean isDirty() {
        return _dirty.get();
    }

    void setDirty(final boolean dirty) {
        _dirty.set(dirty);
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

    void bumpMvvCounter() {
        _mvvCounter.incrementAndGet();
        setDirty(true);
    }

    void bumpMvvOverhead(final int delta) {
        _mvvOverhead.addAndGet(delta);
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
