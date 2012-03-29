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

import java.io.Serializable;

/**
 * Contains an immutable copy of the state from a {@link Value} object suitable
 * for use as the key of a <code>Map</code>.
 * 
 * @version 1.0
 */
public class ValueState implements Serializable {
    public static final long serialVersionUID = -3715600225940676772L;

    private final byte[] _bytes;
    private final boolean _longRecordMode;
    private int _hashCode = -1;

    /**
     * Construct an immutable <code>ValueState</code> by copying the relevant
     * state information from a <code>Value</code>. The <code>hashCode</code>
     * and <code>equals</code> methods of <code>Value</code> and
     * <code>ValueState</code> are compatible so that either a
     * <code>Value</code> or a <code>ValueState</code> may be used as a map key.
     * 
     * @param value
     *            The <code>Value</code> from which the state is copied.
     */
    public ValueState(Value value) {
        _longRecordMode = value.isLongRecordMode();
        int length = value.getEncodedSize();
        _bytes = new byte[length];
        System.arraycopy(value.getEncodedBytes(), 0, _bytes, 0, length);
    }

    /**
     * Construct an immutable <code>ValueState</code> by copying the relevant
     * state information from a <code>Value</code>. The <code>hashCode</code>
     * and <code>equals</code> methods of <code>Value</code> and
     * <code>ValueState</code> are compatible so that either a
     * <code>Value</code> or a <code>ValueState</code> may be used as a map key.
     * If the encoded size of the original <code>value</code> is larger than
     * <code>truncateSize</code>, the result is truncated the that size.
     * 
     * @param value
     *            The <code>Value</code> from which the state is copied.
     * 
     * @param truncateSize
     *            Size at which the the copied encoded byte array is truncated.
     */
    public ValueState(Value value, int truncateSize) {
        _longRecordMode = value.isLongRecordMode();
        int length = value.getEncodedSize();
        if (length > truncateSize)
            length = truncateSize;
        _bytes = new byte[length];
        System.arraycopy(value.getEncodedBytes(), 0, _bytes, 0, length);
    }

    /**
     * Copies the content of this <code>ValueState</code> to the supplied
     * <code>Value</code>.
     * 
     * @param value
     *            The <code>Value</code> to which content should be copied.
     */
    public void copyTo(Value value) {
        if (value.getMaximumSize() < _bytes.length) {
            throw new IllegalArgumentException("Too small");
        }
        value.clear();
        value.ensureFit(_bytes.length);
        System.arraycopy(_bytes, 0, value.getEncodedBytes(), 0, _bytes.length);
        value.setEncodedSize(_bytes.length);
        value.setLongRecordMode(_longRecordMode);
    }

    /**
     * The hash code for this <code>ValueState</code>. The hashCode is the same
     * as for the equivalent <code>Value</code>, that is the <code>Value</code>
     * from which this <code>ValueState</code> was constructed prior to any
     * subsequent modifications.
     * 
     * @return The hashCode.
     */
    @Override
    public int hashCode() {
        if (_hashCode < 0) {
            int hashCode = 0;
            for (int index = 0; index < _bytes.length; index++) {
                hashCode = (hashCode * 17) ^ (_bytes[index] & 0xFF);
            }
            _hashCode = hashCode & 0x7FFFFFFF;
        }
        return _hashCode;
    }

    /**
     * Implements <code>equals</code> in such a way that <code>Value</code> and
     * <code>ValueState</code> can be used interchangeably as map keys.
     * 
     * @return <t>true</t> if the specified object is either a
     *         <code>Value</code> or a <code>ValueState</code> whose state
     *         represents an identical object or primitive value.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Value) {
            Value value = (Value) obj;
            if (value.getEncodedSize() != _bytes.length)
                return false;
            byte[] valueBytes = value.getEncodedBytes();
            for (int i = 0; i < _bytes.length; i++) {
                if (valueBytes[i] != _bytes[i])
                    return false;
            }
            return true;
        } else if (obj instanceof ValueState) {
            ValueState valueState = (ValueState) obj;
            if (valueState._bytes.length != _bytes.length)
                return false;
            for (int i = 0; i < _bytes.length; i++) {
                if (valueState._bytes[i] != _bytes[i])
                    return false;
            }
            return true;
        }
        return false;
    }

    /**
     * @return the encoded byte array. See
     *         {@link com.persistit.Value#getEncodedBytes()}.
     */
    public byte[] getEncodedBytes() {
        return _bytes;
    }

    /**
     * @return the size of the encoded byte array. See
     *         {@link com.persistit.Value#getEncodedSize()}.
     */
    public int getEncodedSize() {
        return _bytes.length;
    }

    boolean isLongRecordMode() {
        return _longRecordMode;
    }
}
