/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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
    public ValueState(final Value value) {
        _longRecordMode = value.isLongRecordMode();
        final int length = value.getEncodedSize();
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
    public ValueState(final Value value, final int truncateSize) {
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
    public void copyTo(final Value value) {
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
    public boolean equals(final Object obj) {
        if (obj instanceof Value) {
            final Value value = (Value) obj;
            if (value.getEncodedSize() != _bytes.length)
                return false;
            final byte[] valueBytes = value.getEncodedBytes();
            for (int i = 0; i < _bytes.length; i++) {
                if (valueBytes[i] != _bytes[i])
                    return false;
            }
            return true;
        } else if (obj instanceof ValueState) {
            final ValueState valueState = (ValueState) obj;
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
