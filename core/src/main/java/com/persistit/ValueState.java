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

import java.io.Serializable;

/**
 * Contains an immutable copy of the state from a {@link Value} object suitable
 * for use as the key of a <tt>Map</tt>.
 * 
 * @version 1.0
 */
public class ValueState implements Serializable {
    public static final long serialVersionUID = -3715600225940676772L;

    private final byte[] _bytes;
    private final boolean _longRecordMode;
    private int _hashCode = -1;

    /**
     * Construct an immutable <tt>ValueState</tt> by copying the relevant state
     * information from a <tt>Value</tt>. The <tt>hashCode</tt> and
     * <tt>equals</tt> methods of <tt>Value</tt> and <tt>ValueState</tt> are
     * compatible so that either a <tt>Value</tt> or a <tt>ValueState</tt> may
     * be used as a map key.
     * 
     * @param value
     *            The <tt>Value</tt> from which the state is copied.
     */
    public ValueState(Value value) {
        _longRecordMode = value.isLongRecordMode();
        int length = value.getEncodedSize();
        _bytes = new byte[length];
        System.arraycopy(value.getEncodedBytes(), 0, _bytes, 0, length);
    }

    /**
     * Construct an immutable <tt>ValueState</tt> by copying the relevant state
     * information from a <tt>Value</tt>. The <tt>hashCode</tt> and
     * <tt>equals</tt> methods of <tt>Value</tt> and <tt>ValueState</tt> are
     * compatible so that either a <tt>Value</tt> or a <tt>ValueState</tt> may
     * be used as a map key. If the encoded size of the original <tt>value</tt>
     * is larger than <tt>truncateSize</tt>, the result is truncated the that
     * size.
     * 
     * @param value
     *            The <tt>Value</tt> from which the state is copied.
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
     * Copies the content of this <tt>ValueState</tt> to the supplied
     * <code>Value</code>.
     * 
     * @param value
     *            The <tt>Value</tt> to which content should be copied.
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
     * The hash code for this <tt>ValueState</tt>. The hashCode is the same as
     * for the equivalent <tt>Value</tt>, that is the <tt>Value</tt> from which
     * this <tt>ValueState</tt> was constructed prior to any subsequent
     * modifications.
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
     * Implements <tt>equals</tt> in such a way that <tt>Value</tt> and
     * <tt>ValueState</tt> can be used interchangeably as map keys.
     * 
     * @return <t>true</t> if the specified object is either a <tt>Value</tt> or
     *         a <tt>ValueState</tt> whose state represents an identical object
     *         or primitive value.
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
