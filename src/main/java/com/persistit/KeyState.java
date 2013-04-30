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
 * Represent an immutable copy of the state from a {@link Key} object suitable
 * for use as the key of a <code>Map</code>.
 * 
 * @version 1.0
 */
public class KeyState implements Comparable, Serializable {
    public static final long serialVersionUID = 107136093587833709L;

    public final static KeyState LEFT_GUARD_KEYSTATE = new KeyState(Key.LEFT_GUARD_KEY);

    public final static KeyState RIGHT_GUARD_KEYSTATE = new KeyState(Key.RIGHT_GUARD_KEY);

    private final byte[] _bytes;
    private int _hashCode = -1;

    /**
     * Construct an immutable <code>KeyState</code> by copying the relevant
     * state information from a <code>Key</code>. The <code>hashCode</code> and
     * <code>equals</code> methods of <code>Key</code> and <code>KeyState</code>
     * are compatible so that either a <code>Key</code> or a
     * <code>KeyState</code> may be used as a map key.
     * 
     * @param key
     *            The <code>Key</code> from which the state is copied.
     */
    public KeyState(final Key key) {
        final int length = key.getEncodedSize();
        _bytes = new byte[length];
        System.arraycopy(key.getEncodedBytes(), 0, _bytes, 0, length);
    }

    public KeyState(final byte[] data) {
        _bytes = data;
    }

    /**
     * Copy the content of this <code>KeyState</code> to the supplied
     * <code>Key</code>.
     * 
     * @param key
     *            The <code>key</code> to which content should be copied.
     */
    public void copyTo(final Key key) {
        if (key.getMaximumSize() < _bytes.length) {
            throw new IllegalArgumentException("Too small");
        }
        key.clear();
        System.arraycopy(_bytes, 0, key.getEncodedBytes(), 0, _bytes.length);
        key.setEncodedSize(_bytes.length);
    }

    /**
     * Compute the hash code for this <code>KeyState</code>. The hashCode is the
     * same as for the equivalent <code>Key</code>, that is the <code>Key</code>
     * from which this <code>KeyState</code> was constructed prior to any
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
     * Implement <code>equals</code> in such a way that <code>Key</code> and
     * <code>KeyState</code> can be used interchangeably as map keys.
     * 
     * @return <code>true</code> if the specified object is either a
     *         <code>Key</code> or a <code>KeyState</code> whose state
     *         represents an identical object or primitive value.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof Key) {
            final Key key = (Key) obj;
            if (key.getEncodedSize() != _bytes.length)
                return false;
            final byte[] valueBytes = key.getEncodedBytes();
            for (int i = 0; i < _bytes.length; i++) {
                if (valueBytes[i] != _bytes[i])
                    return false;
            }
            return true;
        } else if (obj instanceof KeyState) {
            final KeyState keyState = (KeyState) obj;
            if (keyState._bytes.length != _bytes.length)
                return false;
            for (int i = 0; i < _bytes.length; i++) {
                if (keyState._bytes[i] != _bytes[i])
                    return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Implement <code>Comparable</code> in such a way that <code>Key</code> and
     * <code>KeyState</code> can be used interchangeably as map keys.
     * 
     * @return results of comparing the key value represented by this
     *         <code>KeyState</code> with a supplied <code>KeyState</code> or
     *         <code>Key</code>. The result is negative if this key value
     *         preceeds, positive if this key value follows, or zero if this key
     *         value is equal to the supplied key value.
     * 
     * @throws ClassCastException
     *             if the supplied object is neither a <code>Key</code> nor a
     *             <code>KeyState</code>
     */
    @Override
    public int compareTo(final Object obj) {
        int size;
        byte[] bytes;
        if (obj instanceof Key) {
            final Key key = (Key) obj;
            bytes = key.getEncodedBytes();
            size = key.getEncodedSize();
        } else if (obj instanceof KeyState) {
            final KeyState ks = (KeyState) obj;
            bytes = ks._bytes;
            size = bytes.length;
        } else
            throw new ClassCastException();
        final int length = Math.min(size, _bytes.length);
        for (int i = 0; i < length; i++) {
            final int a = _bytes[i] & 0xFF;
            final int b = bytes[i] & 0xFF;
            if (a != b)
                return a < b ? -1 : 1;
        }
        if (size != _bytes.length)
            return _bytes.length < size ? -1 : 1;
        return 0;
    }

    public byte[] getBytes() {
        return _bytes;
    }

    @Override
    public String toString() {
        return new Key(null, this).toString();
    }
}
