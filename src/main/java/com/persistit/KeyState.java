/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

    private byte[] _bytes;
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
    public KeyState(Key key) {
        int length = key.getEncodedSize();
        _bytes = new byte[length];
        System.arraycopy(key.getEncodedBytes(), 0, _bytes, 0, length);
    }

    public KeyState(byte[] data) {
        _bytes = data;
    }

    /**
     * Copy the content of this <code>KeyState</code> to the supplied
     * <code>Key</code>.
     * 
     * @param key
     *            The <code>key</code> to which content should be copied.
     */
    public void copyTo(Key key) {
        if (key.getMaximumSize() < _bytes.length) {
            throw new IllegalArgumentException("Too small");
        }
        key.clear();
        System.arraycopy(_bytes, 0, key.getEncodedBytes(), 0, _bytes.length);
        key.setEncodedSize(_bytes.length);
    }

    /**
     * Compute the hash code for this <code>KeyState</code>. The hashCode is the same as
     * for the equivalent <code>Key</code>, that is the <code>Key</code> from
     * which this <code>KeyState</code> was constructed prior to any subsequent
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
     * Implement <code>equals</code> in such a way that <code>Key</code> and
     * <code>KeyState</code> can be used interchangeably as map keys.
     * 
     * @return <code>true</code> if the specified object is either a
     *         <code>Key</code> or a <code>KeyState</code> whose state
     *         represents an identical object or primitive value.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key) {
            Key key = (Key) obj;
            if (key.getEncodedSize() != _bytes.length)
                return false;
            byte[] valueBytes = key.getEncodedBytes();
            for (int i = 0; i < _bytes.length; i++) {
                if (valueBytes[i] != _bytes[i])
                    return false;
            }
            return true;
        } else if (obj instanceof KeyState) {
            KeyState keyState = (KeyState) obj;
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
     * Implement <code>Comparable</code> in such a way that <code>Key</code>
     * and <code>KeyState</code> can be used interchangeably as map keys.
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
    public int compareTo(Object obj) {
        int size;
        byte[] bytes;
        if (obj instanceof Key) {
            Key key = (Key) obj;
            bytes = key.getEncodedBytes();
            size = key.getEncodedSize();
        } else if (obj instanceof KeyState) {
            KeyState ks = (KeyState) obj;
            bytes = ks._bytes;
            size = bytes.length;
        } else
            throw new ClassCastException();
        int length = Math.min(size, _bytes.length);
        for (int i = 0; i < length; i++) {
            int a = _bytes[i] & 0xFF;
            int b = bytes[i] & 0xFF;
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
