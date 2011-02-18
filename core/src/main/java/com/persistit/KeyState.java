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
 * Contains an immutable copy of the state from a {@link Key} object suitable
 * for use as the key of a <tt>Map</tt>.
 * 
 * @version 1.0
 */
public class KeyState implements Comparable, Serializable {
    public static final long serialVersionUID = 107136093587833709L;

    public final static KeyState LEFT_GUARD_KEYSTATE = new KeyState(
            Key.LEFT_GUARD_KEY);

    public final static KeyState RIGHT_GUARD_KEYSTATE = new KeyState(
            Key.RIGHT_GUARD_KEY);

    private byte[] _bytes;
    private int _hashCode = -1;

    /**
     * Construct an immutable <tt>KeyState</tt> by copying the relevant state
     * information from a <tt>Key</tt>. The <tt>hashCode</tt> and
     * <tt>equals</tt> methods of <tt>Key</tt> and <tt>KeyState</tt> are
     * compatible so that either a <tt>Key</tt> or a <tt>KeyState</tt> may be
     * used as a map key.
     * 
     * @param key
     *            The <tt>Key</tt> from which the state is copied.
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
     * Copies the content of this <tt>KeyState</tt> to the supplied
     * <code>Key</code>.
     * 
     * @param key
     *            The <tt>key</tt> to which content should be copied.
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
     * The hash code for this <tt>KeyState</tt>. The hashCode is the same as for
     * the equivalent <tt>Key</tt>, that is the <tt>Key</tt> from which this
     * <tt>KeyState</tt> was constructed prior to any subsequent modificiations.
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
     * Implements <tt>equals</tt> in such a way that <tt>Key</tt> and
     * <tt>KeyState</tt> can be used interchangeably as map keys.
     * 
     * @return <tt>true</tt> if the specified object is either a <tt>Key</tt> or
     *         a <tt>KeyState</tt> whose state represents an identical object or
     *         primitive value.
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
     * Implements <tt>Comparable</tt> in such a way that <tt>Key</tt> and
     * <tt>KeyState</tt> can be used interchangeably as map keys.
     * 
     * @return results of comparing the key value represented by this
     *         <tt>KeyState</tt> with a supplied <tt>KeyState</tt> or
     *         <tt>Key</tt>. The result is negative if this key value preceeds,
     *         positive if this key value follows, or zero if this key value is
     *         equal to the supplied key value.
     * 
     * @throws ClassCastException
     *             if the supplied object is neither a <tt>Key</tt> nor a
     *             <tt>KeyState</tt>
     */
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
