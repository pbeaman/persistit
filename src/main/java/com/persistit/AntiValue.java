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

import java.util.Arrays;

import com.persistit.exception.InvalidKeyException;

/**
 * Represents the end of a key range to be removed. Used in Transactions.
 * 
 * @author peter
 * @version 1.1
 */
class AntiValue {
    private final int _elisionCount;
    private final byte[] _bytes;

    AntiValue(final int ec, final byte[] bytes) {
        _elisionCount = ec;
        _bytes = bytes;
    }

    static void putAntiValue(final Value value, final Key key1, final Key key2) {
        final int elisionCount = key1.firstUniqueByteIndex(key2);
        final int size = key2.getEncodedSize() - elisionCount;
        final byte[] bytes = new byte[size];
        System.arraycopy(key2.getEncodedBytes(), elisionCount, bytes, 0, size);
        value.putAntiValue((short) elisionCount, bytes);
    }

    static void fixUpKeys(final Exchange exchange, final int elisionCount, final byte[] bytes, final int offset,
            final int length) throws InvalidKeyException {
        final Key spareKey1 = exchange.getAuxiliaryKey1();
        final Key spareKey2 = exchange.getAuxiliaryKey2();
        spareKey1.copyTo(spareKey2);
        final byte[] baseBytes = spareKey2.getEncodedBytes();
        final int baseSize = spareKey2.getEncodedSize();
        if (baseSize < elisionCount || elisionCount + length > Key.MAX_KEY_LENGTH) {
            throw new InvalidKeyException("Key encoding in transaction is invalid");
        }
        System.arraycopy(bytes, offset, baseBytes, elisionCount, length);
        spareKey2.setEncodedSize(elisionCount + length);
    }

    int getElisionCount() {
        return _elisionCount;
    }

    byte[] getBytes() {
        return _bytes;
    }

    @Override
    public String toString() {
        return '{' + (_elisionCount > 0 ? "elision=" + _elisionCount + " " + Arrays.toString(_bytes) : "") + '}';
    }
}
