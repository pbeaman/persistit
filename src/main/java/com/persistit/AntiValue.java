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

import com.persistit.exception.InvalidKeyException;

import java.util.Arrays;

/**
 * Represents the end of a key range to be removed. Used in Transactions.
 * 
 * @author peter
 * @version 1.1
 */
class AntiValue {
    private int _elisionCount;
    private byte[] _bytes;

    AntiValue(int ec, byte[] bytes) {
        _elisionCount = ec;
        _bytes = bytes;
    }

    static void putAntiValue(Value value, Key key1, Key key2) {
        int elisionCount = key1.firstUniqueByteIndex(key2);
        int size = key2.getEncodedSize() - elisionCount;
        byte[] bytes = new byte[size];
        System.arraycopy(key2.getEncodedBytes(), elisionCount, bytes, 0, size);
        value.putAntiValue((short) elisionCount, bytes);
    }

    static void fixUpKeys(Exchange exchange, int elisionCount, byte[] bytes, int offset, int length)
            throws InvalidKeyException {
        Key spareKey1 = exchange.getAuxiliaryKey1();
        Key spareKey2 = exchange.getAuxiliaryKey2();
        spareKey1.copyTo(spareKey2);
        byte[] baseBytes = spareKey2.getEncodedBytes();
        int baseSize = spareKey2.getEncodedSize();
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
