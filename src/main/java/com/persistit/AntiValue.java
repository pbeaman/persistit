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
