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
