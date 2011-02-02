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

import com.persistit.exception.InvalidKeyException;

/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Jul 14, 2005
 */

/**
 * Represents the end of a key range to be removed. Used in Transactions.
 * 
 * @author Peter Beaman
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

    static void fixupKeys(Exchange exchange, int elisionCount, byte[] bytes,
            int offset, int length) throws InvalidKeyException {
        Key spareKey1 = exchange.getAuxiliaryKey1();
        Key spareKey2 = exchange.getAuxiliaryKey2();
        spareKey1.copyTo(spareKey2);
        byte[] baseBytes = spareKey2.getEncodedBytes();
        int baseSize = spareKey2.getEncodedSize();
        if (baseSize < elisionCount
                || elisionCount + length > Key.MAX_KEY_LENGTH) {
            throw new InvalidKeyException(
                    "Key encoding in transaction is invalid");
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
}
