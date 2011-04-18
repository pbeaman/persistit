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

import com.persistit.exception.PersistitException;

public class BufferReplacementStrategyImpl implements BufferReplacementStrategy {

    @Override
    public Buffer getBuffer(Buffer[] bufferList, int bucket) 
        throws PersistitException {
        Buffer buffer = bufferList[bucket];
        while (buffer != null) {
            if (buffer.isAvailable() && 
                buffer.isClean() &&
                buffer.claim(true, 0)) {
                return buffer;
            }
            buffer = buffer.getNextLru();
            if (buffer == bufferList[bucket]) {
                break;
            }
        }
        return null;
    }

}
