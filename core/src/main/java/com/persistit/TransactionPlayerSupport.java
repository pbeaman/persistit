/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

import java.nio.ByteBuffer;

import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

interface TransactionPlayerSupport {
    Persistit getPersistit();

    TreeDescriptor handleToTreeDescriptor(int treeHandle);

    Volume handleToVolume(int volumeHandle);

    void read(long address, int size) throws PersistitIOException;

    ByteBuffer getReadBuffer();

    void convertToLongRecord(Value value, int treeHandle, long address, long commitTimestamp) throws PersistitException;
}