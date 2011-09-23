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
import com.persistit.exception.PersistitIOException;

/**
 * Accessors for package-private methods in com.persistit so that unit tests can
 * be in a different package.
 * 
 * @author peter
 * 
 */
public class TestShim {

    public final static int BUFFER_INDEX_PAGE_OVERHEAD = Buffer.INDEX_PAGE_OVERHEAD;

    public static void rollover(final JournalManager journalManager) throws PersistitIOException {
        journalManager.rollover();
    }

    public static int maxStorableKeySize(final int bufferSize) {
        return Key.maxStorableKeySize(bufferSize);
    }

    public static int maxKeys(final Volume volume) {
        return volume.getPool().getMaxKeys();
    }

    public static Buffer buffer(final Volume volume, final long page) throws Exception {
        return volume.getPool().get(volume, page, false, true);
    }

    public static TimestampAllocator timestampAllocator(final Persistit persistit) {
        return persistit.getTimestampAllocator();
    }

    public static void closeTransaction(Transaction t) throws PersistitException {
        t.close();
    }

}
