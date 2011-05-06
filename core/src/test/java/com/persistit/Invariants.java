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

public class Invariants {
    
    /**
     * For each BufferPool all pages must be either FIXED (volume head pages),
     * valid and on the LRU queue, or invalid and on the INVALID queue.
     * @param db
     */

    public static void checkQueueSizes(final Persistit db) {
        for (final BufferPool pool : db.getBufferPoolHashMap().values()) {
            int volumes = 0;
            // Count the Volume head pages
            for (final Volume volume : db.getVolumes()) {
                if (volume.getPageSize() == pool.getBufferSize()) {
                    volumes++;
                }
            }
            final int lru = pool.countLruQueueEntries();
            final int invalid = pool.countInvalidQueueEntries();
            if (lru + invalid + volumes != pool.getBufferCount()) {
                throw new IllegalStateException(String.format(
                        "lru=%d invalid=%d fixed=%d bufferCount=%d", lru,
                        invalid, volumes, pool.getBufferCount()));
            }
        }
    }
}
