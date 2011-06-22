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

/**
 * This interface defines the replacement policy used by the persistit buffer
 * pool.
 */
public interface BufferReplacementStrategy {

    /**
     * Find a buffer in the given list of buffers starting at a certain point in
     * the list indicated by the bucket parameter
     * 
     * @param bufferList
     *            a doubly-linked list of Buffer objects
     * @param bucket
     *            area of the list to start the search
     * @return a clean replaceable Buffer object if found; otherwise null
     */
    public Buffer getBuffer(final Buffer[] bufferList, final int bucket) throws PersistitException;

}
