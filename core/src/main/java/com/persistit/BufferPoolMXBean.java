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

public interface BufferPoolMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=BufferPool";

    /**
     * Return the size of <code>Buffer</code>s managed by this pool.
     * 
     * @return The size in bytes of each buffer in this pool
     */
    public int getBufferSize();

    /**
     * Return the count of <code>Buffer</code>s managed by this pool.
     * 
     * @return The count
     */
    public int getBufferCount();

    /**
     * Return the count of lookup operations for pages images that resulted in a
     * physical disk read operation. This number, in comparison with the hit
     * counter, indicates how effective the cache is in reducing disk I/O.
     * 
     * @return The get count
     */
    public long getMissCount();

    /**
     * Return the count of lookup operations for pages images in this pool for
     * which the page image was already found in this <code>BufferPool</code>.
     * This number, in comparison with the get counter, indicates how effective
     * the cache is in reducing disk I/O.
     * 
     * @return The hit count
     */
    public long getHitCount();

    /**
     * @return Count of pages newly created in this <code>BufferPool</code>.
     */
    public long getNewCount();

    /**
     * Get the count of valid pages evicted from this <code>BufferPool</code> to
     * make room for newly read or created pages.
     * 
     * @return The evicted page count
     */
    public long getEvictCount();

    /**
     * Get the "hit ratio" - the number of hits divided by the number of overall
     * gets. A value close to 1.0 indicates that most attempts to find data in
     * the <code>BufferPool</code> are successful - i.e., that the cache is
     * effectively reducing the need for disk read operations.
     * 
     * @return The ratio
     */
    public double getHitRatio();

    /**
     * Get the count of valid pages in this pool.
     * 
     * @return The count of valid pages in this pool
     */
    public int getValidPageCount();

    /**
     * Get the count of dirty pages (pages that contain updates not yet written
     * to disk) in this pool.
     * 
     * @return The count of dirty pages in this pool
     */
    public int getDirtyPageCount();

    /**
     * Get the count of pages on which running threads have reader
     * (non-exclusive), but <i>not</i> writer (exclusive) claims in this pool.
     * 
     * @return The count of pages with reader claims
     */
    public int getReaderClaimedPageCount();

    /**
     * Get the count of pages on which running threads have writer (exclusive)
     * claims in this pool.
     * 
     * @return The count of pages with writer claims
     */
    public int getWriterClaimedPageCount();

}
