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

import java.rmi.RemoteException;

import com.persistit.Management.BufferPoolInfo;

public class BufferPoolMXBeanImpl implements BufferPoolMXBean {

    private final static long MAX_STALE = 5000;

    private final Persistit _persistit;

    private final int _bufferSize;

    private BufferPoolInfo _recent;

    static String mbeanName(final int bufferPoolSize) {
        return MXBEAN_NAME + "." + bufferPoolSize;
    }

    BufferPoolMXBeanImpl(final Persistit persistit, final int bufferSize) {
        _persistit = persistit;
        _bufferSize = bufferSize;
        _recent = new BufferPoolInfo();
    }

    private BufferPoolInfo recent() {
        long now = System.currentTimeMillis();
        if (_recent.getAcquisitionTime() < now - MAX_STALE) {
            try {
                BufferPoolInfo[] array = _persistit.getManagement().getBufferPoolInfoArray();
                for (BufferPoolInfo info : array) {
                    if (info.getBufferSize() == _bufferSize) {
                        _recent = info;
                    }
                }
            } catch (RemoteException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if (_recent == null) {
            _recent = new BufferPoolInfo();
        }
        return _recent;
    }

    /**
     * Return the size of <code>Buffer</code>s managed by this pool.
     * 
     * @return The size in bytes of each buffer in this pool
     */
    @Override
    public int getBufferSize() {
        return recent().getBufferSize();
    }

    /**
     * Return the count of <code>Buffer</code>s managed by this pool.
     * 
     * @return The count
     */
    @Override
    public int getBufferCount() {
        return recent().getBufferCount();
    }

    /**
     * Return the count of lookup operations for pages images that resulted in a
     * physical disk read operation. This number, in comparison with the hit
     * counter, indicates how effective the cache is in reducing disk I/O.
     * 
     * @return The get count
     */
    @Override
    public long getMissCount() {
        return recent().getMissCount();
    }

    /**
     * Return the count of lookup operations for pages images in this pool for
     * which the page image was already found in this <code>BufferPool</code>.
     * This number, in comparison with the get counter, indicates how effective
     * the cache is in reducing disk I/O.
     * 
     * @return The hit count
     */
    @Override
    public long getHitCount() {
        return recent().getHitCount();
    }

    /**
     * @return Count of pages newly created in this <code>BufferPool</code>.
     */
    @Override
    public long getNewCount() {
        return recent().getNewCount();
    }

    /**
     * Get the count of valid pages evicted from this <code>BufferPool</code> to
     * make room for newly read or created pages.
     * 
     * @return The evicted page count
     */
    @Override
    public long getEvictCount() {
        return recent().getEvictCount();
    }

    /**
     * Return count of pages written from this pool.
     * 
     * @return The forced write count
     */
    public long getWriteCount() {
        return recent().getWriteCount();
    }

    /**
     * Return count of pages forced to be written due to an updated after a
     * checkpoint
     * 
     * @return The forced checkpoint write count
     */
    public long getForcedCheckpointWriteCount() {
        return recent().getForcedCheckpointWriteCount();
    }

    /**
     * Return count of pages forced to be written when dirty on eviction
     * 
     * @return The forced write count
     */
    public long getForcedWriteCount() {
        return recent().getForcedWriteCount();
    }

    /**
     * Get the "hit ratio" - the number of hits divided by the number of overall
     * gets. A value close to 1.0 indicates that most attempts to find data in
     * the <code>BufferPool</code> are successful - i.e., that the cache is
     * effectively reducing the need for disk read operations.
     * 
     * @return The ratio
     */
    @Override
    public double getHitRatio() {
        return recent().getHitRatio();
    }

    /**
     * Get the count of valid pages in this pool.
     * 
     * @return The count of valid pages in this pool
     */
    @Override
    public int getValidPageCount() {
        return recent().getValidPageCount();
    }

    /**
     * Get the count of dirty pages (pages that contain updates not yet written
     * to disk) in this pool.
     * 
     * @return The count of dirty pages in this pool
     */
    @Override
    public int getDirtyPageCount() {
        return recent().getDirtyPageCount();
    }

    /**
     * Get the count of pages on which running threads have reader
     * (non-exclusive), but <i>not</i> writer (exclusive) claims in this pool.
     * 
     * @return The count of pages with reader claims
     */
    @Override
    public int getReaderClaimedPageCount() {
        return recent().getReaderClaimedPageCount();
    }

    /**
     * Get the count of pages on which running threads have writer (exclusive)
     * claims in this pool.
     * 
     * @return The count of pages with writer claims
     */
    @Override
    public int getWriterClaimedPageCount() {
        return recent().getWriterClaimedPageCount();
    }

}
