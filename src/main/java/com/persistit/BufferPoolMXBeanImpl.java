/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

import java.rmi.RemoteException;

import com.persistit.Management.BufferPoolInfo;
import com.persistit.mxbeans.BufferPoolMXBean;
import com.persistit.mxbeans.Description;

/**
 * MXBean that exposes information about a {@link BufferPool}.
 * 
 * @author peter
 * 
 */
class BufferPoolMXBeanImpl implements BufferPoolMXBean {

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
        final long now = System.currentTimeMillis();
        if (_recent.getAcquisitionTime() < now - MAX_STALE) {
            try {
                final BufferPoolInfo[] array = _persistit.getManagement().getBufferPoolInfoArray();
                for (final BufferPoolInfo info : array) {
                    if (info.getBufferSize() == _bufferSize) {
                        _recent = info;
                    }
                }
            } catch (final RemoteException e) {
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
    @Description("The size in bytes of each buffer in this pool")
    public int getBufferSize() {
        return recent().getBufferSize();
    }

    /**
     * Return the count of <code>Buffer</code>s managed by this pool.
     * 
     * @return The count
     */
    @Override
    @Description("Return the count of Buffer managed by this pool")
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
    @Description("Count of unsuccessful lookup operations that resulted in physical disks")
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
    @Description("Count of successful lookup operations")
    public long getHitCount() {
        return recent().getHitCount();
    }

    /**
     * @return Count of pages newly created in this <code>BufferPool</code>.
     */
    @Override
    @Description("Count of pages newly created")
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
    @Description("Count of pages replaced by other pages")
    public long getEvictCount() {
        return recent().getEvictCount();
    }

    /**
     * Return count of pages written from this pool.
     * 
     * @return The write count
     */
    @Override
    @Description("Count of pages written from this BufferPool")
    public long getWriteCount() {
        return recent().getWriteCount();
    }

    /**
     * Return count of pages forced to be written due to an update after a
     * checkpoint
     * 
     * @return The forced checkpoint write count
     */
    @Override
    @Description("Count of pages forced to be written due to an update after a checkpoint")
    public long getForcedCheckpointWriteCount() {
        return recent().getForcedCheckpointWriteCount();
    }

    /**
     * Return count of pages forced to be written when dirty on eviction
     * 
     * @return The forced write count
     */
    @Override
    @Description("Count of pages forced to be written when dirty on eviction")
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
    @Description("Ratio of hits to total page lookup operations")
    public double getHitRatio() {
        return recent().getHitRatio();
    }

    /**
     * Get the count of valid pages in this pool.
     * 
     * @return The count of valid pages in this pool
     */
    @Override
    @Description("Count of valid pages in this pool")
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
    @Description("The count of dirty pages in this pool")
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
    @Description("The count of pages with reader claims")
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
    @Description("The count of pages with writer claims")
    public int getWriterClaimedPageCount() {
        return recent().getWriterClaimedPageCount();
    }

    /**
     * @return Earliest timestamp of any dirty page in this
     *         <code>BufferPool</code>.
     */
    @Override
    @Description("Earliest timestamp of any dirty page in this >BufferPool")
    public long getEarliestDirtyTimestamp() {
        return recent().getEarliestDirtyTimestamp();
    }

}
