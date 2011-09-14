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

import java.util.concurrent.atomic.AtomicLong;

class VolumeStatistics {

    private volatile long _openTime;
    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;
    private volatile long _lastExtensionTime;
    private volatile long _nextAvailablePage;
    private volatile long _createTime;

    private AtomicLong _readCounter = new AtomicLong();
    private AtomicLong _writeCounter = new AtomicLong();
    private AtomicLong _getCounter = new AtomicLong();
    private AtomicLong _fetchCounter = new AtomicLong();
    private AtomicLong _traverseCounter = new AtomicLong();
    private AtomicLong _storeCounter = new AtomicLong();
    private AtomicLong _removeCounter = new AtomicLong();

    void reset() {
        _openTime = 0;
        _lastReadTime = 0;
        _lastWriteTime = 0;
        _lastExtensionTime = 0;
        _nextAvailablePage = 0;
        _createTime = 0;
        _readCounter.set(0);
        _writeCounter.set(0);
        _getCounter.set(0);
        _fetchCounter.set(0);
        _traverseCounter.set(0);
        _storeCounter.set(0);
        _readCounter.set(0);
    }
    
    
    void bumpReadCounter() {
        _readCounter.incrementAndGet();
        _lastReadTime = System.currentTimeMillis();
    }

    void bumpWriteCounter() {
        _writeCounter.incrementAndGet();
        _lastWriteTime = System.currentTimeMillis();
    }

    void bumpGetCounter() {
        _getCounter.incrementAndGet();
    }

    void bumpFetchCounter() {
        _fetchCounter.incrementAndGet();
    }

    void bumpTraverseCounter() {
        _traverseCounter.incrementAndGet();
    }

    void bumpStoreCounter() {
        _storeCounter.incrementAndGet();
    }

    void bumpRemoveCounter() {
        _removeCounter.incrementAndGet();
    }

    long getNextAvailablePage() {
        return _nextAvailablePage;
    }

    /**
     * Returns the count of physical disk read requests performed on this
     * <code>Volume</code>.
     * 
     * @return The count
     */
    public long getReadCounter() {
        return _readCounter.get();
    }

    /**
     * Returns the count of physical disk write requests performed on this
     * <code>Volume</code>.
     * 
     * @return The count
     */
    public long getWriteCounter() {
        return _writeCounter.get();
    }

    /**
     * Returns the count of logical buffer fetches performed against this
     * <code>Volume</code>. The ratio of get to read operations indicates how
     * effectively the buffer pool is reducing disk I/O.
     * 
     * @return The count
     */
    public long getGetCounter() {
        return _getCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#fetch} operations. These include
     * {@link Exchange#traverse}, {@link Exchange#fetchAndStore} and
     * {@link Exchange#fetchAndRemove} operations. This count is maintained with
     * the stored Volume and is not reset when Persistit closes. It is provided
     * to assist application performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getFetchCounter() {
        return _fetchCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#traverse} operations. These include
     * {@link Exchange#next} and {@link Exchange#_previous} operations. This
     * count is maintained with the stored Volume and is not reset when
     * Persistit closes. It is provided to assist application performance
     * tuning.
     * 
     * @return The count of key traversal operations performed on this in this
     *         Volume.
     */
    public long getTraverseCounter() {
        return _traverseCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#store} operations, including
     * {@link Exchange#fetchAndStore} and {@link Exchange#incrementValue}
     * operations. This count is maintained with the stored Volume and is not
     * reset when Persistit closes. It is provided to assist application
     * performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getStoreCounter() {
        return _storeCounter.get();
    }

    /**
     * Returns the count of {@link Exchange#remove} operations, including
     * {@link Exchange#fetchAndRemove} operations. This count is maintained with
     * the stored Volume and is not reset when Persistit closes. It is provided
     * to assist application performance tuning.
     * 
     * @return The count of records fetched from this Volume.
     */
    public long getRemoveCounter() {
        return _removeCounter.get();
    }

    /**
     * Returns the time at which this <code>Volume</code> was created.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getCreateTime() {
        return _createTime;
    }

    /**
     * Returns the time at which this <code>Volume</code> was last opened.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getOpenTime() {
        return _openTime;
    }

    /**
     * Returns the time at which the last physical read operation was performed
     * on <code>Volume</code>.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastReadTime() {
        return _lastReadTime;
    }

    /**
     * Returns the time at which the last physical write operation was performed
     * on <code>Volume</code>.
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastWriteTime() {
        return _lastWriteTime;
    }

    /**
     * Returns the time at which this <code>Volume</code> was last extended
     * (increased in physical size).
     * 
     * @return The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastExtensionTime() {
        return _lastExtensionTime;
    }

    

    void setOpenTime(long openTime) {
        _openTime = openTime;
    }

    void setLastReadTime(long lastReadTime) {
        _lastReadTime = lastReadTime;
    }

    void setLastWriteTime(long lastWriteTime) {
        _lastWriteTime = lastWriteTime;
    }

    void setLastExtensionTime(long lastExtensionTime) {
        _lastExtensionTime = lastExtensionTime;
    }

    void setNextAvailablePage(long nextAvailablePage) {
        _nextAvailablePage = nextAvailablePage;
    }

    void setCreateTime(long createTime) {
        _createTime = createTime;
    }

}
