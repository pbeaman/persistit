/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Repository of counters for operations on a {@link Volume}
 * 
 * @author peter
 * 
 */
class VolumeStatistics {

    private volatile long _openTime;
    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;
    private volatile long _lastExtensionTime;
    private volatile long _nextAvailablePage;
    private volatile long _createTime;

    private final AtomicLong _readCounter = new AtomicLong();
    private final AtomicLong _writeCounter = new AtomicLong();
    private final AtomicLong _getCounter = new AtomicLong();
    private final AtomicLong _fetchCounter = new AtomicLong();
    private final AtomicLong _traverseCounter = new AtomicLong();
    private final AtomicLong _storeCounter = new AtomicLong();
    private final AtomicLong _removeCounter = new AtomicLong();

    /**
     * @return the count of physical disk read requests performed on this
     *         <code>Volume</code>
     */
    public long getReadCounter() {
        return _readCounter.get();
    }

    /**
     * @return the count of physical disk write requests performed on this
     *         <code>Volume</code>
     */
    public long getWriteCounter() {
        return _writeCounter.get();
    }

    /**
     * @return the count of logical buffer fetches performed against this
     *         <code>Volume</code>.
     */
    public long getGetCounter() {
        return _getCounter.get();
    }

    /**
     * @return the count of {@link Exchange#fetch} operations, including
     *         {@link Exchange#fetchAndStore} and
     *         {@link Exchange#fetchAndRemove}
     */
    public long getFetchCounter() {
        return _fetchCounter.get();
    }

    /**
     * @return the count of {@link Exchange#traverse} operations, including
     *         {@link Exchange#next} and {@link Exchange#_previous}
     */
    public long getTraverseCounter() {
        return _traverseCounter.get();
    }

    /**
     * @return the count of {@link Exchange#store} operations, including
     *         {@link Exchange#fetchAndStore} and
     *         {@link Exchange#incrementValue}
     */
    public long getStoreCounter() {
        return _storeCounter.get();
    }

    /**
     * @return the count of {@link Exchange#remove} operations, including
     *         {@link Exchange#fetchAndRemove}
     */
    public long getRemoveCounter() {
        return _removeCounter.get();
    }

    /**
     * @return The system time at which this <code>Volume</code> was created
     */
    public long getCreateTime() {
        return _createTime;
    }

    /**
     * @return the system time at which this <code>Volume</code> was last opened
     */
    public long getOpenTime() {
        return _openTime;
    }

    /**
     * @return the system time at which the last physical read operation was
     *         performed on <code>Volume</code
     */
    public long getLastReadTime() {
        return _lastReadTime;
    }

    /**
     * @return the system time at which the last physical write operation was
     *         performed on <code>Volume</code>.
     */
    public long getLastWriteTime() {
        return _lastWriteTime;
    }

    /**
     * @return the system time at which this <code>Volume</code> was last
     *         extended (increased in physical size).
     */
    public long getLastExtensionTime() {
        return _lastExtensionTime;
    }

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
