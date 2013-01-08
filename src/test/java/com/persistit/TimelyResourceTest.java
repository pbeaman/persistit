/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static com.persistit.TransactionStatus.UNCOMMITTED;
import static com.persistit.util.Util.NS_PER_S;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Version.PrunableVersion;
import com.persistit.Version.VersionCreator;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.util.Util;

public class TimelyResourceTest extends PersistitUnitTestCase {

    private int _idCounter;

    static class TestResource implements PrunableVersion {
        final int _id;
        final AtomicInteger _pruned = new AtomicInteger();

        TestResource(final int id) {
            _id = id;
        }

        @Override
        public boolean prune() {
            _pruned.incrementAndGet();
            return true;
        }

        @Override
        public String toString() {
            return String.format("<%,d:%,d>", _id, _pruned.get());
        }
    }

    @Test
    public void testAddAndPruneResources() throws Exception {
        testAddAndPruneResources1(false);
        testAddAndPruneResources1(true);
    }

    private void testAddAndPruneResources1(final boolean withTransactions) throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final TimelyResource<TimelyResourceTest, TestResource> tr = new TimelyResource<TimelyResourceTest, TestResource>(
                _persistit, this);
        final long[] history = new long[5];
        final TestResource[] resources = new TestResource[5];
        for (int i = 0; i < 5; i++) {
            if (withTransactions) {
                txn.begin();
            }
            final TestResource resource = new TestResource(i);
            resources[i] = resource;
            tr.addVersion(resource, txn);
            if (withTransactions) {
                txn.commit();
                txn.end();
            }
            history[i] = _persistit.getTimestampAllocator().updateTimestamp();
        }
        assertEquals("Incorrect version count", 5, tr.getVersionCount());

        for (int i = 0; i < 5; i++) {
            final TestResource t = tr.getVersion(history[i], 0);
            assertTrue("Missing version", t != null);
            assertEquals("Wrong version", i, t._id);
        }
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        tr.prune();
        assertEquals("Should have one version left", 1, tr.getVersionCount());
        assertEquals("Wrong version", 4, tr.getVersion(UNCOMMITTED, 0)._id);

        tr.delete(txn);

        assertEquals("Should have two versions left", 2, tr.getVersionCount());
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        tr.prune();
        assertEquals("Should have no versions left", 0, tr.getVersionCount());

        for (int i = 0; i < 5; i++) {
            assertEquals("Should have been pruned", 1, resources[i]._pruned.get());
        }
    }

    @Test
    public void concurrentAddAndPruneResources() throws Exception {
        final TimelyResource<TimelyResourceTest, TestResource> tr = new TimelyResource<TimelyResourceTest, TestResource>(
                _persistit, this);
        final Random random = new Random(1);
        final long expires = System.nanoTime() + 10 * NS_PER_S;
        final AtomicInteger sequence = new AtomicInteger();
        final AtomicInteger rollbackCount = new AtomicInteger();
        final List<Thread> threads = new ArrayList<Thread>();

        while (System.nanoTime() < expires) {
            for (final Iterator<Thread> iter = threads.iterator(); iter.hasNext();) {
                if (!iter.next().isAlive()) {
                    iter.remove();
                }
            }
            while (threads.size() < 20) {
                final Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        doConcurrentTransaction(tr, random, sequence, rollbackCount);
                    }
                });
                threads.add(t);
                t.start();
            }
            Util.sleep(10);
            tr.prune();
        }

        for (final Thread thread : threads) {
            thread.join();
        }

        assertTrue("Every transaction rolled back", rollbackCount.get() < sequence.get());
        System.out.printf("%,d entries, %,d rollbacks\n", sequence.get(), rollbackCount.get());
    }

    private void doConcurrentTransaction(final TimelyResource<TimelyResourceTest, TestResource> tr,
            final Random random, final AtomicInteger sequence, final AtomicInteger rollbackCount) {
        try {
            final Transaction txn = _persistit.getTransaction();
            for (int i = 0; i < 25; i++) {
                txn.begin();
                try {
                    final int id = sequence.incrementAndGet();
                    tr.addVersion(new TestResource(id), txn);
                    final int delay = (1 << random.nextInt(3));
                    // Up to 7/1000 of a second
                    Util.sleep(delay);
                    final TestResource mine = tr.getVersion(txn);
                    assertEquals("Should not have been pruned yet", 0, mine._pruned.get());
                    assertEquals("Wrong resource", id, mine._id);
                    if (random.nextInt(10) == 0) {
                        txn.rollback();
                    } else {
                        txn.commit();
                    }
                } catch (final RollbackException e) {
                    txn.rollback();
                    rollbackCount.incrementAndGet();
                } finally {
                    txn.end();
                }
            }
        } catch (final RollbackException e) {
            rollbackCount.incrementAndGet();
        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @Test
    public void deleteResource() throws Exception {
        final TimelyResource<TimelyResourceTest, TestResource> tr = new TimelyResource<TimelyResourceTest, TestResource>(
                _persistit, this);
        _idCounter = 0;
        final VersionCreator<TimelyResourceTest, TestResource> creator = new VersionCreator<TimelyResourceTest, TestResource>() {

            @Override
            public TestResource createVersion(final TimelyResource<TimelyResourceTest, ? extends TestResource> resource)
                    throws PersistitException {
                return new TestResource(++resource.getContainer()._idCounter);
            }
        };
        final Transaction txn1 = _persistit.getTransaction();
        _persistit.setSessionId(new SessionId());
        final Transaction txn2 = _persistit.getTransaction();
        TestResource v1;
        txn1.begin();
        v1 = tr.getVersion(txn1, creator);
        assert v1._id == _idCounter;
        txn2.begin();
        txn1.incrementStep();
        tr.delete(txn1);
        tr.prune();
        assertEquals("Should still be two versions", 2, tr.getVersionCount());
        txn2.commit();
        txn1.commit();
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        tr.prune();
        assertEquals("Should now have no versions", 0, tr.getVersionCount());

    }

    @Test
    public void versions() throws Exception {
        final TimelyResource<TimelyResourceTest, TestResource> tr = new TimelyResource<TimelyResourceTest, TestResource>(
                _persistit, this);
        _idCounter = 0;
        final VersionCreator<TimelyResourceTest, TestResource> creator = new VersionCreator<TimelyResourceTest, TestResource>() {

            @Override
            public TestResource createVersion(final TimelyResource<TimelyResourceTest, ? extends TestResource> resource)
                    throws PersistitException {
                return new TestResource(++resource.getContainer()._idCounter);
            }
        };
        final Semaphore semaphore1 = new Semaphore(0);
        final Transaction txn = _persistit.getTransaction();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            try {
                txn.begin();
                tr.addVersion(creator.createVersion(tr), txn);
                txn.commit();
            } finally {
                txn.end();
            }
            final Semaphore semaphore2 = new Semaphore(0);
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    final Transaction txn = _persistit.getTransaction();
                    try {
                        txn.begin();
                        final TestResource t = tr.getVersion(txn);
                        assertEquals(t._id, tr.getContainer()._idCounter);
                        semaphore2.release();
                        semaphore1.acquire();
                        txn.commit();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        txn.end();
                    }
                }
            });
            threads[i].start();
            semaphore2.acquire();
        }
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        tr.prune();
        assertEquals(10, tr.getVersionCount());
        semaphore1.release(10);
        for (Thread thread : threads) {
            thread.join();
        }
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        tr.prune();
        assertEquals(1, tr.getVersionCount());
        assertEquals("Surviving primordial version should be last one committed", 10, tr.getVersion(null)._id);
    }

}
