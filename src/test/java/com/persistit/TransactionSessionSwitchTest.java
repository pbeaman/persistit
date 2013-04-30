/**
 * Copyright 2005-2013 Akiban Technologies, Inc.
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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil;

/**
 * <p>
 * Bug https://bugs.launchpad.net/akiban-persistit/+bug/1174352
 * </p>
 * <p>
 * Documentation for com.persistit.Transaction says that with careful management
 * of ownership of a SessionId, it is possible to complete a transaction in a
 * different thread than the one that began it. However, this is not the case:
 * the second thread receives an IllegalMonitorStateException when attempting to
 * commit or abort the transaction.
 * </p>
 * <p>
 * The issue is that the TransactionStatus object used to represent transaction
 * state within the TransactionIndex uses a
 * java.util.concurrent.locks.ReentrantLock to represent its in-use state, and
 * only the thread that locked it may unlock it. *
 * </p>
 * <p>
 */
public class TransactionSessionSwitchTest extends PersistitUnitTestCase {

    private final static int SESSIONS = 100;
    private final static int STEPS = 197;
    private final static int THREADS = 17;
    private final static long TIMEOUT = 10000;

    private Queue<SessionId> sessionQueue = new ArrayBlockingQueue<SessionId>(SESSIONS);
    private Map<SessionId, AtomicInteger> sessionState = new HashMap<SessionId, AtomicInteger>();

    @Test
    public void sessionManagement() throws Exception {
        for (int i = 0; i < SESSIONS; i++) {
            final SessionId sessionId = new SessionId();
            sessionQueue.add(sessionId);
            sessionState.put(sessionId, new AtomicInteger(0));
        }

        Thread[] threads = new Thread[THREADS];

        final Tree tree = _persistit.getVolume("persistit").getTree("tt", true);
        for (int i = 0; i < THREADS; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    SessionId session;
                    while ((session = sessionQueue.poll()) != null) {
                        int state = sessionState.get(session).get();
                        try {
                            _persistit.setSessionId(session);
                            final Transaction txn = _persistit.getTransaction();
                            if (state == 0) {
                                txn.begin();
                            } else if (state > STEPS) {
                                if ((session.hashCode() % 3) == 0) {
                                    txn.rollback();
                                } else {
                                    txn.commit();
                                }
                                txn.end();
                            } else {
                                Exchange ex = _persistit.getExchange(tree.getVolume(), tree.getName(), false);
                                ex.getValue().put(Thread.currentThread().getName());
                                ex.clear().append(session.hashCode()).append(state).store();
                                _persistit.releaseExchange(ex);
                            }
                        } catch (PersistitException e) {
                            throw new RuntimeException(e);
                        } finally {
                            if (state <= STEPS) {
                                sessionState.get(session).incrementAndGet();
                                sessionQueue.add(session);
                            }
                        }
                    }
                }
            });

        }
        ConcurrentUtil.startAndJoinAssertSuccess(TIMEOUT, threads);

        final Exchange ex = _persistit.getExchange(tree.getVolume(), tree.getName(), false);
        for (SessionId session : sessionState.keySet()) {
            int count = 0;
            ex.clear().append(session.hashCode()).append(Key.BEFORE);
            while (ex.next()) {
                count++;
            }
            final int expected = (session.hashCode() % 3) == 0 ? 0 : STEPS;
            assertEquals("Mismatched count", expected, count);
        }
    }
}
