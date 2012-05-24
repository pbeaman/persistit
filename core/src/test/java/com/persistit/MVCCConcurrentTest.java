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

import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.startAndJoinAllAssertSuccess;
import static com.persistit.unit.ConcurrentUtil.ThrowingRunnable;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class MVCCConcurrentTest extends MVCCTestBase {
    private final String KEY1 = "key1";

    @Test
    public void testReadWriteRemoveLongRecNoTrx() {
        final int NUM_OPS = 1000;
        final String LONG_STR = createString(ex1.getVolume().getPageSize() * 50);

        Thread readThread = createThread("READ_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    fetch(ex, KEY1, false);
                    Value value = ex.getValue();
                    if (value.isDefined()) {
                        assertEquals("iteration i " + i, LONG_STR, value.getString());
                    }
                }
                _persistit.releaseExchange(ex);
            }
        });

        Thread writeThread = createThread("WRITE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    store(ex, i, i);
                }
                _persistit.releaseExchange(ex);
            }
        });

        Thread removeThread = createThread("REMOVE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                int j = 0;
                for (int i = 0; i < NUM_OPS; ++i, ++j) {
                    if (j == 0) {
                        store(ex, KEY1, LONG_STR);
                    } else if (j == 5) {
                        remove(ex, KEY1);
                        j = 0;
                    }
                }
            }
        });

        startAndJoinAllAssertSuccess(readThread, writeThread, removeThread);
    }

    //
    // Test helpers
    //

    private Exchange getNewExchange() throws PersistitException {
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }
}