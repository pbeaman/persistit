/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.startAndJoinAssertSuccess;
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

        startAndJoinAssertSuccess(5000, readThread, writeThread, removeThread);
    }

    //
    // Test helpers
    //

    private Exchange getNewExchange() throws PersistitException {
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }
}

