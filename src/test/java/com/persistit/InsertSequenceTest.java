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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Exchange.Sequence;
import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;

public class InsertSequenceTest extends PersistitUnitTestCase {

    Sequence sequence;
    int index;

    private class TestPolicy extends SplitPolicy {

        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            if (buffer.isDataPage()) {
                InsertSequenceTest.this.sequence = sequence;
            }
            return SplitPolicy.PACK_BIAS.splitFit(buffer, kbOffset, insertAt, replace, leftSize, rightSize,
                    currentSize, virtualSize, capacity, splitInfo, sequence);
        }

    }

    @Test
    public void testPacked() throws PersistitException {
        // final Exchange exchange = _persistit.getExchange("persistit",
        // "InsertSequenceTest", true);
        // exchange.removeAll();
        // exchange.setSplitPolicy(new TestPolicy());
        // for (index = 1; index < 100000; index++) {
        // exchange.clear().append(index);
        // exchange.getValue().put("Record #" + index);
        // exchange.store();
        // if (sequence != null) {
        // assertEquals(Sequence.FORWARD, sequence);
        // }
        // sequence = null;
        // }
        // // -1 for the volume head page which isn't on the LRU queue.
        // int bufferCount = exchange.getVolume().getPool().getBufferCount() -
        // 1;
        // assertEquals(bufferCount,exchange.getVolume().getPool().countLruQueueEntries());
        // for (index = 10000; index < 100000; index += 10000) {
        // Buffer buffer = exchange.clear().append(index).fetchBufferCopy(0);
        // assertTrue(buffer.getAvailableSize() < buffer.getBufferSize() * .9);
        // }
        // assertEquals(bufferCount,exchange.getVolume().getPool().countLruQueueEntries());
    }

    @Test
    public void testForwardSequence() throws PersistitException {
        final Exchange exchange = _persistit.getExchange("persistit", "InsertSequenceTest", true);
        exchange.removeAll();
        exchange.setSplitPolicy(new TestPolicy());

        for (index = 1; index < 100000; index++) {
            final String key = "a" + index;
            final boolean expectForward = index % 10 != 0;
            exchange.clear().append(key);
            exchange.getValue().put("Record #" + index);
            exchange.store();
            if (sequence != null) {
                assertEquals(expectForward ? Sequence.FORWARD : Sequence.NONE, sequence);
            }
            sequence = null;
        }
    }

    @Test
    public void testReverseSequence() throws PersistitException {
        final Exchange exchange = _persistit.getExchange("persistit", "InsertSequenceTest", true);
        exchange.removeAll();
        exchange.setSplitPolicy(new TestPolicy());

        for (index = 1000000; index >= 0; index--) {
            final String key = "a" + index;
            final boolean expectReverse = index > 99999;
            exchange.clear().append(key);
            exchange.getValue().put("Record #" + index);
            exchange.store();
            if (sequence != null) {
                assertEquals(expectReverse ? Sequence.REVERSE : Sequence.NONE, sequence);
            }
            sequence = null;
        }
    }

    @Override
    public void runAllTests() throws Exception {
        testForwardSequence();
        testReverseSequence();
    }
}
