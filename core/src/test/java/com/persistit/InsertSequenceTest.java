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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Exchange.Sequence;
import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;
import com.persistit.unit.PersistitUnitTestCase;

public class InsertSequenceTest extends PersistitUnitTestCase {

    Sequence sequence;
    int index;

    private class TestPolicy extends SplitPolicy {

        @Override
        public int splitFit(Buffer buffer, int kbOffset, int insertAt, boolean replace, int leftSize, int rightSize,
                int currentSize, int virtualSize, int capacity, int splitInfo, Sequence sequence) {
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
            String key = "a" + index;
            boolean expectForward = index % 10 != 0;
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
            String key = "a" + index;
            boolean expectReverse = index > 99999;
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
