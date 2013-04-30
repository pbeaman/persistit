/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;

public class ExchangeRebalanceTest extends PersistitUnitTestCase {

    final StringBuilder sb = new StringBuilder();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        while (sb.length() < Buffer.MAX_BUFFER_SIZE) {
            sb.append(RED_FOX);
        }
    }

    /**
     * Construct a tree with two adjacent pages that are nearly full such that
     * the second key B of the right page is long and has very small elided byte
     * count relative to the first key A on that page. Removing key A requires
     * the left sibling to have B as its max key. If B won't fit on the left
     * page, then Persistit splits the left page to make room for it. Note that
     * changes in key or page structure will likely break this test.
     * 
     * @throws Exception
     */
    @Test
    public void testRebalanceException() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "rebalance", true);
        exchange.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        setUpPrettyFullBuffers(exchange, false, RED_FOX.length(), true);
        System.out.printf("\ntestRebalanceException\n");
        _persistit.checkAllVolumes();
        final int beforeRemove = countSiblings(exchange, 0);
        exchange.clear().append("b").previous(true);
        exchange.remove();
        _persistit.getCleanupManager().poll();
        _persistit.checkAllVolumes();
        final int afterRemove = countSiblings(exchange, 0);
        assertEquals("Remove should have caused rebalance", beforeRemove + 1, afterRemove);
    }

    /**
     * Similar logic to {@link #testRebalanceException()} except that the keys
     * are carefully constructed on the index leaf level rather than the data
     * level. To do this we use three long-ish records per major key in the data
     * pages so that the key pattern in the index leaf table aligns as desired.
     * 
     * @throws Exception
     */
    @Test
    public void testRebalanceIndexException() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "rebalance", true);
        exchange.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        setUpPrettyFullBuffers(exchange, true, 0, true);
        System.out.printf("\ntestRebalanceIndexException\n");
        _persistit.checkAllVolumes();
        final int beforeRemove = countSiblings(exchange, 1);

        exchange.clear().append("b").previous(true);
        exchange.cut();
        exchange.remove(Key.GT);
        _persistit.getCleanupManager().poll();
        _persistit.checkAllVolumes();
        final int afterRemove = countSiblings(exchange, 1);
        assertEquals("Remove should have caused rebalance", beforeRemove + 1, afterRemove);

    }

    private void setUpPrettyFullBuffers(final Exchange ex, final boolean asIndex, final int valueLength,
            final boolean discontinuous) throws PersistitException {

        final int depth = asIndex ? 1 : 0;
        int a, b;
        long leftPage = 0;

        // load page A with keys of increasing length
        for (a = 10;; a++) {
            setUpDeepKey(ex, 'a', a);
            storeValue(ex, valueLength, asIndex);
            if (ex.getTree().getDepth() > depth) {
                final Buffer b1 = ex.fetchBufferCopy(depth);
                // Stop when nearly full
                if (b1.getAvailableSize() < valueLength + 20) {
                    break;
                }
                leftPage = b1.getPageAddress();
            }
        }

        // load additional keys into page B
        for (b = a + 1;; b++) {
            if (ex.getTree().getDepth() > depth) {
                final Buffer b2 = ex.fetchBufferCopy(depth);
                if (b2.getPageAddress() != leftPage && b2.getAvailableSize() < valueLength + 20) {
                    break;
                }
            }
            setUpDeepKey(ex, discontinuous && b > a ? 'b' : 'a', b);
            storeValue(ex, valueLength, asIndex);
        }
    }

    private void setUpDeepKey(final Exchange ex, final char fill, final int n) {
        ex.getKey().clear().append(keyString(fill, n, n - 34, 4, n)).append(1);
    }

    private void setupValue(final Exchange ex, final int valueLength) {
        ex.getValue().put(sb.toString().substring(0, valueLength));
    }

    private void storeValue(final Exchange ex, final int valueLength, final boolean asIndex) throws PersistitException {
        if (asIndex) {
            // Fill up one data page so that the base key will be inserted into
            // an index page
            setupValue(ex, ex.getBufferPool().getBufferSize() / 4);
            for (int i = 1; i < 4; i++) {
                ex.cut().append(i).store();
            }
        } else {
            setupValue(ex, valueLength);
            ex.store();
        }
    }

    String keyString(final char fill, final int length, final int prefix, final int width, final int k) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < prefix && i < length; i++) {
            sb.append(fill);
        }
        sb.append(String.format("%0" + width + "d", k));
        for (int i = length - sb.length(); i < length; i++) {
            sb.append(fill);
        }
        sb.setLength(length);
        return sb.toString();
    }

    private int countSiblings(final Exchange exchange, final int level) throws PersistitException {
        int count = 0;
        exchange.clear().append(Key.BEFORE);
        Buffer b = exchange.fetchBufferCopy(level);
        while (true) {
            count++;
            final long rightSibling = b.getRightSibling();
            if (rightSibling != 0) {
                b = exchange.getBufferPool().getBufferCopy(exchange.getVolume(), rightSibling);
            } else {
                break;
            }
        }
        return count;
    }

}
