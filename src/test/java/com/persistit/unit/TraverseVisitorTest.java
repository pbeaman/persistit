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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Exchange.TraverseVisitor;
import com.persistit.Key;
import com.persistit.PersistitUnitTestCase;
import com.persistit.ReadOnlyExchange;
import com.persistit.exception.PersistitException;

public class TraverseVisitorTest extends PersistitUnitTestCase {

    private final AtomicInteger visited = new AtomicInteger();
    private final AtomicInteger visitLimit = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicBoolean reverse = new AtomicBoolean();

    @Test
    public void simpleTraverseVisitor() throws PersistitException {
        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) {
                visited.incrementAndGet();
                return visited.get() < visitLimit.get();
            }
        };
        doCombo(tv);

    }

    @Test
    public void traverseVisitorNonMutatingTraverseMethods() throws Exception {

        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) throws PersistitException {
                visited.incrementAndGet();
                final int v = visited.get();
                return v < visitLimit.get();
            }
        };

        doCombo(tv);
    }

    /**
     * Demonstrates that a TraverseVisitor can (carefully) modify the Key of a
     * supplied ReadOnlyExchange.
     * 
     * @throws Exception
     */
    @Test
    public void testMutateKey() throws Exception {
        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) throws PersistitException {
                visited.incrementAndGet();
                final int v = visited.get();
                if (reverse.get()) {
                    final int k = ex.getKey().reset().decodeInt();
                    ex.getKey().clear().append(k - 1).append(0);
                } else {
                    ex.getKey().append("a");
                }
                return v < visitLimit.get();
            }
        };

        doCombo(tv);

    }

    private void doCombo(final TraverseVisitor tv) throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final String mockValue = createString(64);
        for (int i = 0; i < 1000; i++) {
            ex.clear().append(i);
            ex.getValue().put(mockValue);
            ex.store();
        }
        final int max = Integer.MAX_VALUE;
        doTraverse(ex, tv, false, Key.GT, max, 1000);
        doTraverse(ex, tv, false, Key.GT, max, 1000);
        doTraverse(ex, tv, false, Key.LT, max, 1000);
        doTraverse(ex, tv, false, Key.LT, max, 1000);
        doTraverse(ex, tv, false, Key.GT, 1, 1);
        doTraverse(ex, tv, false, Key.GT, 1, 1);
        doTraverse(ex, tv, false, Key.GT, 10, 10);
        doTraverse(ex, tv, false, Key.GT, 10, 10);
        doTraverse(ex, tv, false, Key.LT, 1, 1);
        doTraverse(ex, tv, false, Key.LT, 1, 1);
        doTraverse(ex, tv, false, Key.LT, 10, 10);
        doTraverse(ex, tv, false, Key.LT, 10, 10);

        doTraverse(ex, tv, true, Key.GT, max, 1000);
        doTraverse(ex, tv, true, Key.GT, max, 1000);
        doTraverse(ex, tv, true, Key.LT, max, 1000);
        doTraverse(ex, tv, true, Key.LT, max, 1000);
        doTraverse(ex, tv, true, Key.GT, 1, 1);
        doTraverse(ex, tv, true, Key.GT, 1, 1);
        doTraverse(ex, tv, true, Key.GT, 10, 10);
        doTraverse(ex, tv, true, Key.GT, 10, 10);
        doTraverse(ex, tv, true, Key.LT, 1, 1);
        doTraverse(ex, tv, true, Key.LT, 1, 1);
        doTraverse(ex, tv, true, Key.LT, 10, 10);
        doTraverse(ex, tv, true, Key.LT, 10, 10);

        doTraverse(ex, tv, false, Key.GTEQ, max, 1000);
        doTraverse(ex, tv, false, Key.GTEQ, max, 1000);
        doTraverse(ex, tv, false, Key.LTEQ, max, 1000);
        doTraverse(ex, tv, false, Key.LTEQ, max, 1000);

        doTraverseFrom(1, ex, tv, false, Key.GT, max, 998);
        doTraverseFrom(1, ex, tv, false, Key.GTEQ, max, 999);
        doTraverseFrom(998, ex, tv, false, Key.LT, max, 998);
        doTraverseFrom(998, ex, tv, false, Key.LTEQ, max, 999);

        doTraverseFrom(500, ex, tv, false, Key.EQ, max, 1);
        doTraverseFrom(1001, ex, tv, false, Key.EQ, max, 0);
    }

    private void doTraverse(final Exchange ex, final TraverseVisitor tv, final boolean deep,
            final Key.Direction direction, final int limit, final int expected) throws PersistitException {
        ex.clear().append(direction == Key.LT || direction == Key.LTEQ ? Key.AFTER : Key.BEFORE);
        doTraverse0(ex, tv, deep, direction, limit, expected);
    }

    private void doTraverseFrom(final int from, final Exchange ex, final TraverseVisitor tv, final boolean deep,
            final Key.Direction direction, final int limit, final int expected) throws PersistitException {
        ex.clear().append(from);
        doTraverse0(ex, tv, deep, direction, limit, expected);
    }

    private void doTraverse0(final Exchange ex, final TraverseVisitor tv, final boolean deep,
            final Key.Direction direction, final int limit, final int expected) throws PersistitException {
        reverse.set(direction == Key.LT || direction == Key.LTEQ);
        visited.set(0);
        visitLimit.set(limit);
        assertEquals(limit < 1000, ex.traverse(direction, deep, Integer.MAX_VALUE, tv));
        assertEquals(expected, visited.get());
    }

}
