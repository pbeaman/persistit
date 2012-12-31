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

import static com.persistit.unit.UnitTestProperties.VOLUME_NAME;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class TreeBuilderTest extends PersistitUnitTestCase {
    private final static int COUNT = 10000;


    @Test
    public void basicTest() throws Exception {

        final TreeBuilder tb = new TreeBuilder(_persistit) {
            @Override
            protected void reportSorted(final long count) {
                System.out.println("Sorted " + count);
            }

            protected void reportMerged(final long count) {
                System.out.println("Merged " + count);
            }

        };
        tb.setReportKeyCountMultiple(COUNT / 2);

        final List<Integer> shuffled = new ArrayList<Integer>(COUNT);
        for (int i = 0; i < COUNT; i++) {
            shuffled.add(i);
        }
        Collections.shuffle(shuffled);
        final Exchange a = _persistit.getExchange(VOLUME_NAME, "a", true);
        final Exchange b = _persistit.getExchange(VOLUME_NAME, "b", true);
        final Exchange c = _persistit.getExchange(VOLUME_NAME, "c", true);

        for (int i = 0; i < COUNT; i++) {
            final int ka = shuffled.get(i);
            final int kb = shuffled.get((i + COUNT / 3) % COUNT);
            final int kc = shuffled.get((i + (2 * COUNT) / 3) % COUNT);
            a.clear().append(ka).append("a");
            a.getValue().put(i);
            b.clear().append(kb).append("b");
            b.getValue().put(i);
            c.clear().append(kc).append("c");
            c.getValue().put(i);
            tb.store(a);
            tb.store(b);
            tb.store(c);

        }

        tb.merge();

        a.clear();
        b.clear();
        c.clear();
        int count = 0;
        while (a.next(true) && b.next(true) && c.next(true)) {
            assertEquals("Expect correct key value", count, a.getKey().decodeInt());
            assertEquals("Expect correct key value", count, b.getKey().decodeInt());
            assertEquals("Expect correct key value", count, c.getKey().decodeInt());
            count++;
        }
        assertEquals("Expect every key value", COUNT, count);
        
    }

    @Test
    public void customizationMethods() throws Exception {
        final AtomicBoolean doReplace = new AtomicBoolean();
        final AtomicInteger duplicateCount = new AtomicInteger();
        final AtomicInteger beforeMergeCount = new AtomicInteger();
        final AtomicInteger afterMergeCount = new AtomicInteger();

        final TreeBuilder tb = new TreeBuilder(_persistit) {
           
            @Override
            protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
                duplicateCount.incrementAndGet();
                return doReplace.get();
            }
        
            @Override
            protected boolean beforeMergeKey(final Exchange ex) throws Exception {
                beforeMergeCount.incrementAndGet();
                return ex.getKey().decodeInt() != 3;
            }
            
            protected void afterMergeKey(final Exchange ex) throws Exception {
                afterMergeCount.incrementAndGet();
            }

        };
        final Exchange ex = _persistit.getExchange(VOLUME_NAME, "a", true);
        
        doReplace.set(true);
        ex.to(1).getValue().put("abc");
        tb.store(ex);
        ex.to(1).getValue().put("def");
        tb.store(ex);
        assertEquals("Should have registered a dup", 1, duplicateCount.get());
        
        doReplace.set(false);
        ex.to(2).getValue().put("abc");
        tb.store(ex);
        ex.to(2).getValue().put("def");
        tb.store(ex);
        assertEquals("Should have registered a dup", 2, duplicateCount.get());
        
        tb.unitTestNextSortVolume();
        
        ex.to(1).getValue().put("ghi");
        tb.store(ex);
        ex.to(2).getValue().put("ghi");
        tb.store(ex);
        assertEquals("Should not have registered a dup yet", 2, duplicateCount.get());

        ex.to(3).getValue().put("abc");
        tb.store(ex);
        
        doReplace.set(false);
        tb.merge();
        
        assertEquals("Should have registered two dups", 4, duplicateCount.get());
        assertEquals("beforeMergeKey should be thrice", 3, beforeMergeCount.get());
        assertEquals("afterMergeKey should be called twice", 2, afterMergeCount.get());
        
        StringBuilder result = new StringBuilder();
        ex.clear().append(Key.BEFORE);
        while (ex.next(true)) {
            result.append(String.format("%s=%s,", ex.getKey(), ex.getValue()));
        }
        assertEquals("Expected result", "{1}=\"def\",{2}=\"abc\",", result.toString());
    }
    
    @Test
    public void duplicatePriority() throws Exception {
        final TreeBuilder tb = new TreeBuilder(_persistit) {
            
            @Override
            protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
                final String s1 = v1.getString();
                final String s2 = v2.getString();
                return s1.compareTo(s2) < 0;
            }
        
        };
        final Exchange ex = _persistit.getExchange(VOLUME_NAME, "a", true);
        final String nul = null;
        
        insertKeys(ex, tb, "x", "m", "n", nul, "a", nul, "q");
        tb.unitTestNextSortVolume();
        insertKeys(ex, tb, nul, "t", "o", "r", nul, nul, nul);
        tb.unitTestNextSortVolume();
        insertKeys(ex, tb, nul, "u", "m", "j", "c", "x", "l");
        tb.unitTestNextSortVolume();
        insertKeys(ex, tb, nul, "m", nul, nul, "a", nul, "q");
        
        tb.merge();
        
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            ex.to(i).fetch();
            if (ex.isValueDefined()) {
                result.append(ex.getValue().getString());
            }
        }
        assertEquals("xuorcxq", result.toString());
    }
    
    private void insertKeys(final Exchange ex, final TreeBuilder tb, final String... args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i] != null) {
                ex.to(i).getValue().put(args[i]);
                tb.store(ex);
            }
        }
    }
    
}
