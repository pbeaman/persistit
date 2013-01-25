/**
 * Copyright © 2011-2013 Akiban Technologies, Inc.  All rights reserved.
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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.unit.UnitTestProperties;

public class TreeBuilderTest extends PersistitUnitTestCase {
    private final static int COUNT = 100000;
    private final AtomicInteger _duplicates = new AtomicInteger();

    private TreeBuilder getBasicTreeBuilder() {
        final TreeBuilder tb = new TreeBuilder(_persistit) {
            @Override
            protected void reportSorted(final long count) {
                System.out.println("Sorted " + count);
            }

            @Override
            protected void reportMerged(final long count) {
                System.out.println("Merged " + count);
            }

            @Override
            protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
                System.out.println("Duplicate key " + key);
                _duplicates.incrementAndGet();
                return false;
            }
        };
        tb.setReportKeyCountMultiple(COUNT / 2);
        return tb;
    }

    @Test
    public void basicTest() throws Exception {

        final TreeBuilder tb = getBasicTreeBuilder();

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
        _persistit.flush();
        assertEquals(0, a.getBufferPool().getDirtyPageCount());
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

            @Override
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

        tb.unitTestNextSortFile();

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

        final StringBuilder result = new StringBuilder();
        ex.clear().append(Key.BEFORE);
        while (ex.next(true)) {
            result.append(String.format("%s=%s,", ex.getKey(), ex.getValue()));
        }
        assertEquals("Expected result", "{1}=\"def\",{2}=\"abc\",", result.toString());
    }

    @Test
    public void duplicatePriority1() throws Exception {
        duplicatePriorityCheck(new TreeBuilder(_persistit) {
            // Larger value wins
            @Override
            protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
                final String s1 = v1.getString();
                final String s2 = v2.getString();
                return s1.compareTo(s2) < 0;
            }

        }, "xuorcxq");
    }

    @Test
    public void duplicatePriority2() throws Exception {
        duplicatePriorityCheck(new TreeBuilder(_persistit) {
            // First value wins
            @Override
            protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
                return false;
            }

        }, "xmnraxq");
    }

    private void duplicatePriorityCheck(final TreeBuilder tb, final String expected) throws Exception {
        final Exchange ex = _persistit.getExchange(VOLUME_NAME, "a", true);
        final String nul = null;

        insertKeys(ex, tb, "x", "m", "n", nul, "a", nul, "q");
        tb.unitTestNextSortFile();
        insertKeys(ex, tb, nul, "t", "o", "r", nul, nul, nul);
        tb.unitTestNextSortFile();
        insertKeys(ex, tb, nul, "u", "m", "j", "c", "x", "l");
        tb.unitTestNextSortFile();
        insertKeys(ex, tb, nul, "m", nul, nul, "a", nul, "q");

        tb.merge();

        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            ex.to(i).fetch();
            if (ex.isValueDefined()) {
                result.append(ex.getValue().getString());
            }
        }
        assertEquals(expected, result.toString());

    }

    private void insertKeys(final Exchange ex, final TreeBuilder tb, final String... args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i] != null) {
                ex.to(i).getValue().put(args[i]);
                tb.store(ex);
            }
        }
    }

    @Test
    public void multipleDirectories() throws Exception {
        final TreeBuilder tb = getBasicTreeBuilder();
        final List<File> directories = new ArrayList<File>();
        final Random random = new Random();
        try {
            for (int i = 0; i < 3; i++) {
                final File file = File.createTempFile("TreeBuilderTest", "");
                file.delete();
                assertTrue("Expect to make directory", file.mkdir());
                directories.add(file);
            }
            tb.setSortTreeDirectories(directories);
            final Exchange ex = _persistit.getExchange(VOLUME_NAME, "TreeBuilderTest", true);
            for (int i = 0; i < COUNT; i++) {
                final int k = random.nextInt();
                ex.to(k);
                ex.getValue().put(RED_FOX + "," + k);
                tb.store(ex);
                if (((i + 1) % (COUNT / 10)) == 0) {
                    tb.unitTestNextSortFile();
                }
            }
            for (final File file : directories) {
                assertTrue("Expect some files in each directory", file.list().length > 0);
            }
            tb.merge();

            for (final File file : directories) {
                assertTrue("Expect no remaining files", file.list().length == 0);
            }

            ex.to(Key.BEFORE);
            int count = 0;
            while (ex.next()) {
                count++;
                final int k = ex.getKey().decodeInt();
                assertEquals(RED_FOX + "," + k, ex.getValue().getString());
            }
            assert count + _duplicates.get() == COUNT;

        } finally {
            for (final File file : directories) {
                UnitTestProperties.cleanUpDirectory(file);
            }
        }
    }

}
