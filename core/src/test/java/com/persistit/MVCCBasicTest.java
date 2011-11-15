/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MVCCBasicTest extends PersistitUnitTestCase {
    private static final String VOL_NAME = "persistit";
    private static final String TREE_NAME = "mvccbasictest";

    private static final String KEY1 = "k1";
    private static final String KEY2 = "k2";
    private static final long VALUE1 = 12345L;
    private static final long VALUE2 = 67890L;

    private Exchange ex1, ex2;
    private Transaction trx1, trx2;
    private SessionId session1, session2;

    public final void setUp() throws Exception {
        super.setUp();

        session1 = new SessionId();
        _persistit.setSessionId(session1);
        ex1 = _persistit.getExchange(VOL_NAME, TREE_NAME, true);
        trx1 = ex1.getTransaction();

        session2 = new SessionId();
        _persistit.setSessionId(session2);
        ex2 = _persistit.getExchange(VOL_NAME, TREE_NAME, true);
        trx2 = ex2.getTransaction();
    }

    public final void tearDown() throws Exception {
        _persistit.releaseExchange(ex1);
        _persistit.releaseExchange(ex2);
        ex1 = ex2 = null;
        trx1 = trx2 = null;
        session1 = session2 = null;
        super.tearDown();
    }


    public void testTwoTrxDifferentTimestamps() throws PersistitException {
        trx1.begin();
        trx2.begin();
        try {
            assertFalse("differing start timestamps", trx1.getStartTimestamp() == trx2.getStartTimestamp());
            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }
    }
    
    public void testSingleTrxWriteAndRead() throws Exception {
        trx1.begin();
        try {
            store(ex1, KEY1, VALUE1);
            assertEquals("fetch before commit", VALUE1, fetch(ex1, KEY1));
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertEquals("fetch after commit", VALUE1, fetch(ex1, KEY1));
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testTwoTrxDistinctWritesOverlappedReads() throws Exception {
        trx1.begin();
        trx2.begin();
        try {
            store(ex1, KEY1, VALUE1);
            store(ex2, KEY2, VALUE2);

            fetch(ex2, KEY1, false);
            assertFalse("trx2 sees uncommitted trx1 value", ex2.getValue().isDefined());

            fetch(ex1, KEY2, false);
            assertFalse("trx1 sees uncommitted trx2 value", ex1.getValue().isDefined());

            trx1.commit();

            fetch(ex2, KEY1, false);
            assertFalse("trx2 sees committed trx1 from future", ex2.getValue().isDefined());

            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        // Both should see both now
        trx1.begin();
        trx2.begin();
        try {
            assertEquals("original trx1 value from new trx1", VALUE1, fetch(ex1, KEY1));
            assertEquals("original trx2 value from new trx1", VALUE2, fetch(ex1, KEY2));
            trx1.commit();

            assertEquals("original trx1 value from new trx2", VALUE1, fetch(ex2, KEY1));
            assertEquals("original trx2 value from new trx2", VALUE2, fetch(ex2, KEY2));
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }
    }

    public void testSingleTrxManyInserts() throws Exception {
        // Enough for a new index level and many splits
        final int INSERT_COUNT = 5000;

        for(int i = 0; i < INSERT_COUNT; ++i) {
            trx1.begin();
            try {
                store(ex1, i, i * 2);
                trx1.commit();
            }
            finally {
                trx1.end();
            }
        }

        trx1.begin();
        try {
            for(int i = 0; i < INSERT_COUNT; ++i) {
                assertEquals(i*2, fetch(ex1, i));
            }
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testIsValuedDefinedTwoTrx() throws Exception {
        trx1.begin();
        trx2.begin();
        try {
            store(ex1, "trx1", 1);
            store(ex2, "trx2", 2);

            assertFalse("trx1 sees uncommitted trx2 key", ex1.clear().append("trx2").isValueDefined());
            assertFalse("trx2 sees uncommitted trx2 key", ex2.clear().append("trx1").isValueDefined());
            
            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertTrue("committed trx1 key", ex1.clear().append("trx1").isValueDefined());
            assertTrue("committed trx2 key", ex1.clear().append("trx2").isValueDefined());
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testNextPrevTraverseTwoTrx() throws Exception {
        trx1.begin();
        try {
            store(ex1, "a", 97);
            //store(ex1, "a","a", 9797); //skipped by non-deep next() and prev()
            store(ex1, "z", 122);
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        trx2.begin();
        try {
            store(ex1, "trx1", 1);
            store(ex2, "trx2", 2);

            ex1.clear().append(Key.BEFORE);
            assertEquals("trx1 next() traversal",
                         kvCollection("a",97,  "trx1",1,  "z",122),
                         traverseAllNext(ex1));

            ex2.clear().append(Key.BEFORE);
            assertEquals("trx2 next() traversal",
                         kvCollection("a",97,  "trx2",2,  "z",122),
                         traverseAllNext(ex2));

            ex1.clear().append(Key.AFTER);
            assertEquals("trx1 previous() traversal",
                         kvCollection("z",122,  "trx1",1,  "a",97),
                         traverseAllPrev(ex1));

            ex2.clear().append(Key.AFTER);
            assertEquals("trx2 previous() traversal",
                         kvCollection("z",122,  "trx2",2,  "a",97),
                         traverseAllPrev(ex2));

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            ex1.clear().append(Key.BEFORE);
            assertEquals("final traversal",
                         kvCollection("a",97,  "trx1",1,  "trx2",2,  "z",122),
                         traverseAllNext(ex1));
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testTraverseDeep() throws Exception {
        // Should be visible to both
        trx1.begin();
        try {
            store(ex1, "a", "A");
            store(ex2, "z", "Z");
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        trx2.begin();
        try {
            store(ex1, "a","a", "trx1");
            store(ex2, "a","b", "trx2");

            ex1.clear().append(Key.BEFORE);
            assertEquals("trx1 next() traversal",
                         kvCollection("{\"a\"}","\"A\"",  "{\"a\",\"a\"}","\"trx1\"",  "{\"z\"}","\"Z\""),
                         traverseAll(ex1, Key.GT, true));

            ex2.clear().append(Key.BEFORE);
            assertEquals("trx1 next() traversal",
                         kvCollection("{\"a\"}","\"A\"",  "{\"a\",\"b\"}","\"trx2\"",  "{\"z\"}","\"Z\""),
                         traverseAll(ex2, Key.GT, true));


            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            ex1.clear().append(Key.BEFORE);
            assertEquals("final traversal",
                         kvCollection("{\"a\"}","\"A\"",  "{\"a\",\"a\"}","\"trx1\"",
                                      "{\"a\",\"b\"}","\"trx2\"",  "{\"z\"}","\"Z\""),
                         traverseAll(ex1, Key.GT, true));
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }


    //
    // Internal test methods
    //
    

    private static class KVPair {
        Object k, v;

        public KVPair(Object k, Object v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public String toString() {
            return String.format("%s->%s", k, v);
        }

        @Override
        public boolean equals(Object rhs) {
            if(this == rhs) return true;
            if(!(rhs instanceof KVPair)) return false;
            KVPair o = (KVPair) rhs;
            return !(k != null ? !k.equals(o.k) : o.k != null) && !(v != null ? !v.equals(o.v) : o.v != null);

        }
    }

    private static Collection<KVPair> traverseAllNext(Exchange e) throws Exception {
        return traverseAll(e, true);
    }

    private static Collection<KVPair> traverseAllPrev(Exchange e) throws Exception {
        return traverseAll(e, false);
    }

    private static Collection<KVPair> traverseAll(Exchange e, boolean goNext) throws Exception {
        List<KVPair> out = new ArrayList<KVPair>();
        while((goNext ? e.next() : e.previous())) {
            Object k = e.getKey().decode();
            Object v = e.getValue().isDefined() ? e.getValue().get() : "UD";
            out.add(new KVPair(k, v));
        }
        return out;
    }

    private static Collection<KVPair> traverseAll(Exchange e, Key.Direction dir, boolean deep) throws Exception {
        List<KVPair> out = new ArrayList<KVPair>();
        while(e.traverse(dir,  deep)) {
            String k = e.getKey().toString();
            String v = e.getValue().toString();
            out.add(new KVPair(k, v));
        }
        return out;
    }

    private static Collection<KVPair> kvCollection(Object ...vals) {
        if((vals.length % 2) != 0) {
            throw new IllegalArgumentException("Must be even number of objects to create pairs from");
        }
        List<KVPair> out = new ArrayList<KVPair>();
        for(int i = 0; i < vals.length; i += 2) {
            out.add(new KVPair(vals[i], vals[i+1]));
        }
        return out;
    }

    private static void store(Exchange ex, Object k, Object v) throws PersistitException {
        ex.clear().append(k).getValue().put(v);
        ex.store();
    }

    private static void store(Exchange ex, Object kp1, Object kp2, Object v) throws PersistitException {
        ex.clear().append(kp1).append(kp2).getValue().put(v);
        ex.store();
    }

    private static Object fetch(Exchange ex, Object k) throws PersistitException {
        return fetch(ex, k, true);
    }

    private static Object fetch(Exchange ex, Object k, boolean getValue) throws PersistitException {
        ex.clear().append(k).fetch();
        return getValue ? ex.getValue().get() : null;
    }

    private void showGUI() throws Exception {
        _persistit.setupGUI(true);
    }
}
