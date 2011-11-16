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

    public void testTraverseShallowTwoTrx() throws Exception {
        trx1.begin();
        try {
            store(ex1, "a", "A");
            store(ex1, "z", "Z");
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        trx2.begin();
        try {
            store(ex1, "b","trx1", 1);
            store(ex1, "c","trx1", 1);
            store(ex1, "trx1", 1);
            store(ex2, "b","trx2", 2);
            store(ex2, "d","trx2", 2);
            store(ex2, "trx2", 2);

            //
            // Both should see a, z, and b->UD. 1 sees c->UD and 2 sees d->UD
            //

            assertEquals("trx1 forward,shallow traversal",
                         kvCollection("a","A",  "b","UD",  "c","UD",  "trx1",1,  "z","Z"),
                         traverseAll(Key.BEFORE, ex1, Key.GT, false));

            assertEquals("trx2 forward,shallow traversal",
                         kvCollection("a","A",  "b","UD",  "d","UD",  "trx2",2,  "z","Z"),
                         traverseAll(Key.BEFORE, ex2, Key.GT, false));

            assertEquals("trx1 reverse,shallow traversal",
                         kvCollection("z","Z",  "trx1",1,  "c","UD",  "b","UD",  "a","A"),
                         traverseAll(Key.AFTER, ex1, Key.LT, false));

            assertEquals("trx2 reverse,shallow traversal",
                         kvCollection("z","Z",  "trx2",2,  "d","UD",  "b","UD",  "a","A"),
                         traverseAll(Key.AFTER, ex2, Key.LT, false));

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            assertEquals("final forward,shallow traversal",
                         kvCollection("a","A",  "b","UD",  "c","UD",  "d","UD",  "trx1",1,  "trx2",2,  "z","Z"),
                         traverseAll(Key.BEFORE, ex1, Key.GT, false));
            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testTraverseDeepTwoTrx() throws Exception {
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
            store(ex1, "b","trx1", 1);
            store(ex1, "c","trx1", 1);
            store(ex1, "trx1", 1);
            store(ex2, "b","trx2", 2);
            store(ex2, "d","trx2", 2);
            store(ex2, "trx2", 2);

            assertEquals("trx1 forward,deep traversal",
                         kvCollection("a","A",  arr("b","trx1"),1,  arr("c","trx1"),1,  "trx1",1,  "z","Z"),
                         traverseAll(Key.BEFORE, ex1, Key.GT, true));

            assertEquals("trx2 forward,deep traversal",
                         kvCollection("a","A",  arr("b","trx2"),2,  arr("d","trx2"),2,  "trx2",2,  "z","Z"),
                         traverseAll(Key.BEFORE, ex2, Key.GT, true));


            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            assertEquals("final forward,deep traversal",
                         kvCollection("a","A",  arr("b","trx1"),1,  arr("b","trx2"),2,  arr("c","trx1"),1,
                                      arr("d","trx2"),2,  "trx1",1,  "trx2",2,  "z","Z"),
                         traverseAll(Key.BEFORE, ex1, Key.GT, true));
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
        Object k1, k2, v;

        public KVPair(Object k1, Object k2, Object v) {
            this.k1 = k1;
            this.k2 = k2;
            this.v = v;
        }

        @Override
        public String toString() {
            if(k2 == null) {
                return String.format("%s->%s", k1, v);
            }
            return String.format("%s,%s->%s", k1, k2, v);
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) return true;
            if(!(o instanceof KVPair)) return false;
            KVPair rhs = (KVPair) o;
            return !(k2 != null ? !k2.equals(rhs.k2) : rhs.k2 != null);

        }
    }

    private static Object[] arr(Object ...values) {
        return values;
    }

    private static void addTraverseResult(Collection<KVPair> collection, Key key, Value value) {
        Object k1, k2 = null;
        switch(key.getDepth()) {
            default: throw new IllegalArgumentException("Unexpected key depth: " + key.getDepth());
            case 2: key.indexTo(1); k2 = key.decode();
            case 1: key.indexTo(0); k1 = key.decode();
        }
        Object v = value.isDefined() ? value.get() : "UD";
        collection.add(new KVPair(k1, k2, v));
    }

    private static Collection<KVPair> traverseAll(Key.EdgeValue startAt, Exchange e, Key.Direction dir, boolean deep) throws Exception {
        e.clear().append(startAt);
        List<KVPair> out = new ArrayList<KVPair>();
        while(e.traverse(dir,  deep)) {
            addTraverseResult(out, e.getKey(), e.getValue());
        }
        return out;
    }

    private static Collection<KVPair> kvCollection(Object ...values) {
        if((values.length % 2) != 0) {
            throw new IllegalArgumentException("Must be even number of objects to create pairs from");
        }
        List<KVPair> out = new ArrayList<KVPair>();
        for(int i = 0; i < values.length; i += 2) {
            Object k1, k2 = null;
            if(values[i].getClass() == values.getClass()) {
                Object[] ks = (Object[])values[i];
                k1 = ks[0];
                k2 = ks[1];
            }
            else {
                k1 = values[i];
            }
            out.add(new KVPair(k1, k2, values[i+1]));
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
