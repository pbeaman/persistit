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
import java.util.Collections;
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
        final Volume vol = ex1.getVolume();
        assertEquals("open read claims",  0, _persistit.getBufferPool(vol.getPageSize()).countInUse(vol, false));
        assertEquals("open write claims", 0, _persistit.getBufferPool(vol.getPageSize()).countInUse(vol, true));
        
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

    public void testSingleTrxMultipleLongRecordVerions() throws Exception {
        final int PAGE_SIZE = ex1.getVolume().getPageSize();
        final int VERSIONS_TO_STORE = 5;

        StringBuilder sb = new StringBuilder(PAGE_SIZE);
        for(int i = 0; i < PAGE_SIZE; ++i) {
            sb.append(Character.forDigit(i % 36, 36));
        }
        final String longStr = sb.toString();

        for(int curVer = 0; curVer < VERSIONS_TO_STORE; ++curVer) {
            trx1.begin();
            try {
                store(ex1, curVer, longStr);
                ex1.getValue().clear();
                ex1.fetch();
                assertEquals("key after fetch pre-commit", curVer, ex1.getKey().decodeInt());
                assertEquals("value after fetch pre-commit", longStr, ex1.getValue().getString());
                trx1.commit();
            }
            finally {
                trx1.end();
            }
        }

        for(int curVer = 0; curVer < VERSIONS_TO_STORE; ++curVer) {
            trx1.begin();
            try {
                fetch(ex1, curVer, false);
                assertEquals("fetched key post-commit",  curVer, ex1.getKey().decodeInt());
                assertEquals("fetched value post-commit", longStr, ex1.getValue().getString());
                trx1.commit();
            }
            finally {
                trx1.end();
            }
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
            List<KVPair> trx1List = kvList("a","A",  "b","UD",  "c","UD",  "trx1",111,  "z","Z");
            List<KVPair> trx2List = kvList("a","A",  "b","UD",  "d","UD",  "trx2",222,  "z","Z");

            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);

            assertEquals("trx1 forward,shallow traversal", trx1List, traverseAllFoward(ex1, false));
            assertEquals("trx2 forward,shallow traversal", trx2List, traverseAllFoward(ex2, false));

            Collections.reverse(trx1List);
            Collections.reverse(trx2List);

            assertEquals("trx1 reverse,shallow traversal", trx1List, traverseAllReverse(ex1, false));
            assertEquals("trx2 reverse,shallow traversal", trx2List, traverseAllReverse(ex2, false));

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            List<KVPair> fList = kvList("a","A",  "b","UD",  "c","UD",  "d","UD",  "trx1",111,  "trx2",222,  "z","Z");

            assertEquals("final forward,shallow traversal", fList, traverseAllFoward(ex1, false));
            Collections.reverse(fList);
            assertEquals("final reverse,shallow traversal", fList, traverseAllReverse(ex1, false));
            
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
            store(ex1, "z", "Z");
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        trx1.begin();
        trx2.begin();
        try {
            List<KVPair> trx1List = kvList("a","A",  arr("b","trx1"),1,  arr("c","trx1"),11,  "trx1",111,  "z","Z");
            List<KVPair> trx2List = kvList("a","A",  arr("b","trx2"),2,  arr("d","trx2"),22,  "trx2",222,  "z","Z");

            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);

            assertEquals("trx1 forward,deep traversal", trx1List, traverseAllFoward(ex1, true));
            assertEquals("trx2 forward,deep traversal", trx2List, traverseAllFoward(ex2, true));

            Collections.reverse(trx1List);
            Collections.reverse(trx2List);

            assertEquals("trx1 reverse,deep traversal", trx1List, traverseAllReverse(ex1, true));
            assertEquals("trx2 reverse,deep traversal", trx2List, traverseAllReverse(ex2, true));

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {

            List<KVPair> fList = kvList("a","A",  arr("b","trx1"),1,  arr("b","trx2"),2,  arr("c", "trx1"),11,
                                        arr("d","trx2"),22,  "trx1",111,  "trx2",222,  "z","Z");

            assertEquals("final forward,deep traversal", fList, traverseAllFoward(ex1, true));
            Collections.reverse(fList);
            assertEquals("final reverse,deep traversal", fList, traverseAllReverse(ex1, true));

            trx1.commit();
        }
        finally {
            trx1.end();
        }
    }

    public void testTwoTrxManyTraverseManyKeys() throws Exception {
        final int MIN_PAGES = 6;
        final int MAX_KV_PER_PAGE = ex1.getVolume().getPageSize() / (8 + 14); // ##,trxX  =>  MVV,VER,LEN,##
        final int KVS_PER_TRX = (MIN_PAGES * MAX_KV_PER_PAGE) / 2;
        final int TOTAL_KVS = KVS_PER_TRX * 2;

        trx1.begin();
        trx2.begin();
        try {
            for(int i = 0; i < TOTAL_KVS; ++i) {
                if(i % 2 == 0) {
                    store(ex1, i, "trx1", i);
                }
                else {
                    store(ex2, i, "trx2", i);
                }
            }

            Exchange[] exchanges = {ex1, ex1};
            Key.Direction[] directions = {Key.GT, Key.LT};
            boolean[] deepFlags = {true, false};

            for(Exchange ex : exchanges) {
                final String expectedSeg2 = (ex == ex1) ? "trx1" : "trx2";
                final Key key = ex.getKey();
                final Value value = ex.getValue();

                for(Key.Direction dir : directions) {
                    final Key.EdgeValue startEdge = (dir == Key.GT) ? Key.BEFORE : Key.AFTER;

                    for(boolean deep : deepFlags) {
                        final String desc = expectedSeg2 + " " + dir + " " + (deep ? "deep" : "shallow") + ", ";

                        int traverseCount = 0;
                        ex.clear().append(startEdge);
                        while(ex.traverse(dir, deep)) {
                            ++traverseCount;
                            if(deep) {
                                assertEquals(desc + "key depth", 2, key.getDepth());
                                int keySeg1 = key.indexTo(0).decodeInt();
                                String keySeg2 = key.indexTo(1).decodeString();
                                int val = value.getInt();
                                assertEquals(desc + "key seg1 equals value", keySeg1, val);
                                assertEquals(desc + "key seg2", expectedSeg2, keySeg2);
                            }
                            else {
                                assertEquals(desc + "key depth", 1, key.getDepth());
                                assertEquals(desc + "value defined", false, value.isDefined());
                            }
                        }

                        assertEquals(desc+"traverse count", KVS_PER_TRX, traverseCount);
                    }
                }
            }

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }
    }

    /*
     * Simple sanity check as KeyFilter inspects the keys but doesn't care, directly, about MVCC
     */
    public void testKeyFilterTraverseTwoTrx() throws Exception {
        trx1.begin();
        trx2.begin();
        try {
            List<KVPair> trx1List = kvList("a","A",  "c","C",  "e","E",  "f","f",  "i","I");
            List<KVPair> trx2List = kvList("b","B",  "d","D",  "g","G",  "h","H",  "j","J");

            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);

            KeyFilter filter = new KeyFilter(new KeyFilter.Term[]{ KeyFilter.rangeTerm("b", "i") });
            trx1List.remove(0);
            trx2List.remove(trx2List.size() - 1);

            assertEquals("trx1 forward filter traversal", trx1List, doTraverse(Key.BEFORE, ex1, Key.GT, filter));
            assertEquals("trx2 forward filter traversal", trx2List, doTraverse(Key.BEFORE, ex2, Key.GT, filter));

            Collections.reverse(trx1List);
            Collections.reverse(trx2List);

            assertEquals("trx1 reverse filter traversal", trx1List, doTraverse(Key.AFTER, ex1, Key.LT, filter));
            assertEquals("trx2 reverse filter traversal", trx2List, doTraverse(Key.AFTER, ex2, Key.LT, filter));

            trx1.commit();
            trx2.commit();
        }
        finally {
            trx1.end();
        }
    }

    /*
     * Bug found independently of MVCC but fixed due to traverse() changes
     */
    public void testShallowTraverseWrongParentValueBug() throws Exception {
        trx1.begin();
        try {
            store(ex1, "a", "A");
            store(ex1, "a","a", "AA");
            store(ex1, "b", "B");
            store(ex1, "z", "Z");

            List<KVPair> kvList = kvList("a","A",  "b","B",  "z","Z");

            assertEquals("forward traversal", kvList, traverseAllFoward(ex1, false));
            Collections.reverse(kvList);
            assertEquals("reverse traversal", kvList, traverseAllReverse(ex1, false));

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
            return k1.equals(rhs.k1) && !(k2 != null ? !k2.equals(rhs.k2) : rhs.k2 != null) && v.equals(rhs.v);

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

    private static List<KVPair> traverseAllFoward(Exchange e, boolean deep) throws Exception {
        return doTraverse(Key.BEFORE, e, Key.GT, deep);
    }

    private static List<KVPair> traverseAllReverse(Exchange e, boolean deep) throws Exception {
        return doTraverse(Key.AFTER, e, Key.LT, deep);
    }

    private static List<KVPair> doTraverse(Key.EdgeValue startAt, Exchange ex, Key.Direction dir, KeyFilter filter) throws Exception {
        ex.clear().append(startAt);
        List<KVPair> out = new ArrayList<KVPair>();
        while(ex.traverse(dir, filter, Integer.MAX_VALUE)) {
            addTraverseResult(out, ex.getKey(), ex.getValue());
        }
        return out;
    }

    private static List<KVPair> doTraverse(Key.EdgeValue startAt, Exchange e, Key.Direction dir, boolean deep) throws Exception {
        e.clear().append(startAt);
        List<KVPair> out = new ArrayList<KVPair>();
        while(e.traverse(dir,  deep)) {
            addTraverseResult(out, e.getKey(), e.getValue());
        }
        return out;
    }

    private static List<KVPair> kvList(Object... values) {
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

    private static void storeAll(Exchange ex, List<KVPair> list) throws PersistitException {
        for(KVPair kv : list) {
            ex.clear().append(kv.k1);
            if(kv.k2 != null) {
                ex.append(kv.k2);
            }
            ex.getValue().put(kv.v);
            ex.store();
        }
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
