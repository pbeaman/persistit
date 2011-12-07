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

import java.util.Collections;
import java.util.List;

public class MVCCBasicTest extends MVCCTestBase {
    private static final String KEY1 = "k1";
    private static final String KEY2 = "k2";
    private static final long VALUE1 = 12345L;
    private static final long VALUE2 = 67890L;

    public void testTwoTrxDifferentTimestamps() throws PersistitException {
        trx1.begin();
        trx2.begin();
        try {
            assertFalse("differing start timestamps", trx1.getStartTimestamp() == trx2.getStartTimestamp());
            trx1.commit();
            trx2.commit();
        } finally {
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
        } finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertEquals("fetch after commit", VALUE1, fetch(ex1, KEY1));
            trx1.commit();
        } finally {
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
        } finally {
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
        } finally {
            trx1.end();
            trx2.end();
        }
    }

    public void testSingleTrxManyInserts() throws Exception {
        // Enough for a new index level and many splits
        final int INSERT_COUNT = 5000;

        for (int i = 0; i < INSERT_COUNT; ++i) {
            trx1.begin();
            try {
                store(ex1, i, i * 2);
                trx1.commit();
            } finally {
                trx1.end();
            }
        }

        trx1.begin();
        try {
            for (int i = 0; i < INSERT_COUNT; ++i) {
                assertEquals(i * 2, fetch(ex1, i));
            }
            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testSingleTrxMultipleLongRecordVersions() throws Exception {
        final int VERSIONS_TO_STORE = 5;
        final String longStr = createString(ex1.getVolume().getPageSize());

        for (int curVer = 0; curVer < VERSIONS_TO_STORE; ++curVer) {
            trx1.begin();
            try {
                store(ex1, curVer, longStr);
                ex1.getValue().clear();
                ex1.fetch();
                assertEquals("key after fetch pre-commit", curVer, ex1.getKey().decodeInt());
                assertEquals("value after fetch pre-commit", longStr, ex1.getValue().getString());
                trx1.commit();
            } finally {
                trx1.end();
            }
        }

        for (int curVer = 0; curVer < VERSIONS_TO_STORE; ++curVer) {
            trx1.begin();
            try {
                fetch(ex1, curVer, false);
                assertEquals("fetched key post-commit", curVer, ex1.getKey().decodeInt());
                assertEquals("fetched value post-commit", longStr, ex1.getValue().getString());
                trx1.commit();
            } finally {
                trx1.end();
            }
        }
    }

    /*
     * Store dozens of small, unique versions of a single key to result in
     * resulting in a LONG MVV value. Check etch pre and post commit.
     */
    public void testLongMVVFromManySmall() throws Exception {
        final int PER_LENGTH = 250;
        final String smallStr = createString(PER_LENGTH);
        final int versionCount = (int) ((ex1.getVolume().getPageSize() / PER_LENGTH) * 1.1);

        for (int i = 1; i <= versionCount; ++i) {
            trx1.begin();
            try {
                final String value = smallStr + i;
                store(ex1, KEY1, value);
                assertEquals("value pre-commit version " + i, value, fetch(ex1, KEY1));
                trx1.commit();
                trx1.end();

                trx1.begin();
                assertEquals("value post-commit version " + i, value, fetch(ex1, KEY1));
                trx1.commit();
            } finally {
                trx1.end();
            }
        }
    }

    /*
     * Store multiple unique versions of a single key, with individual versions
     * are both short and long records, resulting in a LONG MVV value. Check
     * fetch pre and post commit.
     */
    public void testLongMVVFromManySmallAndLong() throws Exception {
        final int pageSize = ex1.getVolume().getPageSize();
        final String longStr = createString(pageSize);
        final double[] valueLengths = { pageSize * 0.05, 10, pageSize * 0.80, 0, pageSize * 0.20, 25, pageSize * 0.40,
                10, pageSize * 0.10, 45, };

        for (int i = 0; i < valueLengths.length; ++i) {
            trx1.begin();
            try {
                final int length = (int) valueLengths[i];
                final String value = longStr.substring(0, length);
                store(ex1, KEY1, value);
                assertEquals("value pre-commit version " + i, value, fetch(ex1, KEY1));
                trx1.commit();
                trx1.end();

                trx1.begin();
                assertEquals("value post-commit version " + i, value, fetch(ex1, KEY1));
                trx1.commit();
            } finally {
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
        } finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertTrue("committed trx1 key", ex1.clear().append("trx1").isValueDefined());
            assertTrue("committed trx2 key", ex1.clear().append("trx2").isValueDefined());
            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testTraverseShallowTwoTrx() throws Exception {
        List<KVPair> baseList = kvList("a","A",  "z","Z");
        trx1.begin();
        try {
            storeAll(ex1, baseList);
            trx1.commit();
        } finally {
            trx1.end();
        }

        List<KVPair> trx1List = kvList("d","D",  "trx1",111,  "x","X");
        List<KVPair> trx2List = kvList("b","B",  "c","C",  "trx2",222);

        trx1.begin();
        trx2.begin();
        try {
            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);
            storeAll(ex1, kvList(arr("e","trx1"),1,  arr("h","trx1"),11));
            storeAll(ex2, kvList(arr("f","trx2"),2,  arr("g","trx2"),22));

            trx1List.addAll(kvList("e","UD",  "h","UD"));
            trx2List.addAll(kvList("f","UD",  "g","UD"));

            trx1List = combine(trx1List, baseList);
            trx2List = combine(trx2List, baseList);

            assertEquals("trx1 forward,shallow traversal", trx1List, traverseAllFoward(ex1, false));
            assertEquals("trx2 forward,shallow traversal", trx2List, traverseAllFoward(ex2, false));

            Collections.reverse(trx1List);
            Collections.reverse(trx2List);

            assertEquals("trx1 reverse,shallow traversal", trx1List, traverseAllReverse(ex1, false));
            assertEquals("trx2 reverse,shallow traversal", trx2List, traverseAllReverse(ex2, false));

            trx1.commit();
            trx2.commit();
        } finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            List<KVPair> fList = combine(trx1List, trx2List);
            assertEquals("final forward,shallow traversal", fList, traverseAllFoward(ex1, false));
            Collections.reverse(fList);
            assertEquals("final reverse,shallow traversal", fList, traverseAllReverse(ex1, false));

            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testTraverseDeepTwoTrx() throws Exception {
        List<KVPair> baseList = kvList("a","A",  "z","Z");

        trx1.begin();
        try {
            storeAll(ex1, baseList);
            trx1.commit();
        } finally {
            trx1.end();
        }

        List<KVPair> trx1List = kvList(arr("b","trx1"),1,  arr("d","trx1"),11,  "trx1",111);
        List<KVPair> trx2List = kvList(arr("b","trx2"),2,  arr("c","trx2"),22,  "trx2",222);

        trx1.begin();
        trx2.begin();
        try {
            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);

            trx1List = combine(trx1List, baseList);
            trx2List = combine(trx2List, baseList);

            assertEquals("trx1 forward,deep traversal", trx1List, traverseAllFoward(ex1, true));
            assertEquals("trx2 forward,deep traversal", trx2List, traverseAllFoward(ex2, true));

            Collections.reverse(trx1List);
            Collections.reverse(trx2List);

            assertEquals("trx1 reverse,deep traversal", trx1List, traverseAllReverse(ex1, true));
            assertEquals("trx2 reverse,deep traversal", trx2List, traverseAllReverse(ex2, true));

            trx1.commit();
            trx2.commit();
        } finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            List<KVPair> fList = combine(trx1List, trx2List);

            assertEquals("final forward,deep traversal", fList, traverseAllFoward(ex1, true));
            Collections.reverse(fList);
            assertEquals("final reverse,deep traversal", fList, traverseAllReverse(ex1, true));

            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testTwoTrxManyTraverseManyKeys() throws Exception {
        final int MIN_PAGES = 6;
        final int MAX_KV_PER_PAGE = ex1.getVolume().getPageSize() / (8 + 14); // ##,trxX
                                                                              // =>
                                                                              // MVV,VER,LEN,##
        final int KVS_PER_TRX = (MIN_PAGES * MAX_KV_PER_PAGE) / 2;
        final int TOTAL_KVS = KVS_PER_TRX * 2;

        trx1.begin();
        trx2.begin();
        try {
            for (int i = 0; i < TOTAL_KVS; ++i) {
                if (i % 2 == 0) {
                    store(ex1, i, "trx1", i);
                } else {
                    store(ex2, i, "trx2", i);
                }
            }

            Exchange[] exchanges = { ex1, ex1 };
            Key.Direction[] directions = { Key.GT, Key.LT };
            boolean[] deepFlags = { true, false };

            for (Exchange ex : exchanges) {
                final String expectedSeg2 = (ex == ex1) ? "trx1" : "trx2";
                final Key key = ex.getKey();
                final Value value = ex.getValue();

                for (Key.Direction dir : directions) {
                    final Key.EdgeValue startEdge = (dir == Key.GT) ? Key.BEFORE : Key.AFTER;

                    for (boolean deep : deepFlags) {
                        final String desc = expectedSeg2 + " " + dir + " " + (deep ? "deep" : "shallow") + ", ";

                        int traverseCount = 0;
                        ex.clear().append(startEdge);
                        while (ex.traverse(dir, deep)) {
                            ++traverseCount;
                            if (deep) {
                                assertEquals(desc + "key depth", 2, key.getDepth());
                                int keySeg1 = key.indexTo(0).decodeInt();
                                String keySeg2 = key.indexTo(1).decodeString();
                                int val = value.getInt();
                                assertEquals(desc + "key seg1 equals value", keySeg1, val);
                                assertEquals(desc + "key seg2", expectedSeg2, keySeg2);
                            } else {
                                assertEquals(desc + "key depth", 1, key.getDepth());
                                assertEquals(desc + "value defined", false, value.isDefined());
                            }
                        }

                        assertEquals(desc + "traverse count", KVS_PER_TRX, traverseCount);
                    }
                }
            }

            trx1.commit();
            trx2.commit();
        } finally {
            trx1.end();
            trx2.end();
        }
    }

    /*
     * Simple sanity check as KeyFilter inspects the keys but doesn't care,
     * directly, about MVCC
     */
    public void testKeyFilterTraverseTwoTrx() throws Exception {
        trx1.begin();
        trx2.begin();
        try {
            List<KVPair> trx1List = kvList("a", "A", "c", "C", "e", "E", "f", "f", "i", "I");
            List<KVPair> trx2List = kvList("b", "B", "d", "D", "g", "G", "h", "H", "j", "J");

            storeAll(ex1, trx1List);
            storeAll(ex2, trx2List);

            KeyFilter filter = new KeyFilter(new KeyFilter.Term[] { KeyFilter.rangeTerm("b", "i") });
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
        } finally {
            trx1.end();
        }
    }

    /*
     * Bug found independently of MVCC but fixed due to traverse() changes
     */
    public void testShallowTraverseWrongParentValueBug() throws Exception {
        trx1.begin();
        try {
            List<KVPair> kvList = kvList("a","A",  "b","B",  "z","Z");
            storeAll(ex1, kvList);
            store(ex1, "a","a", "AA");

            assertEquals("forward traversal", kvList, traverseAllFoward(ex1, false));
            Collections.reverse(kvList);
            assertEquals("reverse traversal", kvList, traverseAllReverse(ex1, false));

            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testSingleTrxStoreRemoveFetch() throws Exception {
        trx1.begin();
        try {
            store(ex1, KEY1, VALUE1);
            assertEquals("fetched value pre-remove pre-commit", VALUE1, fetch(ex1, KEY1));

            assertTrue("key existed pre-remove", remove(ex1, KEY1));

            fetch(ex1, KEY1, false);
            assertFalse("fetched value defined post-remove pre-commit", ex1.getValue().isDefined());

            ex1.clear().append(KEY1);
            assertFalse("key defined post-remove pre-commit", ex1.isValueDefined());

            trx1.commit();
        } finally {
            trx1.end();
        }

        trx1.begin();
        try {
            fetch(ex1, KEY1, false);
            assertFalse("fetched value defined post-remove pre-commit", ex1.getValue().isDefined());

            ex1.clear().append(KEY1);
            assertFalse("key defined post-remove pre-commit", ex1.isValueDefined());

            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testTwoTrxRemoveRanges() throws Exception {
        List<KVPair> bothList = kvList("a", "A", "m", "M", "z", "Z");
        trx1.begin();
        try {
            storeAll(ex1, bothList);
            trx1.commit();
        } finally {
            trx1.end();
        }

        Key ka = new Key(_persistit);
        Key kb = new Key(_persistit);

        trx1.begin();
        trx2.begin();
        try {
            List<KVPair> trx1List1 = kvList("b", "B", "e", "e", "f", "f", "x", "X");
            storeAll(ex1, trx1List1);

            List<KVPair> trx2List = kvList("d","D",  "n","N",  "v","V",  "y","Y");
            storeAll(ex2, trx2List);

            // Explicitly testing overlapping ranges, as the overlaps should
            // not be visible to each other

            ka.clear().append("b");
            kb.clear().append("v");
            assertTrue("trx1 keys removed", ex1.removeKeyRange(ka, kb));

            List<KVPair> trx1List2 = kvList("a", "A", "x", "X", "z", "Z");
            assertEquals("trx1 traverse post removeKeyRange", trx1List2, traverseAllFoward(ex1, true));
            assertEquals("trx2 traverse post trx1 removeKeyRange", combine(bothList, trx2List), traverseAllFoward(ex2,
                    true));

            ka.clear().append("n");
            kb.clear().append(Key.AFTER);
            assertTrue("trx2 keys removed", ex2.removeKeyRange(ka, kb));
            assertEquals("trx2 traverse post removeAll", kvList("a","A",  "d","D",  "m","M"), traverseAllFoward(ex2, true));
            assertEquals("trx1 traverse post trx2 removeAll", trx1List2, traverseAllFoward(ex1, true));

            trx1.commit();
            trx2.commit();
        } finally {
            trx1.end();
            trx2.end();
        }

        trx1.begin();
        try {
            assertEquals("traverse post-commit", kvList("a","A",  "d","D",  "x","X"), traverseAllFoward(ex1, true));
            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    public void testRemoveWithSplitsSmall() throws Exception {
        final int keyCount = _persistit.getBufferPool(ex1.getVolume().getPageSize()).getMaxKeys();
        insertRemoveAllAndVerify(keyCount);
    }

    public void testRemoveWithSplitsMedium() throws Exception {
        final int keyCount = _persistit.getBufferPool(ex1.getVolume().getPageSize()).getMaxKeys() * 5;
        insertRemoveAllAndVerify(keyCount);
    }

    public void testRemoveWithSplitsLarge() throws Exception {
        final int keyCount = _persistit.getBufferPool(ex1.getVolume().getPageSize()).getMaxKeys() * 10;
        insertRemoveAllAndVerify(keyCount);
    }

    private void insertRemoveAllAndVerify(int keyCount) throws Exception {
        trx1.begin();
        try {
            for (int i = 0; i < keyCount; ++i) {
                ex1.getValue().clear();
                ex1.clear().append(String.format("%05d", i)).store();
            }
            trx1.commit();
        } finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertEquals("traversed count initial", keyCount, traverseAllFoward(ex1, true).size());
            ex1.removeAll();
            assertEquals("traversed count post-remove pre-commit", 0, traverseAllFoward(ex1, true).size());
            trx1.commit();
        } finally {
            trx1.end();
        }

        trx1.begin();
        try {
            assertEquals("traverse post-remove post-commit", 0, traverseAllFoward(ex1, true).size());
            trx1.commit();
        } finally {
            trx1.end();
        }
    }
}
