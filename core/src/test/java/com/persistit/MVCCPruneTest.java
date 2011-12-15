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
import com.persistit.exception.RollbackException;

import java.util.ArrayList;
import java.util.List;

public class MVCCPruneTest extends MVCCTestBase {
    final static String KEY = "a";
    final static String VALUE = "A";
    final static String VALUE_TRX1 = "A_trx1";
    final static String VALUE_TRX2 = "A_trx2";

    public void testPruneNonExistingKey() throws PersistitException {
        ex1.getValue().clear();
        ex1.clear().append(KEY);

        ex1.prune();

        assertEquals("key unchanged", KEY, ex1.getKey().decode());
        assertEquals("value unchanged/defined", false, ex1.getValue().isDefined());

        assertEquals("key is defined", false, ex1.isValueDefined());

        ex1.fetch();
        assertEquals("key found by fetch", false, ex1.getValue().isDefined());
    }

    public void testPrunePrimordial() throws PersistitException {
        // get a primordial by storing outside of transaction
        store(ex1, KEY, VALUE);
        assertEquals("initial primordial fetch", VALUE, fetch(ex1, KEY));

        ex1.clear().append(KEY);
        ex1.prune();

        assertEquals("value after prune", VALUE, fetch(ex1, KEY));
        assertEquals("version count after prune", 1, storedVersionCount(ex1, KEY));
    }

    public void testPrunePrimordialAndOneConcurrent() throws PersistitException {
        storePrimordial(ex1, KEY, VALUE);

        trx1.begin();
        try {
            store(ex1, KEY, VALUE_TRX1);
            assertEquals("fetch after trx store", VALUE_TRX1, fetch(ex1, KEY));

            prune(ex2, KEY);

            assertEquals("fetch after prune", VALUE_TRX1, fetch(ex1, KEY));
            assertEquals("version count after prune", 2, storedVersionCount(ex1, KEY));
        } finally {
            trx1.end();
        }
    }

    public void testPrunePrimordialAndCommitted() throws PersistitException {
        storePrimordial(ex1, KEY, VALUE);

        trx1.begin();
        try {
            store(ex1, KEY, VALUE_TRX1);
            trx1.commit();
        } finally {
            trx1.end();
        }

        assertEquals("version count after trx store", 2, storedVersionCount(ex1, KEY));
        prune(ex1, KEY);
        assertEquals("version count after prune", 1, storedVersionCount(ex1, KEY));
    }

    public void testPruneManyCommitted() throws PersistitException {
        final int TRX1_COUNT = 5;
        storePrimordial(ex1, KEY, VALUE);

        for (int i = 1; i <= TRX1_COUNT; ++i) {
            trx1.begin();
            try {
                store(ex1, KEY, VALUE + i);
                trx1.commit();
            } finally {
                trx1.end();
            }
        }

        final String highestCommitted = VALUE + TRX1_COUNT;

        trx2.begin();
        try {
            assertEquals("value after many commits", highestCommitted, fetch(ex2, KEY));
            store(ex2, KEY, VALUE_TRX2);
            assertEquals("value from trx2 after store", VALUE_TRX2, fetch(ex2, KEY));

            prune(ex1, KEY);

            assertEquals("value from trx2 after prune", VALUE_TRX2, fetch(ex2, KEY));
            assertEquals("value from no trx after prune", highestCommitted, fetch(ex1, KEY));

            assertEquals("version count after prune, trx1&2 active", 2, storedVersionCount(ex1, KEY));

            trx1.begin();
            try {
                assertEquals("value from trx1 after prune", highestCommitted, fetch(ex1, KEY));
                trx1.commit();
            } finally {
                trx1.end();
            }

            trx2.commit();
        } finally {
            trx2.end();
        }

        assertEquals("value from no trx post second commit", VALUE_TRX2, fetch(ex1, KEY));
        prune(ex2, KEY);
        assertEquals("value from no trx post second prune", VALUE_TRX2, fetch(ex1, KEY));
        assertEquals("version count post second prune, no trx active", 1, storedVersionCount(ex2, KEY));
    }

    public void testPruneAborted() throws PersistitException {
        storePrimordial(ex1, KEY, VALUE);

        trx1.begin();
        try {
            store(ex1, KEY, VALUE_TRX1);
            assertEquals("value from trx1 store", VALUE_TRX1, fetch(ex1, KEY));
            trx1.rollback();
        } catch (RollbackException e) {
            // Expected
        } finally {
            trx1.end();
        }

        assertEquals("version count post-rollback", 2, storedVersionCount(ex1, KEY));
        assertEquals("value post rollback pre-prune no trx", VALUE, fetch(ex2, KEY));
        trx2.begin();
        try {
            assertEquals("value post rollback pre-prune in trx", VALUE, fetch(ex2, KEY));
            trx2.commit();
        } finally {
            trx2.end();
        }

        prune(ex1, KEY);
        assertEquals("version count post-rollback post prune", 1, storedVersionCount(ex1, KEY));
        assertEquals("value post-rollback post-prune", VALUE, fetch(ex1, KEY));
    }

    public void testPruneRemoved() throws PersistitException {
        storePrimordial(ex1, KEY, VALUE);

        trx1.begin();
        try {
            assertEquals("key removed", true, remove(ex1, KEY));
            assertEquals("key exists post-remove", false, ex1.isValueDefined());
            assertEquals("version count post-remove pre-prune pre-commit", 2, storedVersionCount(ex2, KEY));
            prune(ex2, KEY);
            assertEquals("version count post-remove post-prune pre-commit", 2, storedVersionCount(ex2, KEY));
            trx1.commit();
        } finally {
            trx1.end();
        }

        prune(ex2, KEY);
        // NOTE: Next assert is confirming non-removal of a key that can't be
        // quick deleted.
        // Will need to be adjusted if that changes (directly, pruner thread,
        // etc).
        assertEquals("version count prune after commit", 1, storedVersionCount(ex2, KEY));

        ex2.clear().append(KEY);
        assertEquals("value prune after commit not in txn", false, ex2.isValueDefined());

        trx1.begin();
        try {
            assertEquals("key exists prune after commit in trx", false, ex1.isValueDefined());
            trx1.commit();
        } finally {
            trx1.end();
        }
    }

    /*
     * Tests currently heuristic for pruning on split. That is, always prune
     * entire page when deciding to split. Will need updated if that changes.
     */
    public void testPruneOnSplit() throws PersistitException {
        final int MAX_KEYS = 2500;

        String curKey = "";
        boolean hadSplit = false;
        for (int i = 0; i < MAX_KEYS; ++i) {
            _persistit.getTransactionIndex().cleanup();
            trx1.begin();
            try {
                curKey = String.format("k%4d", i);
                store(ex1, curKey, i);
                hadSplit = ex1.getStoreCausedSplit();
                trx1.commit();
                if (hadSplit) {
                    break;
                }
            } finally {
                trx1.end();
            }
        }

        assertEquals("had split before inserting max number of keys", true, hadSplit);

        Value ex1Value = ex1.getValue();
        ex1.ignoreMVCCFetch(true);
        ex1.clear().append(Key.BEFORE);
        while (ex1.next()) {
            boolean isMVV = MVV.isArrayMVV(ex1Value.getEncodedBytes(), 0, ex1Value.getEncodedSize());
            assertEquals("last key is MVV", ex1.getKey().decodeString().equals(curKey), isMVV);
        }
        ex1.ignoreMVCCFetch(false);
    }

    public void testPruneAlternatingAbortedAndCommittedVersions() throws PersistitException {
        final char VERSIONS[] = { 'A', 'C', 'A', 'C', 'A' };
        storePrimordial(ex1, KEY, VALUE);

        for (int i = 0; i < VERSIONS.length; ++i) {
            trx1.begin();
            try {
                store(ex1, KEY, VALUE + VERSIONS[i] + i);
                if (VERSIONS[i] == 'A') {
                    trx1.rollback();
                } else {
                    trx1.commit();
                }
            } finally {
                trx1.end();
            }
        }
        int VERSIONS_NOW_REMOVED_BY_PRUNING_BEFORE_STORE = 2;
        assertEquals("stored versions", VERSIONS.length + 1 - VERSIONS_NOW_REMOVED_BY_PRUNING_BEFORE_STORE,
                storedVersionCount(ex2, KEY));

        trx1.begin();
        try {
            String value = VALUE + "_final";
            store(ex1, KEY, value);
            assertEquals("trx value fetched before pre-prune pre-commit", value, fetch(ex1, KEY));

            prune(ex1, KEY);
            assertEquals("trx value fetched before post-prune pre-commit", value, fetch(ex1, KEY));

            trx1.commit();
        } finally {
            trx1.end();
        }

        assertEquals("stored versions", 2, storedVersionCount(ex2, KEY));
    }

    public void testPruneRunOfAbortedAndCommittedVersions() throws PersistitException {
        final char VERSIONS[] = { 'A', 'A', 'A', 'C', 'A', 'A', 'C' };

        for (int i = 0; i < VERSIONS.length; ++i) {
            trx1.begin();
            try {
                store(ex1, KEY, VALUE + i + VERSIONS[i]);
                if (VERSIONS[i] == 'A') {
                    trx1.rollback();
                } else {
                    trx1.commit();
                }
            } finally {
                trx1.end();
            }
        }

        int VERSIONS_NOW_REMOVED_BY_PRUNING_BEFORE_STORE = 5;

        assertEquals("stored versions pre-prune", VERSIONS.length - VERSIONS_NOW_REMOVED_BY_PRUNING_BEFORE_STORE,
                storedVersionCount(ex2, KEY));
        prune(ex1, KEY);
        assertEquals("stored versions post-prune", 1, storedVersionCount(ex2, KEY));
    }

    //
    // Test helper methods
    //

    private void prune(Exchange ex, Object k) throws PersistitException {
        _persistit.getTransactionIndex().cleanup();
        _persistit.getTransactionIndex().cleanup();
        ex.clear().append(k);
        ex.prune();
    }

    private class VersionInfoVisitor implements MVV.VersionVisitor {
        List<Long> _versions = new ArrayList<Long>();

        @Override
        public void init() {
        }

        @Override
        public void sawVersion(long version, int valueLength, int offset) throws PersistitException {
            _versions.add(version);
        }

        int sawCount() {
            return _versions.size();
        }
    }

    private int storedVersionCount(Exchange ex, Object k1) throws PersistitException {
        ex.ignoreMVCCFetch(true);
        try {
            ex.clear().append(k1);
            ex.fetch();

            VersionInfoVisitor visitor = new VersionInfoVisitor();
            Value value = ex.getValue();
            MVV.visitAllVersions(visitor, value.getEncodedBytes(), value.getEncodedSize());

            ex.clear().getValue().clear();
            return visitor.sawCount();
        } finally {
            ex.ignoreMVCCFetch(false);
        }
    }

    private void storePrimordial(Exchange ex, Object k, Object v) throws PersistitException {
        if (trx1.isActive()) {
            throw new IllegalStateException("Can only store primordial when outside transaction");
        }
        store(ex, k, v);
        assertEquals("initial primordial fetch", v, fetch(ex, k));
    }
}
