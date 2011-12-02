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

    public void testPrunePrimordial() throws Exception {
        // get a primordial by storing outside of transaction
        store(ex1, KEY, VALUE);
        assertEquals("initial primordial fetch", VALUE, fetch(ex1, KEY));

        ex1.clear().append(KEY);
        ex1.prune();

        assertEquals("value after prune", VALUE, fetch(ex1, KEY));
        assertEquals("version count after prune", 1, storedVersionCount(ex1, KEY));
    }

    public void testPrunePrimordialAndOneConcurrent() throws Exception {
        // get a primordial by storing outside of transaction
        store(ex1, KEY, VALUE);
        assertEquals("initial primordial fetch", VALUE, fetch(ex1, KEY));

        trx1.begin();
        try {
            store(ex1, KEY, VALUE_TRX1);
            assertEquals("fetch after trx store", VALUE_TRX1, fetch(ex1, KEY));

            ex2.clear().append(KEY);
            ex2.prune();

            assertEquals("fetch after prune", VALUE_TRX1, fetch(ex1, KEY));
            assertEquals("version count after prune", 2, storedVersionCount(ex1, KEY));
        }
        finally {
            trx1.end();
        }
    }

    public void testPrunePrimordialAndCommitted() throws Exception {
        // get a primordial by storing outside of transaction
        store(ex1, KEY, VALUE);
        assertEquals("initial primordial fetch", VALUE, fetch(ex1, KEY));

        trx1.begin();
        try {
            store(ex1, KEY, VALUE_TRX1);
            trx1.commit();
        }
        finally {
            trx1.end();
        }

        assertEquals("version count after trx store", 2, storedVersionCount(ex1, KEY));

        ex1.clear().append(KEY);
        ex1.prune();

        assertEquals("version count after prune", 1, storedVersionCount(ex1, KEY));
    }


    //
    // Test helper methods
    //

    
    private class VersionInfoVisitor implements MVV.VersionVisitor {
        List<Long> _versions = new ArrayList<Long>();

        @Override
        public void init() {}

        @Override
        public void sawVersion(long version, int valueLength, int offset) throws PersistitException {
            _versions.add(version);
        }

        int sawCount() {
            return _versions.size();
        }
    }

    private int storedVersionCount(Exchange ex, Object k1) throws PersistitException {
        _persistit.getTransactionIndex().cleanup();
        ex.ignoreMVCCFetch(true);
        try {
            ex.clear().append(k1);
            ex.fetch();

            VersionInfoVisitor visitor = new VersionInfoVisitor();
            Value value = ex.getValue();
            MVV.visitAllVersions(visitor, value.getEncodedBytes(), value.getEncodedSize());

            return visitor.sawCount();
        }
        finally {
            ex.ignoreMVCCFetch(false);
        }
    }
}
