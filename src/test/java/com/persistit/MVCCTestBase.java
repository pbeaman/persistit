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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Util;

public abstract class MVCCTestBase extends PersistitUnitTestCase {
    protected static final String TEST_VOLUME_NAME = "persistit";
    protected static final String TEST_TREE_NAME = "mvcctestbase";

    protected Exchange ex1, ex2;
    protected Transaction trx1, trx2;
    protected SessionId session1, session2;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ex1 = createUniqueExchange();
        trx1 = ex1.getTransaction();
        session1 = _persistit.getSessionId();

        ex2 = createUniqueExchange();
        trx2 = ex2.getTransaction();
        session2 = _persistit.getSessionId();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            assertEquals("open read claims", 0, countClaims(false));
            assertEquals("open write claims", 0, countClaims(true));

            _persistit.releaseExchange(ex1);
            _persistit.releaseExchange(ex2);
            ex1 = ex2 = null;
            trx1 = trx2 = null;
            session1 = session2 = null;
        } finally {
            super.tearDown();
        }
    }

    //
    // Internal test methods
    //

    protected int countClaims(final boolean writer) {
        final Volume vol = ex1.getVolume();
        int count;
        int retries = 5;
        while ((count = _persistit.getBufferPool(vol.getPageSize()).countInUse(vol, writer)) != 0) {
            try {
                if (--retries < 0) {
                    break;
                }
                Util.sleep(100);
            } catch (final PersistitInterruptedException e) {
                break;
            }
        }
        return count;
    }

    protected static class KVPair implements Comparable<KVPair> {
        Object k1, k2, v;

        public KVPair(final Object k1, final Object k2, final Object v) {
            this.k1 = k1;
            this.k2 = k2;
            this.v = v;
        }

        @Override
        public String toString() {
            if (k2 == null) {
                return String.format("%s->%s", k1, v);
            }
            return String.format("%s,%s->%s", k1, k2, v);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof KVPair))
                return false;
            final KVPair rhs = (KVPair) o;
            return k1.equals(rhs.k1) && !(k2 != null ? !k2.equals(rhs.k2) : rhs.k2 != null) && v.equals(rhs.v);

        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public int compareTo(final KVPair kvPair) {
            if (!(k1 instanceof Comparable)) {
                throw new IllegalArgumentException("Not comparable: " + k1);
            }
            int comp = ((Comparable) k1).compareTo(kvPair.k1);
            if (k2 != null && kvPair.k2 != null && comp == 0) {
                comp = ((Comparable) k2).compareTo(kvPair.k2);
            }
            return comp;
        }

        public void fillKey(final Key key) {
            key.clear();
            key.append(k1);
            if (k2 != null) {
                key.append(k2);
            }
        }
    }

    protected static Object[] arr(final Object... values) {
        return values;
    }

    protected static void addTraverseResult(final Collection<KVPair> collection, final Key key, final Value value) {
        Object k1, k2 = null;
        switch (key.getDepth()) {
        default:
            throw new IllegalArgumentException("Unexpected key depth: " + key.getDepth());
        case 2:
            key.indexTo(1);
            k2 = key.decode();
        case 1:
            key.indexTo(0);
            k1 = key.decode();
        }
        final Object v = value.isDefined() ? value.get() : "UD";
        collection.add(new KVPair(k1, k2, v));
    }

    protected static List<KVPair> traverseAllFoward(final Exchange e, final boolean deep) throws Exception {
        e.clear().append(Key.BEFORE);
        return doTraverse(e, Key.GT, deep);
    }

    protected static List<KVPair> traverseAllReverse(final Exchange e, final boolean deep) throws Exception {
        e.clear().append(Key.AFTER);
        return doTraverse(e, Key.LT, deep);
    }

    protected static List<KVPair> doTraverse(final Key.EdgeValue startAt, final Exchange ex, final Key.Direction dir,
            final KeyFilter filter) throws Exception {
        ex.clear().append(startAt);
        final List<KVPair> out = new ArrayList<KVPair>();
        while (ex.traverse(dir, filter, Integer.MAX_VALUE)) {
            addTraverseResult(out, ex.getKey(), ex.getValue());
        }
        return out;
    }

    protected static List<KVPair> doTraverse(final Exchange e, final Key.Direction dir, final boolean deep)
            throws Exception {
        final List<KVPair> out = new ArrayList<KVPair>();
        while (e.traverse(dir, deep)) {
            addTraverseResult(out, e.getKey(), e.getValue());
        }
        return out;
    }

    protected static List<KVPair> kvList(final Object... values) {
        if ((values.length % 2) != 0) {
            throw new IllegalArgumentException("Must be even number of objects to create pairs from");
        }
        final List<KVPair> out = new ArrayList<KVPair>();
        for (int i = 0; i < values.length; i += 2) {
            Object k1, k2 = null;
            if (values[i].getClass() == values.getClass()) {
                final Object[] ks = (Object[]) values[i];
                k1 = ks[0];
                k2 = ks[1];
            } else {
                k1 = values[i];
            }
            out.add(new KVPair(k1, k2, values[i + 1]));
        }
        return out;
    }

    protected static void storeAll(final Exchange ex, final List<KVPair> list) throws PersistitException {
        for (final KVPair kv : list) {
            kv.fillKey(ex.getKey());
            ex.getValue().put(kv.v);
            ex.store();
        }
    }

    protected static List<KVPair> combine(final List<KVPair> list1, final List<KVPair> list2) {
        final List<KVPair> outList = new ArrayList<KVPair>();
        outList.addAll(list1);
        outList.addAll(list2);
        // sort and unique them
        final TreeSet<KVPair> set = new TreeSet<KVPair>(outList);
        outList.clear();
        outList.addAll(set);
        return outList;
    }

    protected static void store(final Exchange ex, final Object k, final Object v) throws PersistitException {
        ex.clear().append(k).getValue().put(v);
        ex.store();
    }

    protected static void store(final Exchange ex, final Object kp1, final Object kp2, final Object v)
            throws PersistitException {
        ex.clear().append(kp1).append(kp2).getValue().put(v);
        ex.store();
    }

    protected static Object fetch(final Exchange ex, final Object k) throws PersistitException {
        return fetch(ex, k, true);
    }

    protected static Object fetch(final Exchange ex, final Object k, final boolean getValue) throws PersistitException {
        ex.getValue().clear();
        ex.clear().append(k).fetch();
        return getValue ? ex.getValue().get() : null;
    }

    protected static boolean remove(final Exchange ex, final Object k) throws PersistitException {
        ex.clear().append(k);
        return ex.remove();
    }

    protected static void removeAll(final Exchange ex, final List<KVPair> list) throws PersistitException {
        for (final KVPair kv : list) {
            kv.fillKey(ex.getKey());
            ex.remove();
        }
    }

    protected Exchange createUniqueExchange() throws PersistitException {
        final SessionId session = new SessionId();
        _persistit.setSessionId(session);
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }

    protected static void storeLongMVV(final Exchange ex, final Object k) throws PersistitException {
        ex.clear().append(k);
        final String longStr = createString(ex.maxValueSize(ex.getKey().getEncodedSize()) - MVV.overheadLength(1));
        store(ex, k, longStr);
        assertEquals("stored long record at key " + k, true, ex.isValueLongRecord());
    }

    protected void showGUI() {
        try {
            _persistit.setupGUI(true);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
