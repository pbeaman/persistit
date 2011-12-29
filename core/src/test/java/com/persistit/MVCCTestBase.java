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
import java.util.TreeSet;

public abstract class MVCCTestBase extends PersistitUnitTestCase {
    protected static final String TEST_VOLUME_NAME = "persistit";
    protected static final String TEST_TREE_NAME = "mvcctestbase";

    protected Exchange ex1, ex2;
    protected Transaction trx1, trx2;
    protected SessionId session1, session2;

    public final void setUp() throws Exception {
        super.setUp();

        ex1 = createUniqueExchange();
        trx1 = ex1.getTransaction();
        session1 = _persistit.getSessionId();

        ex2 = createUniqueExchange();
        trx2 = ex2.getTransaction();
        session2 = _persistit.getSessionId();
    }

    public final void tearDown() throws Exception {
        try {
            assertEquals("open read claims",  0, countClaims(false));
            assertEquals("open write claims", 0, countClaims(true));
            
            _persistit.releaseExchange(ex1);
            _persistit.releaseExchange(ex2);
            ex1 = ex2 = null;
            trx1 = trx2 = null;
            session1 = session2 = null;
        }
        finally {
            super.tearDown();
        }
    }


    //
    // Internal test methods
    //

    
    protected int countClaims(boolean writer) {
        final Volume vol = ex1.getVolume();
        return _persistit.getBufferPool(vol.getPageSize()).countInUse(vol, writer);
    }

    protected static class KVPair implements Comparable<KVPair> {
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

        @SuppressWarnings({"unchecked"})
        @Override
        public int compareTo(KVPair kvPair) {
            if(!(k1 instanceof Comparable)) {
                throw new IllegalArgumentException("Not comparable: " + k1);
            }
            int comp = ((Comparable)k1).compareTo(kvPair.k1);
            if(k2 != null && kvPair.k2 != null && comp == 0) {
                comp = ((Comparable)k2).compareTo(kvPair.k2);
            }
            return comp;
        }
        
        public void fillKey(Key key) {
            key.clear();
            key.append(k1);
            if (k2 != null) {
                key.append(k2);
            }
        }
    }

    protected static Object[] arr(Object ...values) {
        return values;
    }

    protected static String createString(int exactLength) {
        StringBuilder sb = new StringBuilder(exactLength);
        // Simple 0..9a..z string
        for(int i = 0; i < 36; ++i) {
            sb.append(Character.forDigit(i, 36));
        }
        final String numAndLetters = sb.toString();
        while(sb.length() < exactLength) {
            sb.append(numAndLetters);
        }
        return sb.toString().substring(0, exactLength);
    }

    protected static void addTraverseResult(Collection<KVPair> collection, Key key, Value value) {
        Object k1, k2 = null;
        switch(key.getDepth()) {
            default: throw new IllegalArgumentException("Unexpected key depth: " + key.getDepth());
            case 2: key.indexTo(1); k2 = key.decode();
            case 1: key.indexTo(0); k1 = key.decode();
        }
        Object v = value.isDefined() ? value.get() : "UD";
        collection.add(new KVPair(k1, k2, v));
    }

    protected static List<KVPair> traverseAllFoward(Exchange e, boolean deep) throws Exception {
        e.clear().append(Key.BEFORE);
        return doTraverse(e, Key.GT, deep);
    }

    protected static List<KVPair> traverseAllReverse(Exchange e, boolean deep) throws Exception {
        e.clear().append(Key.AFTER);
        return doTraverse(e, Key.LT, deep);
    }

    protected static List<KVPair> doTraverse(Key.EdgeValue startAt, Exchange ex, Key.Direction dir, KeyFilter filter) throws Exception {
        ex.clear().append(startAt);
        List<KVPair> out = new ArrayList<KVPair>();
        while(ex.traverse(dir, filter, Integer.MAX_VALUE)) {
            addTraverseResult(out, ex.getKey(), ex.getValue());
        }
        return out;
    }

    protected static List<KVPair> doTraverse(Exchange e, Key.Direction dir, boolean deep) throws Exception {
        List<KVPair> out = new ArrayList<KVPair>();
        while(e.traverse(dir,  deep)) {
            addTraverseResult(out, e.getKey(), e.getValue());
        }
        return out;
    }

    protected static List<KVPair> kvList(Object... values) {
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

    protected static void storeAll(Exchange ex, List<KVPair> list) throws PersistitException {
        for(KVPair kv : list) {
            kv.fillKey(ex.getKey());
            ex.getValue().put(kv.v);
            ex.store();
        }
    }
    
    protected static List<KVPair> combine(List<KVPair> list1, List<KVPair> list2) {
        List<KVPair> outList = new ArrayList<KVPair>();
        outList.addAll(list1);
        outList.addAll(list2);
        // sort and unique them
        TreeSet<KVPair> set = new TreeSet<KVPair>(outList);
        outList.clear();
        outList.addAll(set);
        return outList;
    }

    protected static void store(Exchange ex, Object k, Object v) throws PersistitException {
        ex.clear().append(k).getValue().put(v);
        ex.store();
    }

    protected static void store(Exchange ex, Object kp1, Object kp2, Object v) throws PersistitException {
        ex.clear().append(kp1).append(kp2).getValue().put(v);
        ex.store();
    }

    protected static Object fetch(Exchange ex, Object k) throws PersistitException {
        return fetch(ex, k, true);
    }

    protected static Object fetch(Exchange ex, Object k, boolean getValue) throws PersistitException {
        ex.getValue().clear();
        ex.clear().append(k).fetch();
        return getValue ? ex.getValue().get() : null;
    }

    protected static boolean remove(Exchange ex, Object k) throws PersistitException {
        ex.clear().append(k);
        return ex.remove();
    }

    protected static void removeAll(Exchange ex, List<KVPair> list) throws PersistitException {
        for(KVPair kv : list) {
            kv.fillKey(ex.getKey());
            ex.remove();
        }
    }

    protected Exchange createUniqueExchange() throws PersistitException {
        SessionId session = new SessionId();
        _persistit.setSessionId(session);
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }

    protected void showGUI() {
        try {
            _persistit.setupGUI(true);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
