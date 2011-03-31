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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class TransactionalCacheTest extends PersistitUnitTestCase {

    private TableStatus _tableStatus = new TableStatus(_persistit);

    public static class TableStatus extends TransactionalCache implements
            Serializable {

        private static final long serialVersionUID = 2823468378367226075L;

        private final static byte COUNT = 1;
        private final static byte MAX = 2;
        private final static byte MIN = 3;

        Map<Integer, AtomicLong> _minima = new HashMap<Integer, AtomicLong>();
        Map<Integer, AtomicLong> _maxima = new HashMap<Integer, AtomicLong>();
        Map<Integer, AtomicLong> _count = new HashMap<Integer, AtomicLong>();

        public static class CountUpdate extends UpdateInt {

            CountUpdate() {
                super(COUNT);
            }

            CountUpdate(final int tableId) {
                this();
                _arg = tableId;
            }

            @Override
            protected void apply(TransactionalCache tc) {
                TableStatus ts = (TableStatus) tc;
                AtomicLong a = ts._count.get(Integer.valueOf(_arg));
                if (a == null) {
                    a = new AtomicLong();
                    ts._count.put(Integer.valueOf(_arg), a);
                }
                a.incrementAndGet();
            }

            @Override
            public String toString() {
                return String.format("<Count:%d>", _arg);
            }
        }

        public static class MaxUpdate extends UpdateLongArray {

            MaxUpdate() {
                super(MAX);
            }

            MaxUpdate(final int tableId, final long proposed) {
                this();
                _args = new long[] { tableId, proposed };
            }

            @Override
            protected void apply(TransactionalCache tc) {
                TableStatus ts = (TableStatus) tc;
                AtomicLong a = ts._maxima.get(Integer.valueOf((int) _args[0]));
                if (a == null) {
                    a = new AtomicLong();
                    ts._maxima.put(Integer.valueOf((int) _args[0]), a);
                }
                a.set(Math.max(a.get(), _args[1]));
            }

            @Override
            public String toString() {
                return String.format("<Max:%d(%d)>", _args[0], _args[1]);
            }
        }

        public static class MinUpdate extends UpdateLongArray {

            MinUpdate() {
                super(MIN);
            }

            MinUpdate(final int tableId, final long proposed) {
                this();
                _args = new long[] { tableId, proposed };
            }

            @Override
            protected void apply(TransactionalCache tc) {
                TableStatus ts = (TableStatus) tc;
                AtomicLong a = ts._minima.get(Integer.valueOf((int) _args[0]));
                if (a == null) {
                    a = new AtomicLong();
                    ts._minima.put(Integer.valueOf((int) _args[0]), a);
                }
                a.set(Math.min(a.get(), _args[1]));
            }

            @Override
            public String toString() {
                return String.format("<Min:%d(%d)>", _args[0], _args[1]);
            }

        }

        private TableStatus(final Persistit db) {
            super(db);
        }

        @Override
        protected Update createUpdate(byte opCode) {
            switch (opCode) {
            case 1:
                return new CountUpdate();
            case 2:
                return new MaxUpdate();
            case 3:
                return new MinUpdate();
            default:
                throw new IllegalArgumentException("Invalid opCode: " + opCode);
            }
        }

        @Override
        protected long cacheId() {
            return serialVersionUID;
        }

        @Override
        public TableStatus copy() {
            TableStatus ts = new TableStatus(_persistit);
            ts._checkpoint = _checkpoint;
            ts._count = new HashMap<Integer, AtomicLong>(_count);
            ts._minima = new HashMap<Integer, AtomicLong>(_minima);
            ts._maxima = new HashMap<Integer, AtomicLong>(_maxima);
            ts._previousVersion = _previousVersion;
            return ts;
        }

        @Override
        public void save() {
            try {
                final Exchange exchange = _persistit.getExchange("persistit",
                        "TableStatus", true);
                exchange.append(_checkpoint.getTimestamp());
                exchange.remove(Key.GTEQ);
                saveMap(exchange, "count", _count);
                saveMap(exchange, "minima", _minima);
                saveMap(exchange, "maxima", _maxima);
            } catch (PersistitException e) {

            }
        }

        private void saveMap(final Exchange exchange, final String category,
                final Map<Integer, AtomicLong> map) throws PersistitException {
            exchange.append(category);
            for (final Entry<Integer, AtomicLong> entry : map.entrySet()) {
                exchange.append(entry.getKey().intValue());
                exchange.getValue().put(entry.getValue().longValue());
                exchange.store();
                exchange.cut();
            }
            exchange.cut();
        }

        @Override
        public void load() {
            try {
                final Exchange exchange = _persistit.getExchange("persistit",
                        "TableStatus", true);
                if (exchange.append(Key.AFTER).previous()) {
                    final long timestamp = exchange.getKey().reset()
                            .decodeLong();
                    _checkpoint = new Checkpoint(timestamp, 0);
                    loadMap(exchange, "count", _count);
                    loadMap(exchange, "minima", _minima);
                    loadMap(exchange, "maxima", _maxima);
                }
            } catch (PersistitException e) {

            }

        }

        private void loadMap(final Exchange exchange, final String category,
                final Map<Integer, AtomicLong> map) throws PersistitException {
            map.clear();
            exchange.append(category).append(Key.BEFORE);
            while (exchange.next()) {
                final int tableId = exchange.getKey().indexTo(-1).decodeInt();
                map.put(Integer.valueOf(tableId), new AtomicLong(exchange
                        .getValue().getLong()));
            }
            exchange.cut().cut();
        }

        // ====
        // Public API methods of this TableStatus.

        public void incrementCount(final int tableId) {
            update(new CountUpdate(tableId));
        }

        public void proposeMin(final int tableId, final long proposed) {
            update(new MinUpdate(tableId, proposed));
        }

        public void proposeMax(final int tableId, final long proposed) {
            update(new MaxUpdate(tableId, proposed));
        }

        public long getCount(final int tableId) {
            AtomicLong a = _count.get(Integer.valueOf(tableId));
            if (a != null) {
                return a.get();
            }
            return -1;
        }

        public long getMax(final int tableId) {
            AtomicLong a = _maxima.get(Integer.valueOf(tableId));
            if (a != null) {
                return a.get();
            }
            return Long.MIN_VALUE;
        }

        public long getMin(final int tableId) {
            AtomicLong a = _minima.get(Integer.valueOf(tableId));
            if (a != null) {
                return a.get();
            }
            return Long.MAX_VALUE;
        }

        @Override
        public String toString() {
            return String.format("TableStatus counts=%s, maxima=%s, minima=%s",
                    _count, _maxima, _minima);
        }

    }

    @Test
    public void testSimpleUpdates() throws Exception {
        final Transaction transaction = _persistit.getTransaction();
        transaction.begin();
        try {
            _tableStatus.incrementCount(1);
            _tableStatus.incrementCount(2);
            _tableStatus.incrementCount(3);
            _tableStatus.incrementCount(1);
            _tableStatus.proposeMax(1, 15);
            _tableStatus.proposeMax(1, 12);
            transaction.commit(true);
        } finally {
            transaction.end();
        }

        assertEquals(2, _tableStatus.getCount(1));
        assertEquals(1, _tableStatus.getCount(2));
        assertEquals(1, _tableStatus.getCount(3));
        assertEquals(-1, _tableStatus.getCount(4));
        assertEquals(Long.MAX_VALUE, _tableStatus.getMin(1));
        assertEquals(15, _tableStatus.getMax(1));
        _persistit.flush();

        _tableStatus.save();
        final TableStatus copy = _tableStatus.copy();
        copy._count.clear();
        copy._maxima.clear();
        copy._minima.clear();

        copy.load();

        assertEquals(_tableStatus.toString(), copy.toString());
        assertTrue(_tableStatus._count != copy._count);
        assertTrue(_tableStatus._maxima != copy._maxima);
        assertTrue(_tableStatus._minima != copy._minima);

//        showJournal();
    }

    @Test
    public void testNormalCheckpoint() throws Exception {
        final Transaction transaction = _persistit.getTransaction();
        for (int count = 0; count < 20; count++) {
            transaction.begin();
            try {
                _tableStatus.incrementCount(1);
                _tableStatus.incrementCount(2);
                _tableStatus.incrementCount(3);
                _tableStatus.incrementCount(1);
                _tableStatus.proposeMax(1, count % 37);
                _tableStatus.proposeMin(1, count % 37);
                transaction.commit(true);
            } finally {
                transaction.end();
            }
        }

        _persistit.checkpoint();
        _persistit.close();
//        showJournal();
        final Properties properties = _persistit.getProperties();
        _persistit = new Persistit();
        final TableStatus copy = new TableStatus(_persistit);
        copy.register();
        _persistit.initialize(properties);
        assertEquals(_tableStatus.toString(), copy.toString());
    }
    
    @Test
    public void testCrashCheckpoint() throws Exception {
        final Transaction transaction = _persistit.getTransaction();
        for (int count = 0; count < 20; count++) {
            transaction.begin();
            try {
                _tableStatus.incrementCount(1);
                _tableStatus.incrementCount(2);
                _tableStatus.incrementCount(3);
                _tableStatus.incrementCount(1);
                _tableStatus.proposeMax(1, count % 37);
                _tableStatus.proposeMin(1, count % 37);
                transaction.commit(true);
            } finally {
                transaction.end();
            }
        }

        _persistit.checkpoint();
        _persistit.getJournalManager().force();
        
//        System.out.println("===========");
//        showJournal();
//        System.out.println("===========");
        
        for (int count = 0; count < 20; count++) {
            transaction.begin();
            try {
                _tableStatus.incrementCount(1);
                _tableStatus.incrementCount(2);
                _tableStatus.incrementCount(3);
                _tableStatus.incrementCount(1);
                _tableStatus.proposeMax(1, count % 37);
                _tableStatus.proposeMin(1, count % 37);
                transaction.commit(true);
            } finally {
                transaction.end();
            }
        }
        _persistit.getJournalManager().flush();
        
        _persistit.crash();
        
//        showJournal();
        final Properties properties = _persistit.getProperties();
        _persistit = new Persistit();
        final TableStatus copy = new TableStatus(_persistit);
        copy.register();
        _persistit.initialize(properties);
        
        assertEquals(_tableStatus.toString(), copy.toString());
    }

    @Override
    public void setUp() throws Exception {
        _tableStatus.register();
        super.setUp();
    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

    private void showJournal() throws Exception {
        JournalTool jt = new JournalTool(
                _persistit,
                new String[] { "path=/tmp/persistit_test_data/persistit_journal" });
        jt.scan();
    }
}
