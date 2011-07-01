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

package com.persistit.unit;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

public class SimpleTest1 extends PersistitUnitTestCase {

    private static String[] _args = new String[0];

    private String _volumeName = "persistit";

    @Test
    public void test1() throws PersistitException {
        store1();
        fetch1a();
        fetch1b();
        fetch1c();
    }

    private void checkEmpty() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1", true);
        exchange.append(Key.BEFORE);
        final boolean empty = !exchange.traverse(Key.GT, true);
        assertTrue(empty);
    }

    private void store1() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1", true);
        exchange.removeAll();
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.getValue().put("Record #" + i);
            exchange.store();
        }
    }

    private void fetch1a() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1", false);
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            assertTrue(exchange.getValue().isDefined());
            assertEquals("Record #" + i, exchange.getValue().getString());
        }

    }

    private void fetch1b() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1", false);
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i % 20 + 64));
            sb.append((char) (i / 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            final int k = (i / 20) + (i % 20) * 20;
            assertEquals(exchange.getValue().getString(), "Record #" + k);
        }

    }

    private void fetch1c() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1", false);
        int count;

        exchange.getKey().clear().append(Key.BEFORE);
        final StringBuilder sb = new StringBuilder();

        for (count = 1; exchange.next() && count < 10000; count++) {
            sb.setLength(0);
            sb.append((char) (count / 20 + 64));
            sb.append((char) (count % 20 + 64));
            String key = exchange.getKey().reset().decodeString();
            assertEquals(sb.toString(), key);
        }
        assertEquals(400, count);

        exchange.getKey().clear().append(Key.AFTER);
        for (count = 399; exchange.previous() && count > -10000; count--) {
            sb.setLength(0);
            sb.append((char) (count / 20 + 64));
            sb.append((char) (count % 20 + 64));
            String key = exchange.getKey().reset().decodeString();
            assertEquals(sb.toString(), key);
        }
        assertEquals(0, count);
    }

    @Test
    public void test2() throws PersistitException {
        store2();
        fetch2();
    }

    private void store2() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1LongRecord", true);
        exchange.getValue().setMaximumSize(32 * 1024 * 1024);
        final StringBuilder sb = new StringBuilder();
        int length = 19;
        while (length < 10000000) {
            sb.setLength(0);
            sb.append(com.persistit.util.Util.format(length));
            sb.append("  ");
            sb.setLength(length);
            // System.out.print("Record length " + length);
            exchange.getValue().put(sb.toString());
            // System.out.print(" encoded: "
            // + exchange.getValue().getEncodedSize());
            exchange.clear().append(length).store();
            // System.out.println(" - stored");
            length *= 2;
        }
    }

    private void fetch2() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1LongRecord", true);
        exchange.getValue().setMaximumSize(32 * 1024 * 1024);
        final StringBuilder sb = new StringBuilder();
        final StringBuilder sb2 = new StringBuilder();
        int length = 19;
        while (length < 10000000) {
            sb.setLength(0);
            sb.append(com.persistit.util.Util.format(length));
            sb.append("  ");
            sb.setLength(length);
            // System.out.print("Record length " + length);
            exchange.clear().append(length).fetch();
            exchange.getValue().getString(sb2);
            assertEquals(sb.toString(), sb2.toString());
            // System.out.println(" - read");
            length *= 2;
        }
    }

    @Test
    public void test3() throws PersistitException {
        // Tests fix for split calculation failure.
        //

        final StringBuilder sb = new StringBuilder(4000);
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1BadSplit", true);
        exchange.removeAll();
        final Key key = exchange.getKey();
        final Value value = exchange.getValue();

        key.clear().append("A").append(1);
        setupString(sb, 3000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(2);
        setupString(sb, 3000);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1566).append(3);
        setupString(sb, 119);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1568).append(4);
        setupString(sb, 2258);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1569).append(3);
        setupString(sb, 119);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1571).append(3);
        setupString(sb, 3052);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1573).append(3);
        setupString(sb, 119);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1573).append(4);
        setupString(sb, 2203);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1573).append(3);
        setupString(sb, 2524);
        value.putString(sb);
        exchange.store();

        key.clear().append("stress2").append(1566).append(3);
        setupString(sb, 119);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());

        key.clear().append("stress2").append(1568).append(4);
        setupString(sb, 2258);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());

        key.clear().append("stress2").append(1569).append(3);
        setupString(sb, 119);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());

        key.clear().append("stress2").append(1571).append(3);
        setupString(sb, 3052);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());

        key.clear().append("stress2").append(1573).append(4);
        setupString(sb, 2203);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());

        key.clear().append("stress2").append(1573).append(3);
        setupString(sb, 2524);
        exchange.fetch();
        assertEquals(sb.toString(), exchange.getValue().getString());
    }

    @Test
    public void test4() throws PersistitException {
        // Tests join calculation.
        //

        final StringBuilder sb = new StringBuilder(4000);
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1BadJoin", true);
        exchange.removeAll();
        final Key key = exchange.getKey();
        final Value value = exchange.getValue();

        key.clear().append("A").append(1);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(2);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(3);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("B").append(
                "... a pretty long key value. The goal is to get the the record "
                        + "for this key into the penultimate slot of the left page, followed "
                        + "by a short key on the edge.  Then delete that short key, so that"
                        + "this becomes the edge key.");
        setupString(sb, 10);
        value.putString(sb);
        exchange.store();
        // Here's where we want the page to split...
        key.clear().append("B").append("z");
        setupString(sb, 20);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(1);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        for (int len = 1000; len < 2600; len += 100) {
            key.clear().append("A").append(1);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

            key.clear().append("A").append(2);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

            key.clear().append("A").append(3);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

            key.clear().append("C").append(1);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();
        }

        // Now the page should be split with the {"B", "z"} on the edge.
        // Need an additional 4540 bytes, leaving 60 bytes free.

        key.clear().append("C").append(1);
        setupString(sb, 4040); // adds 1540
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 4040); // adds 1540
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 4040); // adds 1540
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(1);
        setupString(sb, 2500 + 356);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(1);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.fetch();

        key.clear().append("C").append(3);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.remove(); // may cause wedge failure.

    }

    @Test
    public void test5() throws PersistitException {
        final StringBuilder sb = new StringBuilder(1024 * 1024 * 16);
        final StringBuilder sb2 = new StringBuilder(1024 * 1024 * 16);

        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1BadStoreOverLengthRecord", true);
        exchange.removeAll();
        final Key key = exchange.getKey();
        final Value value = exchange.getValue();
        value.setMaximumSize(1024 * 1024 * 32);

        key.clear().append("A").append(1);
        final int length = 8160 * 1024 * 2 + 1;
        // System.out.print(" " + length);
        setupString(sb, length);
        value.putString(sb);
        exchange.store();
        exchange.fetch();
        value.getString(sb2);
        final int length2 = sb2.length();
        assertEquals(length, length2);
        assertTrue(sb.toString().equals(sb2.toString()));
    }

    void setupString(final StringBuilder sb, final int length) {
        sb.setLength(length);
        final String s = "length=" + length;
        sb.replace(0, s.length(), s);
        for (int i = s.length(); i < length; i++) {
            sb.setCharAt(i, ' ');
        }
    }

    public static void pause(final String prompt) {
        System.out.print(prompt + "  Press ENTER to continue");
        System.out.flush();
        try {
            while (System.in.read() != '\r') {
            }
        } catch (final IOException ioe) {
        }
        System.out.println();
    }

    public static void main(final String[] args) throws Exception {
        _args = args;
        new SimpleTest1().initAndRunTest();
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    @Override
    public void runAllTests() throws Exception {
        String protocol = "none";
        if (_args.length > 1) {
            for (int index = 1; index < _args.length; index++) {
                protocol = _args[index];
                if (protocol.startsWith("p")) {
                    _volumeName = "persistit";
                }
                if (protocol.startsWith("t")) {
                    _volumeName = "tempvol";
                }

                System.out.println("SimpleTest1 protocol: " + protocol);
                if (protocol.equals("p")) {
                    store1();
                    fetch1a();
                    fetch1b();
                } else if (protocol.equals("p1")) {
                    store1();
                } else if (protocol.equals("p2")) {
                    fetch1a();
                    fetch1b();
                } else if (protocol.equals("t")) {
                    store1();
                    fetch1a();
                    fetch1b();
                } else if (protocol.equals("t1")) {
                    store1();
                } else if (protocol.equals("t2")) {
                    checkEmpty();
                } else {
                    System.out.println("? " + protocol);
                }
            }
        } else {
            test1();
            test2();
            test3();
            test4();
            test5();
        }
    }

}
