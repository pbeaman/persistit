/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 24, 2004
 */
package com.persistit.unit;

import java.util.Properties;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

public class RecoveryTest extends PersistitUnitTestCase {

    private static String[] _args = new String[0];

    private String _volumeName = "persistit";

    public void test1() throws Exception {
        store1();
        _persistit.close();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        fetch1a();
        fetch1b();
    }

    private void checkEmpty() throws PersistitException {
        System.out.print("test1_0 ");
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", true);
        exchange.append(Key.BEFORE);
        final boolean empty = !exchange.traverse(Key.GT, true);
        assertTrue(empty);
        System.out.println("- done");
    }

    private void store1() throws PersistitException {
        System.out.print("test1_1 ");
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", true);
        exchange.removeAll();
        final StringBuffer sb = new StringBuffer();

        for (int i = 1; i < 50000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.getValue().put("Record #" + i);
            exchange.store();
        }
    }

    private void fetch1a() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", false);
        final StringBuffer sb = new StringBuffer();

        for (int i = 1; i < 50000; i++) {
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
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", false);
        final StringBuffer sb = new StringBuffer();
        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i % 20 + 64));
            sb.append((char) (i / 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            final int k = (i / 20) + (i % 20) * 20;
            assertEquals(exchange.getValue().getString(), "Record #" + k);
        }

        System.out.println("- done");
    }

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
        }
    }

}
