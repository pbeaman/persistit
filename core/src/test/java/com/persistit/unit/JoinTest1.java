/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.unit;

import java.io.IOException;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

public class JoinTest1 extends PersistitUnitTestCase {

    String _volumeName = "persistit";

    @Test
    public void test1() throws PersistitException {
        // Tests join calculation.
        //
        System.out.print("test1 ");

        final StringBuilder sb = new StringBuilder(4000);
        final Exchange exchange = _persistit.getExchange(_volumeName, "SimpleTest1BadJoin", true);
        exchange.removeAll();
        final Key key = exchange.getKey();
        final Value value = exchange.getValue();

        key.clear().append("A").append(0);
        setupString(sb, 1675);
        value.putString(sb);
        exchange.store();

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

        key.clear().append("A").append(4);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("B").append(
                "... a pretty long key value. The goal is to get the the record "
                        + "for this key into the penultimate slot of the left page, followed "
                        + "by a short key on the edge.  Then delete that short key, so that"
                        + "this becomes the edge key.");
        setupString(sb, 1);
        value.putString(sb);
        exchange.store();

        // Here's where we want the page to split...
        key.clear().append("B").append("z");
        setupString(sb, 102);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(0);
        setupString(sb, 720);
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

        key.clear().append("A").append(3);
        setupString(sb, 1000 + 2160);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(1);
        setupString(sb, 1000 + 2640);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 1000 + 2640);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(3);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        for (int len = 1000; len < 1600; len += 1) {
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

            key.clear().append("A").append(4);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

            key.clear().append("C").append(1);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

            key.clear().append("C").append(2);
            setupString(sb, len);
            value.putString(sb);
            exchange.store();

        }

        // Now the page should be split with the {"B", "z"} on the edge.

        key.clear().append("A").append(1);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.fetch();

        key.clear().append("C").append(3);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.remove(); // may cause wedge failure.

        System.out.println("- done");
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
        new JoinTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
    }
}
