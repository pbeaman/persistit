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

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * Demonstrates/tests code that performs case-insensitive LIKE traversal.
 * 
 * @author peter
 * 
 */
public class TraverseCaseInsensitiveLikeTest extends PersistitUnitTestCase {

    final static String[] NAMES = { "Alpha", "Beta", "beta", "atomic", "Chutney", "ChuKoo", "CHUKOO", "cHuKoO", "chuckie",};

    final static String[] PREFIXES = { "ALPHA:1", "A:2", "B:2", "BZ:0", "CHUT:1", "CHU:5" };

    @Test
    public void testCaseInsensitiveLike() throws PersistitException {
        final Exchange exchange = _persistit.getExchange("persistit", "like", true);
        for (String name : NAMES) {
            exchange.to(name).store();
        }

        for (String prefix : PREFIXES) {
            final int expected = Integer.parseInt(prefix.split(":")[1]);
            assertEquals(expected, traverseCount(exchange, prefix.split(":")[0]));
        }
    }

    private int traverseCount(final Exchange exchange, final String prefix) throws PersistitException {
        System.out.println("Prefix=" + prefix);
        exchange.to(prefix);
        int count = 0;
        int traverses = 0;
        Key.Direction direction = Key.GTEQ;
        StringBuilder sb = new StringBuilder();
        while (exchange.traverse(direction, false)) {
            traverses++;
            direction = Key.GT;
            sb.setLength(0);
            exchange.getKey().decodeString(sb);
            int p;
            int end = Math.min(sb.length(), prefix.length());
            for (p = 0; p < end && Character.toUpperCase(sb.charAt(p)) == prefix.charAt(p); p++)
                ;
            if (p == end && end == prefix.length()) {
                count++;
                System.out.println("  matched=" + sb + "  traverses=" + traverses);
            } else {
                if (p == end) {
                    p--;
                }
                /*
                 * p now points to a character in the to string that possibly
                 * needs to change to lower case. Now look for a character that
                 * can change to lower case.
                 */
                for (; p >= 0; p--) {
                    char c = sb.charAt(p);
                    char z = Character.toLowerCase(prefix.charAt(p));
                    if (c < z) {
                        sb.setCharAt(p, z);
                        if (p + 1 == prefix.length()) {
                            direction = Key.GTEQ;
                        }
                        sb.setLength(p + 1);
                        exchange.getKey().to(sb);
                        break;
                    }
                }
                if (p < 0) {
                    break;
                }
            }
        }

        return count;
    }
}
