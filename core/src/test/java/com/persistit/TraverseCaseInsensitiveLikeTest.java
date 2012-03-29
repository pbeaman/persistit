/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
