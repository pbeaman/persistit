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

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

public class ScriptedKeyFilterTest {

    private final static String SCRIPT_NAME = "KeyFilterTestScript1";

    enum KW {
        KEY, KF, LTEQ, GTEQ, LT, GT, NUDGE
    }

    enum Nudge {
        RIGHT, LEFT, DEEPER, BEFORE, AFTER
    }

    @Test
    public void testScript() throws Exception {
        KeyFilter kf = null;
        Key key = new Key((Persistit) null);
        boolean okay = true;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(ScriptedKeyFilterTest.class
                .getResourceAsStream(SCRIPT_NAME)));
        String line;
        int lineCount = 0;
        while ((line = reader.readLine()) != null) {
            lineCount++;
            boolean star = false;
            try {
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("*")) {
                    star = true;
                }
                int p = line.indexOf(' ');
                String kw = line.substring(star ? 1 : 0, p);
                String arg = line.substring(p + 1);
                if (star) {
                    System.out.println("Break at line " + lineCount);
                }
                switch (KW.valueOf(kw)) {
                case KEY:
                    key.clear();
                    if (!new KeyParser(arg).parseKey(key)) {
                        System.out.println("Failed line: " + lineCount + ": " + line + "  parse returned false");
                        okay = false;
                    }
                    break;
                case NUDGE:
                    nudge(key, arg);
                    break;
                case KF:
                    kf = (new KeyParser(arg)).parseKeyFilter();
                    break;
                case LT:
                    okay &= check(key, kf, Key.LT, arg, lineCount);
                    break;
                case LTEQ:
                    okay &= check(key, kf, Key.LTEQ, arg, lineCount);
                    break;
                case GT:
                    okay &= check(key, kf, Key.GT, arg, lineCount);
                    break;

                case GTEQ:
                    okay &= check(key, kf, Key.GTEQ, arg, lineCount);
                    break;
                }
            } catch (Exception e) {
                okay = false;
                System.out.println();
                System.out.println("Exception " + e + " on line " + lineCount + ": " + line);
                e.printStackTrace();
            }

        }
        assertTrue(okay);
    }

    private void nudge(final Key key, final String arg) {
        switch (Nudge.valueOf(arg)) {
        case LEFT:
            key.nudgeLeft();
            break;
        case RIGHT:
            key.nudgeRight();
            break;
        case DEEPER:
            key.nudgeDeeper();
            break;
        case BEFORE:
            key.appendBefore();
            break;
        case AFTER:
            key.appendAfter();
            break;
        }
    }

    private boolean check(final Key key, final KeyFilter kf, final Key.Direction direction, final String expected,
            final int lineCount) {
        boolean result = kf.next(key, direction);
        if (result) {
            if (expected.equals(key.toString())) {
                return true;
            } else {
                System.out
                        .println("Failed line " + lineCount + ": expected=" + expected + "  actual=" + key.toString());
                return false;
            }
        } else {
            if ("<false>".equals(expected)) {
                return true;
            } else {
                System.out.println("Failed line " + lineCount + ": expected=" + expected + "  actual=<false>");
                return false;
            }
        }
    }
}
