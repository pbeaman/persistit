/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        final Key key = new Key((Persistit) null);
        boolean okay = true;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                ScriptedKeyFilterTest.class.getResourceAsStream(SCRIPT_NAME)));
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
                final int p = line.indexOf(' ');
                final String kw = line.substring(star ? 1 : 0, p);
                final String arg = line.substring(p + 1);
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
            } catch (final Exception e) {
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
        final boolean result = kf.next(key, direction);
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
