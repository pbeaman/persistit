/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.stress;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Runs the stress test suites defined in the _classes list below. Arguments:
 * 
 * <dl>
 * <dt>duration=nnn</dt>
 * <dd>Duration of the entire run, in seconds. This number is divided by the
 * number of classes to determine the duration of each suite</dd>
 * <dt>datapath=/xxx/yyy/zzz</dt>
 * <dd>Directory path (no trailing '/') where Persistit will write journal and
 * volume files.</dd>
 * </dl>
 * 
 * @author peter
 * 
 */
public class Nightly {

    private static List<Class<? extends AbstractSuite>> _classes = new ArrayList<Class<? extends AbstractSuite>>();
    static {
        _classes.add(InsertUUIDs.class);
        _classes.add(Mixture1.class);
        _classes.add(Mixture2.class);
        _classes.add(Mixture3.class);
        _classes.add(MixtureTxn1.class);
        _classes.add(MixtureTxn2.class);
        _classes.add(PersistitMap1.class);
        _classes.add(StartStop.class);
        _classes.add(Stress10Suite.class);
        _classes.add(Stress12txnSuite.class);
        _classes.add(Stress4Suite.class);
        _classes.add(Stress8txnSuite.class);
    }

    private final static String DURATION_PARAM = "duration=";

    public static void main(final String[] args) throws Exception {
        for (int index = 0; index < args.length; index++) {
            if (args[index].startsWith(DURATION_PARAM)) {
                args[index] = DURATION_PARAM
                        + (Integer.parseInt(args[index].substring(DURATION_PARAM.length())) / _classes.size());
            }
        }

        PrintWriter pw = new PrintWriter(new FileWriter("Nightly_Result.txt"));
        for (final Class<? extends AbstractSuite> clazz : _classes) {
            AbstractSuite suite = clazz.getConstructor(args.getClass()).newInstance(new Object[] { args });
            System.out.printf("\n--------------------------------------------------------\nStart %s at %s\n", suite
                    .getName(), now());
            suite.runTest();
            System.out.printf("\n--------------------------------------------------------\n  End %s at %s\n", suite
                    .getName(), now());
            pw.printf("%s,%s,%d\n", suite.getName(), suite.isFailed() ? "FAILED" : "PASSED", suite.getRate());
            pw.flush();
        }
        pw.close();
    }

    private static String now() {
        return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());
    }
}
