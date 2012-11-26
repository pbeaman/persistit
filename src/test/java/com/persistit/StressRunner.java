/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.persistit.stress.AbstractSuite;
import com.persistit.stress.AccumulatorRestartSuite;
import com.persistit.stress.InsertUUIDs;
import com.persistit.stress.Mixture1;
import com.persistit.stress.Mixture2;
import com.persistit.stress.Mixture3;
import com.persistit.stress.MixtureTxn1;
import com.persistit.stress.MixtureTxn2;
import com.persistit.stress.PersistitMap1;
import com.persistit.stress.PreloadMixtureTxn1;
import com.persistit.stress.StartStop;
import com.persistit.stress.Stress10Suite;
import com.persistit.stress.Stress12txnSuite;
import com.persistit.stress.Stress4Suite;
import com.persistit.stress.Stress8txnSuite;
import com.persistit.util.ArgParser;

/**
 * Runs the stress test suites defined in the _classes list below. Arguments:
 * 
 * <dl>
 * <dt>tests=testname,test*pattern</dt>
 * <dd>Comma-separated list of test class names; allows "*" and "?" as wildcards
 * </dd>
 * <dt>duration=nnn</dt>
 * <dd>Duration of the entire run, in seconds. This number is divided by the
 * number of classes to determine the duration of each suite</dd>
 * <dt>datapath=/xxx/yyy/zzz</dt>
 * <dd>Directory path (no trailing '/') where Persistit will write journal and
 * volume files.</dd>
 * <dt>report=/xxx/yyy/zzz/reportname</dt>
 * <dd>Path to file in which summary report will be written</dd>
 * </dl>
 * 
 * @author peter
 * 
 */
public class StressRunner {

    private final static String[] ARGS_TEMPLATE = {
            "duration|int::10|Maximum duration in seconds",
            "report|String:StressRunner_" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", System.currentTimeMillis()),
            "tests|String:*|List of comma-separated patterns" };

    private static List<Class<? extends AbstractSuite>> _classes = new ArrayList<Class<? extends AbstractSuite>>();
    static {
        _classes.add(AccumulatorRestartSuite.class);
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
        _classes.add(PreloadMixtureTxn1.class);
    }

    private final static String DURATION_PARAM = "duration=";

    public static void main(final String[] args) throws Exception {

        final ArgParser ap = new ArgParser(StressRunner.class.getSimpleName(), args, ARGS_TEMPLATE);

        final List<Class<? extends AbstractSuite>> classes = new ArrayList<Class<? extends AbstractSuite>>();
        for (final String s : ap.getStringValue("tests").split(",")) {
            final String regex = s.replace("*", ".*").replace("?", ".");
            final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            for (final Class<? extends AbstractSuite> cl : _classes) {
                if (pattern.matcher(cl.getSimpleName()).matches()) {
                    classes.add(cl);
                }
            }
        }
        if (classes.isEmpty()) {
            System.out.println("No tests specified: " + ap.getStringValue("tests"));
            System.exit(1);
        }

        final int duration = ap.getIntValue("duration") / classes.size();
        final PrintWriter pw = new PrintWriter(new FileWriter(ap.getStringValue("report")));
        int failed = 0;

        for (final Class<? extends AbstractSuite> clazz : classes) {
            final List<String> suiteArgs = ap.getUnparsedList();
            if (ap.isSpecified("duration")) {
                suiteArgs.add(DURATION_PARAM + duration);
            }
            final AbstractSuite suite = clazz.getConstructor(args.getClass()).newInstance(
                    new Object[] { suiteArgs.toArray(new String[suiteArgs.size()]) });

            System.out.printf("\nStart %s at %s\n--------------------------------------------------------\n",
                    suite.getName(), now());
            suite.runTest();
            System.out.printf("\n--------------------------------------------------------\n  End %s at %s\n",
                    suite.getName(), now());
            pw.printf("%s,%s,%d\n", suite.getName(), suite.isFailed() ? "FAILED" : "PASSED", suite.getRate());
            pw.flush();
            if (suite.isFailed()) {
                failed++;
            }
        }
        pw.close();

        // If there were errors, leave a non-zero status code for framework
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static String now() {
        return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());
    }
}
