package com.persistit.stress;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Nightly {

    @SuppressWarnings("unchecked")
    private static Class<? extends AbstractSuite>[] _classes = new Class[] { InsertUUIDs.class, Mixture1.class,
            Mixture2.class, Mixture3.class, MixtureTxn1.class, MixtureTxn2.class, PersistitMap1.class, StartStop.class,
            Stress10Suite.class, Stress12txnSuite.class, Stress4Suite.class, Stress8txnSuite.class };

    public static void main(final String[] args) throws Exception {
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
