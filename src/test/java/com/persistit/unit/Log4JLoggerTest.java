/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Log4JAdapter;

public class Log4JLoggerTest extends PersistitUnitTestCase {

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new Log4JLoggerTest().runAllTests();
    }

    @Override
    public void runAllTests() throws Exception {
        final Logger logger = Logger.getLogger("com.persistit.unit.JDK14LoggerTest");
        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();
        Persistit persistit = new Persistit();
        persistit.setPersistitLogger(new Log4JAdapter(logger));
        persistit.initialize(UnitTestProperties.getAlternateProperties(true));
        try {
            test1();
        } finally {
            persistit.close();
        }
    }

}
