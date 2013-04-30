/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.unit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.PersistitUnitTestCase;
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
        final Persistit persistit = new Persistit();
        persistit.setPersistitLogger(new Log4JAdapter(logger));
        persistit.setProperties(UnitTestProperties.getAlternateProperties(true));
        persistit.initialize();
        try {
            test1();
        } finally {
            persistit.close();
        }
    }

}
