/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Apr 6, 2004
 */
package com.persistit.unit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Log4JAdapter;

public class Log4JLoggerTest extends PersistitTestCase {
    
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new Log4JLoggerTest().runTest();
    }

    public void runTest() throws Exception {
        final Logger logger =
            Logger.getLogger("com.persistit.unit.JDK14LoggerTest");
        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();
        Persistit persistit = new Persistit(); 
        persistit.setPersistitLogger(new Log4JAdapter(logger));
        persistit.initialize(UnitTestProperties.getAlternateProperties());
        try {
            test1();
        } finally {
            persistit.close();
        }        
    }

}
