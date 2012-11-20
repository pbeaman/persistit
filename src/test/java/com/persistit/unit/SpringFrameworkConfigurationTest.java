/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.persistit.Persistit;

/**
 * Test simple scenario to ensure a second Persistit instance referring to the
 * same volumes can't start.
 * 
 * @author peter
 * 
 */
public class SpringFrameworkConfigurationTest {

    @Test
    public void configurePersistitFromSpring() throws Exception {
        ApplicationContext context = 
                new ClassPathXmlApplicationContext("com/persistit/unit/SpringFrameworkConfiguraitonTest.xml");

        final Persistit persistit = (Persistit)context.getBean("persistit");
        assertTrue("Persistit should be initialized", persistit.isInitialized());
    }

}
