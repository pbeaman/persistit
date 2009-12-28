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

import java.io.Serializable;

import com.persistit.Exchange;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;


public class ValueTest5
extends PersistitUnitTestCase
{
    /**
     * Tests JSA 1.1 default serialization. Requires the
     * enableCompatibleConstructors to be true.
     */
	
    Exchange _exchange;
    
    
    public enum Job
    {
        COOK,
        BOTTLEWASHER,
        PROGRAMMER,
        JANITOR,
        SALEMAN,
    };
    
    public static class S
    implements Serializable
    {
        private final static long serialVersionUID = 1;
        Job _myJob;
        Job _yourJob;
        
        S(Job m, Job y)
        {
            _myJob = m;
            _yourJob = y;
        }
        
        public String toString()
        {
            return _myJob + ":" + _yourJob;
        }

    }
    
    @Override
    public void setUp() throws Exception {
    	super.setUp();
    	_exchange = _persistit.getExchange("persistit", getClass().getSimpleName(), true);
    }
    
    @Override
    public void tearDown() throws Exception {
    	_persistit.releaseExchange(_exchange);
    	super.tearDown();
    }

    public void test1()
    throws PersistitException
    {
        System.out.print("test1 ");
        S s = new S(Job.COOK, Job.BOTTLEWASHER);
        _exchange.getValue().put(s);
        _exchange.clear().append("test1").store();
        Object x = _exchange.getValue().get();
        assertEquals("COOK:BOTTLEWASHER", x.toString());

        System.out.println("- done");
    }
    
    
    
    public static void main(String[] args)
    throws Exception
    {
        new ValueTest5().initAndRunTest();
    }
    
    public void runAllTests()
    throws Exception
    {
        _exchange = _persistit.getExchange("persistit", "ValueTest5", true);
        CoderManager cm = _persistit.getCoderManager();
        
        test1();
        
        _persistit.setCoderManager(cm);
    }

}
