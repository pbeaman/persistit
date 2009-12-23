package com.persistit.unit;

import java.util.Properties;

import junit.framework.TestCase;

import com.persistit.Persistit;

public abstract class PersistitUnitTestCase extends TestCase {

    protected Persistit _persistit = new Persistit();
    
    protected Properties getProperties() {
        return UnitTestProperties.getProperties();
    }
    
    @Override
    public void setUp() throws Exception {
        _persistit.initialize(getProperties());
    }
    
    @Override
    public void tearDown() throws Exception {
        _persistit.close();
    }
    
    public abstract void runAllTests() throws Exception;
    
    public void setPersistit(final Persistit persistit) {
        _persistit = persistit;
    }
    
    protected void initAndRunTest() throws Exception {
        _persistit.initialize(getProperties());
        try {
        	runAllTests();
        } catch (final Throwable t) {
            t.printStackTrace();
        } finally {
            _persistit.close();
        }
    }
    
}
