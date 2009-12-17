package com.persistit.jmx;

import javax.management.ReflectionException;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;

interface Adapter
{
    boolean isComposite();
    
    boolean isArray();
    
    OpenType getOpenType();
    
    Class getActualClass(); 
    
    Adapter getElementAdapter();
    
    Object wrapValue(Object value)
    throws OpenDataException, ReflectionException;
    
    void toXml(StringBuilder sb);

}
