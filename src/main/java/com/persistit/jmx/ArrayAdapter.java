package com.persistit.jmx;

import javax.management.ReflectionException;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;


class ArrayAdapter
extends SimpleAdapter
{
    Adapter _elementAdapter;
    
    ArrayAdapter(int dimensions, Adapter element)
    throws OpenDataException
    {
        super(element.getOpenType());
        _elementAdapter = element;
        _openType = new ArrayType(dimensions, element.getOpenType());
    }
    
    public boolean isArray()
    {
        return true;
    }

    public Adapter getElementAdapter()
    {
        return _elementAdapter;
    }
    
    public Object wrapValue(Object value)
    throws OpenDataException, ReflectionException

    {
        if (value == null) return value;
        Object[] valueArray = (Object[])value;
        if (_elementAdapter.isComposite())
        {
            CompositeTypeAdapter compositeAdapter =
                (CompositeTypeAdapter)_elementAdapter;
            
            TabularData tabularData = 
                new TabularDataSupport(compositeAdapter.getTabularType());
            
            for (int index = 0; index < valueArray.length; index++)
            {
                tabularData.put(
                    (CompositeData)compositeAdapter.wrapValue(valueArray[index]));
            }
            return tabularData;
        }
        else
        {
            return valueArray;
        }
    }
    
    public void toXml(StringBuilder sb)
    {
        int p = sb.length();
        _elementAdapter.toXml(sb);
        p = sb.indexOf(" ", p);
        sb.insert(p, " isarray=\"true\"");
    }
    
}
