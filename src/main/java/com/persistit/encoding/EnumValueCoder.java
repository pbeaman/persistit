/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
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
 */
package com.persistit.encoding;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

public class EnumValueCoder
implements ValueCoder
{

    /**
     * <p>
     * Encodes the supplied <tt>Object</tt> into the supplied <tt>Value</tt>.
     * This method will be called only if this <tt>ValueCoder</tt> has been
     * registered with the current {@link CoderManager} to encode objects
     * having the class of the supplied object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the 
     * <tt>Value</tt> and its size should be updated to reflect the appended
     * key segment.  Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to 
     * manipulate the byte array directly.  More commonly, the implementation
     * of this method will simply call the appropriate <tt>put</tt> methods to
     * write the interior field values into the <tt>Value</tt> object.
     * </p>
     * @param value     The <tt>Value</tt> to which the interior data of the
     *                  supplied <tt>Object</tt> should be encoded
     * @param object    The object value to encode.  This parameter will never
     *                  be <tt>null</tt> because Persistit encodes nulls with
     *                  a built-in encoding.
     * @param context   An arbitrary object that can optionally be supplied by
     *                  the application to convey an application-specific 
     *                  context for the operation. (See {@link CoderContext}.)
     *                  The default value is <tt>null</tt>.
     */
    public void put(Value value, Object object, CoderContext context)
    throws ConversionException
    {
        String name = ((Enum)object).name();
        value.put(name);
    }
    
    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <tt>Value</tt>, and returns it.
     * This method will be called only if this <tt>ValueCoder</tt> has been
     * registered with the current {@link CoderManager} to encode objects
     * having supplied <tt>Class</tt> value. Persistit will never
     * call this method to decode a value that was <tt>null</tt> when written
     * because null values are handled by built-in encoding logic. 
     * </p>
     * @param value     The <tt>Value</tt> from which interior fields of the
     *                  object are to be retrieved
     * @param clazz     The class of the object to be returned.
     * @param context   An arbitrary object that can optionally be supplied by
     *                  the application to convey an application-specific 
     *                  context for the operation. (See {@link CoderContext}.)
     *                  The default value is <tt>null</tt>.
     * @return          An <tt>Object</tt> having the same class as the suppled
     *                  <tt>clazz</tt> parameter.
     * @throws ConversionException
     */
    public Object get(Value value, Class clazz, CoderContext context)
    throws ConversionException
    {
        String name = value.getString();
        while (clazz.getSuperclass() != Enum.class)
        {
            clazz = clazz.getSuperclass();
        }
        return Enum.valueOf(clazz, name);
    }
}
