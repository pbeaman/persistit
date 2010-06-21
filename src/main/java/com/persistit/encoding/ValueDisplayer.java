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
 * Created on Jul 1, 2004
 */
package com.persistit.encoding;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * An extension of {@link ValueCoder} that adds the {@link #display} method.
 * This method populates a supplied <tt>StringBuffer</tt> with a displayable,
 * human-readable representation of the Object value that was formerly written
 * to a {@link Value} by this <tt>ValueDisplayer</tt>.
 * </p>
 * <p>
 * The {@link Value#toString toString} method and
 * {@link Value#decodeDisplayable(boolean, StringBuffer) decodeDisplayable}
 * methods of <tt>Value</tt> preferentially use the registered
 * <tt>ValueDisplayer</tt>, if present, to generate a String representation of
 * an object value encoded in a <tt>Value</tt>.
 * </p>
 * 
 * @version 1.0
 */
public interface ValueDisplayer extends ValueCoder {
    /**
     * <p>
     * Writes a String representation of the value into a supplied
     * <tt>StringBuffer</tt>. This is used in utility programs to display stored
     * content without actually deserialized Objects represented by the value.
     * </p>
     * <p>
     * This method will be called only if this <tt>ValueDisplayer</tt> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the supplied <tt>Class</tt> value. Persistit will never call this method
     * to decode a value that was <tt>null</tt> when written because null values
     * are handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            The <tt>StringBuffer</tt> into which the decoded value is to
     *            be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * 
     * @throws ConversionException
     */
    public void display(Value value, StringBuffer target, Class clazz,
            CoderContext context) throws ConversionException;
}
