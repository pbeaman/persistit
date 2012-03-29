/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.encoding;

import com.persistit.Key;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * An extension of {@link KeyCoder} that adds the {@link #displayKeySegment}
 * method. This method populates a supplied <code>StringBuilder</code> with a
 * displayable, human-readable representation of the Object value that was
 * formerly written to a {@link Key} by this <code>KeyDisplayer</code>.
 * </p>
 * <p>
 * The {@link Key#toString toString} method and
 * {@link Key#decodeDisplayable(boolean, StringBuilder) decodeDisplayable}
 * methods of <code>Value</code> preferentially use the registered
 * <code>KeyDisplayer</code>, if present, to generate a String representation of
 * an object value encoded in a <code>Key</code>.
 * </p>
 * 
 * @version 1.0
 */
public interface KeyDisplayer extends KeyCoder {
    /**
     * <p>
     * Populates the state of the supplied target <code>Object</code> by
     * decoding the next key segment of the supplied <code>Key</code>. This
     * method will be called only if this <code>KeyRenderer</code> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the supplied <code>Class</code> value. In addition, Persistit will never
     * call this method to decode a value that was <code>null</code> when
     * written because null values are handled by built-in encoding logic.
     * </p>
     * 
     * @param key
     *            The <code>Key</code> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            An object into which the key segment is to be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * 
     * @throws ConversionException
     */
    public void displayKeySegment(Key key, StringBuilder target, Class<?> clazz, CoderContext context)
            throws ConversionException;

}
