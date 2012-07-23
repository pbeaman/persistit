/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.encoding;

import com.persistit.Key;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * Interface to support encoding of Objects in {@link Key}s. This interface adds
 * a method to compute the hash code of an encoded key segment. It is useful for
 * {@link KeyCoder}s that remove information from the original object and
 * therefore are unable to simply restore the originl object to compute its hash
 * code.
 * 
 * 
 * @version 1.0
 */
public interface KeyHasher extends KeyCoder {

    /**
     * <p>
     * Compute the hash code of a key segment. This method is invoked when
     * {@link Key#decodeHashCode(Object)} attempts to compute the hash code of a
     * key segment that was previously append by the {@link #appendKeySegment}
     * method.
     * </p>
     * <p>
     * This method will be called only if this <code>KeyCoder</code> is
     * registered with the current {@link CoderManager} to encode and decode
     * objects of the class that is actually represented in the <code>Key</code>
     * . The class with which the key was encoded is provided as an argument.
     * This permits one <code>KeyCoder</code> to handle multiple classes because
     * the implementation can dispatch to code appropriate for the particular
     * supplied class. The implementation should construct and return an Object
     * having the same class as the supplied class.
     * </p>
     * <p>
     * When this method is called the value {@link Key#getIndex()} will be the
     * offset within the key of the first encoded byte. The key segment is
     * zero-byte terminated.
     * </p>
     * 
     * @param key
     *            The {@link Key} from which data should be decoded
     * @param clazz
     *            The expected <code>Class</code> of the returned object
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * @return the hash code
     * @throws ConversionException
     */

    public int decodeHashCode(final Key key, final Class<?> clazz, final CoderContext context);
}
