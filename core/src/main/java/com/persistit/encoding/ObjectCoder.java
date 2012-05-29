/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

/**
 * <p>
 * Combines the {@link KeyCoder}, {@link KeyRenderer}, {@link ValueCoder} and
 * {@link ValueRenderer} into a single interface that allows Persistit to store
 * and retrieve arbitrary objects - even non-Serializable objects - without
 * byte-code enhancement, without incurring the space or time overhead of Java
 * serialization, or the need to modify the class to perform custom
 * serialization. During initialization, an application typically associates an
 * <code>ObjectCoder</code> with each the <code>Class</code> of each object that
 * will be stored in or fetched from the Persistit database. The
 * <code>ObjectCoder</code> implements all of the logic necessary to encode and
 * decode the state of objects of that class to and from Persistit storage
 * structures. Although Persistit is not designed to provide transparent
 * persistence, the <code>ObjectCoder</code> interface simplifies object
 * persistence code.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface ObjectCoder extends KeyRenderer, ValueRenderer {
}
