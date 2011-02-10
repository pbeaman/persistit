/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
 * <tt>ObjectCoder</tt> with each the <tt>Class</tt> of each object that will be
 * stored in or fetched from the Persistit database. The <tt>ObjectCoder</tt>
 * implements all of the logic necessary to encode and decode the state of
 * objects of that class to and from Persistit storage structures. Although
 * Persistit is not designed to provide transparent persistence, the
 * <tt>ObjectCoder</tt> interface simplifies object persistence code.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface ObjectCoder extends KeyRenderer, ValueRenderer {
}
