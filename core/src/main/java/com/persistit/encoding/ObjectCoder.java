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
 * Created on Oct 18, 2004
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
