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

import java.io.Serializable;

import com.persistit.Key;
import com.persistit.Value;

/**
 * A marker interface for an application-specific object that may be passed to a
 * {@link KeyCoder} or {@link ValueCoder}. This object may be used to pass
 * application context information from the application to the coder instance.
 * The following methods accept a <code>CoderContext</code>:
 * <ul>
 * <li>{@link Key#append(Object, CoderContext)}</li>
 * <li>{@link Key#decode(Object, CoderContext)}</li>
 * <li>{@link Key#decodeString(CoderContext)}</li>
 * <li>{@link Key#decodeString(StringBuilder, CoderContext)}</li>
 * <li>{@link Key#decodeDisplayable(boolean, StringBuilder, CoderContext)}</li>
 * <li>{@link Value#put(Object, CoderContext)}</li>
 * <li>{@link Value#get(Object, CoderContext)}</li>
 * </ul>
 * This interface has no behavior; it simply marks classes that are intended for
 * this purpose to enhance type safety. Note that <code>CoderContext</code>
 * extends <code>java.io.Serializable</code>, meaning that any object you
 * provide as a CoderContext must behave correctly when serialized and
 * deserialized.
 * 
 * @version 1.0
 */
public interface CoderContext extends Serializable {

}
