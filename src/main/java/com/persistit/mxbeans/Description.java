/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.mxbeans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.management.DescriptorKey;

/**
 * 
 * This annotation is intended to work somewhat like the Description annotation
 * specified in JMX 2 (JSR 255) which is currently inactive. The annotation lets
 * you add a description to the Descriptor attribute of an MBeanFeatureInfo.
 * Unfortunately that does not directly affect the result returned by
 * {@link javax.management.MBeanFeatureInfo#getDescription()} but that can be
 * accomplished through the MXBeanWrapper class which marshals the value from
 * the Descriptor to the description attribute.
 * @see <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4853303">bug 4853303</a>
 */
@Target({ ElementType.METHOD, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
public @interface Description {
    String DESCRIPTION = "Description";

    @DescriptorKey(DESCRIPTION)
    String value();
}
