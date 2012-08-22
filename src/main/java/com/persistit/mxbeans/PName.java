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
 * Annotation used to declare the externally visible name of a parameter on an
 * MXBean operation. The {@link MXBeanWrapper} class marshals the value added
 * here to the descriptor to the appropriate MBeanParameterInfo attribute.
 */
@Target({ ElementType.METHOD, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
public @interface PName {
    String PNAME = "PName";

    @DescriptorKey(PNAME)
    String value();
}
