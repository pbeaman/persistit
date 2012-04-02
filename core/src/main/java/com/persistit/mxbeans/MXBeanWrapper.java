package com.persistit.mxbeans;

import javax.management.Descriptor;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class MXBeanWrapper<T> extends StandardMBean {

    public MXBeanWrapper(T mbeanInterface, Class<T> implementation) throws NotCompliantMBeanException {
        super(mbeanInterface, implementation, true);
    }
    
    public String getDescription(final MBeanAttributeInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String)descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }
    
    public String getDescription(MBeanOperationInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String)descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    public String getDescription(MBeanParameterInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String)descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }
}
