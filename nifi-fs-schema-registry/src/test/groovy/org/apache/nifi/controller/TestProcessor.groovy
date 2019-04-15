package org.apache.nifi.controller

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException

class TestProcessor extends AbstractProcessor {
    static final PropertyDescriptor REGISTRY = new PropertyDescriptor.Builder()
        .name("registry")
        .identifiesControllerService(FileSystemSchemaRegistry.class)
        .addValidator(Validator.VALID)
        .build()

    static final List<PropertyDescriptor> DESC = [ REGISTRY ]
    @Override
    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        DESC
    }

    @Override
    void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
