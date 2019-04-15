package org.apache.nifi.controller

import org.apache.nifi.schemaregistry.services.SchemaRegistry
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

class FileSystemSchemaRegistryTest {
    TestRunner runner
    SchemaRegistry registry
    @Before
    void setup() {
        runner = TestRunners.newTestRunner(TestProcessor.class)
        registry = new FileSystemSchemaRegistry()
    }

    @Test
    void test() {

    }
}
