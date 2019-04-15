package org.apache.nifi.controller

import org.apache.nifi.schemaregistry.services.SchemaRegistry
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.StandardSchemaIdentifier
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
        runner.addControllerService("registry", registry)
        runner.setProperty(registry, "message", "src/test/resources/message.avsc")
        runner.enableControllerService(registry)
        runner.setProperty(TestProcessor.REGISTRY, "registry")
        runner.assertValid()
    }

    @Test
    void test() {
        RecordSchema messageSchema = registry.retrieveSchema(new StandardSchemaIdentifier.Builder().name("message").build())
        assert messageSchema
    }
}
