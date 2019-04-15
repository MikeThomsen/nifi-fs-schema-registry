package org.apache.nifi.controller;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileSystemSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    private Map<String, RecordSchema> schemas;

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent() && schemas.containsKey(schemaIdentifier.getName().get())) {
            return schemas.get(schemaIdentifier.getName().get());
        }

        return null;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Iterate over dynamic properties, validating the schemas, and adding results
        validationContext.getProperties().entrySet().stream().filter(entry -> entry.getKey().isDynamic()).forEach(entry -> {
            String subject = entry.getKey().getDisplayName();
            String input = entry.getValue();

            try {
                FileInputStream fis = new FileInputStream(input);
                String raw = IOUtils.toString(fis, "UTF-8");
                fis.close();

                final Schema avroSchema = new Schema.Parser().parse(raw);
                AvroTypeUtil.createSchema(avroSchema, raw, SchemaIdentifier.EMPTY);
            } catch (final Exception e) {
                results.add(new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(String.format("Not a valid Avro Schema at path %s; error: %s ", input, e.getMessage()))
                        .build());
            }
        });
        return results;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        final Map<String, RecordSchema> _temp = new HashMap<>();
        context.getProperties().entrySet().stream().forEach(entry -> {
            String path = context.getProperty(entry.getKey()).getValue();
            try {
                FileInputStream fis = new FileInputStream(path);
                String raw = IOUtils.toString(fis, "UTF-8");
                fis.close();

                final Schema avroSchema = new Schema.Parser().parse(raw);
                RecordSchema schema = AvroTypeUtil.createSchema(avroSchema, raw, SchemaIdentifier.EMPTY);
                _temp.put(entry.getValue(), schema);
            } catch (Exception ex) {
                throw new ProcessException(ex);
            }
        });

        schemas = _temp;
    }
}
