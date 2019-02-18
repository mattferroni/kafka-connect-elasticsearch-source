/**
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.bottega52.schema;

import it.bottega52.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SchemaConverter {

    public static final Logger logger = LoggerFactory.getLogger(SchemaConverter.class);

    public static Schema buildSchemaForDocument(Map<String, Object> documentAsMap, String indexName) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(Utils.sanitizeName(indexName));
        recursiveSchemaDecorationFromDocument(documentAsMap, schemaBuilder);
        return schemaBuilder.build();
    }

    private static void recursiveSchemaDecorationFromDocument(Map<String, Object> documentAsMap, SchemaBuilder schemaBuilder) {
        // No nested structure, prefix is an empty string
        recursiveSchemaDecorationFromDocument("", documentAsMap, schemaBuilder);
    }

    private static void recursiveSchemaDecorationFromDocument(String container, Map<String, Object> documentAsMap, SchemaBuilder schemaBuilder) {
        documentAsMap.keySet().forEach(key -> {
                Object value = documentAsMap.get(key);
                if (value instanceof String) {
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_STRING_SCHEMA);
                } else if (value instanceof Integer) {
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_INT32_SCHEMA);
                } else if (value instanceof Long) {
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_INT64_SCHEMA);
                } else if (value instanceof Float) {
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_FLOAT32_SCHEMA);
                } else if (value instanceof Double) {
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_FLOAT64_SCHEMA);
                } else if (value instanceof List) {
                    if (!((List) value).isEmpty()) {
                        // Assuming that every item of the list has the same schema
                        Object firstItem = ((List) value).get(0);
                        if (firstItem instanceof String) {
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder
                                            .array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .build()
                            ).build();
                        } else  if (firstItem instanceof Integer) {
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                                            .optional()
                                            .build()
                            ).build();
                        } else if (firstItem instanceof Long) {
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                                            .optional()
                                            .build()
                            ).build();
                        } else if (firstItem instanceof Float) {
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA)
                                            .optional()
                                            .build()
                            ).build();
                        } if (firstItem instanceof Double) {
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                                            .optional()
                                            .build()
                            ).build();
                        } else if (firstItem instanceof List) {
                            // Nested schema: recursive strategy
                            SchemaBuilder nestedSchema = SchemaBuilder.struct()
                                    .name(Utils.sanitizeName(container, key))
                                    .optional();
                            recursiveSchemaDecorationFromDocument(
                                    Utils.sanitizeName(container, key).concat("."),
                                    (Map<String, Object>) firstItem,
                                    nestedSchema);
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(nestedSchema.build())
                            );
                        } else if (firstItem instanceof Map) {
                            // Nested schema: recursive strategy
                            SchemaBuilder nestedSchema = SchemaBuilder.struct()
                                    .name(Utils.sanitizeName(container, key))
                                    .optional();
                            recursiveSchemaDecorationFromDocument(
                                    Utils.sanitizeName(container, key).concat("."),
                                    (Map<String, Object>) firstItem,
                                    nestedSchema);
                            schemaBuilder.field(
                                    Utils.sanitizeName(key),
                                    SchemaBuilder.array(nestedSchema.build())
                            );
                        } else {
                            logger.error("Error detecting field type for pair: ('{}' -> '{}') - Defaulting to an optional String", Utils.sanitizeName(key), value);
                            schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_STRING_SCHEMA);
                        }

                    }
                } else if (value instanceof Map) {
                    SchemaBuilder nestedSchema = SchemaBuilder.struct()
                            .name(Utils.sanitizeName(container, key))
                            .optional();
                    recursiveSchemaDecorationFromDocument(
                            Utils.sanitizeName(container, key).concat("."),
                            (Map<String, Object>) value,
                            nestedSchema
                    );
                    schemaBuilder.field(Utils.sanitizeName(key), nestedSchema.build());
                } else {
                    logger.error("Error detecting field type for pair: ('{}' -> '{}') - Defaulting to an optional String", Utils.sanitizeName(key), value);
                    schemaBuilder.field(Utils.sanitizeName(key), Schema.OPTIONAL_STRING_SCHEMA);
                }
            }
        );
    }

}
