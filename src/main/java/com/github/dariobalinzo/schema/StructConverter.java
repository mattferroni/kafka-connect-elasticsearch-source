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

package com.github.dariobalinzo.schema;

import com.github.dariobalinzo.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructConverter {

    public static final Logger logger = LoggerFactory.getLogger(StructConverter.class);

    public static Struct buildStructForDocument(Map<String, Object> documentAsMap, Schema schema) {
        final Struct struct = new Struct(schema);
        recursiveStructDecorationFromDocument(documentAsMap, struct,schema);
        return struct;
    }

    private static void recursiveStructDecorationFromDocument(Map<String, Object> documentAsMap, Struct struct, Schema schema) {
        // No nested structure, prefix is an empty string
        recursiveStructDecorationFromDocument("",documentAsMap, struct,schema);
    }

    private static void recursiveStructDecorationFromDocument(String container, Map<String, Object> documentAsMap, Struct struct, Schema schema) {
        documentAsMap.keySet().forEach(key -> {
                Object value = documentAsMap.get(key);
                if (value instanceof String || value instanceof Integer || value instanceof Long
                        || value instanceof Double || value instanceof Float) {
                    struct.put(Utils.sanitizeName(key), value);
                } else if (value instanceof List) {
                    if (!((List) value).isEmpty()) {
                        // Assuming that every item of the list has the same schema
                        Object item = ((List) value).get(0);
                        struct.put(Utils.sanitizeName(key),new ArrayList<>());

                        if (item instanceof String || item instanceof Integer || item instanceof Long
                            || item instanceof Double || item instanceof Float) {
                            struct.getArray(Utils.sanitizeName(key)).addAll((List) value);

                        // TODO: list
                        // TODO: review this
                        } else if (item instanceof Map) {
                            List<Struct> array = (List<Struct>) ((List) value)
                                    .stream()
                                    .map(innerKey -> {
                                        Struct nestedStruct = new Struct(schema.field(Utils.sanitizeName(container,key)).schema().valueSchema());
                                        recursiveStructDecorationFromDocument(
                                                Utils.sanitizeName(container,key)+".",
                                                (Map<String, Object>) innerKey, nestedStruct, schema.field(Utils.sanitizeName(key)).schema().valueSchema());
                                        return nestedStruct;
                                    }).collect(Collectors.toCollection(ArrayList::new));
                            struct.put(Utils.sanitizeName(key),array);
                        } else {
                            logger.error("Defaulting to an optional String for pair: ('{}' -> '{}')", Utils.sanitizeName(key), value);
                            struct.put(Utils.sanitizeName(key), value);
                        }

                    }
                } else if (value instanceof Map) {
                    Struct nestedStruct = new Struct(schema.field(Utils.sanitizeName(key)).schema());
                    recursiveStructDecorationFromDocument(
                            Utils.sanitizeName(container,key).concat("."),
                            (Map<String, Object>) value,
                            nestedStruct,
                            schema.field(Utils.sanitizeName(key)).schema()
                    );
                    struct.put(Utils.sanitizeName(key),nestedStruct);
                } else {
                    logger.error("Defaulting to an optional String for pair: ('{}' -> '{}')", Utils.sanitizeName(key), value);
                    struct.put(Utils.sanitizeName(key), value);
                }
            }
        );
    }

}
