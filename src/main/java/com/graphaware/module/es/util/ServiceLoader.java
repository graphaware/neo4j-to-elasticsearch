package com.graphaware.module.es.util;

import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.es.mapping.MappingDefinition;

public class ServiceLoader {

    public static Mapping loadMapping(String mappingClazz) {
        Mapping mapping;
        try {
            Class<? extends Mapping> clazz = (Class<? extends Mapping>) Class
                    .forName(mappingClazz);
            Mapping definition = clazz.newInstance();
            if (definition instanceof Mapping && definition instanceof MappingDefinition) {
                mapping = (Mapping) definition;
            } else {
                throw new IllegalArgumentException(mappingClazz + " is not a Mapping class");
            }

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | IllegalArgumentException e) {
            throw new RuntimeException("Could not instantiate mapping class " + mappingClazz + " : " + e.getMessage(), e);
        }

        return mapping;
    }
}
