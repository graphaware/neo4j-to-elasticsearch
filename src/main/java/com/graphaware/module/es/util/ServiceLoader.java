package com.graphaware.module.es.util;

import com.graphaware.module.es.mapping.Mapping;

public class ServiceLoader {

    public static Mapping loadMapping(String mappingClazz) {
        Mapping mapping;
        try {
            Class<?> clazz = Class
                    .forName(mappingClazz);
            Mapping definition = (Mapping) clazz.newInstance();
            if (definition instanceof Mapping) {
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
