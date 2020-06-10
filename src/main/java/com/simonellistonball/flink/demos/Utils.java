package com.simonellistonball.flink.demos;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    public static final String KAFKA_PREFIX = "kafka.";
    public static final String K_PROPERTIES_FILE = "properties.file";

    private static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static Properties readKafkaProperties(ParameterTool params) {
        Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

        LOG.info("### Kafka parameters:");
        for (String key : properties.stringPropertyNames()) {
            LOG.info("Kafka param: {}={}", key, properties.get(key));
        }
        return properties;
    }

    public static ParameterTool parseArgs(String[] args) throws IOException {

        // Processing job properties
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.has(K_PROPERTIES_FILE)) {
            params = ParameterTool.fromPropertiesFile(params.getRequired(K_PROPERTIES_FILE)).mergeWith(params);
        }

        LOG.info("### Job parameters:");
        for (String key : params.getProperties().stringPropertyNames()) {
            LOG.info("Job Param: {}={}", key, params.get(key));
        }
        return params;
    }
}