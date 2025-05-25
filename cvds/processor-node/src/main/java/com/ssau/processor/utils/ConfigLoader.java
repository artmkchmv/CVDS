package com.ssau.processor.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConfigLoader {

    private static Properties props = null;

    private ConfigLoader() {}

    public static synchronized Properties loadDefault() throws IOException {
        return load("processor.properties");
    }

    public static synchronized Properties load(String fileName) throws IOException {
        if (props == null) {
            props = new Properties();
            try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
                if (input == null) {
                    throw new IOException("Configuration file '" + fileName + "' not found in classpath");
                }
                props.load(input);
                log.info("Configuration loaded from {}", fileName);
            } catch (IOException ex) {
                log.error("Error loading configuration file '{}': {}", fileName, ex.getMessage());
                throw ex;
            }
        }
        return props;
    }
}
