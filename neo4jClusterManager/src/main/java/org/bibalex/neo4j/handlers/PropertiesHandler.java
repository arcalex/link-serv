package org.bibalex.neo4j.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesHandler {
    static Properties clusterProp = new Properties();
    static Properties configProp = new Properties();

    public static void initializeProperties() {
        try {
            InputStream clusterInput = PropertiesHandler.class.getClassLoader().getResourceAsStream("../conf/cluster.properties");
            InputStream configInput = PropertiesHandler.class.getClassLoader().getResourceAsStream("config.properties");
            clusterProp.load(clusterInput);
            configProp.load(configInput);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getClusterProperty(String key) {
        return clusterProp.getProperty(key);
    }

    public static String getConfigProperty(String key) {
        return configProp.getProperty(key);
    }
}
