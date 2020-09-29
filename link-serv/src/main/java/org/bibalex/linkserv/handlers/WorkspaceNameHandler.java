package org.bibalex.linkserv.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class WorkspaceNameHandler {

    private static final Logger LOGGER = LogManager.getLogger(WorkspaceNameHandler.class);

    public Map<String, String> splitWorkspaceName(String workspaceName) {

        LOGGER.info("Splitting: \"" + workspaceName + "\"");
        Map<String, String> workspaceParameters = new HashMap<String, String>();
        String[] parameters = workspaceName.split(",", 2);

        workspaceParameters.put(PropertiesHandler.getProperty("workspaceTimestamp"), parameters[0]);
        workspaceParameters.put(PropertiesHandler.getProperty("workspaceURL"), parameters[1]);

        LOGGER.info("URL: " + workspaceParameters.get("url"));
        LOGGER.info("Timestamp: " + workspaceParameters.get("timestamp"));

        if (!validateURL(workspaceParameters.get("url")))
            return null;
        return workspaceParameters;
    }

    public boolean validateURL(String url) {
        if (url.matches("(?:https?:\\/\\/)?(?:[^?\\/\\s]+[?\\/])(.*)"))
            return true;
        return false;
    }
}
