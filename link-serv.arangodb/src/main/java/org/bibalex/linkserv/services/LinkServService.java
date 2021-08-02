package org.bibalex.linkserv.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bibalex.linkserv.handlers.*;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static java.util.Objects.isNull;
import static org.bibalex.linkserv.handlers.PropertiesHandler.getProperty;

@Service
public class LinkServService {



    private static final Logger LOGGER = LogManager.getLogger(LinkServService.class);

    public LinkServService(){
    }

    public String updateGraph(String jsonGraph, String workspaceName) {
        /** TODO: clean up **/
        WorkspaceNameHandler workspaceNameHandler = new WorkspaceNameHandler();
        ArangoDBHandler arangoDBHandler = new ArangoDBHandler();
        JSONHandler jsonHandler  = new JSONHandler();

        boolean done, multipleIdentifiers = false;
        String identifier = "";
        String timestamp = "";
        LOGGER.info("Update Graph");

        if (workspaceName.equals("*") || workspaceName.isEmpty())
            multipleIdentifiers = true;
        else {
            Map<String, String> workspaceNameParameters = workspaceNameHandler.splitWorkspaceName(workspaceName);
            if (workspaceNameParameters == null)
                return getProperty("badRequestResponseStatus");
            identifier = workspaceNameParameters.get(getProperty("workspaceIdentifier"));
            timestamp = workspaceNameParameters.get(getProperty("workspaceTimestamp"));
        }
        jsonHandler.initialize(multipleIdentifiers);
        LOGGER.info("JSONHandler: " + jsonHandler.hashCode());
        if (!jsonGraph.startsWith("{") && jsonGraph.contains("&")) {
            jsonGraph = jsonGraph.split("&")[1];
        }
        // Gephi uses \r as delimiter between lines
        String[] jsonLines = jsonGraph.replace("\n", "").split("\\r");
        LOGGER.info("Parsing incoming request body");
        Long startTime = new Date().getTime();

        for (String jsonLine : jsonLines) {
            if (!jsonLine.equals("")) {
                LOGGER.info(jsonLine);
                done = jsonHandler.addNodesAndEdgesFromJSONLine(jsonLine, identifier, timestamp);
                if (!done)
                    return getProperty("badRequestResponseStatus");
            }
        }

        done = arangoDBHandler.addNodesAndRelationships(jsonHandler.getGraphNodes(), jsonHandler.getGraphEdges());
        if (done) {
            Float graphUpdateTime =  Float.valueOf(new Date().getTime() - startTime) / 1000;
            LOGGER.info("Graph updated in: " + graphUpdateTime + " seconds");

            return (String.valueOf(new JSONObject().put("Update Time", (graphUpdateTime + " s"))));
        } else{
            LOGGER.info("Could not Update Graph");
        }
        LOGGER.debug("JSON Data: " + jsonGraph);
        return "";
    }

    public String getGraph(String identifier, String timestamp, Integer depth, Integer timeElasticity) {
        JSONHandler jsonHandler  = new JSONHandler();

        String timeRangeDelimiter = PropertiesHandler.getProperty("timeRangeDelimiter");
        ArrayList<String> graphArray;
        String startTimestamp="";
        String endTimestamp="";
        jsonHandler.initialize(false);

        if (timestamp.contains(timeRangeDelimiter)) {
            String[] timestamps = timestamp.split(timeRangeDelimiter, 2);
            startTimestamp = timestamps[0];
            endTimestamp = timestamps[1];
            graphArray = jsonHandler.getGraph(identifier, startTimestamp, endTimestamp, depth);
        } else if(!isNull(timeElasticity)){

            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmSS");
                Date d = df.parse(timestamp);
                Calendar cal = Calendar.getInstance();
                cal.setTime(d);
                cal.add(Calendar.MINUTE, -timeElasticity);
                startTimestamp = df.format(cal.getTime());
                cal.setTime(d);
                cal.add(Calendar.MINUTE, timeElasticity);
                endTimestamp = df.format(cal.getTime());

            } catch (ParseException e) {
                e.printStackTrace();
            }

            graphArray = jsonHandler.getGraph(identifier, startTimestamp, endTimestamp, depth);
        }
        else {
            graphArray = jsonHandler.getGraph(identifier, timestamp, depth);
        }
        return formulateResponse(graphArray, "\r\n");
    }

    public String getVersions(String identifier, String dateTime) {
        JSONHandler jsonHandler  = new JSONHandler();
        jsonHandler.initialize(false);
        ArrayList<String> responseStringArray = jsonHandler.getVersions(identifier, dateTime);
        return formulateResponse(responseStringArray, ",");
    }

    public String getLatestVersion(String identifier) {
        JSONHandler jsonHandler  = new JSONHandler();
        jsonHandler.initialize(false);
        ArrayList<String> reponseStringArray = jsonHandler.getLatestVersion(identifier);
        return formulateResponse(reponseStringArray, "\r\n");
    }

    public String getVersionCountsYearly(String identifier) {
        JSONHandler jsonHandler  = new JSONHandler();
        jsonHandler.initialize(false);
        String response = jsonHandler.getVersionCountsYearly(identifier);
        LOGGER.info("Response: " + response);
        return response;
    }

    public String getVersionCountsMonthly(String identifier, int year) {
        JSONHandler jsonHandler  = new JSONHandler();
        jsonHandler.initialize(false);
        String response = jsonHandler.getVersionCountsMonthly(identifier, year);
        LOGGER.info("Response: " + response);
        return response;
    }

    public String getVersionsCountDaily(String identifier, int year, int month) {
        JSONHandler jsonHandler  = new JSONHandler();
        jsonHandler.initialize(false);
        String response = jsonHandler.getVersionCountsDaily(identifier, year, month);
        LOGGER.info("Reponse: " + response);
        return response;
    }

    private String formulateResponse(ArrayList<String> stringResponse, String delimiter) {
        if (stringResponse.isEmpty()) {
            LOGGER.info("No results found");
            return "";
        }

        String response = stringResponse.remove(0);
        for (String responseObject : stringResponse) {
            response += delimiter + responseObject;
        }
        if (response.isEmpty()) {
            LOGGER.info("No match found");
        } else {
            LOGGER.debug("Graph returned: " + response);
            LOGGER.info("Match has been successfully returned");
        }
        return response;
    }
}
