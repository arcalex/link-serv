package linkserv;

import models.HistogramEntry;
import models.OutlinkNode;
import models.RootNode;
import org.neo4j.driver.Result;
import org.neo4j.procedure.*;
import constants.Constants;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class ReadProcedures extends Procedures{

    @Procedure(value = "linkserv.getRootNode", mode = Mode.READ)
    public Stream<RootNode> getRootNode(@Name("url") String url,
                                        @Name("timestamp") String timestamp) {

        String[] queryFragments = new String[]{"WHERE v.", Constants.versionProperty, " = \\\"", timestamp, "\\\""};
        return buildQueryandExecuteGetNodes(url, queryFragments, false).stream();
    }

    @Procedure(value = "linkserv.getRootNodes", mode = Mode.READ)
    public Stream<RootNode> getRootNodes(@Name("url") String url,
                                         @Name("startTimestamp") String startTimestamp,
                                         @Name("endTimestamp") String endTimestamp) {

        // Assume we're handling only ISO 8601
        startTimestamp = startTimestamp.isEmpty() ? "00000101000000" : startTimestamp;
        endTimestamp = endTimestamp.isEmpty() ? getCurrentTime() : endTimestamp;

        String[] queryFragments = new String[]{"WHERE v.", Constants.versionProperty,
                " >= \\\"", startTimestamp, "\\\" AND v.", Constants.versionProperty,
                " <= \\\"", endTimestamp, "\\\""};
        return buildQueryandExecuteGetNodes(url, queryFragments, false).stream();
    }

    @Procedure(value = "linkserv.getVersions", mode = Mode.READ)
    public Stream<RootNode> getVersions(@Name("url") String url,
                                        @Name("date") String date) {

        String[] queryFragments = new String[]{"WHERE v.", Constants.versionProperty, " STARTS WITH \\\"", date, "\\\""};
        return buildQueryandExecuteGetNodes(url, queryFragments, false).stream();
    }

    @Procedure(value = "linkserv.getLatestVersion", mode = Mode.READ)
    public Stream<RootNode> getLatestVersion(@Name("url") String url) {
        String[] queryFragments = new String[]{""};
        return buildQueryandExecuteGetNodes(url, queryFragments, true).stream();
    }

    @Procedure(value = "linkserv.getOutlinkNodes", mode = Mode.READ)
    public Stream<OutlinkNode> getOutlinkNodes(@Name("nodeName") String nodeName, @Name("timestamp") String nodeVersion) {
        ArrayList<OutlinkNode> outlinkNodes = new ArrayList<>();
        String[] queryFragments = new String[]{"CALL cluster.executeQuery(\"MATCH (parent1:", Constants.parentNodeLabel, " {", Constants.nameProperty, ": \\\"", nodeName,
                "\\\"})-[:", Constants.versionRelationshipType, "]->(version1:", Constants.versionNodeLabel, " {", Constants.versionProperty, ": \\\"",
                nodeVersion, "\\\"})-[r:", Constants.linkRelationshipType, "]->(parent2) OPTIONAL MATCH (parent2)-[:",
                Constants.versionRelationshipType, "]->(version2:", Constants.versionNodeLabel, " {",
                Constants.versionProperty, ":\\\"", nodeVersion, "\\\"}) RETURN ID(parent1), ID(parent2), parent2.", Constants.nameProperty,
                ", ID(version1), ID(r), ID(version2), version2.", Constants.versionProperty, ";\",{});"};

        StringBuilder queryBuilder = new StringBuilder("");
        String query;

        for (String fragment : queryFragments) {
            queryBuilder.append(fragment);
        }
        query = queryBuilder.toString();

        Result result = runNeo4jQuery(query);
        while (result.hasNext()) {
            outlinkNodes.add(new OutlinkNode(result.next().get("results").asMap()));
        }

        return outlinkNodes.stream();
    }

    @Procedure(value = "linkserv.getVersionCountYearly", mode = Mode.READ)
    public Stream<HistogramEntry> getVersionCountYearly(@Name("nodeName") String nodeName) {

        String[] queryFragments = new String[]{"CALL cluster.executeQuery(\"MATCH (parent:", Constants.parentNodeLabel, "{", Constants.nameProperty,
                ":\\\"", nodeName, "\\\"", "})-[:", Constants.versionRelationshipType, "]->(v:", Constants.versionNodeLabel,
                ") RETURN SUBSTRING(v.", Constants.versionProperty, ",0 ,4) AS key, COUNT(v) AS count;\",{});"};

        return getHistogramEntries(queryFragments);
    }

    @Procedure(value = "linkserv.getVersionCountMonthly", mode = Mode.READ)
    public Stream<HistogramEntry> getVersionCountMonthly(@Name("nodeName") String nodeName, @Name("year") Number year) {

        String[] queryFragments = new String[]{"CALL cluster.executeQuery(\"MATCH (parent:", Constants.parentNodeLabel, "{", Constants.nameProperty,
                ":\\\"", nodeName, "\\\"", "})-[:", Constants.versionRelationshipType, "]->(v:", Constants.versionNodeLabel,
                ") WHERE SUBSTRING(v.", Constants.versionProperty, ",0 ,4)=", "\\\"", String.valueOf(year), "\\\"",
                " RETURN SUBSTRING(v.", Constants.versionProperty, ",4 ,2) AS key, COUNT(v) AS count;\",{});"};

        return getHistogramEntries(queryFragments);
    }

    @Procedure(value = "linkserv.getVersionCountDaily", mode = Mode.READ)
    public Stream<HistogramEntry> getVersionCountDaily(@Name("nodeName") String nodeName,
                                                       @Name("year") Number year, @Name("month") Number month) {

        String monthStr = month.toString();
        monthStr = monthStr.length()==1 ? "0"+monthStr : monthStr;
        String[] queryFragments = new String[]{"CALL cluster.executeQuery(\"MATCH (parent:", Constants.parentNodeLabel, "{", Constants.nameProperty,
                ":\\\"", nodeName, "\\\"", "})-[:", Constants.versionRelationshipType, "]->(v:", Constants.versionNodeLabel,
                ") WHERE SUBSTRING(v.", Constants.versionProperty, ",0, 4)=", "\\\"", String.valueOf(year), "\\\"",
                " AND SUBSTRING(v.", Constants.versionProperty, ",4 ,2)=", "\\\"", monthStr, "\\\"",
                " RETURN SUBSTRING(v.", Constants.versionProperty, ",6 ,2) AS key, COUNT(v) AS count;\",{});"};

        return getHistogramEntries(queryFragments);
    }

    private ArrayList<RootNode> buildQueryandExecuteGetNodes(String url, String[] queryFragments, boolean isGetLatestVersion) {
        ArrayList<RootNode> rootNodes = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("");
        String[] clusterFragments = {"CALL cluster.executeQuery(\""};
        String[] matchFragments = {"MATCH (n:", Constants.parentNodeLabel, "{", Constants.nameProperty,
                ":\\\"", url, "\\\"})-[:", Constants.versionRelationshipType, "]->(v:",
                Constants.versionNodeLabel, ")\n"};
        String[] returnFragments = {"\n RETURN n.", Constants.nameProperty, ", v.",
                Constants.versionProperty, ", ID(n)"};
        String[] returnLatestVersionFragments = isGetLatestVersion ?
                new String[]{" ORDER BY v.", Constants.versionProperty, " DESC LIMIT 1;\",{});"} : new String[]{";\",{});"};
        String query;

        List<String> queryFragmentsList;
        queryFragmentsList = new ArrayList<>(Arrays.asList(clusterFragments));
        queryFragmentsList.addAll(Arrays.asList(matchFragments));
        queryFragmentsList.addAll(Arrays.asList(queryFragments));
        queryFragmentsList.addAll(Arrays.asList(returnFragments));
        queryFragmentsList.addAll(Arrays.asList(returnLatestVersionFragments));

        for (String fragment : queryFragmentsList) {
            queryBuilder.append(fragment);
        }
        query = queryBuilder.toString();

        Result result = runNeo4jQuery(query);
        while (result.hasNext()) {
            rootNodes.add(new RootNode(result.next().get("results").asMap()));
        }

        return rootNodes;
    }

    private String getCurrentTime() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        df.setTimeZone(tz);
        return df.format(new Date());
    }

    private Stream<HistogramEntry> getHistogramEntries(String[] queryFragments) {

        ArrayList<HistogramEntry> histogramEntries = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("");
        String query;

        for (String fragment : queryFragments) {
            queryBuilder.append(fragment);
        }
        query = queryBuilder.toString();

        Result result = runNeo4jQuery(query);
        while (result.hasNext()) {
            histogramEntries.add(new HistogramEntry(result.next().get("results").asMap()));
        }

        return histogramEntries.stream();
    }
}