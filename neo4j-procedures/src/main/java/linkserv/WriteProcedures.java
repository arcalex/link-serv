package linkserv;

import helpers.Neo4jHelper;
import models.Output;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.driver.Result;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import constants.Constants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

public class WriteProcedures extends Procedures{

    @Procedure(value = "linkserv.addNodesAndRelationships", mode = Mode.WRITE)
    public Stream<Output> addNodesAndRelationships(@Name("JsonArrayOfNodeAndEdges") String jsonGraph, @Name("requestHash") String requestHash) {
        try {
            FileWriter myWriter = new FileWriter(Constants.neo4jImportPath + Constants.nodesFile + requestHash + Constants.csvExtension);
            myWriter.write("url\ttimestamp\toutlinks\n");
            myWriter.close();
            JSONArray graphJsonArray =  new JSONArray(jsonGraph);
            for(int i=0; i<graphJsonArray.length(); i++){
                JSONObject nodeWithOutlinks = graphJsonArray.getJSONObject(i);
                JSONArray arrJson = nodeWithOutlinks.getJSONArray(Constants.outlinksKey);
                ArrayList<String> outlinks = new ArrayList<>();
                for(int j = 0; j < arrJson.length(); j++)
                    outlinks.add(arrJson.getString(j));
                writeToFile(nodeWithOutlinks.getString(Constants.nodeKey), nodeWithOutlinks.getString(Constants.timestampKey), outlinks, requestHash);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(neo4jHelper == null)
            neo4jHelper = new Neo4jHelper();

        loadCSVFile(requestHash);
        ArrayList<Output> outputs = new ArrayList<>();
        outputs.add(new Output(true));
        return outputs.stream();
    }

    private void writeToFile(String url, String timestamp, List outlinks, String requestHash){
        String line = url +"\t" + timestamp+"\t";
        for(int i =0 ; i< outlinks.size() ; i++){
            if(i == outlinks.size() -1)
                line += outlinks.get(i);
            else
                line += outlinks.get(i) +"|";
        }
        try {
            Files.write(Paths.get(Constants.neo4jImportPath + Constants.nodesFile + requestHash + Constants.csvExtension),
                    (line + "\n").getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
        }
    }

    private List addNodeWithItsVersion() {
        return Arrays.asList("MERGE (parent:", Constants.parentNodeLabel,
                " {", Constants.nameProperty, ": $url }) MERGE (parent)-[:", Constants.versionRelationshipType,
                "]->(version:", Constants.versionNodeLabel, " {", Constants.versionProperty,
                ": $version }) ");
    }

    public void loadCSVFile(String requestHash){
        StringBuilder queryBuilder = new StringBuilder("");

        List<String> queryFragmentsList = new ArrayList<>();
        queryFragmentsList.add("USING PERIODIC COMMIT 10000 ");
        queryFragmentsList.addAll(Arrays.asList("LOAD CSV WITH HEADERS FROM 'file:///",  Constants.nodesFile, requestHash + Constants.csvExtension, "' AS line fieldterminator '\\t' "));
        queryFragmentsList.add("CALL cluster.executeQuery(\"");
        queryFragmentsList.addAll(addNodeWithItsVersion());
        queryFragmentsList.add("WITH parent, version ");
        queryFragmentsList.add("UNWIND split($outlinks , '|') AS outlink ");
        queryFragmentsList.addAll(Arrays.asList(" MERGE (outNode:", Constants.parentNodeLabel,
                " {", Constants.nameProperty, ":outlink})",
                " MERGE (version)-[:", Constants.linkRelationshipType, "]->(outNode) return parent.", Constants.nameProperty,
                "\",{url:line.url, version:line.timestamp,outlinks:line.outlinks}) yield results return results;"));

        for (String fragment : queryFragmentsList) {
            queryBuilder.append(fragment);
        }

        neo4jHelper = new Neo4jHelper();
        String query = queryBuilder.toString();
        Result result = neo4jHelper.runQuery(query);
        if(result.hasNext()){

        }

        File file2 = new File(Constants.neo4jImportPath + Constants.nodesFile + requestHash + Constants.csvExtension);
        file2.delete();
    }
}
