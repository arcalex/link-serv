package org.bibalex.neo4j.handlers;

import java.io.*;
import java.util.Scanner;

public class FileHandler {

    private File idFile;

    public FileHandler() {
        idFile = new File(PropertiesHandler.getConfigProperty("idFile"));
    }

    public long getLastNodeId() {

        try {
            Scanner myReader = new Scanner(idFile);
            Long lastNodeId = Long.valueOf(myReader.nextLine());
            myReader.close();
            return lastNodeId;
        } catch (FileNotFoundException e) {
            try {
                idFile.createNewFile();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        return 0;
    }

    public void setLastNodeId(long lastNodeId) {
        try {
            FileWriter myWriter = new FileWriter(idFile);
            myWriter.write(String.valueOf(lastNodeId));
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
