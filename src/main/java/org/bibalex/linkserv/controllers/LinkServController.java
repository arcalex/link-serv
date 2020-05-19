package org.bibalex.linkserv.controllers;

import org.bibalex.linkserv.errors.OperationNotFoundException;
import org.bibalex.linkserv.handlers.PropertiesHandler;
import org.bibalex.linkserv.services.LinkServService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class LinkServController {

    @Autowired
    private LinkServService linkServService;

    @RequestMapping(value = "/{workspaceName}", method = RequestMethod.POST)
    public ResponseEntity<String> updateGraph(@PathVariable("workspaceName") String workspaceName,
                                              @RequestBody String jsonGraph,
                                              @RequestParam String operation) {

        LOGGER.info("Updating Graph with Parameters: " + workspaceName);
        PropertiesHandler.initializeProperties();
        if (operation.equals(PropertiesHandler.getProperty("updateGraph"))) {
            LOGGER.info("Response Status: 200");
            return ResponseEntity.ok(linkServService.updateGraph(jsonGraph));
        } else {
            throw new OperationNotFoundException(operation);
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{workspaceName}")
    public ResponseEntity<Object> getGraph(@PathVariable("workspaceName") String workspaceName,
                                           @RequestParam String operation,
                                           @RequestParam(required = false, defaultValue = "1") Integer depth) {

        LOGGER.info("Getting Graph with Parameters: " + workspaceName + " and Depth: " + depth);
        PropertiesHandler.initializeProperties();
        if (operation.equals(PropertiesHandler.getProperty("getGraph"))) {
            LOGGER.info("Response Status: 200");
            return ResponseEntity.ok(linkServService.getGraph(workspaceName, depth));
        } else {
            throw new OperationNotFoundException(operation);
        }

    }
}
