package com.example.connectors;

import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Registers /v1/metadata/id so the CFK liveness probe gets a 200
 * when running on cp-kafka-connect (community edition).
 */
public class MetadataRestExtension implements ConnectRestExtension {

    @Override
    public String version() { return "1.0"; }

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        restPluginContext.configurable().register(MetadataResource.class);
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    @Path("/v1/metadata/id")
    public static class MetadataResource {
        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public Response getId() {
            return Response.ok("{\"id\":\"connect\"}").build();
        }
    }
}
