package com.tailoredshapes.gridql.load;

import com.sun.net.httpserver.HttpServer;
import com.tailoredshapes.stash.Stash;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

import static com.tailoredshapes.stash.Stash.stash;
import static com.tailoredshapes.underbar.io.IO.slurp;
import static com.tailoredshapes.underbar.ocho.Die.die;
import static org.junit.jupiter.api.Assertions.*;

public class ReSTRepositoryTest {

    private static ReSTRepository repo;
    private static HttpServer httpServer;

    @BeforeAll
    public static void setUp() throws Exception {
        repo = new ReSTRepository("http://localhost:6432");


        httpServer = HttpServer.create(new InetSocketAddress(6432), 0);
        httpServer.createContext("/", exchange -> {
            byte[] response = null;

            switch(exchange.getRequestMethod()){
                case "DELETE":
                    assertEquals("", exchange.getRequestURI().getPath());
                    response = "deleted".getBytes();
                    break;
                case "POST":
                    assertEquals(stash("test", true), Stash.parseJSON(slurp(exchange.getRequestBody())));
                    response = "created".getBytes();
                    break;
                case "PUT":
                    assertEquals(stash("test", false), Stash.parseJSON(slurp(exchange.getRequestBody())));
                    response = "updated".getBytes();
                    break;
                default:
                    die("Unsupported HTTP Method");
            }

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        httpServer.start();
    }

    @AfterAll
    static void afterAll() {
        httpServer.stop(0);
    }

    @Test
    void create() {
        var payload = stash("test", true);
        String body = repo.create(payload).join();
        assertEquals("created", body);
    }

    @Test
    void update() {
        var payload = stash("test", false);
        String body =repo.update("1", payload).join();
        assertEquals("updated", body);
    }

    @Test
    void delete() {
        String body =repo.delete("1").join();
        assertNull(body);
    }
}