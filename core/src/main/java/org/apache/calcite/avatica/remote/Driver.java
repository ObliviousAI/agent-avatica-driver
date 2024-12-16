/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.prefs.Preferences;


/**
 * Avatica Remote JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  public static final String CONNECT_STRING_PREFIX = "jdbc:avatica:remote:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  /**
   * Defines the method of message serialization used by the Driver
   */
  public enum Serialization {
    JSON,
    PROTOBUF
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "org-apache-calcite-jdbc.properties",
        "Avatica Remote JDBC Driver",
        "unknown version",
        "Avatica",
        "unknown version");
  }

  @Override
  protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
    return list;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    final ConnectionConfig config = connection.config();

    // Perform the login and launch the renewal thread if necessary
    final KerberosConnection kerberosUtil = createKerberosUtility(config);
    if (null != kerberosUtil) {
      kerberosUtil.login();
      connection.setKerberosConnection(kerberosUtil);
    }

    // Create a single Service and set it on the Connection instance
    final Service service = createService(connection, config);
    connection.setService(service);
    return new RemoteMeta(connection, service);
  }

  KerberosConnection createKerberosUtility(ConnectionConfig config) {
    final String principal = config.kerberosPrincipal();
    if (null != principal) {
      return new KerberosConnection(principal, config.kerberosKeytab());
    }
    return null;
  }

  /**
   * Creates a {@link Service} with the given {@link AvaticaConnection} and configuration.
   *
   * @param connection The {@link AvaticaConnection} to use.
   * @param config     Configuration properties
   * @return A Service implementation.
   */
  Service createService(AvaticaConnection connection, ConnectionConfig config) {
    final Service.Factory metaFactory = config.factory();
    final Service service;
    if (metaFactory != null) {
      service = metaFactory.create(connection);
    } else if (config.url() != null) {
      final AvaticaHttpClient httpClient = getHttpClient(connection, config);
      final Serialization serializationType = getSerialization(config);

      LOG.debug("Instantiating {} service", serializationType);
      switch (serializationType) {
      case JSON:
        service = new RemoteService(httpClient);
        break;
      case PROTOBUF:
        service = new RemoteProtobufService(httpClient, new ProtobufTranslationImpl());
        break;
      default:
        throw new IllegalArgumentException("Unhandled serialization type: " + serializationType);
      }
    } else {
      service = new MockJsonService(Collections.emptyMap());
    }
    return service;
  }

  /**
   * Creates the HTTP client that communicates with the Avatica server.
   *
   * @param connection The {@link AvaticaConnection}.
   * @param config     The configuration.
   * @return An {@link AvaticaHttpClient} implementation.
   */
  AvaticaHttpClient getHttpClient(AvaticaConnection connection, ConnectionConfig config) {
    URL url;
    String urlStr;
    if (config.useClientSideLb()) {
      urlStr = config.getLBStrategy().getLbURL(config);
    } else {
      urlStr = config.url();
    }
    try {
      url = new URL(urlStr);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    AvaticaHttpClientFactory httpClientFactory = config.httpClientFactory();

    return httpClientFactory.getClient(url, config, connection.getKerberosConnection());
  }

  @Override
  public Connection connect(String url, Properties info)
      throws SQLException {
    String consoleUrl = extractUrlFromConnectString(url);
    String apiKey = extractApiKeyFromUrl(consoleUrl);
    consoleUrl = removeParamFromConsoleUrl(consoleUrl);

    String authBaseUrl;
    String avaticaUrl;
    // Make a get request to consoleUrl/config/client to get the authBaseUrl and avaticaUrl
    try {
      String endPoint;
      if (consoleUrl.endsWith("/")) {
        endPoint = "config/client";
      } else {
        endPoint = "/config/client";
      }
      URL urlObj = new URL(consoleUrl + endPoint);
      HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);

      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("Failed to get client configuration. Response code: " + responseCode);
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
      StringBuilder response = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
      reader.close();

      JSONObject jsonResponse = new JSONObject(response.toString());
      authBaseUrl = jsonResponse.getString("AGENT_CONSOLE_URL");
      avaticaUrl = jsonResponse.getString("AVATICA_URL");
    } catch (Exception e) {
      throw new SQLException("Failed to get client configuration. Console not reachable", e);
    }

    url = replaceUrlInConnectString(avaticaUrl);

    int retries = 0;
    int currentRetry = 0;
    long failoverSleepTime = 0;
    do {
      long startTime = System.currentTimeMillis();
      // Get the device details to create device signature at the backend
      String os = System.getProperty("os.name", "unknown");
      String machine_uuid = getAndSetUuid();
      if (apiKey == null) { // If apiKey is not present in the URL, get it from the properties
        apiKey = info.getProperty("apiKey");
      }
      String client_type = "sql";
      JSONObject jsonPayload = new JSONObject();
      jsonPayload.put("apikey", apiKey);
      jsonPayload.put("machine_uuid", machine_uuid);
      jsonPayload.put("client", client_type);
      jsonPayload.put("os", os);
      jsonPayload.put("nic_id", getNICId());

      String targetUrl = authBaseUrl + "/jupyter/login/request";

      HttpPost postRequest = new HttpPost(targetUrl);
      postRequest.setHeader("Content-Type", "application/json");
      postRequest.setEntity(new StringEntity(jsonPayload.toString(), ContentType.APPLICATION_JSON));

      String authToken;
      String refreshToken;
      // Response handler that processes the server's response and extracts tokens
      HttpClientResponseHandler<String[]> responseHandler =
          new HttpClientResponseHandler<String[]>() {
            @Override
            public String[] handleResponse(
                ClassicHttpResponse response
            ) throws HttpException, IOException {
              int statusCode = response.getCode();
              if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity == null) {
                  throw new RuntimeException("No response entity returned.");
                }

                try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
                  boolean seenFirst = false;
                  String line;
                  while ((line = reader.readLine()) != null) {
                    if (line.startsWith("data: ")) {
                      // The first "data: " line might be a starter line; skip it
                      if (!seenFirst) {
                        seenFirst = true;
                        continue;
                      }
                      String jsonData = line.substring(6).trim(); // Remove "data: "
                      // Concatenate all the lines to form the JSON string

                      try {
                        JsonNode tokenNode = extractToken(jsonData);
                        if (!"approved".equals(tokenNode.get("approval_status").asText())) {
                          throw new RuntimeException("Token request not approved.");
                        }
                        String token = tokenNode.get("access_token").asText();
                        String refreshToken = tokenNode.get("refresh_token").asText();
                        return new String[]{token, refreshToken};
                      } catch (Exception e) {
                        throw new RuntimeException("Error parsing token JSON", e);
                      }
                    }
                  }
                  throw new RuntimeException("Failed to login. No valid data found in response.");
                }
              } else {
                throw new RuntimeException("Failed to retrieve auth token. Status code: " + statusCode);
              }
            }
          };

      try (CloseableHttpClient client = HttpClients.createDefault()) {
        String[] tokens = client.execute(postRequest, responseHandler);
        authToken = tokens[0];
        refreshToken = tokens[1];
      } catch (Exception e) {
        throw new RuntimeException("Error retrieving auth token", e);
      }

      // Put auth token and refresh token in the info properties
      info.put("token", authToken);
      info.put("refresh_token", refreshToken);

      AvaticaConnection conn = (AvaticaConnection) super.connect(url, info);
      if (conn == null) {
        // It's not an url for our driver
        return null;
      }

      ConnectionConfig config = conn.config();
      if (config.useClientSideLb()) {
        retries = config.getLBConnectionFailoverRetries();
        failoverSleepTime = config.getLBConnectionFailoverSleepTime();
      }

      Service service = conn.getService();

      // super.connect(...) should be creating a service and setting it in the AvaticaConnection
      assert null != service;
      try {
        service.apply(
            new Service.OpenConnectionRequest(conn.id,
                Service.OpenConnectionRequest.serializeProperties(info)));
        return conn;
      } catch (Exception e) {
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        LOG.warn("Connection Failed: {}", e.getMessage());
        LOG.debug("Failure detected in: {} milliseconds", elapsedTime);
        if (currentRetry < retries) {
          currentRetry++;
          if (failoverSleepTime > 0) {
            try {
              LOG.info("Sleeping for {} milliseconds before load balancer failover",
                  failoverSleepTime);
              Thread.sleep(failoverSleepTime);
            } catch (InterruptedException ex) {
              throw new SQLException(ex);
            }
          }
          LOG.info("Load balancer failover retry: {}", currentRetry);
        } else {
          throw e;
        }
      }
    } while (true);
  }

  Serialization getSerialization(ConnectionConfig config) {
    final String serializationStr = config.serialization();
    Serialization serializationType = Serialization.JSON;
    if (null != serializationStr) {
      try {
        serializationType =
            Serialization.valueOf(serializationStr.toUpperCase(Locale.ROOT));
      } catch (Exception e) {
        // Log a warning instead of failing harshly? Intentionally no loggers available?
        throw new RuntimeException(e);
      }
    }

    return serializationType;
  }

  private String getAndSetUuid() {
    // Set or get the UUID from java preferences
    String uuid;
    Preferences prefs = Preferences.userRoot().node("agent/client");
    uuid = prefs.get("uuid", null);
    // Get the last access time from java preferences
    long lastTime = prefs
        .getLong("lastTime", 0);
    // If the uuid is not set or the last time is more than 1 minute ago, set the uuid
    if (uuid == null || (System.currentTimeMillis() - lastTime) > 60000) {
      uuid = UUID.randomUUID().toString();
      prefs.put("uuid", uuid);
    }
    // Update the last time
    prefs.putLong("lastTime", System.currentTimeMillis());
    // Flush the preferences
    try {
      prefs.flush();
    } catch (Exception e) {
      return uuid;  // If the flush fails, return the uuid anyway. Uuid gets automatically flushed or maybe regenerated again on next access
    }
    return uuid;
  }

  // Method to extract token from the jsonData
  private JsonNode extractToken(String jsonData) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readTree(jsonData);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract token from response.", e);
    }
  }

  private String extractUrlFromConnectString(String connectString) {
    if (connectString.startsWith(CONNECT_STRING_PREFIX)) {
      return connectString.substring(CONNECT_STRING_PREFIX.length() + 4);
    }
    return connectString;
  }

  private String replaceUrlInConnectString(String url) {
    return CONNECT_STRING_PREFIX + "url=" + url;
  }

  private String extractApiKeyFromUrl(String url) {
    try {
      URL urlObj = new URL(url);
      String query = urlObj.getQuery();
      if (query == null) {    // If no parameters are present in the URL
        return null;
      }
      String[] queryParts = query.split("&");
      for (String queryPart : queryParts) {
        String[] keyValue = queryPart.split("=");
        if (keyValue[0].equals("apiKey")) {
          return keyValue[1];
        }
      }
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  private String removeParamFromConsoleUrl(String url) {
    try {
      URL urlObj = new URL(url);
      return urlObj.getProtocol() + "://" + urlObj.getHost() + ":" + urlObj.getPort();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  // Get NIC Id from the system. Returns first not null nic id of all interfaces
  private String getNICId() {
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        byte[] mac = networkInterface.getHardwareAddress();
        if (mac != null) {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < mac.length; i++) {
            sb.append(String.format(Locale.US ,"%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
          }
          return sb.toString();
        }
      }
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
// End Driver.java
