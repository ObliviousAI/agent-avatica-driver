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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.prefs.Preferences;
import java.net.InetAddress;

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

  @Override protected String getConnectStringPrefix() {
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

  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
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
   * @param config Configuration properties
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
   * @param config The configuration.
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
  @Override public Connection connect(String url, Properties info)
      throws SQLException {
    int retries = 0;
    int currentRetry = 0;
    long failoverSleepTime = 0;
    do {
      long startTime = System.currentTimeMillis();
      // Get the device details to create device signature at the backend
      String os = System.getProperty("os.name", "unknown");
      String machine_uuid = getAndSetUuid();
      String hostAddress;
      try {
        hostAddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        hostAddress = "unknown";
      }
      info.put("os", os);
      info.put("uuid", machine_uuid);
      info.put("ip", hostAddress);

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
    String uuid = null;
    uuid = Preferences.userNodeForPackage(Driver.class).get("uuid", null);
    // Get the last access time from java preferences
    long lastTime = Preferences.userNodeForPackage(Driver.class).getLong("lastTime", 0);
    // If the uuid is not set or the last time is more than 25 minutes ago, set the uuid
    if (uuid == null || (System.currentTimeMillis() - lastTime) > 1500000) {
      uuid = UUID.randomUUID().toString();
      Preferences.userNodeForPackage(Driver.class).put("uuid", uuid);
    }
    // Update the last time
    Preferences.userNodeForPackage(Driver.class).putLong("lastTime", System.currentTimeMillis());
    // Flush the preferences
    try {
      Preferences.userNodeForPackage(Driver.class).flush();
    } catch (Exception e) {
      return uuid;  // If the flush fails, return the uuid anyway. Uuid gets automatically flushed or maybe regenerated again on next access
    }
    return uuid;
  }
}

// End Driver.java
