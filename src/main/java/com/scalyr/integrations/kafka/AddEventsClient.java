package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * AddEventsClients provides abstraction for making Scalyr addEvents API calls.
 * It performs JSON object serialization and the addEvents POST request.
 *
 * @see <a href="https://app.scalyr.com/help/api"></a>
 */
public class AddEventsClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(AddEventsClient.class);

  private final CloseableHttpClient client = HttpClients.createDefault();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final HttpPost httpPost;

  /**
   * @param scalyrUrl in format https://app.scalyr.com
   * @throws ConnectException with invalid URL, which will cause Kafka Connect to terminate the ScalyrSinkTask.
   */
  public AddEventsClient(String scalyrUrl) {
    this.httpPost = new HttpPost(buildAddEventsUri(scalyrUrl));
    addHeaders(this.httpPost);
  }

  /**
   * Make addEvents POST API call to Scalyr with the events object.
   * The events object should only contain events for a single session.
   */
  public void log(Map<String, Object> events) throws Exception {
    if (log.isTraceEnabled()) {
      log.trace("Sending addEvents payload {}", objectMapper.writeValueAsString(events));
    }

    httpPost.setEntity(new EntityTemplate(outputStream -> objectMapper.writeValue(outputStream, events)));
    try (CloseableHttpResponse response = client.execute(httpPost)) {
      log.info("post result {}", response.getStatusLine().getStatusCode());
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new RuntimeException("addEvents failed with code " + response.getStatusLine().getStatusCode()
          + ", message " + EntityUtils.toString(response.getEntity()));
      }
    }
  }

  /**
   * Validates url and creates addEvents Scalyr URI
   * @return Scalyr addEvents URI.  e.g. https://apps.scalyr.com/addEvents
   */
  private URI buildAddEventsUri(String url) {
    try {
      URIBuilder urlBuilder = new URIBuilder(url);

      if (urlBuilder.getScheme() == null || urlBuilder.getHost() == null) {
        throw new ConnectException("Invalid Scalyr URL: " + url);
      }
      urlBuilder.setPath("addEvents");
      return  urlBuilder.build();
    } catch (URISyntaxException e) {
      throw new ConnectException(e);
    }
  }

  /**
   * Add addEvents POST request headers
   */
  private void addHeaders(HttpPost httpPost) {
    httpPost.addHeader("Content-type", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Accept", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Connection", "Keep-Alive");
    httpPost.addHeader("User-Agent", "Scalyr-Kafka-Connector");
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
