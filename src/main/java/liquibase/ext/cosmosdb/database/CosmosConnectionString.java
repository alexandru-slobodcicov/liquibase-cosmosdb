package liquibase.ext.cosmosdb.database;

import com.azure.core.util.UrlBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static liquibase.ext.cosmosdb.statement.JsonUtils.OBJECT_MAPPER;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CosmosConnectionString {

    public static final String ACCOUNT_ENDPOINT_PROPERTY = "AccountEndpoint";
    public static final String ACCOUNT_KEY_PROPERTY = "AccountKey";
    public static final String DATABASE_NAME_PROPERTY = "DatabaseName";

    public static final String COSMOSDB_PREFIX = "cosmosdb://";
    public static final String COSMOSDB_JSON_PREFIX = COSMOSDB_PREFIX + "{";
    public static final String HTTPS_PREFIX = "https://";

    @Getter
    private final String connectionString;

    @Getter
    private final Map<String, String> properties;

    public static CosmosConnectionString fromConnectionString(final String connectionString) {
        if (StringUtils.isBlank(connectionString) || !connectionString.startsWith(COSMOSDB_PREFIX)) {
            throw new IllegalArgumentException("connectionString should not be empty and has to start with: " + COSMOSDB_PREFIX);
        }
        if (connectionString.startsWith(COSMOSDB_JSON_PREFIX)) {
            return fromJsonConnectionString(connectionString);
        } else {
            return fromUrlConnectionString(connectionString);
        }
    }

    public static CosmosConnectionString fromJsonConnectionString(final String jsonConnectionString) {
        if (StringUtils.isBlank(jsonConnectionString) || !jsonConnectionString.startsWith(COSMOSDB_JSON_PREFIX)) {
            throw new IllegalArgumentException("jsonConnectionString should not be empty and has to start with: " + COSMOSDB_JSON_PREFIX);
        }

        try {
            final String json = jsonConnectionString.replaceFirst(COSMOSDB_PREFIX, StringUtils.EMPTY);

            @SuppressWarnings("unchecked")
            final Map<String, String> properties = OBJECT_MAPPER.readValue(json, Map.class);

            return new CosmosConnectionString(jsonConnectionString, Optional.ofNullable(properties).orElse(Collections.emptyMap()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not parse connection Json String: ", e);
        }
    }

    public static CosmosConnectionString fromUrlConnectionString(final String url) {
        if (StringUtils.isBlank(url) || !url.startsWith(COSMOSDB_PREFIX)) {
            throw new IllegalArgumentException("Url should not be empty and has to start with: " + COSMOSDB_PREFIX);
        }

        try {
            int beginIndex = url.indexOf(':', COSMOSDB_PREFIX.length());
            int endIndex = url.indexOf(':', beginIndex + 1);
            final String accountKey = url.substring(beginIndex + 1, endIndex);

            final String httpsUrl = StringUtils.remove(url, accountKey + ":")
                    .replaceFirst(COSMOSDB_PREFIX, HTTPS_PREFIX);

            final UrlBuilder urlBuilder = UrlBuilder.parse(httpsUrl);
            final Map<String, String> properties = new HashMap<>(urlBuilder.getQuery());
            urlBuilder.setQuery(null);

            final String databaseName = urlBuilder.getPath().replace("/", "");
            urlBuilder.setPath(null);

            properties.put(ACCOUNT_ENDPOINT_PROPERTY, urlBuilder.toString());
            properties.put(ACCOUNT_KEY_PROPERTY, accountKey);
            properties.put(DATABASE_NAME_PROPERTY, databaseName);

            return new CosmosConnectionString(url, properties);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse connection Url String: ", e);
        }
    }

    public Optional<String> getProperty(final String propertyName) {
        return Optional.ofNullable(properties.get(propertyName));
    }

    public Optional<String> getAccountEndpoint() {
        return getProperty(ACCOUNT_ENDPOINT_PROPERTY);
    }

    public Optional<String> getAccountKey() {
        return getProperty(ACCOUNT_KEY_PROPERTY);
    }

    public Optional<String> getDatabaseName() {
        return getProperty(DATABASE_NAME_PROPERTY);
    }

}
