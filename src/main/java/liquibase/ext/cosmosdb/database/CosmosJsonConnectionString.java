package liquibase.ext.cosmosdb.database;

import com.google.gson.Gson;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CosmosJsonConnectionString {

    public static final String ACCOUNT_ENDPOINT_PROPERTY = "AccountEndpoint";

    public static final String ACCOUNT_KEY_PROPERTY = "AccountKey";

    public static final String DATABASE_NAME_PROPERTY = "DatabaseName";

    public static final String COSMOSDB_PREFIX = "cosmosdb://";

    @Getter
    private final String connectionString;

    @Getter
    private final Map<String, String> properties;

    public static CosmosJsonConnectionString fromJsonConnectionString(final String jsonConnectionString) {
        if(StringUtils.isBlank(jsonConnectionString) || !jsonConnectionString.startsWith(COSMOSDB_PREFIX)) {
            throw new IllegalArgumentException("jsonConnectionString should not be empty and has to start with: " + COSMOSDB_PREFIX);
        }

        final String json = jsonConnectionString.replaceFirst(COSMOSDB_PREFIX, StringUtils.EMPTY);
        final Gson gson = new Gson();
        final Map<String, String> properties = gson.<Map<String, String>>fromJson(json, Map.class);
        return new CosmosJsonConnectionString(jsonConnectionString, Optional.ofNullable(properties).orElse(Collections.emptyMap()));
    }

    public Optional<String> getProperty(final String propertyName){
        return Optional.ofNullable(properties.get(propertyName));
    }

    public Optional<String> getAccountEndpoint(){
        return getProperty(ACCOUNT_ENDPOINT_PROPERTY);
    }

    public Optional<String> getAccountKey(){
        return getProperty(ACCOUNT_KEY_PROPERTY);
    }

    public Optional<String> getDatabaseName(){
        return getProperty(DATABASE_NAME_PROPERTY);
    }

}
