package liquibase.ext.cosmosdb.database;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static liquibase.ext.cosmosdb.database.CosmosConnectionString.*;
import static org.assertj.core.api.Assertions.*;

class CosmosConnectionStringTest {

    public static final String TEST_COSMOS_JSON_CONNECTION_STRING_1 =
            "cosmosdb://{\"AccountEndpoint\" : \"https://ech-0a9d975b:8080\", \"AccountKey\" : \"C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==\", \"DatabaseName\" : \"testdb1\" , \"ssl\" : \"true\" }";

    public static final String TEST_COSMOS_URL_CONNECTION_STRING_1 =
            "cosmosdb://ech-0a9d975b:C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==:8080/testdb1?ssl=false";


    @Test
    void fromJsonConnectionStringTest() {

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString(null)
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString("")
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString("mongodb://{wrong driver name}")
        );

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(
                () -> fromConnectionString("cosmosdb://{not a json")
        );

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(
                () -> fromConnectionString("cosmosdb://{wrong json payload}")
        );

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(
                () -> fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\", \"DatabaseName\" : \"db2\"}")
        );
    }

    @Test
    void fromUrlConnectionStringTest() {

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString(null)
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString("")
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromConnectionString("mongodb://wrong driver name")
        );

        assertThatExceptionOfType(Exception.class).isThrownBy(
                () -> fromUrlConnectionString("cosmosdb://\"DatabaseName\" : \"db1\", \"DatabaseName\" : \"db2\"")
        );
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void getJsonPropertyTest() {
        assertThat(fromJsonConnectionString("cosmosdb://{}").getProperty("any").isPresent()).isFalse();
        assertThat(fromJsonConnectionString("cosmosdb://{\"aProperty\" : \"aValue\"}").getProperty("any").isPresent()).isFalse();
        assertThat(fromJsonConnectionString("cosmosdb://{\"aProperty\" : \"aValue\"}").getProperty("aProperty").get()).isEqualTo("aValue");

        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\"}").getProperty(ACCOUNT_ENDPOINT_PROPERTY).get()).isEqualTo("http://localhost:8080/");
        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\"}").getAccountEndpoint().get()).isEqualTo("http://localhost:8080/");

        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountKey\" : \"key\"}").getProperty(ACCOUNT_KEY_PROPERTY).get()).isEqualTo("key");
        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountKey\" : \"key\"}").getAccountKey().get()).isEqualTo("key");

        assertThat(fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\"}").getProperty(DATABASE_NAME_PROPERTY).get()).isEqualTo("db1");
        assertThat(fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\"}").getDatabaseName().get()).isEqualTo("db1");

        CosmosConnectionString cosmosConnectionString
                = fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\", \"AccountKey\" : \"key\", \"DatabaseName\" : \"db1\"}");
        assertThat(cosmosConnectionString.getAccountEndpoint().get()).isEqualTo("http://localhost:8080/");
        assertThat(cosmosConnectionString.getAccountKey().get()).isEqualTo("key");
        assertThat(cosmosConnectionString.getDatabaseName().get()).isEqualTo("db1");
        assertThat(cosmosConnectionString.getProperty("any").isPresent()).isFalse();
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void getUrlPropertyTest() {
        assertThat(fromUrlConnectionString("cosmosdb://").getProperty("any").isPresent()).isFalse();
        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?aProperty=aValue").getProperty("any").isPresent()).isFalse();
        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?aProperty=aValue").getProperty("aProperty").get()).isEqualTo("aValue");

        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?aProperty=aValue").getProperty(ACCOUNT_ENDPOINT_PROPERTY).get()).isEqualTo("https://localhost:8080");
        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?aProperty=aValue").getAccountEndpoint().get()).isEqualTo("https://localhost:8080");

        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?aProperty=aValue&AccountKey=key").getProperty(ACCOUNT_KEY_PROPERTY).get()).isEqualTo("key");
        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db?AccountKey=key").getAccountKey().get()).isEqualTo("key");

        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db1?AccountKey=key").getProperty(DATABASE_NAME_PROPERTY).get()).isEqualTo("db1");
        assertThat(fromUrlConnectionString("cosmosdb://localhost:key:8080/db1?AccountKey=key").getDatabaseName().get()).isEqualTo("db1");

        CosmosConnectionString cosmosConnectionString
                = fromUrlConnectionString("cosmosdb://localhost:key:8080/db1?AccountKey=key");
        assertThat(cosmosConnectionString.getAccountEndpoint().get()).isEqualTo("https://localhost:8080");
        assertThat(cosmosConnectionString.getAccountKey().get()).isEqualTo("key");
        assertThat(cosmosConnectionString.getDatabaseName().get()).isEqualTo("db1");
        assertThat(cosmosConnectionString.getProperty("any").isPresent()).isFalse();
    }

    @Test
    void fromConnectionStringTest() {
        final CosmosConnectionString cosmosJsonConnectionString
                = fromConnectionString(TEST_COSMOS_JSON_CONNECTION_STRING_1);

        assertThat(cosmosJsonConnectionString)
                .returns(Optional.of("https://ech-0a9d975b:8080"), CosmosConnectionString::getAccountEndpoint)
                .returns(Optional.of("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="), CosmosConnectionString::getAccountKey)
                .returns(Optional.of("testdb1"), CosmosConnectionString::getDatabaseName)
                .returns(Optional.of("true"), c -> c.getProperty("ssl"))
                .returns(Optional.empty(), c -> c.getProperty("notExisting"))
                .returns(TEST_COSMOS_JSON_CONNECTION_STRING_1, CosmosConnectionString::getConnectionString);

        final CosmosConnectionString cosmosUrlConnectionString
                = fromConnectionString(TEST_COSMOS_URL_CONNECTION_STRING_1);

        assertThat(cosmosUrlConnectionString)
                .returns(Optional.of("https://ech-0a9d975b:8080"), CosmosConnectionString::getAccountEndpoint)
                .returns(Optional.of("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="), CosmosConnectionString::getAccountKey)
                .returns(Optional.of("testdb1"), CosmosConnectionString::getDatabaseName)
                .returns(Optional.of("false"), c -> c.getProperty("ssl"))
                .returns(Optional.empty(), c -> c.getProperty("notExisting"))
                .returns(TEST_COSMOS_URL_CONNECTION_STRING_1, CosmosConnectionString::getConnectionString);



    }
}