package liquibase.ext.cosmosdb.database;

import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;

import static liquibase.ext.cosmosdb.database.CosmosJsonConnectionString.*;
import static org.assertj.core.api.Assertions.*;

class CosmosJsonConnectionStringTest {

    @Test
    void fromJsonConnectionStringTest() {

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromJsonConnectionString(null)
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromJsonConnectionString("")
        );

        assertThatIllegalArgumentException().isThrownBy(
                () -> fromJsonConnectionString("mongodb://wrong driver name")
        );

        assertThatExceptionOfType(JsonSyntaxException.class).isThrownBy(
                () -> fromJsonConnectionString("cosmosdb://not a json")
        );

        assertThatExceptionOfType(JsonSyntaxException.class).isThrownBy(
                () -> fromJsonConnectionString("cosmosdb://{wrong json payload}")
        );

        assertThatExceptionOfType(JsonSyntaxException.class).isThrownBy(
                () -> fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\", \"DatabaseName\" : \"db2\"}")
        );
    }

    @Test
    void getPropertyTest() {
        assertThat(fromJsonConnectionString("cosmosdb://").getProperty("any").isPresent()).isFalse();
        assertThat(fromJsonConnectionString("cosmosdb://{}").getProperty("any").isPresent()).isFalse();
        assertThat(fromJsonConnectionString("cosmosdb://{\"aProperty\" : \"aValue\"}").getProperty("any").isPresent()).isFalse();
        assertThat(fromJsonConnectionString("cosmosdb://{\"aProperty\" : \"aValue\"}").getProperty("aProperty").get()).isEqualTo("aValue");

        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\"}").getProperty(ACCOUNT_ENDPOINT_PROPERTY).get()).isEqualTo("http://localhost:8080/");
        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\"}").getAccountEndpoint().get()).isEqualTo("http://localhost:8080/");

        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountKey\" : \"key\"}").getProperty(ACCOUNT_KEY_PROPERTY).get()).isEqualTo("key");
        assertThat(fromJsonConnectionString("cosmosdb://{\"AccountKey\" : \"key\"}").getAccountKey().get()).isEqualTo("key");

        assertThat(fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\"}").getProperty(DATABASE_NAME_PROPERTY).get()).isEqualTo("db1");
        assertThat(fromJsonConnectionString("cosmosdb://{\"DatabaseName\" : \"db1\"}").getDatabaseName().get()).isEqualTo("db1");

        CosmosJsonConnectionString cosmosJsonConnectionString
                = fromJsonConnectionString("cosmosdb://{\"AccountEndpoint\" : \"http://localhost:8080/\", \"AccountKey\" : \"key\", \"DatabaseName\" : \"db1\"}");
        assertThat(cosmosJsonConnectionString.getAccountEndpoint().get()).isEqualTo("http://localhost:8080/");
        assertThat(cosmosJsonConnectionString.getAccountKey().get()).isEqualTo("key");
        assertThat(cosmosJsonConnectionString.getDatabaseName().get()).isEqualTo("db1");
        assertThat(cosmosJsonConnectionString.getProperty("any").isPresent()).isFalse();
    }
}