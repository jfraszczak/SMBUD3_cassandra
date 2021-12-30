import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.MalformedURLException;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class Main {

    public static <RegularInsert> void main(String[] args) throws MalformedURLException {
        String hostname = "34.141.248.85";
        int port = 9042;
        String region = "europe-west4";
        String keyspace = "mykeyspace";
        String tableName = "vaccinations";

        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress(hostname, port));
        builder.withLocalDatacenter(region);
        CqlSession session = builder.build();

        CSVReader csvReader = new CSVReader(keyspace, tableName, session);
        csvReader.main();

        DataExtractor dataExtractor = new DataExtractor(keyspace, tableName, session);
        dataExtractor.extract("2020-12-27", "ABR");
    }
}
