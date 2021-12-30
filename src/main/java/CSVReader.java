import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class CSVReader {

    private String keyspace;
    private String tableName;
    private CqlSession session;

    public CSVReader(String keyspace, String tableName, CqlSession session){
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.session = session;
    }

    public void main(String... args) throws MalformedURLException {
        URL url = new URL("https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-latest.csv");

        CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase();

        try(CSVParser csvParser = CSVParser.parse(url, StandardCharsets.UTF_8, csvFormat)) {
            for(CSVRecord csvRecord : csvParser) {

                VaccinationData vaccinationData = new VaccinationData(csvRecord);
                System.out.println(vaccinationData.administrationDate + "," + vaccinationData.supplier + "," + vaccinationData.area + "," + vaccinationData.ageGroup);

                DataInserter dataInserter = new DataInserter(this.keyspace, this.tableName, this.session);
                dataInserter.generateSingleRecords(vaccinationData);

            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}