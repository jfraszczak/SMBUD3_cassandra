import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.util.ArrayList;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public class DataExtractor {

    private String keyspace;
    private String tableName;
    private CqlSession session;

    public DataExtractor(String keyspace, String tableName, CqlSession session){
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.session = session;
    }

    public void extract(String administrationDate, String area){

        ArrayList<String> suppliers = new ArrayList<String>();
        suppliers.add("Pfizer/BioNTech");
        suppliers.add("Moderna");
        suppliers.add("Janssen");
        suppliers.add("Vaxzevria (AstraZeneca)");

        ArrayList<String> ageGroups = new ArrayList<String>();
        ageGroups.add("20-29");
        ageGroups.add("30-39");
        ageGroups.add("40-49");
        ageGroups.add("50-59");
        ageGroups.add("60-69");
        ageGroups.add("70-79");
        ageGroups.add("80-89");
        ageGroups.add("90+");

        for(String supplier : suppliers){
            for(String ageGroup : ageGroups){

                SimpleStatement statement = selectFrom(keyspace, "vaccinations_counter")
                        .all()
                        .whereColumn("administration_date").isEqualTo(literal(administrationDate))
                        .whereColumn("area").isEqualTo(literal(area))
                        .whereColumn("supplier").isEqualTo(literal(supplier))
                        .whereColumn("age_group").isEqualTo(literal(ageGroup))
                        .build();

                ResultSet rs = session.execute(statement);

                try {
                    Row result = rs.one();

                    System.out.println(administrationDate + ", " +
                            area + ", " +
                            supplier + ", " +
                            ageGroup + ", " +
                            result.getString("region_name") + ", " +
                            result.getString("nuts1_code") + ", " +
                            result.getString("nuts2_code") + ", " +
                            result.getInt("region_istat_code") + ", " +
                            result.getLong("male_count") + ", " +
                            result.getLong("female_count") + ", " +
                            result.getLong("first_doses") + ", " +
                            result.getLong("second_doses") + ", " +
                            result.getLong("post_infection_doses") + ", " +
                            result.getLong("booster_doses"));

                } catch (java.lang.NullPointerException e) {}


            }
        }


    }
}
