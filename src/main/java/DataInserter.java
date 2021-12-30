import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import jnr.ffi.annotations.In;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class DataInserter {

    private String keyspace;
    private String tableName;
    private CqlSession session;

    public DataInserter(String keyspace, String tableName, CqlSession session){
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.session = session;
    }

    private void insert(
            String vaccineId,
            String patientId,
            String area,
            String supplier,
            String administrationDate,
            String ageGroup,
            String sex,
            String doseType,
            String nuts1Code,
            String nuts2Code,
            int regionIstatCode,
            String regionName) {

        RegularInsert insert = insertInto(keyspace, tableName)
                .value("vaccine_id", literal(vaccineId))
                .value("patient_id", literal(patientId))
                .value("area", literal(area))
                .value("supplier", literal(supplier))
                .value("administration_date", literal(administrationDate))
                .value("age_group", literal(ageGroup))
                .value("sex", literal(sex))
                .value("dose_type", literal(doseType))
                .value("nuts1_code", literal(nuts1Code))
                .value("nuts2_code", literal(nuts2Code))
                .value("region_istat_code", literal(regionIstatCode))
                .value("region_name", literal(regionName));

        SimpleStatement statement = insert.build();
        session.execute(statement);


        String sexCounterToUpdate;
        if(sex.equals("male")) sexCounterToUpdate = "male_count";
        else sexCounterToUpdate = "female_count";

        String doseTypeCounterToUpdate;
        if(doseType.equals("first_dose")) doseTypeCounterToUpdate = "first_doses";
        else if(doseType.equals("second_dose")) doseTypeCounterToUpdate = "second_doses";
        else if(doseType.equals("post_infection_dose")) doseTypeCounterToUpdate = "post_infection_doses";
        else doseTypeCounterToUpdate = "booster_doses";




        SimpleStatement update = QueryBuilder.update(keyspace,"vaccinations_counter")
                .increment(sexCounterToUpdate)
                .increment(doseTypeCounterToUpdate)
                .whereColumn("administration_date").isEqualTo(literal(administrationDate))
                .whereColumn("area").isEqualTo(literal(area))
                .whereColumn("supplier").isEqualTo(literal(supplier))
                .whereColumn("age_group").isEqualTo(literal(ageGroup))
                .whereColumn("nuts1_code").isEqualTo(literal(nuts1Code))
                .whereColumn("nuts2_code").isEqualTo(literal(nuts2Code))
                .whereColumn("region_istat_code").isEqualTo(literal(regionIstatCode))
                .whereColumn("region_name").isEqualTo(literal(regionName))
                .build();

        session.execute(update);

        System.out.println("INSERTED RECORD WITH ID: " + vaccineId);

    }

    private String randomDoseType(ArrayList<String> dosesTypes, ArrayList<Integer> dosesTypesCount){

        int numberOfAvailableTypes = 0;
        for (Integer count : dosesTypesCount) {
            if (count > 0) numberOfAvailableTypes++;
        }

        int randomIndex = (int)(Math.random() * numberOfAvailableTypes);
        String doseType = "";

        for (int i = 0; i < dosesTypesCount.size(); i++) {
            if (dosesTypesCount.get(i) > 0){
                if(randomIndex == 0){
                    doseType = dosesTypes.get(i);
                    break;
                }
                randomIndex--;
            }
        }

        return doseType;
    }

    public void generateSingleRecords(VaccinationData vaccinationData) throws InterruptedException {
        Integer maleCount =  vaccinationData.maleCount;
        Integer femaleCount =  vaccinationData.femaleCount;
        Integer firstDoses = vaccinationData.firstDoses;
        Integer secondDoses = vaccinationData.secondDoses;
        Integer postInfectionDoses = vaccinationData.postInfectionDoses;
        Integer boosterDoses = vaccinationData.boosterDoses;

        ArrayList<String> dosesTypes = new ArrayList<>(4);
        dosesTypes.add("first_dose");
        dosesTypes.add("second_dose");
        dosesTypes.add("post_infection_dose");
        dosesTypes.add("booster_dose");

        ArrayList<Integer> dosesTypesCount = new ArrayList<>(4);
        dosesTypesCount.add(firstDoses);
        dosesTypesCount.add(secondDoses);
        dosesTypesCount.add(postInfectionDoses);
        dosesTypesCount.add(boosterDoses);

        int numberOfDoses = maleCount + femaleCount;

        String sex = "";
        String doseType = "";

        for(int i = 0; i < numberOfDoses; i++){

            if(maleCount > 0 && femaleCount > 0){
                if(Math.random() < 0.5) {
                    sex = "male";
                    maleCount--;
                }
                else{
                    sex = "female";
                    femaleCount--;
                }
            }
            else if(maleCount > 0 && femaleCount == 0) sex = "male";
            else if(maleCount == 0 && femaleCount > 0) sex = "female";

            doseType = randomDoseType(dosesTypes, dosesTypesCount);

            UUID uuid = UUID.randomUUID();
            String vaccineId = uuid.toString();

            uuid = UUID.randomUUID();
            String patientId = uuid.toString();

            insert(vaccineId,
                    patientId,
                    vaccinationData.area,
                    vaccinationData.supplier,
                    vaccinationData.administrationDate,
                    vaccinationData.ageGroup,
                    sex,
                    doseType,
                    vaccinationData.nuts1Code,
                    vaccinationData.nuts2Code,
                    vaccinationData.regionIstatCode,
                    vaccinationData.regionName
                    );

            TimeUnit.SECONDS.sleep(1);
        }




    }


}
