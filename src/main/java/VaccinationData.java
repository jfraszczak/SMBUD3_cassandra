import org.apache.commons.csv.CSVRecord;

public class VaccinationData {

    public String administrationDate;
    public String supplier;
    public String area;
    public String ageGroup;

    public int maleCount;
    public int femaleCount;
    public int firstDoses;
    public int secondDoses;
    public int postInfectionDoses;
    public int boosterDoses;

    public String nuts1Code;
    public String nuts2Code;
    public int regionIstatCode;
    public String regionName;

    public VaccinationData(CSVRecord csvRecord){
        this.administrationDate = csvRecord.get("data_somministrazione");
        this.supplier = csvRecord.get("fornitore");
        this.area = csvRecord.get("area");
        this.ageGroup = csvRecord.get("fascia_anagrafica");

        this.maleCount =  Integer.parseInt(csvRecord.get("sesso_maschile"));
        this.femaleCount =  Integer.parseInt(csvRecord.get("sesso_femminile"));
        this.firstDoses = Integer.parseInt(csvRecord.get("prima_dose"));
        this.secondDoses = Integer.parseInt(csvRecord.get("seconda_dose"));
        this.postInfectionDoses = Integer.parseInt(csvRecord.get("pregressa_infezione"));
        this.boosterDoses = Integer.parseInt(csvRecord.get("dose_addizionale_booster"));

        this.nuts1Code = csvRecord.get("codice_NUTS1");
        this.nuts2Code = csvRecord.get("codice_NUTS2");
        this.regionIstatCode = Integer.parseInt(csvRecord.get("codice_regione_ISTAT"));
        this.regionName = csvRecord.get("nome_area");
    }



}
