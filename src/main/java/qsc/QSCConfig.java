package qsc;

public class QSCConfig {

    public final String basePath;

    public final String kbName;

    public final String statsFilename;

    public String db_type;
    public String db_info;
    public String db_info_origin;
    public boolean only_val;
    public String base;
    public String acquisition_function;
    public QSCConfig(
            String basePath, String kbName, String statsFilename, boolean only_val,
            String base, String db_type, String db_info, String acquisition_function
    ) {
        this.basePath = basePath;
        this.kbName = kbName;
        this.statsFilename = statsFilename;
        this.only_val = only_val;
        this.base = base;
        this.db_type = db_type;
        this.db_info = db_info;
        this.db_info_origin = db_info;
        this.acquisition_function = acquisition_function;
    }
}
