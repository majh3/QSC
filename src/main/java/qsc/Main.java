package qsc;

import org.apache.commons.cli.*;

public class Main {
    public static final String DEFAULT_PATH = "empty";
    public static final int DEFAULT_THREADS = 1;
    public static final int HELP_WIDTH = 125;
    public static final int DEFAULT_FREQUENCY = 500;
    public static final int DEFAULT_RULE_LEN = 3;
    public static final int DEFAULT_MINE_TIME = 30;
    public static final String DEFAULT_DB_TYPE = "duckdb";
    public static final String DEFAULT_DB_INFO = "./dbfile";
    public static final String DEFAULT_COMPRESS_METHOD = "method";
    public static final String DEFAULT_STATS_FILENAME = "stats.csv";
    public static final String DEFAULT_ACQUISITION_FUNCTION = "qehvi";
    public static final int DEFAULT_ALPHA = 20;

    private static final String SHORT_OPT_HELP = "h";
    private static final String SHORT_OPT_INPUT = "I";
    private static final String SHORT_OPT_ONLY_VALIDATION = "V";
    private static final String SHORT_OPT_BASE = "B";
    private static final String SHORT_OPT_STATS = "s";
    private static final String SHORT_OPT_DB_TYPE = "d";
    private static final String SHORT_OPT_DB_INFO = "i";
    private static final String SHORT_OPT_DB_INFO_ORIGIN = "o";
    private static final String SHORT_OPT_ACQUISITION_FUNCTION = "a";
    private static final String LONG_OPT_HELP = "help";
    private static final String LONG_OPT_INPUT = "input";
    private static final String LONG_OPT_ACQUISITION_FUNCTION = "acquisition_function";
    private static final String LONG_OPT_ONLY_VALIDATION = "onlyval";
    private static final String LONG_OPT_STATS = "stats";
    private static final String LONG_OPT_BASE = "base";
    private static final String LONG_OPT_DB_TYPE = "db";
    private static final String LONG_OPT_DB_INFO = "dbinfo";
    private static final String LONG_OPT_DB_INFO_ORIGIN = "dbinfo-origin";
    private static final Option OPTION_HELP = Option.builder(SHORT_OPT_HELP).longOpt(LONG_OPT_HELP)
            .desc("Display this help").build();
    private static final Option OPTION_INPUT_PATH = Option.builder(SHORT_OPT_INPUT).longOpt(LONG_OPT_INPUT)
            .numberOfArgs(2).argName("path> <name").type(String.class)
            .desc("The path of the root directory of the program").build();
    private static final Option OPTION_ONLY_VALIDATION = Option.builder(SHORT_OPT_ONLY_VALIDATION).longOpt(LONG_OPT_ONLY_VALIDATION)
            .desc("Only validate the result").build();
    private static final Option OPTION_STATS = Option.builder(SHORT_OPT_STATS).longOpt(LONG_OPT_STATS)
            .argName("stats filename").hasArg().type(String.class).desc("The stats filename").build();
    private static final Option OPTION_BASE = Option.builder(SHORT_OPT_BASE).longOpt(LONG_OPT_BASE)
            .argName("base path").hasArg().type(String.class).desc("The base path").build();
    private static final Option OPTION_DB_TYPE = Option.builder(SHORT_OPT_DB_TYPE).longOpt(LONG_OPT_DB_TYPE)
            .argName("db type").hasArg().type(String.class).desc("The db type").build();
    private static final Option OPTION_DB_INFO = Option.builder(SHORT_OPT_DB_INFO).longOpt(LONG_OPT_DB_INFO)
            .argName("db info").hasArg().type(String.class).desc("The db info").build();
    private static final Option OPTION_DB_INFO_ORIGIN = Option.builder(SHORT_OPT_DB_INFO_ORIGIN).longOpt(LONG_OPT_DB_INFO_ORIGIN)
            .argName("db info origin").hasArg().type(String.class).desc("The origin db info (optional)").build();
    private static final Option OPTION_ACQUISITION_FUNCTION = Option.builder(SHORT_OPT_ACQUISITION_FUNCTION).longOpt(LONG_OPT_ACQUISITION_FUNCTION)
            .argName("acquisition function").hasArg().type(String.class).desc("The acquisition function").build();
    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        QSC queryComp = parseArgs(options, args);
        if (null != queryComp) {
            queryComp.run();
        }
        System.exit(0);
    }

    protected static QSC parseArgs(Options options, String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption(OPTION_HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(HELP_WIDTH);
            formatter.printHelp("java -jar querycomp.jar", options, true);
            return null;
        }

        String input_path = DEFAULT_PATH;
        String input_kb_name = null;
        if (cmd.hasOption(OPTION_INPUT_PATH)) {
            String[] values = cmd.getOptionValues(OPTION_INPUT_PATH);
            input_path = values[0];
            input_kb_name = values[1];
            System.out.printf("Input path set to: %s/%s\n", input_path, input_kb_name);
        }
        if (null == input_kb_name) {
            System.err.println("Missing input KB name");
            return null;
        }
        String statsFilename = DEFAULT_STATS_FILENAME;
        if (cmd.hasOption(OPTION_STATS)) {
            String value = cmd.getOptionValue(OPTION_STATS);
            if (null != value) {
                statsFilename = value;
                
            }
        }
        System.out.println("Stats filename set to: " + statsFilename);
        boolean only_val = cmd.hasOption(SHORT_OPT_ONLY_VALIDATION);
        if(only_val){
            System.out.println("Only validate the result");
        }{
            System.out.println("Validate the result after compression");
        }
        String base = DEFAULT_PATH;
        if(cmd.hasOption(SHORT_OPT_BASE)){
            base = cmd.getOptionValue(SHORT_OPT_BASE);
        }
        System.out.println("Base path set to: " + base);
        String acquisition_function = DEFAULT_ACQUISITION_FUNCTION;
        if(cmd.hasOption(SHORT_OPT_ACQUISITION_FUNCTION)){
            acquisition_function = cmd.getOptionValue(SHORT_OPT_ACQUISITION_FUNCTION);
        }
        System.out.println("Acquisition function set to: " + acquisition_function);
        String db_type = DEFAULT_DB_TYPE;
        if(cmd.hasOption(SHORT_OPT_DB_TYPE)){
            db_type = cmd.getOptionValue(SHORT_OPT_DB_TYPE);
        }
        System.out.println("DB type set to: " + db_type);
        String db_info = DEFAULT_DB_INFO;
        if(cmd.hasOption(SHORT_OPT_DB_INFO)){
            db_info = cmd.getOptionValue(SHORT_OPT_DB_INFO);
        }
        System.out.println("DB info set to: " + db_info);
        String db_info_origin = db_info;
        if (cmd.hasOption(SHORT_OPT_DB_INFO_ORIGIN)) {
            db_info_origin = cmd.getOptionValue(SHORT_OPT_DB_INFO_ORIGIN);
        }
        System.out.println("Origin DB info set to: " + db_info_origin);

        QSCConfig config = new QSCConfig(
                input_path, input_kb_name, statsFilename,
                only_val, base, db_type, db_info, acquisition_function
        );
        config.db_info_origin = db_info_origin;
        return new QSC(config);
    }

    protected static Options buildOptions() {
        Options options = new Options();

        options.addOption(OPTION_HELP);
        options.addOption(OPTION_STATS);
        options.addOption(OPTION_INPUT_PATH);

        options.addOption(OPTION_ONLY_VALIDATION);
        options.addOption(OPTION_BASE);
        options.addOption(OPTION_DB_TYPE);
        options.addOption(OPTION_DB_INFO);
        options.addOption(OPTION_DB_INFO_ORIGIN);
        options.addOption(OPTION_ACQUISITION_FUNCTION);
        return options;
    }
}
