package qsc;

import de.unima.ki.anyburl.Learn;
import fr.lirmm.graphik.graal.api.core.RuleSet;
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet;
import fr.lirmm.graphik.graal.io.dlp.DlgpParser;
import fr.lirmm.graphik.graal.rulesetanalyser.Analyser;
import fr.lirmm.graphik.graal.rulesetanalyser.util.AnalyserRuleSet;
import qsc.db.DatabaseManager;
import qsc.dbrule.*;
import qsc.hyper.*;
import qsc.util.ArrayTreeSet;
import qsc.util.Monitor;
import qsc.util.Pair;
import qsc.util.Quadruple;
import qsc.util.Triple;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


import java.util.ArrayList;
import java.util.List;
import java.nio.file.Paths;
import java.util.*;

public class QSC {
    public static final boolean DEBUG = false;
    public static final int MAX_RAW_SIZE = 1500;
    public int last_x = 0;
    public static final int THREADS = 32;
    public static final String DATA_DIR_NAME = "datasets_csv";
    public static final String OUT_DIR_NAME = "expriments_output";
    public static final String RDF_FILE_NAME = "entity.tsv";
    public static final int INIT_SIZE = 10;
    public static final int N_ITERATIONS = 10;
    private DatabaseManager db;
    private DatabaseManager origin_db;

    protected final QSCConfig config;

    protected RuleSet ruleSet = new LinkedListRuleSet();
    protected DBRule[] dbrules;
    /**
     * Reference point for BO (original space): [compression_ratio, latency].
     * If null, Python side will fall back to its default selection logic.
     */
    private double[] boReferencePoint = null;
    public QSC(QSCConfig config) {
        this.config = config;
    }

    protected void showConfig() {
        Monitor.logINFO("Base Path:\t"+config.basePath);
        Monitor.logINFO("KB Name:\t"+config.kbName);
        Monitor.logINFO("Stats Filename:\t"+config.statsFilename);
        Monitor.logINFO("Only Validation:\t"+config.only_val);
        Monitor.logINFO("Base Path:\t"+config.base);
        Monitor.logINFO("DB Type:\t"+config.db_type);
        Monitor.logINFO("DB Info:\t"+config.db_info);
        Monitor.logINFO("Acquisition Function:\t"+config.acquisition_function);
        Monitor.logINFO("\n");
    }
    private String getDatabasePath(){

        return config.basePath  + "/" + DATA_DIR_NAME +"/"+ config.kbName;
    }
    private String getNTPath(){

        return config.basePath  + "/" + DATA_DIR_NAME + "/" + config.kbName;
    }
    private String getRDFPath(){
        return getNTPath() + "/" + RDF_FILE_NAME;
    }
    private String getExtratedRuleDumpPath(){

        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename +"/"  + config.kbName + "_rules";
    }

    private String getCompressedPath(){

        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename +"/"  + config.kbName + "_compressed";
    }
    private String getFilteredRulesPath(){

        String exp_group_name = config.statsFilename.split("#")[0];
        String fus_dir = config.statsFilename.split("#")[0] + "#"+config.kbName+"_random";
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + fus_dir +"/"  + config.kbName + "_compressed";
    }
    private String getFilteredRulesPath_2(){

        String exp_group_name = config.statsFilename.split("#")[0];
        String fus_dir = config.statsFilename.split("#")[0] + "#"+config.kbName+"_random";
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + fus_dir +"/filtered_rules";
    }
    private String getMonitorPath(){

        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME+"/"+ exp_group_name +"/" + config.kbName +"/" + config.statsFilename;
    }
    private String getBOPath(){

        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME+"/"+ exp_group_name +"/" + config.kbName +"/BO"; 
    }

    private DBRule fr2DBrule(fr.lirmm.graphik.graal.api.core.Rule fr_rule, int id){
        String rule = fr_rule.toString();
        rule = rule.replace("\\2", "").replace("\1", "").split("\\] \\[")[1].replace("]", "").replace("[", "").replace("\"", "");
        rule = rule.split(" -> ")[1] + " :- " + rule.split(" -> ")[0];
        return new DBRule(rule, id);
    }

    private boolean checkRuleLegal(DBRule rule){

        for(Argument arg:rule.head.args){
            if(!arg.isConstant){
                boolean inBody = false;
                for(Predicate body_pred:rule.body){
                    for(Argument body_arg:body_pred.args){
                        if(body_arg.isConstant){
                            continue;
                        }
                        if(body_arg.name.equals(arg.name)){
                            inBody = true;
                            break;
                        }
                    }
                    if(inBody){
                        break;
                    }
                }
                if(!inBody){
                    return false;
                }
            }
        }
        return true;
    }
    private Pair<List<DBRule>, RuleSet> filterFUS_compress_random_sequntial(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size) throws Exception {
        return filterFUS_compress_random_sequntial(added_rules_with_supp, db, origin_db, all_size, 500);
    }
    private Pair<List<DBRule>, RuleSet> filterFUS_compress_random_sequntial(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size, int maxRules) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        long init_start = System.currentTimeMillis();
        int rule_count_sum = 0;
        int rule_count_contribute = 0;
        int rule_count = 0;
        int patience = 0;
        int patience_last = 0;
        int bound;
        if(DEBUG){
            bound = 30;
        }else{
            bound = 300; //300
        }
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        long filter_start = System.currentTimeMillis();
        Monitor.logINFO("[]init time: "+(filter_start - init_start));
        int[] supps = new int[sorted_rules.size()];
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = origin_db.checkRuleNeg(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);

            filtered_dbrules.add(this_dbrule);
            filtered_ruleSet.add(this_rule_fr);

            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }
            patience_last = patience;

            db.setRuleSet(ruleSet);
            // db.offline_rewrite();
            
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted, 1);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            all_deleted_list.add(quadruple.getFourth().copy());
            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Long end_compress = System.currentTimeMillis();

            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }
            if(rule_count_sum >= bound || (rule_count_sum >= maxRules)){
                Monitor.logINFO("Reached maximum rule count limit: " + maxRules);
                break;
            }
        }
        long filter_end = System.currentTimeMillis();
        Monitor.logINFO("[]filter time: "+(filter_end - filter_start));
        this.dbrules = dbruleSet.toArray(new DBRule[0]);

        db.fastRecoverFromOriginal(origin_db);
        

        BayesianOptimizationResult result = performBayesianOptimization(
            dbruleSet, ruleSet, 
            CRs, all_deleted_list, db, origin_db, 
            true, 
            true, 
            init_start
        );
        
        return new Pair<List<DBRule>, RuleSet> (
            result.filteredDbruleSet,result.finalRuleSet);
    }

    private Quadruple<List<DBRule>, RuleSet, List<Double>, List<ArrayTreeSet>> filterFUS_compress_wo_bo(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size) throws Exception {
        return filterFUS_compress_wo_bo(added_rules_with_supp, db, origin_db, all_size, null);
    }
    
    private Quadruple<List<DBRule>, RuleSet, List<Double>, List<ArrayTreeSet>> filterFUS_compress_wo_bo(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size, Integer maxRules) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        long init_start = System.currentTimeMillis();
        int rule_count_sum = 0;
        int rule_count_contribute = 0;
        int rule_count = 0;
        int patience = 0;
        int patience_last = 0;
        int bound;
        if(DEBUG){
            bound = 30;
        }else{
            bound = 300; //300
        }
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        long filter_start = System.currentTimeMillis();
        Monitor.logINFO("[]init time: "+(filter_start - init_start));
        int[] supps = new int[sorted_rules.size()];
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = origin_db.checkRuleNeg(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);

            filtered_dbrules.add(this_dbrule);
            filtered_ruleSet.add(this_rule_fr);

            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }
            patience_last = patience;

            db.setRuleSet(ruleSet);
            // db.offline_rewrite();
            
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted, 1);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            all_deleted_list.add(quadruple.getFourth().copy());
            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Long end_compress = System.currentTimeMillis();

            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }
            
            // Stop if maximum rule count is specified and reached
            if(dbruleSet.size() >= bound || (maxRules != null && rule_count_sum >= maxRules)){
                Monitor.logINFO("Reached maximum rule count limit: " + maxRules);
                break;
            }
        }
        long filter_end = System.currentTimeMillis();
        Monitor.logINFO("[]filter time: "+(filter_end - filter_start));
        this.dbrules = dbruleSet.toArray(new DBRule[0]);
        
        return new Quadruple<List<DBRule>, RuleSet, List<Double>, List<ArrayTreeSet>> (dbruleSet, ruleSet, CRs, all_deleted_list);
    }
    private Triple<List<DBRule>, RuleSet, int[]> filterFUS_compress_random_sequntial_wo_bo(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size) throws Exception {
        return filterFUS_compress_random_sequntial_wo_bo(added_rules_with_supp, db, origin_db, all_size, false);
    }

    private Triple<List<DBRule>, RuleSet, int[]> filterFUS_compress_random_sequntial_wo_bo(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size, boolean remove) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        long init_start = System.currentTimeMillis();
        int rule_count_sum = 0;
        int rule_count_contribute = 0;
        int rule_count = 0;
        int patience = 0;
        int patience_last = 0;
        int bound;
        if(DEBUG){
            bound = 30;
        }else{
            bound = 300; //300
        }
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        long filter_start = System.currentTimeMillis();
        Monitor.logINFO("[]init time: "+(filter_start - init_start));
        ArrayList<Integer> supps = new ArrayList<>();
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            int supp = origin_db.checkRuleNeg_supp_sql(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            boolean Neg = supp < 0;
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);

            filtered_dbrules.add(this_dbrule);
            filtered_ruleSet.add(this_rule_fr);

            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }
            patience_last = patience;

            db.setRuleSet(ruleSet);
            // db.offline_rewrite();
            
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted, 1);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            supps.add((Integer) supp);
            if(remove){
                db.removelTuples_Batch(quadruple.getFourth());
            }
            all_deleted_list.add(quadruple.getFourth().copy());
            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Long end_compress = System.currentTimeMillis();
            
            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }
        }
        long filter_end = System.currentTimeMillis();
        Monitor.logINFO("[]filter time: "+(filter_end - filter_start));
        this.dbrules = dbruleSet.toArray(new DBRule[0]);
        db.rules = this.dbrules;
        // rerank the rules by the supp descending order
        List<DBRule> reranked_dbrules = new ArrayList<>();
        RuleSet reranked_ruleSet = new LinkedListRuleSet();
        List<Integer> reranked_supps = new ArrayList<>();
        
        // Create index list and sort by support in descending order
        List<Integer> indices = new ArrayList<>();
        for(int i = 0; i < dbruleSet.size(); i++){
            indices.add(i);
        }
        indices.sort((a, b) -> Integer.compare(supps.get(b), supps.get(a))); // Sort in descending order
        
        // Rearrange according to sorted indices
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        List<fr.lirmm.graphik.graal.api.core.Rule> fr_rules_list = new ArrayList<>();
        while(fr_rules.hasNext()){
            fr_rules_list.add(fr_rules.next());
        }
        
        for(int i : indices){
            reranked_dbrules.add(dbruleSet.get(i));
            reranked_ruleSet.add(fr_rules_list.get(i));
            reranked_supps.add(supps.get(i));
        }

        return new Triple<List<DBRule>, RuleSet, int[]> (
            reranked_dbrules, reranked_ruleSet, reranked_supps.stream().mapToInt(Integer::intValue).toArray());
    }
    // Bayesian optimization result class
    private static class BayesianOptimizationResult {
        public final List<DBRule> filteredDbruleSet;
        public final RuleSet finalRuleSet;
        public final double bestRuleNum;
        
        public BayesianOptimizationResult(List<DBRule> filteredDbruleSet, RuleSet finalRuleSet, double bestRuleNum) {
            this.filteredDbruleSet = filteredDbruleSet;
            this.finalRuleSet = finalRuleSet;
            this.bestRuleNum = bestRuleNum;
        }
    }
    // Unified Bayesian optimization function
    private BayesianOptimizationResult performBayesianOptimization(
            List<DBRule> dbruleSet, 
            RuleSet ruleSet, 
            List<Double> CRs, 
            List<ArrayTreeSet> all_deleted_list,
            DatabaseManager db, 
            DatabaseManager origin_db,
            boolean useSequentialOptimization,
            boolean enableRecompressTest,
            long init_start) throws Exception {
        
        long bo_start = System.currentTimeMillis();
        BayesianOptimizationPy optimizer = new BayesianOptimizationPy(
            config.acquisition_function, getBOPath(), config.basePath, boReferencePoint
        );
        long bo_end = System.currentTimeMillis();
        Monitor.optTime += (bo_end - bo_start);
        
        double[][] pred_X = new double[CRs.size()][2];
        for(int i = 0; i < CRs.size(); i++){
            pred_X[i][0] = i+1;
            pred_X[i][1] = CRs.get(i);
        }
        
        int ninit = CRs.size() > INIT_SIZE? INIT_SIZE:CRs.size();
        int niter;
        if(DEBUG){
            niter = 2;
            ninit = 2;
        }else{
            if(CRs.size() > INIT_SIZE){
                niter = CRs.size()>N_ITERATIONS? N_ITERATIONS:CRs.size()-INIT_SIZE;
            }else{
                niter = 0;
            }
        }
        
        double[] best_x;
        // if(useSequentialOptimization){
        //     best_x = optimizer.optimize_sequntial(ninit, niter, pred_X, this, all_deleted_list);
        // }else{
        //     best_x = optimizer.optimize(ninit, niter, pred_X, this, all_deleted_list);
        // }
        best_x = optimizer.optimize(ninit, niter, pred_X, this, all_deleted_list);
        long op_end = System.currentTimeMillis();
        Monitor.logINFO("[]bo time: "+(op_end - bo_start));
        
        // best_x[0] represents the number of selected rules (needs integer conversion and boundary protection)
        int best_rule_num = (int) Math.round(best_x[0]);
        best_rule_num = Math.max(0, Math.min(best_rule_num, dbruleSet.size()));
        this.dbrules = dbruleSet.subList(0, best_rule_num).toArray(new DBRule[0]);
        
        RuleSet final_ruleSet = new LinkedListRuleSet();
        int index = 0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext() && index < best_rule_num){
            final_ruleSet.add(fr_rules.next());
            index++;
        }
        
        String[] fus_string = new String[this.dbrules.length];
        this.ruleSet = final_ruleSet;
        for(int i = 0; i < this.dbrules.length; i++) {
            fus_string[i] = dbruleSet.get(i).toString();
        }
        
        db.rules = dbruleSet.toArray(new DBRule[0]);
        Monitor.logINFO("final test for best rule num: "+best_rule_num);
        db.fastRecoverFromOriginal(origin_db);
        
        System.out.println("best_x[0]: "+best_x[0]);
        getQueryTime(best_x[0], all_deleted_list, 0);
        
        // if(enableRecompressTest){
        //     getQueryTime_recompress(best_x[0], all_deleted_list);
        // }
        
        long final_end = System.currentTimeMillis();
        Monitor.logINFO("[]final time: "+(final_end - init_start));
        
        return new BayesianOptimizationResult(
            dbruleSet.subList(0, best_rule_num), 
            this.ruleSet, 
            best_x[0]
        );
    }
    private void compressRuntime(DatabaseManager db, DBRule[] rules, DatabaseManager origin_db) throws Exception {

        ArrayTreeSet deleted = new ArrayTreeSet();
        for(DBRule rule:rules){
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(rule, 0, 10000, deleted, 1);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
        }
        db.removelTuples_Batch(deleted);
        // print the compression ratio
        Monitor.logINFO("compression ratio: "+(double)db.countAllRecords()/(double)origin_db.countAllRecords());
    }
    private Quadruple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>> filterFUS_compress_random(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int all_size) throws Exception {

        ArrayList<String> sorted_rules_ = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules_.sort((o1, o2) -> {
            int s1 = added_rules_with_supp.get(o1);
            int s2 = added_rules_with_supp.get(o2);
            if (s1 != s2) {
                return Integer.compare(s2, s1); 
            }

            int len1 = o1.split(":-")[1].split(", ").length;
            int len2 = o2.split(":-")[1].split(", ").length;
            return Integer.compare(len1, len2);
        });
        int rule_count_perf = 0;
        int max_raw_size = MAX_RAW_SIZE;
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        List<DBRule> raw_dbrules = new ArrayList<>();
        HashMap<String, Integer> raw_rules_with_supp = new HashMap<>();
        for(String rule:sorted_rules_){

            DBRule this_dbrule = new DBRule(rule, rule_count_perf);
            if(!checkRuleLegal(this_dbrule)){
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            raw_dbrules.add(this_dbrule);

            long Neg_start = System.currentTimeMillis();
            int supp=0;
            try{
                supp = origin_db.checkRuleNeg_supp_sql(this_dbrule);
            }catch(Exception e){
                System.out.println("error: "+rule);
                continue;
            }
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(supp < 0){
                continue;
            }
            raw_rules_with_supp.put(rule, supp);
            rule_count_perf++;
            if(rule_count_perf > max_raw_size){
                break;
            }
        }

        ArrayList<String> sorted_rules = new ArrayList<>(raw_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            int s1 = raw_rules_with_supp.get(o1);
            int s2 = raw_rules_with_supp.get(o2);
            if (s1 != s2) {
                return Integer.compare(s2, s1); 
            }

            int len1 = o1.split(":-")[1].split(", ").length;
            int len2 = o2.split(":-")[1].split(", ").length;
            return Integer.compare(len1, len2);
        });

        for (int i = 0; i < sorted_rules.size(); i++) {
            String baseRule = sorted_rules.get(i);
            int baseSupp = raw_rules_with_supp.get(baseRule);
            int j = i + 1;
            while (j < sorted_rules.size() && raw_rules_with_supp.get(sorted_rules.get(j)) == baseSupp) {
                String cmpRule = sorted_rules.get(j);

                DBRule r1 = new DBRule(baseRule, 0);
                DBRule r2 = new DBRule(cmpRule, 0);

                long st = System.currentTimeMillis();
                int entailed = db.checkRulesSubsumedEach(r1, r2);
                long ed = System.currentTimeMillis();
                Monitor.RuleEntailTime += (ed - st);

                if (entailed < 0) {          
                    raw_rules_with_supp.remove(cmpRule);
                    sorted_rules.remove(j);
                } else if (entailed > 0) {   
                    raw_rules_with_supp.remove(baseRule);
                    sorted_rules.set(i, cmpRule);
                    baseRule = cmpRule;

                    sorted_rules.remove(j);
                } else {
                    j++;  
                }
            }
        }
        int fus_rule_count = 0;
        int rule_count_contribute = 0;
        int rule_count_filtered = 0;
        int rule_count = 0;
        int patience = 0;
        int bound;
        if(DEBUG){
            bound = 30;
        }else{
            bound = 300; 
        }
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();

        Monitor.logINFO("new added avaliable#rules: "+added_rules_with_supp.size());  
        List<DBRule> unfilter_rules = new ArrayList<>();
        for(String rule:sorted_rules){

            rule_count_filtered++;
            assert dbruleSet.size() == ruleSet.size();

            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);

            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };

            fus_rule_count++;
            dbruleSet.add(this_dbrule);

            filtered_dbrules.add(this_dbrule);
            filtered_ruleSet.add(this_rule_fr);

            Long end = System.currentTimeMillis();
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }

            db.setRuleSet(ruleSet);

            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted, 1);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            rule_count_contribute++;
            all_deleted_list.add(quadruple.getFourth().copy());

            Monitor.logINFO("[new rule in]");

            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Monitor.logINFO("this compression ratio: "+compression_ratio);
            Monitor.logINFO("this patience: " + patience);
            Long end_compress = System.currentTimeMillis();
            Monitor.logINFO("this time now: "+(end_compress - all_start));
            Monitor.logINFO("count: "+dbruleSet.size());

            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }

        }
        Monitor.logINFO("rule count all: " + added_rules_with_supp.size());
        Monitor.logINFO("rule count perfect: " + rule_count_perf);
        Monitor.logINFO("rule count sorted: " + sorted_rules.size());
        Monitor.logINFO("rule count filtered: " + rule_count_filtered);
        Monitor.logINFO("rule count fus: " + fus_rule_count);
        Monitor.logINFO("rule count contribute: " + rule_count_contribute);

        this.dbrules = dbruleSet.toArray(new DBRule[0]);

        db.fastRecoverFromOriginal(origin_db);

        long bo_start = System.currentTimeMillis();
        BayesianOptimizationPy optimizer = new BayesianOptimizationPy(
            config.acquisition_function, getBOPath(), config.basePath, boReferencePoint
        );
        long bo_end = System.currentTimeMillis();
        Monitor.optTime +=  (bo_end - bo_start);
        double[][] pred_X = new double[CRs.size()][2];
        for(int i = 0; i < CRs.size(); i++){
            pred_X[i][0] = i+1;
            pred_X[i][1] = CRs.get(i);
        }
        int ninit = CRs.size() > 5? 5:CRs.size();
        int niter;
        if(DEBUG){
            niter = 2;
            ninit = 2;
        }else{
            if(CRs.size() > 5){
                niter = CRs.size()>15? 10:CRs.size()-5;
            }else{
                niter = 0;
            }
        }

        double[] best_x = optimizer.optimize(ninit, niter, pred_X, this, all_deleted_list);

        int best_rule_num = (int) best_x[0];

        this.dbrules = dbruleSet.subList(0, best_rule_num).toArray(new DBRule[0]);
        RuleSet final_ruleSet = new LinkedListRuleSet();
        int index = 0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext()){
            if(index > best_rule_num){
                break;
            }
            final_ruleSet.add(fr_rules.next());
            index++;
        }
        String[] fus_string = new String[dbruleSet.size()];
        this.ruleSet = final_ruleSet;
        for(int i = 0; i < this.dbrules.length; i++) {
            fus_string[i] = dbruleSet.get(i).toString();
        }
        db.rules = dbruleSet.toArray(new DBRule[0]);
        Monitor.logINFO("final test for best rule num: "+best_rule_num);
        db.fastRecoverFromOriginal(origin_db);
        getQueryTime(best_x[0], all_deleted_list, 0);

        Monitor.dump();
        return new Quadruple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>> (dbruleSet.subList(0, best_rule_num), unfilter_rules, this.ruleSet,unfilter_dbrules);
    }

    public double getQueryTime_cache(double x, List<List<Double>> cached_latencies) throws Exception{

        return (double) cached_latencies.get((int) x-1).get(6)/1000000;
    }
    public double getQueryTime(double x, List<ArrayTreeSet> deleted, int last_x_in) throws Exception{
        return getQueryTimeInternal(x, deleted, last_x_in);
    }

    public double getQueryTime(double x, List<ArrayTreeSet> deleted) throws Exception{
        return getQueryTimeInternal(x, deleted, this.last_x);
    }

    private double getQueryTimeInternal(double x, List<ArrayTreeSet> deleted, int last_x_in) throws Exception{
        // x 可能来自 BO 的 double 输出，这里统一做整数化并做边界保护，避免越界/崩溃
        int maxX = Math.min(deleted.size(), deleted.size());
        int xi = (int) Math.round(x);
        xi = Math.max(0, Math.min(xi, maxX));
        int lastXi = Math.max(0, Math.min(last_x_in, maxX));

        RuleSet this_ruleSet = new LinkedListRuleSet();
        int index=0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext() && index < xi){
            this_ruleSet.add(fr_rules.next());
            index++;
        }
        db.setRuleSet(this_ruleSet);
        db.rules = Arrays.copyOfRange(dbrules, 0, xi);
        db.offline_rewrite();

        long delta_start = System.nanoTime();
        ArrayTreeSet delta = new ArrayTreeSet();
        if(xi != lastXi){
            boolean delete = lastXi < xi;
            int start = delete?lastXi:xi;
            int end = delete?xi:lastXi;
            for(int i = start; i < end; i++){
                delta.addAll(deleted.get(i).clone());
            }
            if(delete){
                db.removelTuples_Batch(delta);
            }else{
                db.appendTuples(delta);
            }
        }
        long delta_end = System.nanoTime();

        Monitor.resetQueryLatency();
        db.test_query(origin_db, true, false, false, null, true);

        Monitor.logINFO("##### test selected #####");
        Monitor.logINFO("[]delta time: "+((float)(delta_end - delta_start)/1000000));
        Monitor.logINFO("[]query time: "+((float)Monitor.getQueryLatency()/1000000));
        Monitor.logINFO("[]last_x: "+last_x);
        Monitor.logINFO("[]xi: "+xi);
        if(xi > 0){
            Monitor.logINFO("[]CR: "+((float)db.countAllRecords()/origin_db.countAllRecords()));
            Monitor.logINFO("[]db: "+origin_db.countAllRecords()+", original db: "+origin_db.countAllRecords());
        }else{
            Monitor.logINFO("[]CR: 1.0");
            Monitor.logINFO("[]db: "+db.countAllRecords()+"original db: "+origin_db.countAllRecords());
        }

        this.last_x = xi;
        // return (double) Monitor.getQueryLatency()/1000000;
        return (double) Monitor.getQueryLatency()/Monitor.origin_time;
    }
    public void compressDB_sequntial(DBRule[] rules) throws Exception{
        db.fastRecoverFromOriginal(origin_db);
        ArrayTreeSet deleted_all = new ArrayTreeSet();
        for(int i = 0; i < rules.length; i++){
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(rules[i], 1000000, 1000000, deleted_all, 1);
            deleted_all.addAll(quadruple.getFourth());
        }
        db.removelTuples_Batch(deleted_all);
    }
    public double getQueryTime_recompress(double x) throws Exception{
        RuleSet this_ruleSet = new LinkedListRuleSet();
        int index=0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext()){
            this_ruleSet.add(fr_rules.next());
            index++;
            if(index >= (int)x){
                break;
            }
        }
        db.setRuleSet(this_ruleSet);

        db.rules = Arrays.copyOfRange(dbrules, 0, (int)x);
        db.offline_rewrite();

        long compress_start = System.nanoTime();
        compressDB_sequntial(Arrays.copyOfRange(dbrules, 0, (int)x)); 
        long compress_end = System.nanoTime();
        Monitor.logINFO("[]delta 2 time: "+((float)(compress_end - compress_start)/1000000));
        Monitor.resetQueryLatency();

        db.test_query(origin_db, true, false, false, null, true);

        last_x = (int)x;

        return (double) Monitor.getQueryLatency()/1000000;
    }

    private HashMap<String, Integer> ExtractRulesFromKB(String load_NT_file_path, String rule_path) throws FileNotFoundException, InterruptedException {

        String tmp_train_rdf_path = load_NT_file_path;
        File tmp_extracted_rules_dir = new File(rule_path);
        String rule_dir = rule_path+"/rules";
        if(!tmp_extracted_rules_dir.exists()){
            tmp_extracted_rules_dir.mkdirs();
        }

        File[] rule_files = tmp_extracted_rules_dir.listFiles((dir, name) -> name.startsWith("rules"));
        if(rule_files!=null&&rule_files.length != 0){

            if(rule_files != null){
                for(File old_rule:rule_files){
                    old_rule.delete();
                }
            }   
        }
        if(true){
            Properties anyburl_config = new Properties();

            anyburl_config.setProperty("CONSTANTS_OFF", "true");
            anyburl_config.setProperty("THRESHOLD_CORRECT_PREDICTIONS", 30+"");

            if(DEBUG){
                anyburl_config.setProperty("WORKER_THREADS", "10");
                anyburl_config.setProperty("SNAPSHOTS_AT", "20");
            }else{
                anyburl_config.setProperty("WORKER_THREADS", "100");
                anyburl_config.setProperty("SNAPSHOTS_AT", 200+"");

            }

            anyburl_config.setProperty("THRESHOLD_CONFIDENCE", "1");
            anyburl_config.setProperty("REWRITE_REFLEXIV", "false");
            anyburl_config.setProperty("SAFE_PREFIX_MODE", "true");
            // if(!config.constant){

            //     anyburl_config.setProperty("MAX_LENGTH_GROUNDED", "0");
            //     anyburl_config.setProperty("ZERO_RULES_ACTIVE", "false");
            //     anyburl_config.setProperty("MAX_LENGTH_ACYCLIC", "0");
            // }
            anyburl_config.setProperty("SAMPLE_SIZE", "5000000");
            anyburl_config.setProperty("BEAM_SAMPLING_MAX_BODY_GROUNDINGS", "2000000");

            anyburl_config.setProperty("BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS", "1000000");
            anyburl_config.setProperty("PATH_OUTPUT", rule_dir);
            anyburl_config.setProperty("PATH_TRAINING", tmp_train_rdf_path);

            anyburl_config.setProperty("MAX_LENGTH_CYCLIC", 3+"");
            Learn.extractRules(anyburl_config);
        }

        File dir = new File(rule_path);
        File[] files = dir.listFiles((dir1, name) -> name.startsWith("rules-"));
        HashMap<String, Integer> rules_with_supp = new HashMap<>();
        if (files == null || files.length == 0) {
            System.err.println("No rule files found in: " + rule_path);
            return rules_with_supp;
        }
        Set<String> rule_str_set = new HashSet<>();
        for(File file:files){
            try(BufferedReader reader = new BufferedReader(new FileReader(file))){
                String line;
                while((line = reader.readLine()) != null){
                    rule_str_set.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<String> rules = new ArrayList<>(rule_str_set);
        String this_rule;
        for(String rule:rules){
            String[] ruleParts = rule.split("\t")[3].split(" <= ");

            if(ruleParts.length < 2){
                System.out.println("no body rule: " + rule);
                continue;
            }
            try{
                this_rule = ruleParts[0] + " :- " + ruleParts[1]+".";
                this_rule = this_rule.replace(",e",",").replace("(e", "(");
                this_rule = this_rule.replace(" r", " ").substring(1);

                rules_with_supp.put(this_rule, Integer.parseInt(rule.split("\t")[0]));
            }catch(Exception e){
                throw e;
            }
        }
        return rules_with_supp;
    }

    private void compress_run() throws Exception {

        System.out.println("Monitor file path: " + getMonitorPath());
        Monitor.setDumpPath(getMonitorPath());
        showConfig();

        if(config.only_val){
            Monitor.logINFO("[only val]");
            if(config.base != null && !"empty".equals(config.base)){
                db = new DatabaseManager(getDatabasePath(), config.base, config.db_type, config.db_info, THREADS, THREADS, dbId(2), DEBUG);
            } else {
                db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, THREADS, THREADS, dbId(2), DEBUG);
            }
            origin_db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, THREADS, THREADS*2, dbId(3), DEBUG);
            db.offline_rewrite();
            compressRuntime(db, db.rules, origin_db);

            db.test_query(origin_db, true, true, true, getDatabasePath());
            Monitor.dump();

            db.releaseCon();
            origin_db.releaseCon();
            return;
        }
        origin_db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, THREADS, THREADS*2, dbId(3), DEBUG);
        origin_db.set_readonly_mode();
        String compressed_path = getCompressedPath();
        if(new File(compressed_path).exists()){
            long time_start = System.currentTimeMillis();
            db = new DatabaseManager(compressed_path, config.db_type, config.db_info, THREADS, THREADS, dbId(2), DEBUG);
            long end = System.currentTimeMillis();
            Monitor.loadTime += (end - time_start);
            db.offline_rewrite();
        }else{
            long time_start = System.currentTimeMillis();
            db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, THREADS, THREADS, dbId(1), DEBUG);
            db.set_readonly_mode();
            long db_load_time = System.currentTimeMillis();
            Monitor.loadTime += (db_load_time - time_start);

            Path filter_rules_path = Paths.get(getFilteredRulesPath());
            RuleSet ruleSet = new LinkedListRuleSet();
            List<DBRule> dbruleSet = new Vector<>();

            if(!filter_rules_path.toFile().exists() && !Paths.get(getFilteredRulesPath_2()).toFile().exists()){
                if(!(new File(getRDFPath())).exists()){
                    db.dumpRdf(getNTPath());
                }
                HashMap<String, Integer> sum_relation_records = new HashMap<>();
                for(String rel:db.relation2id.keySet()){
                    sum_relation_records.put(rel, db.countRecords(rel));
                }

                Monitor.logINFO("[exp start]");
                Monitor.resetQueryLatency();
                HashMap<String, Integer> extracted_rules_with_supp = ExtractRulesFromKB(getRDFPath(), getExtratedRuleDumpPath());

                long filter_start = System.currentTimeMillis();

                Quadruple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>> Quintuple = filterFUS_compress_random(extracted_rules_with_supp, db, origin_db, origin_db.original_sum_records);
                long filter_end = System.currentTimeMillis();
                Monitor.filterTime += (filter_end - filter_start);
                dbruleSet = Quintuple.getFirst();
                ruleSet = Quintuple.getThird();
                db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));
                Monitor.logINFO("filtered#rule :"+dbruleSet.size());
            }
            db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));
            db.setRuleSet(ruleSet);
            db.offline_rewrite();
            db.dumpCompressed(compressed_path, dbrules);

            db.dumpRules(compressed_path, dbrules);
            double compression_ratio = (double) db.countAllRecords() / origin_db.countAllRecords();
            Monitor.logINFO("this compression ratio: "+compression_ratio);
            db.test_query(origin_db, true, true, true, getDatabasePath());

        }
        db.releaseCon();

        origin_db.releaseCon();
        return ;
    }
    public int getRuleSize(DBRule[] rules){
        int size = 0;
        for(DBRule rule:rules){
            size += (rule.body.size()+1);
        }
        return size;
    }

    /**
     * 多进程并发运行时避免多个进程共享同一个 DuckDB 数据文件（例如 db2/db2.wal）。
     * 规则：所有传给 DatabaseManager/ConnectionPool 的 id 都会加上一个进程级 offset。
     *
     * 可通过环境变量 QSC_DB_ID_OFFSET 覆盖（用于可复现实验）。
     */
    private final int dbIdOffset = computeDbIdOffset();

    private static int computeDbIdOffset() {
        String env = System.getenv("QSC_DB_ID_OFFSET");
        if (env != null && !env.isBlank()) {
            try {
                return Integer.parseInt(env.trim());
            } catch (NumberFormatException ignore) {
                // fall through to PID-based offset
            }
        }
        long pid = ProcessHandle.current().pid();
        // 给 base id（1/2/3...）留出空间，避免冲突；同时控制数值规模不要过大
        return (int) ((pid % 10000L) * 100L);
    }

    private int dbId(int baseId) {
        return baseId + dbIdOffset;
    }

    public final void run() throws RuntimeException {

        long start = System.currentTimeMillis();
        try {
            compress_run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start) / 1000000000 + "s");
    }
    
    
}
