package qsc.rewriter;
import fr.lirmm.graphik.graal.io.dlp.DlgpParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import fr.lirmm.graphik.graal.api.core.Atom;
import fr.lirmm.graphik.graal.api.core.ConjunctiveQuery;
import fr.lirmm.graphik.graal.api.core.InMemoryAtomSet;
import fr.lirmm.graphik.graal.api.core.Predicate;
import fr.lirmm.graphik.graal.api.core.Rule;
import fr.lirmm.graphik.graal.api.core.RuleSet;
import fr.lirmm.graphik.graal.api.core.RulesCompilation;
import fr.lirmm.graphik.graal.api.core.Term;
import fr.lirmm.graphik.graal.api.io.ParseException;
import fr.lirmm.graphik.graal.io.dlp.DlgpWriter;
import fr.lirmm.graphik.util.stream.CloseableIteratorWithoutException;
import qsc.dbrule.Argument;
import qsc.dbrule.Constants;
import qsc.dbrule.DBRule;
import qsc.util.Pair;
import fr.lirmm.graphik.graal.backward_chaining.pure.AggregAllRulesOperator;
import fr.lirmm.graphik.graal.backward_chaining.pure.AggregSingleRuleOperator;
import fr.lirmm.graphik.graal.backward_chaining.pure.BasicAggregAllRulesOperator;
import fr.lirmm.graphik.graal.backward_chaining.pure.PureRewriter;
import fr.lirmm.graphik.graal.backward_chaining.pure.RewritingOperator;
import fr.lirmm.graphik.graal.core.compilation.NoCompilation;

public class Rewriter {
    public PureRewriter rewriter;
    public RuleSet ruleSet;
    public RulesCompilation compilation;
    public String target;
    public String query;
    protected List<Vector<String>> relationMeta = new Vector<Vector<String>>();
    protected HashMap<String, Integer> relation2id = new HashMap<String, Integer>();
    public HashMap<String, Constants> relConstants = new HashMap<String, Constants>(); 
    CloseableIteratorWithoutException<ConjunctiveQuery> execute;
    public Rewriter(RuleSet rule_set, List<Vector<String>> relationMeta, HashMap<String, Integer> relation2id){
        boolean unfolding = true;

        this.relationMeta = relationMeta;
        this.relation2id = relation2id;
        this.compilation = NoCompilation.instance();

        ruleSet = rule_set;
        for(Rule rule:rule_set){
            InMemoryAtomSet head = rule.getHead();
            Set<Predicate> predicates = head.getPredicates();
            for(Predicate predicate:predicates){

                String relation = predicate.getIdentifier().toString();
                CloseableIteratorWithoutException<Atom> atoms  = head.atomsByPredicate(predicate);
                while(atoms.hasNext()){
                    Atom atom = atoms.next();
                    int pos = 0;
                    for(Term term:atom.getTerms()){
                        if(term.isConstant()){
                            String constant =  term.toString().replace("\"", "").replace("$",":");
                            if(!relConstants.containsKey(relation)){
                                relConstants.put(relation, new Constants());
                            }
                            try{
                                relConstants.get(relation).addConstant(constant, pos);
                            }catch(Exception e){
                                System.out.println(relation);
                                System.out.println(constant);
                                System.out.println(pos);
                            }
                        }
                        pos++;
                    }
                }
            }
        }
		RewritingOperator operator = null;	
        int ope = 1;
		if(ope==0) {
			operator = new AggregSingleRuleOperator();
		} else if (ope==1) {
			operator = new AggregAllRulesOperator();
		} else {
			operator = new BasicAggregAllRulesOperator();
		}

        this.rewriter = new PureRewriter(operator, unfolding);
    }

    public void setExecute(String t) throws ParseException{
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(t)).get(1));
        if(arity==1){
            query = getAndRewriteQuery("X", t, "type");
            this.target = "type";

        }else{
            query = getAndRewriteQuery("X", "Y", t);
            this.target = t;
        }
        ConjunctiveQuery parsed_query = DlgpParser.parseQuery(query);

        execute = rewriter.execute(parsed_query, ruleSet, compilation);
    }

    public void setExecute(String t, String[] tuple) throws ParseException{
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(t)).get(1));
        if(arity==1){
            query = getAndRewriteQuery(tuple[0], t, "type");
            this.target = "type";

        }else{
            query = getAndRewriteQuery(tuple[0], String.valueOf(tuple[1]), t);
            this.target = t;
        }
        ConjunctiveQuery parsed_query = DlgpParser.parseQuery(query);
        execute = rewriter.execute(parsed_query, ruleSet, compilation);
    }

    public String getAndRewriteQuery(String arg1, String arg2, String table_name) throws ParseException{
        String q = "?("+arg1+","+arg2+")";
        q += ":- "+table_name+"("+arg1+","+arg2+").";
        return q;
    }

    public DBRule[] rewrite_query_without_time(String query) throws ParseException{
        ConjunctiveQuery parsed_query = DlgpParser.parseQuery(query);
        CloseableIteratorWithoutException<ConjunctiveQuery> execute = rewriter.execute(parsed_query, ruleSet, compilation);

        Vector<DBRule> rewrite_rules = new Vector<DBRule>();
        while(true){
            if(!execute.hasNext()){
                break;
            }
            ConjunctiveQuery sparql_query = execute.next();
            DBRule rewritedQuery = string2Query(sparql_query, query.split(":-")[1].replace(".", ""));
            if(rewritedQuery != null){
                rewrite_rules.add(rewritedQuery);
            }
        }
        return rewrite_rules.toArray(new DBRule[rewrite_rules.size()]);
    }
    public Pair<DBRule[], Long> rewrite_query(String query) throws ParseException{
        long rewrite_time = 0;
        long parse_start = System.nanoTime();
        ConjunctiveQuery parsed_query;
        try{
            parsed_query = DlgpParser.parseQuery(query);
        }catch(Exception e){
            throw e;
        }

        CloseableIteratorWithoutException<ConjunctiveQuery> execute = rewriter.execute(parsed_query, ruleSet, compilation);
        long parse_end = System.nanoTime();
        rewrite_time+= parse_end - parse_start;
        Vector<DBRule> rewrite_rules = new Vector<DBRule>();
        while(true){
            long rewrite_start = System.nanoTime();
            if(!execute.hasNext()){
                break;
            }
            ConjunctiveQuery sparql_query = execute.next();
            long rewrite_end =  System.nanoTime();
            rewrite_time += (rewrite_end - rewrite_start);
            DBRule rewritedQuery = string2Query(sparql_query, query.split(":-")[1].replace(".", ""));
            if(rewritedQuery != null){
                rewrite_rules.add(rewritedQuery);
            }
        }
        return new Pair<DBRule[], Long>(rewrite_rules.toArray(new DBRule[rewrite_rules.size()]), rewrite_time);
    }
    public Pair<DBRule[], Long> rewrite_query_runtime(String query) throws ParseException{
        long rewrite_time = 0;
        long parse_start = System.nanoTime();
        ConjunctiveQuery parsed_query;
        try{
            parsed_query = DlgpParser.parseQuery(query);
        }catch(Exception e){
            throw e;
        }

        CloseableIteratorWithoutException<ConjunctiveQuery> execute = rewriter.execute(parsed_query, ruleSet, compilation);
        long parse_end = System.nanoTime();
        rewrite_time+= parse_end - parse_start;
        Vector<DBRule> rewrite_rules = new Vector<DBRule>();
        while(true){

            if(!execute.hasNext()){
                break;
            }
            ConjunctiveQuery sparql_query = execute.next();

            DBRule rewritedQuery = string2Query_runtime(sparql_query);
            if(rewritedQuery != null){
                rewrite_rules.add(rewritedQuery);
            }
        }
        return new Pair<DBRule[], Long>(rewrite_rules.toArray(new DBRule[rewrite_rules.size()]), rewrite_time);
    }

    public DBRule[] rewrite_all(String table_name) throws ParseException{
        Constants constant_poss = relConstants.get(table_name);
        Vector<DBRule> rewrite_rules = new Vector<DBRule>();

        if(constant_poss==null){
            setExecute(table_name);
            while(execute.hasNext()){
                DBRule rewritedQuery = string2Query(execute.next(), true);
                if(rewritedQuery != null){
                    rewrite_rules.add(rewritedQuery);
                }
            }
        }else{
            constant_poss.addConstant("X", 0);
            constant_poss.addConstant("Y", 1);
            for(String c1:constant_poss.constant1){
                for(String c2:constant_poss.constant2){
                    setExecute(table_name, new String[]{c1, c2});
                    while(execute.hasNext()){
                        DBRule rewritedQuery = string2Query(execute.next(), true);
                        if(rewritedQuery != null){
                            rewrite_rules.add(rewritedQuery);
                        }
                    }
                }
            }

        }
        return rewrite_rules.toArray(new DBRule[rewrite_rules.size()]);
    }

    public DBRule string2Query_runtime(ConjunctiveQuery query){

        String body_string = query.toString().split(":")[1].strip().replace("[", "").replace("]", "").replace("\\2", "").replace("\"", "").replace("\\1", "");
        String head_string = "?("+query.toString().split(":")[0].strip().split("\\(")[1];

        DBRule db_query = new DBRule(head_string+":-"+body_string, 0);
        return db_query;

    }
    public DBRule string2Query(ConjunctiveQuery query, String original_query){

        String body_string = query.toString().split(":")[1].strip().replace("[", "").replace("]", "").replace("\\2", "").replace("\"", "").replace("\\1", "");
        String head_string = original_query.split(":-")[0].strip();

        DBRule db_query = new DBRule(head_string+":-"+body_string, 0);
        return db_query;

    }
    public DBRule string2Query(ConjunctiveQuery query, boolean origin){

        CloseableIteratorWithoutException<Atom> atom_iter = query.getAtomSet().iterator();

        String body_string = "";
        Map<String, String> var2term = new HashMap<String, String>();
        int variable_count = 0;
        List<Term> terms = query.getAnswerVariables();
        String query_string = target+"(";
        for(Term term:terms){
            if(!term.isConstant()){
                if(!var2term.containsKey(term.toString().replace("\"", ""))){
                    var2term.put(term.toString().replace("\"", ""), Argument.VAR_ARRAY.get(variable_count++));
                }
                query_string+=var2term.get(term.toString().replace("\"", ""));
            }else{
                query_string+=term.toString().replace("\"", "");
            }

            if(terms.indexOf(term)<terms.size()-1){
                query_string+=",";
            }
        }
        query_string += ")";
        String head_string = query_string;
        query_string += " :- ";

        while(atom_iter.hasNext()){
            Atom atom = atom_iter.next();
            String predicate = (String) atom.getPredicate().getIdentifier();
            if(predicate.equals("=")) return null;
            int arity = atom.getPredicate().getArity();

            body_string += predicate + "(";
            for(int i = 0; i < arity; i++){

                if(atom.getTerm(i).isConstant()){

                    body_string+=atom.getTerm(i).toString().replace("\"", "");
                }else if(!var2term.containsKey(atom.getTerm(i).toString().replace("\"", ""))){
                    var2term.put(atom.getTerm(i).toString().replace("\"", ""), Argument.VAR_ARRAY.get(variable_count++));
                    body_string += var2term.get(atom.getTerm(i).toString().replace("\"", ""));
                }else{
                    body_string += var2term.get(atom.getTerm(i).toString().replace("\"", ""));
                }
                if(i<arity-1){
                    body_string += ",";
                }
            }
            body_string += ")";
            if(atom_iter.hasNext()){
                body_string += ", ";
            }
        }
        if(!origin && body_string.equals(head_string)){
            return null;
        }

        query_string += body_string;
        DBRule db_query = new DBRule(query_string, 0);
        return db_query;

    }
    }
