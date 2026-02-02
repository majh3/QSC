package qsc.dbrule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

public class DBRule {
    public int id;
    public int closure = -1;
    public Predicate head;
    public Vector<Predicate> body = new Vector<Predicate>();
    public HashMap<String, String> all_Variables_map = new HashMap<String, String>();
    public HashMap<String, Vector<Argument>> joinMap = new HashMap<String, Vector<Argument>>();
    public Vector<Argument> constantsBody = new Vector<Argument>();
    public Vector<Predicate> allPredicates = new Vector<Predicate>();

    private Integer forcedMainTableIndex = null;

    public DBRule(String rule, int id){
        this.id = id;
        if(rule.endsWith(".")){
            rule = rule.substring(0, rule.length()-1);
        }
        String[] parts = rule.split(":-");
        String head_string = parts[0].trim();
        String[] body_strings = parts[1].trim().split(", ");

        String[] head_parts = head_string.split("\\(");
        String head_functor = head_parts[0].trim();
        String[] head_args;
        try{
            head_args = head_parts[1].trim().split("\\)")[0].split(",");
        }catch(Exception e){
            System.out.println("head_string: "+head_string);
            head_args = head_parts[1].trim().split("\\)")[0].split(",");
        }

        Vector<Argument> head_args_array = new Vector<Argument>();
        Integer pred_id = 0;
        for(int i = 0; i < head_args.length; i++){
            String arg = head_args[i];
            if(!Argument.checkVariable(arg)){

                head_args_array.add(new Argument(arg, true, pred_id, i));

            }else{

                if(!all_Variables_map.containsKey(arg)){
                    all_Variables_map.put(arg, Argument.DEFAULT_VAR_ARRAY.get(all_Variables_map.size()));
                }
                head_args_array.add(new Argument(all_Variables_map.get(arg), false, pred_id, i));
                if (!joinMap.containsKey(head_args_array.get(i).name)){
                    joinMap.put(head_args_array.get(i).name, new Vector<Argument>());
                }

            }
        }

        pred_id++;
        head = new Predicate(head_functor, (Argument[])head_args_array.toArray(new Argument[head_args_array.size()]));
        allPredicates.add(head);
        HashMap<String, Integer> predcnt = new HashMap<String, Integer>();

        for (String body_string : body_strings){
            String[] body_parts = body_string.split("\\(");
            String body_functor = body_parts[0].trim();
            String[] body_args = new String[0];
            try{
                body_args = body_parts[1].trim().split("\\)")[0].split(",");
            }
            catch(Exception e){
                System.out.println("body_string: "+body_string);
            }
            if(body_functor.contains("=")){

                String constant = "";
                int k = 0;
                for(String arg: body_args){
                    if(arg.matches("^[0-9]+$")){
                        constant = arg;
                        continue;
                    }
                    int i = 0;

                    for(String argh:head_args){
                        if(arg.equals(argh)){
                            k=i;
                        }
                        i++;
                    }
                }
                head.args[k].name = constant;
                head.args[k].isConstant = true;
                continue;
            }
            Vector<Argument> body_args_array = new Vector<Argument>();
            for(int i = 0; i < body_args.length; i++){
                String arg = body_args[i];
                if(!Argument.checkVariable(arg)){

                    body_args_array.add(new Argument(arg, true, pred_id, i));
                    try{
                        constantsBody.add(body_args_array.get(i));                }catch(Exception e){
                        System.out.println("body_args_array.get(i).name: "+body_args_array.get(i).name);
                    }
                }else{

                    if(!all_Variables_map.containsKey(arg)){
                        all_Variables_map.put(arg, Argument.DEFAULT_VAR_ARRAY.get(all_Variables_map.size()));
                    }

                    body_args_array.add(new Argument(all_Variables_map.get(arg), false, pred_id, i));
                    try{
                        if (!joinMap.containsKey(body_args_array.get(i).name)){
                            joinMap.put(body_args_array.get(i).name, new Vector<Argument>());
                        }
                    }catch(Exception e){
                        System.out.println("body_args_array.get(i).name: "+body_args_array.get(i).name);
                    }
                    joinMap.get(body_args_array.get(i).name).add(body_args_array.get(i));
                }
            }

            pred_id++;

            if(!predcnt.containsKey(body_functor)){
                predcnt.put(body_functor, 1);
            }else{
                predcnt.put(body_functor, predcnt.get(body_functor)+1);
            }
            Predicate pred = new Predicate(body_functor, (Argument[])body_args_array.toArray(new Argument[body_args_array.size()]));
            allPredicates.add(pred);
            body.add(pred);
        }

        for (Predicate pred : body){
            Integer cunt = predcnt.get(pred.functor);
            if(cunt>1){            
                pred.duplicateID = cunt;
                predcnt.put(pred.functor, cunt-1);
            }
        }
    }
    public void setRuleId(int id){
        this.id = id;
    }

    public void replaceConstantsWithMap(HashMap<String, Integer> entity_map){

         for(int i = 0; i < head.args.length; i++){
            if(head.args[i].isConstant){
                try{
                    head.args[i].name = entity_map.get(head.args[i].name).toString();
                }catch(Exception e){
                    System.out.println("head.args[i].name: "+head.args[i].name);
                }
            }
         }
         for(Predicate pred : body){
            for(int i = 0; i < pred.args.length; i++){
                if(pred.args[i].isConstant){
                    pred.args[i].name = entity_map.get(pred.args[i].name).toString();
                }
            }
         }
    }
    @Override
    public String toString(){
        String str = head.functor + "(";
        for (int i = 0; i < head.args.length; i++){
            str += head.args[i].name;
            if (i < head.args.length - 1){
                str += ",";
            }
        }
        str += ") :- ";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            str += pred.functor + "(";
            for (int j = 0; j < pred.args.length; j++){
                str += pred.args[j].name;
                if (j < pred.args.length - 1){
                    str += ",";
                }
            }
            str += ")";
            if (i < body.size() - 1){
                str += ", ";
            }
        }
        return str;
    }

    public String toString_Map(String[] mapping){
        String str = head.functor + "(";
        for (int i = 0; i < head.args.length; i++){
            if(head.args[i].isConstant){
                str += mapping[Integer.parseInt(head.args[i].name)];
            }else{
                str += head.args[i].name;
            }
            if (i < head.args.length - 1){
                str += ", ";
            }
        }
        str.replace(":", "$");
        str += ") :- ";
        String str2 = "";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            str2 += pred.functor + "(";
            for (int j = 0; j < pred.args.length; j++){
                if(pred.args[j].isConstant){
                    str2 += mapping[Integer.parseInt(pred.args[j].name)];
                }else{
                    str2 += pred.args[j].name;
                }
                if (j < pred.args.length - 1){
                    str2 += ", ";
                }
            }
            str2 += ")";
            if (i < body.size() - 1){
                str2 += ", ";
            }
        }
        str2.replace(":", "$");
        return str+str2;
    }

    protected Vector<Vector<Argument>> getJoins(){
        Vector<Vector<Argument>> joins = new Vector<Vector<Argument>>();
        for (Map.Entry<String, Vector<Argument>> entry : joinMap.entrySet()){
            Vector<Argument> join_arg = entry.getValue();
            Vector<Argument> join = new Vector<Argument>();
            if(join_arg.size() < 2){
                continue;
            }
            for(Argument arg: join_arg){
                if(arg.predicate_id>0){
                    join.add(arg);
                }
            }
            joins.add(join);
        }
        return joins;
    }

    public String rule2SQL_where(boolean tag){
    String sql = " FROM ";

       for (int i = 0; i < body.size(); i++){
           Predicate pred = body.get(i);
           sql += pred.getSQLName();
           if (i < body.size() - 1){
               sql += ", ";
           }
       }
       String where_cls = "";
       where_cls += " WHERE ";

       Vector<Vector<Argument>> joins = getJoins();
       for (int i = 0; i < joins.size(); i++){
           Vector<Argument> join = joins.get(i);
           Argument arg1 = join.get(0);
           Predicate pred1 = allPredicates.get(arg1.predicate_id);
           for(int j = 1; j < join.size(); j++){
               Argument arg = join.get(j);
               Predicate pred = allPredicates.get(arg.predicate_id);
               where_cls += (pred.getColumnName(arg.position) + " = " + pred1.getColumnName(arg1.position));
               if ((j < join.size() - 1)||(i < joins.size() - 1)){
                   where_cls += " AND ";
               }

           }
       }

       if(constantsBody.size() > 0 && !(where_cls.endsWith("AND ")) && joins.size() > 0){
               where_cls += " AND ";
       }
       for (int i = 0; i < constantsBody.size(); i++){
           Argument constant = constantsBody.get(i);
           Predicate pred = allPredicates.get(constant.predicate_id);
           where_cls += (pred.getColumnName(constant.position) + " = "  + constant.getName());
           if (i < constantsBody.size() - 1){
               where_cls += " AND ";
           }
       }

       if(tag){
           if(!(where_cls.endsWith("AND ") || where_cls.endsWith("WHERE "))){
               where_cls += " AND ";
           }
           for (int i = 0; i < body.size(); i++){
               Predicate pred = body.get(i);
               where_cls += pred.getSQLName() + ".exists = '1'";
               if (i < body.size() - 1){
                   where_cls += " AND ";
               }
           }
       }

       while(where_cls.endsWith("AND ")){
           where_cls = where_cls.substring(0, where_cls.length()-5);
       }
       if(!where_cls.equals(" WHERE ")){
           sql += where_cls;
       }
       return sql;
   }

    public HashMap<String, String> getTarget2bodyColumns(){
        HashMap<String, String> target2bodyColumns = new HashMap<String, String>();
        for (int i = 0; i < head.args.length; i++){
            Argument arg = head.args[i];
            if (arg.isConstant){
                target2bodyColumns.put(head.functor + "_" + (i+1), "\'" + arg.name + "\'");
            }else{
                Argument target_arg = joinMap.get(arg.name).get(0);
                Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                target2bodyColumns.put(head.functor + "_" + (i+1), target_pred.functor + "_" + (target_arg.position+1));
            }
        }
        return target2bodyColumns;
    }

    public String ruleHead2SQL_Set(){

        String sql = "SELECT ";
        Argument[] head_args = head.args;
        for (int i = 0; i < head_args.length; i++){ 
            Argument arg = head_args[i];
            if (arg.isConstant){
                sql += (arg.getName() + " AS " + head.getSoleColumnName(i));
            }else{
                try{
                    Argument target_arg = joinMap.get(arg.name).get(0);
                    Predicate source_pred = allPredicates.get(target_arg.predicate_id);
                    sql += (source_pred.getColumnName(target_arg.position) + " AS " + head.getSoleColumnName(i));
                }catch(Exception e){
                    throw e;
                }
            }
            if (i < head_args.length - 1){
                sql += ", ";
            }
        }
        sql += ", ";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            for(int j = 0; j < pred.args.length; j++){
                sql += pred.getColumnName(j);
                sql += ", ";
            }
        }

        if(sql.endsWith(", ")){
            sql = sql.substring(0, sql.length()-2);
        }
        if(sql.equals("SELECT ")){
            sql += " * ";
        }

        sql += rule2SQL_where(false);
        return sql;
    }

    public String ruleHead2SQL(int pad, int rule_id){

        String sql = "SELECT ";
        String target_functor = head.functor;

        Argument[] head_args = head.args;
        if(rule_id>=0){
            sql+= rule_id + " AS rule_id, ";
        }
        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (arg.isConstant){
                sql += ("\'" + arg.name+ "\'" + " AS " + target_functor + "_" + (i+1));
            }else{
                Argument target_arg;
                try{
                    target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    sql += (target_pred.bodySelCls()+"."+target_pred.functor+'_'+(target_arg.position+1) + " AS " + target_functor + "_" + (i+1));
                }catch(Exception e){
                    System.out.println("arg.name: " + arg.name);
                }

            }
            if (i < head_args.length - 1){
                sql += ", ";
            }
        }
        sql += ", ";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            for(int j = 0; j < pred.args.length; j++){
                sql += pred.getColumnName(j);
                sql += ", ";
            }
        }

        for (int i = 0; i < pad - argSize(); i++){
            sql += "NULL AS pad_" + i;
            if (i < pad - argSize() - 1){
                sql += ", ";
            }
        }
        if(sql.endsWith(", ")){
            sql = sql.substring(0, sql.length()-2);
        }
        if(sql.equals("SELECT ")){
            sql += " * ";
        }

        sql += rule2SQL_where(false);

        String sql_outer = "SELECT * FROM (" + sql + ") AS TMP WHERE EXISTS ( SELECT 1 FROM " + target_functor + " WHERE";

        for (int i = 0; i < head_args.length; i++){
            sql_outer += " " + target_functor + "_" + (i+1) + " = TMP." + target_functor + "_" + (i+1);
            if (i < head_args.length - 1){
                sql_outer += " AND";
            }
        }
        return sql_outer+" );";
    }
    public String estSQL(){

        String est_sql = "SELECT ";
        String target_functor = head.functor;

        Argument[] head_args = head.args;
        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (arg.isConstant){
                est_sql += ("\'" + arg.name+ "\'" + " AS " + target_functor + "_" + (i+1));
            }else{
                Argument target_arg;
                try{
                    target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    est_sql += (target_pred.bodySelCls()+"."+target_pred.functor+'_'+(target_arg.position+1) + " AS " + target_functor + "_" + (i+1));
                }catch(Exception e){
                    System.out.println("arg.name: " + arg.name);
                }
            }
            if (i < head_args.length - 1){
                est_sql += ", ";
            }
        }
        est_sql += " FROM ";

        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            est_sql += pred.fromCls();
            if (i < body.size() - 1){
                est_sql += ", ";
            }
        }
        est_sql += ", "+target_functor;
        String where_cls = " WHERE ";

        Vector<Vector<Argument>> joins = getJoins();
        for (int i = 0; i < joins.size(); i++){
            Vector<Argument> join = joins.get(i);
            Argument arg1 = join.get(0);
            Predicate pred1 = allPredicates.get(arg1.predicate_id);
            for(int j = 1; j < join.size(); j++){
                Argument arg = join.get(j);
                Predicate pred = allPredicates.get(arg.predicate_id);
                where_cls += (pred.bodySelCls()+"."+pred.functor + "_" + (arg.position+1) + " = " + pred1.bodySelCls()+"."+pred1.functor + "_" + (arg1.position+1));
                if ((j < join.size() - 1)||(i < joins.size() - 1)){
                    where_cls += " AND ";
                }

            }
        }

        if(constantsBody.size() > 0 && !(where_cls.endsWith("AND ")) && joins.size() > 0){
                where_cls += " AND ";
        }
        for (int i = 0; i < constantsBody.size(); i++){
            Argument constant = constantsBody.get(i);
            Predicate pred = allPredicates.get(constant.predicate_id);
            where_cls += (pred.bodySelCls()+"."+pred.functor + "_" + (constant.position+1) + " = "  + constant.name);
            if (i < constantsBody.size() - 1){
                where_cls += " AND ";
            }
        }

        while(where_cls.endsWith("AND ")){
            where_cls = where_cls.substring(0, where_cls.length()-5);
        }
        est_sql += where_cls;
        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (!arg.isConstant){
                try{
                    Argument target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    est_sql += (target_pred.bodySelCls()+"."+target_pred.functor+'_'+(target_arg.position+1)+"= " + target_functor + "_" + (i+1));
                }catch(Exception e){
                    System.out.println("arg.name: "+arg.name);
                }
            }
            if (i < head_args.length - 1){
                est_sql += " AND ";
            }
        }
        return est_sql;

    }

    public String rule2SQL(boolean tag, boolean query){

        return rule2SQL(tag, query, false);
    }
    public String rule2SQL_Neg(){

        String sql = "SELECT ";

        Argument[] head_args = head.args;
        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (arg.isConstant){
                sql += ("\'" + arg.name + "\'" + " AS " + head.functor + "_" + (i+1));
            }else{
                Argument target_arg;
                try{
                    target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    sql += (target_pred.bodySelCls() + "." + target_pred.functor + "_" + (target_arg.position+1) + " AS " + head.functor + "_" + (i+1));
                }catch(Exception e){
                    System.out.println("arg.name: " + arg.name);
                }
            }
            if (i < head_args.length - 1){
                sql += ", ";
            }
        }

        sql += " FROM ";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            sql += pred.fromCls();
            if (i < body.size() - 1){
                sql += ", ";
            }
        }

        String where_cls = " WHERE ";

        Vector<Vector<Argument>> joins = getJoins();
        for (int i = 0; i < joins.size(); i++){
            Vector<Argument> join = joins.get(i);
            Argument arg1 = join.get(0);
            Predicate pred1 = allPredicates.get(arg1.predicate_id);
            for(int j = 1; j < join.size(); j++){
                Argument arg = join.get(j);
                Predicate pred = allPredicates.get(arg.predicate_id);
                where_cls += (pred.bodySelCls() + "." + pred.functor + "_" + (arg.position+1) + " = " + pred1.bodySelCls() + "." + pred1.functor + "_" + (arg1.position+1));
                if ((j < join.size() - 1) || (i < joins.size() - 1)){
                    where_cls += " AND ";
                }
            }
        }

        if(constantsBody.size() > 0 && !(where_cls.endsWith("AND ")) && joins.size() > 0){
            where_cls += " AND ";
        }
        for (int i = 0; i < constantsBody.size(); i++){
            Argument constant = constantsBody.get(i);
            Predicate pred = allPredicates.get(constant.predicate_id);
            where_cls += (pred.bodySelCls() + "." + pred.functor + "_" + (constant.position+1) + " = " + constant.name);
            if (i < constantsBody.size() - 1){
                where_cls += " AND ";
            }
        }

        if(!where_cls.equals(" WHERE ")){
            where_cls += " AND ";
        }

        String notExistsSql = "NOT EXISTS (SELECT * FROM " + head.functor + " WHERE ";
        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (arg.isConstant){
                notExistsSql += (head.functor + "." + head.functor + "_" + (i+1) + " = \'" + arg.name + "\'");
            }else{
                Argument target_arg;
                try{
                    target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    notExistsSql += (head.functor + "." + head.functor + "_" + (i+1) + " = " + target_pred.bodySelCls() + "." + target_pred.functor + "_" + (target_arg.position+1));
                }catch(Exception e){
                    System.out.println("arg.name: " + arg.name);
                }
            }
            if (i < head_args.length - 1){
                notExistsSql += " AND ";
            }
        }
        notExistsSql += ")";

        where_cls += notExistsSql;

        while(where_cls.endsWith("AND ")){
            where_cls = where_cls.substring(0, where_cls.length()-5);
        }

        if(!where_cls.equals(" WHERE ")){
            sql += where_cls;
        }

        sql += " LIMIT 1";

        return sql;
    }
    public String rule2SQL_supp(){

        String sql = "SELECT DISTINCT ";
        Argument[] head_args = head.args;

        for (int i = 0; i < head_args.length; i++){
            Argument arg = head_args[i];
            if (arg.isConstant){
                sql += ("'" + arg.name + "' AS " + head.functor + "_" + (i+1));
            }else{
                Argument target_arg;
                try{
                    target_arg = joinMap.get(arg.name).get(0);
                    Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                    sql += (target_pred.bodySelCls() + "." + target_pred.functor + "_" + (target_arg.position+1) + " AS " + head.functor + "_" + (i+1));
                }catch(Exception e){
                    System.out.println("arg.name: " + arg.name);
                }
            }
            if (i < head_args.length - 1){
                sql += ", ";
            }
        }

        sql += " FROM ";
        for (int i = 0; i < body.size(); i++){
            Predicate pred = body.get(i);
            sql += pred.fromCls();
            if (i < body.size() - 1){
                sql += ", ";
            }
        }

        String where_cls = " WHERE ";
        Vector<Vector<Argument>> joins = getJoins();
        for (int i = 0; i < joins.size(); i++){
            Vector<Argument> join = joins.get(i);
            Argument arg1 = join.get(0);
            Predicate pred1 = allPredicates.get(arg1.predicate_id);
            for(int j = 1; j < join.size(); j++){
                Argument arg = join.get(j);
                Predicate pred = allPredicates.get(arg.predicate_id);
                where_cls += (pred.bodySelCls() + "." + pred.functor + "_" + (arg.position+1) + " = " + pred1.bodySelCls() + "." + pred1.functor + "_" + (arg1.position+1));
                if ((j < join.size() - 1) || (i < joins.size() - 1)){
                    where_cls += " AND ";
                }
            }
        }

        if(constantsBody.size() > 0 && !(where_cls.endsWith("AND ")) && joins.size() > 0){
            where_cls += " AND ";
        }
        for (int i = 0; i < constantsBody.size(); i++){
            Argument constant = constantsBody.get(i);
            Predicate pred = allPredicates.get(constant.predicate_id);
            where_cls += (pred.bodySelCls() + "." + pred.functor + "_" + (constant.position+1) + " = " + constant.name);
            if (i < constantsBody.size() - 1){
                where_cls += " AND ";
            }
        }

        while(where_cls.endsWith("AND ")){
            where_cls = where_cls.substring(0, where_cls.length()-5);
        }
        if(!where_cls.equals(" WHERE ")){
            sql += where_cls;
        }
        return sql;
    }

    public String rule2SQL(boolean tag, boolean query, boolean single_var){

        String sql = "SELECT DISTINCT ";

        if(query){ 

            for (int i = 0; i < body.size(); i++){
                Predicate pred = body.get(i);
                for(int j = 0; j < pred.args.length; j++){
                    sql += pred.getColumnName(j);
                    sql += ", ";
                }
            }
            if(sql.endsWith(", ")){
                sql = sql.substring(0, sql.length()-2);
            }
            if(sql.equals("SELECT ")){
                sql += " * ";
            }
        }
        else{  
            Argument[] head_args = head.args;
            boolean hasSelectItems = false;

            if(single_var){

                for (int i = 0; i < head_args.length; i++){
                    Argument arg = head_args[i];
                    if (arg.isConstant){

                        if(hasSelectItems){
                            sql += ", ";
                        }
                        if(!head.getSoleColumnName(i).contains("?")){
                            sql += ( arg.getName() + " AS " + head.getSoleColumnName(i));
                        }else{
                            sql +=  arg.getName();
                        }
                        hasSelectItems = true;
                    }else{
                        Argument target_arg;
                        try{
                            target_arg = joinMap.get(arg.name).get(0);
                            Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                            if(hasSelectItems){
                                sql += ", ";
                            }
                            if(!head.getColumnName(target_arg.position).contains("?")){
                                sql += (target_pred.getColumnName(target_arg.position) + " AS " + head.getSoleColumnName(i));
                            }else{
                                sql += target_pred.getColumnName(target_arg.position);
                            }
                            hasSelectItems = true;
                        }catch(Exception e){

                        }
                    }
                }
            } else {

                for (int i = 0; i < head_args.length; i++){
                    Argument arg = head_args[i];
                    if (arg.isConstant){

                        if(hasSelectItems){
                            sql += ", ";
                        }
                        if(!head.getSoleColumnName(i).contains("?")){
                            sql += ( arg.getName() + " AS " + head.getSoleColumnName(i));
                        }else{
                            sql +=  arg.getName();
                        }
                        hasSelectItems = true;
                    }else{
                        Argument target_arg;
                        try{
                            target_arg = joinMap.get(arg.name).get(0);
                            Predicate target_pred = allPredicates.get(target_arg.predicate_id);
                            if(hasSelectItems){
                                sql += ", ";
                            }
                            if(!head.getColumnName(target_arg.position).contains("?")){
                                sql += (target_pred.getColumnName(target_arg.position) + " AS " + head.getSoleColumnName(i));
                            }else{
                                sql += target_pred.getColumnName(target_arg.position);
                            }
                            hasSelectItems = true;
                        }catch(Exception e){

                            if(hasSelectItems){
                                sql += ", ";
                            }
                            sql += "NULL AS " + head.getSoleColumnName(i);
                            hasSelectItems = true;
                        }
                    }
                }
            }

            if(!hasSelectItems){
                sql += "1";
            }
        }
        sql += rule2SQL_where(tag);

        return sql;
    }
    public String rule2SQLDelete(){

        String sql = "DELETE FROM "+head.functor + " USING ";

        for (int i = 0; i < body.size(); i++){
            if(body.get(i).functor.equals(head.functor)){
                continue;
            }
            Predicate pred = body.get(i);
            sql += pred.fromCls();
            if (i < body.size() - 1){
                sql += ", ";
            }
        }
        if(sql.endsWith("USING ")){
            sql = sql.substring(0, sql.length()-6);
        }
        String where_cls = "";
        where_cls += " WHERE ";

        Vector<Vector<Argument>> joins = getJoins();
        for (int i = 0; i < joins.size(); i++){
            Vector<Argument> join = joins.get(i);
            Argument arg1 = join.get(0);
            Predicate pred1 = allPredicates.get(arg1.predicate_id);
            for(int j = 1; j < join.size(); j++){
                Argument arg = join.get(j);
                Predicate pred = allPredicates.get(arg.predicate_id);
                where_cls += (pred.bodySelCls()+"."+pred.functor + "_" + (arg.position+1) + " = " + pred1.bodySelCls()+"."+pred1.functor + "_" + (arg1.position+1));
                if ((j < join.size() - 1)||(i < joins.size() - 1)){
                    where_cls += " AND ";
                }

            }
        }

        if(constantsBody.size() > 0 && !(where_cls.endsWith("AND ")) && joins.size() > 0){
                where_cls += " AND ";
        }
        for (int i = 0; i < constantsBody.size(); i++){
            Argument constant = constantsBody.get(i);
            Predicate pred = allPredicates.get(constant.predicate_id);
            where_cls += (pred.bodySelCls()+"."+pred.functor + "_" + (constant.position+1) + " = "  + constant.name);
            if (i < constantsBody.size() - 1){
                where_cls += " AND ";
            }
        }

        while(where_cls.endsWith("AND ")){
            where_cls = where_cls.substring(0, where_cls.length()-5);
        }
        if(!where_cls.equals(" WHERE ")){
            sql += where_cls;
        }
        return sql;
    }

    public int argSize(){

        int size = 0;
        for (Predicate pred : body){
            size += pred.args.length;
        }
        size += head.args.length;
        return size;
    }

    public boolean isValid(){

        for(Argument arg: head.args){
            if(arg.name.contains("me_myself_i")){
                return false;
            }
        }
        for(Predicate pred: body){
            for(Argument arg: pred.args){
                if(arg.name.contains("me_myself_i")){
                    return false;
                }
            }
        }
        return true;
    }
    public DBRule clone(){
        return new DBRule(this.toString(), this.id);
    }
    public HashSet<String> getPredicateString(){
        HashSet<String> predSet = new HashSet<String>();
        for (Predicate pred : allPredicates){
            predSet.add(pred.functor);
        }
        return predSet;
    }

    public String rule2SQL_EXISTS(boolean tag, boolean query){
        String sql = "SELECT DISTINCT ";

        Integer firstSelectPredicateIdx = null;

        if(!query && body.size() > 1){

            firstSelectPredicateIdx = selectMainTable();
        }

        if(query){ 

            for (int i = 0; i < body.size(); i++){
                Predicate pred = body.get(i);
                for(int j = 0; j < pred.args.length; j++){
                    sql += pred.getColumnName(j);
                    sql += ", ";
                }
            }
            if(sql.endsWith(", ")){
                sql = sql.substring(0, sql.length()-2);
            }
            if(sql.equals("SELECT ")){
                sql += " * ";
            }
        }
        else{  
            Argument[] head_args = head.args;
            boolean hasSelectItems = false;

            for (int i = 0; i < head_args.length; i++){
                Argument arg = head_args[i];
                if (arg.isConstant){

                    if(hasSelectItems){
                        sql += ", ";
                    }
                    if(!head.getSoleColumnName(i).contains("?")){
                        sql += ( arg.getName() + " AS " + head.getSoleColumnName(i));
                    }else{
                        sql +=  arg.getName();
                    }
                    hasSelectItems = true;
                }else{
                    Argument target_arg;
                    try{
                        target_arg = joinMap.get(arg.name).get(0);
                        Predicate target_pred = allPredicates.get(target_arg.predicate_id);

                        if(firstSelectPredicateIdx == null){
                            for(int bIdx = 0; bIdx < body.size(); bIdx++){
                                if(body.get(bIdx) == target_pred){
                                    firstSelectPredicateIdx = bIdx;
                                    break;
                                }
                            }
                        }

                        boolean shouldAddColumn = false;
                        String columnToAdd = "";

                        if(firstSelectPredicateIdx != null && firstSelectPredicateIdx < body.size()){
                            Predicate mainPred = body.get(firstSelectPredicateIdx);

                            for(int pos = 0; pos < mainPred.args.length; pos++){
                                if(!mainPred.args[pos].isConstant && mainPred.args[pos].name.equals(arg.name)){
                                    if(!head.getSoleColumnName(i).contains("?")){
                                        columnToAdd = mainPred.getColumnName(pos) + " AS " + head.getSoleColumnName(i);
                                    }else{
                                        columnToAdd = mainPred.getColumnName(pos);
                                    }
                                    shouldAddColumn = true;
                                    break;
                                }
                            }
                        }else{

                            if(!head.getSoleColumnName(i).contains("?")){
                                columnToAdd = target_pred.getColumnName(target_arg.position) + " AS " + head.getSoleColumnName(i);
                            }else{
                                columnToAdd = target_pred.getColumnName(target_arg.position);
                            }
                            shouldAddColumn = true;
                        }

                        if(shouldAddColumn && !columnToAdd.isEmpty()){
                            if(hasSelectItems){
                                sql += ", ";
                            }
                            sql += columnToAdd;
                            hasSelectItems = true;
                        }
                    }catch(Exception e){

                    }
                }
            }

            if(!hasSelectItems){
                sql += "1";
            }
        }

        while(sql.endsWith(", ")){
            sql = sql.substring(0, sql.length()-2);
        }

        while(sql.endsWith(", ")){
            sql = sql.substring(0, sql.length()-2);
        }

        this.forcedMainTableIndex = firstSelectPredicateIdx;

        sql += rule2SQL_where_EXISTS(tag);

        this.forcedMainTableIndex = null;

        if(!query && body.size() > 1 && firstSelectPredicateIdx != null){
            validateExistsSQL(sql, firstSelectPredicateIdx);
        }

        return sql;
    }

    private void validateExistsSQL(String sql, int mainTableIndex) {
        if(mainTableIndex >= body.size()) return;

        Predicate mainTable = body.get(mainTableIndex);

        for(int i = 0; i < body.size(); i++){
            if(i == mainTableIndex) continue;
            Predicate otherTable = body.get(i);
            String otherTableName = otherTable.functor;

            if(sql.contains("\"" + otherTableName + "\".") && !sql.contains("FROM \"" + otherTableName + "\"")){
                System.err.println("警告: EXISTS查询中SELECT子句引用了非主表的列: " + otherTableName);
                System.err.println("SQL: " + sql);
            }
        }
    }

    public String rule2SQL_where_EXISTS(boolean tag){

        if(body.size()==0){
            return ""; 
        }
        if(body.size()==1){
            return rule2SQL_where(tag); 
        }

        int mainIdx;
        if(this.forcedMainTableIndex != null){
            mainIdx = this.forcedMainTableIndex;
        }else{
            mainIdx = selectMainTable();
        }
        if(mainIdx != 0){
            Predicate tmp = body.get(0);
            body.set(0, body.get(mainIdx));
            body.set(mainIdx, tmp);
            mainIdx = 0; 
        }

        int atomNum = body.size();
        String[] alias = new String[atomNum];
        for(int i=0;i<atomNum;i++){
            alias[i] = body.get(i).bodySelCls();
        }

        java.util.HashMap<String,String> varMap = new java.util.HashMap<>();

        java.util.List<String> firstAtomConstantConds = new java.util.ArrayList<>();

        Predicate firstPred = body.get(0); 
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" FROM ").append(firstPred.fromCls());

        for(int pos=0;pos<firstPred.args.length;pos++){
            Argument arg = firstPred.args[pos];
            String colName = alias[0] + "." + firstPred.getSoleColumnName(pos);
            if(arg.isConstant){
                firstAtomConstantConds.add(colName + " = " + arg.name);
            }else{
                varMap.put(arg.name, colName);
            }
        }

        if(tag){
            firstAtomConstantConds.add(alias[0] + ".exists = '1'");
        }

        String nestedExists = buildNestedExistsChainForward(1, alias, varMap, tag);

        java.util.List<String> outerConds = new java.util.ArrayList<>(firstAtomConstantConds);
        if(!nestedExists.isEmpty()){
            outerConds.add(nestedExists);
        }
        sqlBuilder.append(" WHERE ").append(String.join(" AND ", outerConds));

        return sqlBuilder.toString();
    }

    private String buildNestedExistsChainForward(int startIdx, String[] alias, java.util.HashMap<String,String> outerVarMap, boolean tag){
        if(startIdx >= body.size()){
            return ""; 
        }

        Predicate pred = body.get(startIdx);
        String ai = alias[startIdx];

        java.util.List<String> conds = new java.util.ArrayList<>();

        for(int pos=0; pos<pred.args.length; pos++){
            Argument arg = pred.args[pos];
            String colName = ai + "." + pred.getSoleColumnName(pos);
            if(arg.isConstant){
                conds.add(colName + " = " + arg.name);
            }else{
                if(outerVarMap.containsKey(arg.name)){

                    conds.add(colName + " = " + outerVarMap.get(arg.name));
                }
            }
        }

        if(tag){
            conds.add(ai + ".exists = '1'");
        }

        java.util.HashMap<String,String> nextVarMap = new java.util.HashMap<>(outerVarMap);
        for(int pos=0; pos<pred.args.length; pos++){
            Argument arg = pred.args[pos];
            if(!arg.isConstant && !nextVarMap.containsKey(arg.name)){
                String colName = ai + "." + pred.getSoleColumnName(pos);
                nextVarMap.put(arg.name, colName);
            }
        }

        String innerExists = buildNestedExistsChainForward(startIdx+1, alias, nextVarMap, tag);
        if(!innerExists.isEmpty()){
            conds.add(innerExists);
        }

        return "EXISTS (SELECT 1 FROM " + pred.fromCls() + " WHERE " + String.join(" AND ", conds) + ")";
    }

    private int selectMainTable(){

        for(Argument headArg : head.args){
            if(!headArg.isConstant){
                Vector<Argument> joinArgs = joinMap.get(headArg.name);
                if(joinArgs != null){
                    for(Argument joinArg : joinArgs){
                        if(joinArg.predicate_id > 0){
                            for(int i = 0; i < body.size(); i++){
                                if(allPredicates.get(joinArg.predicate_id) == body.get(i)){
                                    return i;
                                }
                            }
                        }
                    }
                }
            }
        }

        return 0;
    }

}
