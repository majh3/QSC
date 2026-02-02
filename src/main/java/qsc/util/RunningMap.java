package qsc.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunningMap {
    private Map<double[], List<Double>> map = new HashMap<>();

    public void set(double[] key, double value) {
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<>());
        }
        map.get(key).add(value);
    }
    public double get(double[] key) {

        List<Double> values = map.get(key);
        if (values == null) {
            return Double.MAX_VALUE;
        }
        double sum = 0;
        for (double value : values) {
            sum += (double) value;
        }
        return sum / values.size();

    }
    public int size(){
        return map.size();
    }
    public double[][] getKeys() {
        double[][] keys = new double[map.size()][];
        int i = 0;
        for (double[] key : map.keySet()) {
            keys[i] = key;
            i++;
        }
        return keys;
    }
    public double[] getValues() {
        double[] values = new double[map.size()];
        int i = 0;
        for (List<Double> value : map.values()) {
            double sum = 0;
            for (double v : value) {
                sum += v;
            }
            values[i] = sum / value.size();
            i++;
        }
        return values;
    }
}