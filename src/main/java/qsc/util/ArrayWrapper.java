package qsc.util;

import java.util.Arrays;

public class ArrayWrapper {
    private final int[] array;

    public ArrayWrapper(int[] array) {
        this.array = array;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);  
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ArrayWrapper that = (ArrayWrapper) obj;
        return Arrays.equals(array, that.array);  
    }

    public int[] getArray() {
        return array;
    }
}