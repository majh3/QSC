package qsc.db;

import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

public class CpuSet extends Structure {
    public static final int CPU_SETSIZE = 1024;
    public static final int __NCPUBITS = 64;

    public long[] __bits = new long[CPU_SETSIZE / __NCPUBITS];

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("__bits");
    }

    public void CPU_ZERO() {
        Arrays.fill(__bits, 0);
    }

    public void CPU_SET(int cpu) {
        int idx = cpu / __NCPUBITS;
        int bit = cpu % __NCPUBITS;
        __bits[idx] |= (1L << bit);
    }
}