package qsc.db;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.jna.Pointer;
import com.sun.jna.Platform;

public class AffinityThreadFactory implements ThreadFactory {
    private final ConnectionPool connectionPool;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final int availableCores;
    private final AtomicInteger coreCounter = new AtomicInteger(0);

    public AffinityThreadFactory(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.namePrefix = "AffinityThread-";
        this.availableCores = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public Thread newThread(Runnable r) {
        int coreId = coreCounter.getAndIncrement() % availableCores;
        Thread t = new Thread(() -> {
            setAffinity(coreId);
            r.run();
        }, namePrefix + threadNumber.getAndIncrement());

        return t;
    }

    private void setAffinity(int coreId) {
        if (Platform.isLinux()) {
            CpuSet cpuset = new CpuSet();
            cpuset.CPU_ZERO();
            cpuset.CPU_SET(coreId);
            cpuset.write();

            long threadId = NativeLibrary.INSTANCE.pthread_self();
            int result = CLibrary.INSTANCE.pthread_setaffinity_np(threadId, cpuset.size(), cpuset.getPointer());
            if (result != 0) {
                System.err.println("Failed to set thread affinity for core " + coreId);
            }
        } else if (Platform.isWindows()) {
            Pointer hThread = NativeLibrary.INSTANCE.GetCurrentThread();
            if (hThread == null) {
                System.err.println("Failed to get current thread handle.");
                return;
            }
            long mask = 1L << coreId;
            long result = CLibrary.INSTANCE.SetThreadAffinityMask(hThread, mask);
            if (result == 0) {
                System.err.println("Failed to set thread affinity for core " + coreId);
            }
        } else {
            System.err.println("Unsupported platform for setting thread affinity.");
        }
    }
}



