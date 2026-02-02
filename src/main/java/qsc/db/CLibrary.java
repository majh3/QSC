package qsc.db;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public interface CLibrary extends Library {
    CLibrary INSTANCE = Native.load(
        Platform.isWindows() ? "kernel32" : "c",
        CLibrary.class
    );
    int pthread_setaffinity_np(long thread, long cpusetsize, Pointer cpuset);
    long SetThreadAffinityMask(Pointer hThread, long dwThreadAffinityMask);
}