package qsc.db;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public interface NativeLibrary extends Library {
    NativeLibrary INSTANCE = Native.load(
        Platform.isWindows() ? "kernel32" : "c",
        NativeLibrary.class
    );

    long pthread_self();
    Pointer GetCurrentThread();
}