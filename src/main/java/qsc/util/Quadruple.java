package qsc.util;

public class Quadruple<T,V,U,W> {
    private final T first;
    private final V second;
    private final U third;
    private final W fourth;

    public Quadruple(T first, V second, U third, W fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public T getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    public U getThird() {
        return third;
    }

    public W getFourth() {
        return fourth;
    }

}