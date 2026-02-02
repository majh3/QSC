package qsc.util;

public class Quintuple<T,V,U,W,X> {
    private final T first;
    private final V second;
    private final U third;
    private final W fourth;
    private final X fifth;

    public Quintuple(T first, V second, U third, W fourth, X fifth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
        this.fifth = fifth;
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

    public X getFifth() {
        return fifth;
    }
}