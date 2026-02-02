package qsc.util;

import java.util.HashSet;
import java.util.Iterator;

public class ArrayTreeSet implements Iterable<int[]> {
    private final HashSet<ArrayWrapper> set;

    public ArrayTreeSet() {
        this.set = new HashSet<>();
    }
    public ArrayTreeSet copy() {
        ArrayTreeSet newSet = new ArrayTreeSet();
        for(ArrayWrapper arrayWrapper : set) {
            newSet.add(arrayWrapper.getArray());
        }
        return newSet;
    }

    public boolean add(int[] array) {
        return set.add(new ArrayWrapper(array));
    }

    public boolean contains(int[] array) {
        return set.contains(new ArrayWrapper(array));
    }
    public boolean isEmpty(){
        return set.isEmpty();
    }

    public boolean addAll(ArrayTreeSet otherSet) {
        boolean changed = false;
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.add(arrayWrapper)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean removeAll(ArrayTreeSet otherSet) {
        boolean changed = false;
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.remove(arrayWrapper)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean containsAll(ArrayTreeSet otherSet) {
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (!set.contains(arrayWrapper)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ArrayTreeSet clone() {
        ArrayTreeSet clonedSet = new ArrayTreeSet();
        clonedSet.set.addAll(this.set);  
        return clonedSet;
    }

    public int size() {
        return set.size();
    }

    @Override
    public Iterator<int[]> iterator() {
        return new Iterator<int[]>() {
            private final Iterator<ArrayWrapper> it = set.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public int[] next() {
                return it.next().getArray();
            }
        };
    }

    public void clear() {
        set.clear();
    }
}