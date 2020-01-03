package com.giobart.mapreduce;

import com.giobart.mapreduce.impl.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface MapReduce <INk,INv,Mk,Mv,R> {
    void solve();
    void setRead(Supplier<Pair<INk,INv>> supplier);
    void setWrite(Consumer<List<R>> consumer);
    void setMap(Function<Pair<INk,INv>, Stream<Pair<Mk,Mv>>> mapFunction);
    void setCompare(Comparator<Pair<Mk,Mv>> compareFunction);
    void setReduce(BiFunction<Mk, List<Mv>, List<R>> reduceFunction);
}
