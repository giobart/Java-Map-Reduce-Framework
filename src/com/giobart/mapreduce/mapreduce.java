package com.giobart.mapreduce;

import com.giobart.mapreduce.impl.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public interface MapReduce <INk,INv,Mk,Mv,R> {
    void solve();
    void setRead(Stream<Pair<INk,INv>> input);
    void setWrite(Consumer<Stream<R>> consumer);
    void setMap(Function<Pair<INk,INv>, Stream<Pair<Mk,Mv>>> mapFunction);
    void setCompare(Comparator<Pair<Mk,Mv>> compareFunction);
    void setReduce(Function<Stream<Pair<Mk, List<Mv>>> ,Stream<R>> reduceFunction);
}
