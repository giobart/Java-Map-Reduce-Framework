package com.giobart.mapreduce.impl;

import com.giobart.mapreduce.MapReduceTemplate;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleMapReduce<INk,INv,Mk,Mv,R> extends MapReduceTemplate<INk,INv,Mk,Mv,R> {

    @Override
    protected Stream<Pair<Mk,Mv>> map(Stream<Pair<INk,INv>> input, Function<Pair<INk, INv>, Stream<Pair<Mk, Mv>>> mapFunction ) {
        return input.flatMap(mapFunction);
    }

    @Override
    protected Stream<Pair<Mk, List<Mv>>> dataShuffling(Stream<Pair<Mk,Mv>> mappedStream, Comparator<Pair<Mk,Mv>> compareFunction) {
        return mappedStream
                .sorted(compareFunction)
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())))
                .entrySet()
                .stream()
                .map(entry -> new Pair<>(entry.getKey(),entry.getValue()));
    }

    @Override
    protected Stream<List<R>> reduce(Stream<Pair<Mk, List<Mv>>> collected, BiFunction<Mk, List<Mv> ,List<R>> reduceFunction) {
        return collected
                .map(entry -> reduceFunction
                        .apply(entry.getKey(),entry.getValue()));
    }

    @Override
    protected void write(Stream<List<R>> towrite,Consumer<List<R>> consumer) {
        towrite.forEach(consumer);
    }


}
