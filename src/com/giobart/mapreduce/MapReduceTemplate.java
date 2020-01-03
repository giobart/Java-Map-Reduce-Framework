package com.giobart.mapreduce;

import com.giobart.mapreduce.impl.BadFrameworkInstanceException;
import com.giobart.mapreduce.impl.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class MapReduceTemplate<INk,INv,Mk,Mv,R> implements MapReduce<INk,INv,Mk,Mv,R> {

    /*
    * Set of functions provided by the framework user in order to solve the map reduce problem
    * */
    private Supplier<Pair<INk, INv>> readFunction;
    private Function<Pair<INk, INv>, Stream<Pair<Mk, Mv>>> mapFunction;
    private Comparator<Pair<Mk,Mv>> compareFunction;
    private BiFunction<Mk, List<Mv> ,List<R>> reduceFunction;
    private Consumer<List<R>> writeFunction;


    /*
    * Set of internal method that must be extended by an instance of the Template
    * they allow to solve the problem with different optimizations and paradigm just by
    * defining the way the single methods operate.
    * */
    protected abstract Stream<Pair<INk,INv>> read(Supplier<Pair<INk, INv>> supplier);
    protected abstract Stream<Pair<Mk,Mv>> map(Stream<Pair<INk,INv>> el,Function<Pair<INk, INv>, Stream<Pair<Mk, Mv>>> mapFunction);
    protected abstract Stream<Pair<Mk, List<Mv>>> dataShuffling(Stream<Pair<Mk,Mv>> mappedStream, Comparator<Pair<Mk,Mv>> compareFunction);
    protected abstract Stream<List<R>> reduce(Stream<Pair<Mk, List<Mv>>> collected, BiFunction<Mk, List<Mv> ,List<R>> reduceFunction);
    protected abstract void write(Stream<List<R>> towrite, Consumer<List<R>> consumer);

    /*
    * Method used to solve the Map Reduce problem exploiting the given functions
    * */
    public final void solve(){
        if(readFunction ==null) missingFunction("readFunction");
        if(mapFunction==null) missingFunction("mapFunction");
        if(compareFunction==null) missingFunction("compareFunction");
        if(reduceFunction==null) missingFunction("reduceFunction");
        if(writeFunction ==null) missingFunction("writeFunction");

        Stream<Pair<INk,INv>> input = read(readFunction);
        Stream<Pair<Mk,Mv>> mapped = map(input,mapFunction);
        Stream<Pair<Mk, List<Mv>>> collected = dataShuffling(mapped,compareFunction);
        Stream<List<R>> result = reduce(collected,reduceFunction);
        write(result, writeFunction);
    }

    @Override
    public void setRead(Supplier<Pair<INk, INv>> supplier) {
        this.readFunction =supplier;
    }

    @Override
    public void setWrite(Consumer<List<R>> consumer) {
        this.writeFunction =consumer;
    }

    @Override
    public void setMap(Function<Pair<INk, INv>, Stream<Pair<Mk, Mv>>> mapFunction) {
        this.mapFunction=mapFunction;
    }

    @Override
    public void setCompare(Comparator<Pair<Mk,Mv>> compareFunction) {
        this.compareFunction=compareFunction;
    }

    @Override
    public void setReduce(BiFunction<Mk, List<Mv>,List<R>> reduceFunction) {
        this.reduceFunction=reduceFunction;
    }

    //used to throw exception in case of a missing mandatory function
    private void missingFunction(String name){
        throw new BadFrameworkInstanceException("Missing function named: "+name);
    }

}
