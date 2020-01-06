package com.giobart;

import com.giobart.mapreduce.MapReduce;
import com.giobart.mapreduce.impl.Pair;
import com.giobart.mapreduce.impl.SimpleMapReduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class InvertedIndexExample {

    public static void main(String[] args) throws IOException {

        /*Framework test example*/
        /*generates an Inverted Index (for words of length greater than 3)*/

        /*Documents path input*/
        Scanner scanner = new Scanner(System.in);
        System.out.println("Insert the absolute path where the *.txt files are located");
        String path = scanner.next();
        File directory = new File(path);

        if(!directory.isDirectory()){
            throw new IllegalArgumentException("Invalid path");
        }

        /*Inverted index output as Stream of pair where
         key: a word
         value: the value of each pair is the list of occurrences for that word
         */
        MapReduce<String,String,String, Pair<String,Integer>, Pair<String, List<Pair<String,Integer>>>> mapReduce;
        mapReduce = new SimpleMapReduce<>();

        /*
        *  return a stream of pairs (fileName, contents), where filename is the name of the text file and contents is a list of strings,
        * one for each line of the file.
        * */
        mapReduce.setRead(
                Arrays.stream(Objects.requireNonNull(directory.listFiles(pathname -> pathname.getName().contains(".txt"))))
                .map(f-> {
                    try {
                        return new Pair<>(f.getName(),Files.lines(Path.of(f.getAbsolutePath())).reduce("", (s1,s2) ->s1+"\n"+s2 ));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new Pair<>(f.getName(),"");
                })
        );

        /*
        * return a stream of pairs containing,
        * key: the word
        * value: pair of word file name and line
        * */
        mapReduce.setMap(pair -> {
            List<String> lines = pair.getValue().lines().collect(Collectors.toList());
            return IntStream.range(0, lines.size())
                    .mapToObj(i -> new Pair<>(i,lines.get(i)))
                    .map(p -> new Pair<>(pair.getKey(),p))
                    .flatMap(p ->
                            Arrays.stream(
                                    p.getValue()
                                    .getValue()
                                    .split("[\\s,;.—_\"!?“‘'-\\-()”\\t:\\[\\]]+"))
                                    .map(String::toLowerCase)
                                    .filter(w -> w.length()>=3)
                                    .map( w -> new Pair<>(w,new Pair<>(p.getKey(),p.getValue().getKey()))));
        });

        /*
        * compare strings according to the standard alphanumeric ordering of the key.
         * */
        mapReduce.setCompare(Comparator.comparing(Pair::getKey));

        /*
        * no reduce needed, just identity function
        * */
        mapReduce.setReduce(
                collected -> collected
        );


        /*
        * writes the stream in a CSV (Comma Separated Value) file with the format
        * word, filename, line
        * */
        final File f = new File(Path.of(path,"output.csv").toUri());
        f.createNewFile();
        if(!f.canWrite()){
            throw new IOException("No write permission in the given directory");
        }
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(f));
        mapReduce.setWrite(stream ->{
                        stream
                                .sorted(Comparator.comparing(Pair::getKey))
                                .forEach(
                                (pair)->
                                    pair.getValue().forEach(
                                        p -> {
                                            try {
                                                fileWriter.append(pair.getKey()).append(",")
                                                        .append(p.getKey()).append(",")
                                                        .append(p.getValue().toString()).append("\n");
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    )
                        );
                        try {
                            fileWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                );


        /*
        * framework start
        * */
        mapReduce.solve();

    }
}
