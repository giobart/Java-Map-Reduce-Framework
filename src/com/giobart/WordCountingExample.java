package com.giobart;

import com.giobart.mapreduce.MapReduce;
import com.giobart.mapreduce.impl.Pair;
import com.giobart.mapreduce.impl.SimpleMapReduce;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordCountingExample {

    public static void main(String[] args) throws IOException {

        /*Framework test example*/
        /*counts the occurrences of words of length greater than 3 in a given set of documents*/

        Scanner scanner = new Scanner(System.in);
        System.out.println("Insert the absolute path where the *.txt files are located");
        String path = scanner.next();
        File directory = new File(path);

        if(!directory.isDirectory()){
            throw new IllegalArgumentException("Invalid path");
        }


        MapReduce<String,String,String, Integer, Pair<String,Integer>> mapReduce;
        mapReduce = new SimpleMapReduce<>();

        /*
        *  return a stream of pairs (fileName, contents), where filename is the name of the text file and contents is a list of strings,
        * one for each line of the file.
        * */
        mapReduce.setRead(
                Arrays.stream(Objects.requireNonNull(directory.listFiles(pathname -> pathname.getName().contains(".txt"))))
                .flatMap(f ->
                        {
                            Stream<Pair<String,String>> result = Stream.empty();
                            try {
                                result =  Files.lines(Path.of(f.getAbsolutePath())).map(str -> new Pair<>(f.getName(),str));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return result;
                        }
                )
        );

        /*
        * return a stream of pairs containing, for each word (of length greater than 3) in a line,
        * the pair (w, k) where k is the number of occurrences of w in that line.
        * */
        mapReduce.setMap(pair ->
            Arrays.stream(pair.getValue().split("[\\s,;.—_\"!?\\n“‘'-\\-()”\\t:\\[\\]]+"))
                    .filter(w -> w.length()>=3)
                    .map(String::toLowerCase)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .entrySet()
                    .stream()
                    .map(entry -> new Pair<>(entry.getKey(),entry.getValue().intValue()))
        );

        /*
        * compare strings according to the standard alphanumeric ordering.
         * */
        mapReduce.setCompare(Comparator.comparing(Pair::getKey));

        /*
        * takes as input a stream of pairs (w, lst) where w is a string and lst is a list of integers.
        * It returns a corresponding stream of pairs (w, sum) where sum is the sum of the integers in lst
        * */
        mapReduce.setReduce(
                collected ->
                        collected
                                .map(entry -> new Pair<>(entry.getKey(),entry.getValue().stream().mapToInt(Integer::intValue).sum()))
        );


        /*
        * writes the stream in a CSV (Comma Separated Value) file, one pair per line, in alphanumeric ordering
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
                                (pair)-> {
                                    try {
                                        fileWriter.append(pair.getKey()).append(",").append(String.valueOf(pair.getValue())).append("\n");
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
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
