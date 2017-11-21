package afilippo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tasks {
    private static final String FILE_REPORT = "report.txt";
    public static void main(String[] args) throws Exception {
        String inputDirectory = "input";
        String wordcountDirectory = "output-wordcount";
        String orderedDirectory = "output-ordered";
        String task2OutputDirectory = "output-7-word";
        String task3OutputDirectory = "output-stopwordscount";
        String namesDirectory = "output-names";
        String namesOrderedDirectory = "output-ordered-names";
        String task4OutputDirectory = "output-5-name";

        //
        //    ─█───███─████──███──████─█──█─█──█─███
        //    ██─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ─█───███─████──█─█──████─████─█─██─███
        //    ─█─────█─█──█─█████─█──█─█──█─██─█─█
        //    ─█───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Считаем wordcount
        // Это база для остальных заданий

        Report task1 = new Report("Задание 1")
            .addAction(
                "Считаем wordcount",
                calculateTaskTime(new WordCount(), inputDirectory, wordcountDirectory)
            );

        //
        //    ████───███─████──███──████─█──█─█──█─███
        //    █──█─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ──██───███─████──█─█──████─████─█─██─███
        //    ██───────█─█──█─█████─█──█─█──█─██─█─█
        //    ████───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Вывести 7-ое по популярности слово

        Report task2 = new Report("Задание 2")
            .addAction(
                "Сортируем слова",
                calculateTaskTime(new WordsOrder(), wordcountDirectory, orderedDirectory)
            )
            .addAction(
                "Вывод только 7-го слова",
                calculateTaskTime(new NValue(), orderedDirectory, task2OutputDirectory, "6")
            );

        //
        //    ███───███─████──███──████─█──█─█──█─███
        //    ──█─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ███───███─████──█─█──████─████─█─██─███
        //    ──█─────█─█──█─█████─█──█─█──█─██─█─█
        //    ███───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Посчитать процент стоп-слов

        Report task3 = new Report("Задание 3")
            .addAction(
                "Считаем процент стоп-слов",
                calculateTaskTime(new StopWordsCount(), wordcountDirectory, task3OutputDirectory, "stop_words_en.txt")
            );

        //
        //    █──────███─████──███──████─█──█─█──█─███
        //    █──█─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ████───███─████──█─█──████─████─█─██─███
        //    ───█─────█─█──█─█████─█──█─█──█─██─█─█
        //    ───█───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Посчитать имена и вывести пятое по популярности
        // Находим имена и кладем их куда нибудь
        Report task4 = new Report("Задание 4")
            .addAction(
                "Находим имена и кладем их в папку",
                calculateTaskTime(new NamesPercent(), wordcountDirectory, namesDirectory)
            )
            .addAction(
                "Сортируем имена по убыванию частоты",
                calculateTaskTime(new WordsOrder(), namesDirectory, namesOrderedDirectory)
            )
            .addAction(
                "Выводим только 5-е имя (5 => 0 to 4)",
                calculateTaskTime(new NValue(), namesOrderedDirectory, task4OutputDirectory, "4")
            );


        Files.write(
            Paths.get(FILE_REPORT),
            String.join(
                "\r\n\r\n",
                task1.getString(),
                task2.getString(),
                task3.getString(),
                task4.getString()
            ).getBytes()
        );
    }

    private static double calculateTaskTime(Tool tool, String... parameters) throws Exception{
        long from = System.currentTimeMillis();

        ToolRunner.run(new Configuration(), tool, parameters);

        long to = System.currentTimeMillis();
        return (to - from)/(double)1000;
    }

    private static class Report {
        private List<String> stringList;

        Report(String name) {
            stringList = new ArrayList<>();
            stringList.add(name);
        }

        Report addAction(String description, double time){
            stringList.add(formatTimeWithText(time, description));

            return this;
        }

        private String formatTimeWithText(double time, String text){
            return String.format("%.3f", time) + "s" + " - " + text;
        }

        public String getString() throws IOException {
            return String.join("\r\n", stringList);
        }
    }
}