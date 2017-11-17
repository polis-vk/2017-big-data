package afilippo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Tasks {
    public static void main(String[] args) throws Exception{
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
        ToolRunner.run(
                new Configuration(),
                new WordCount(),
                new String[]{inputDirectory, wordcountDirectory}
                );

        //
        //    ████───███─████──███──████─█──█─█──█─███
        //    █──█─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ──██───███─████──█─█──████─████─█─██─███
        //    ██───────█─█──█─█████─█──█─█──█─██─█─█
        //    ████───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Вывести 7-ое по популярности слово
        // Сначала сортируем слова
        ToolRunner.run(
                new Configuration(),
                new WordsOrder(),
                new String[]{wordcountDirectory, orderedDirectory}
        );
        // Затем выводим только 7-е слово (7 => 0 to 6)
        ToolRunner.run(
                new Configuration(),
                new NValue(),
                new String[]{orderedDirectory, task2OutputDirectory, "6"}
        );

        //
        //    ███───███─████──███──████─█──█─█──█─███
        //    ──█─────█─█──█──█─█──█──█─█──█─█──█─█
        //    ███───███─████──█─█──████─████─█─██─███
        //    ──█─────█─█──█─█████─█──█─█──█─██─█─█
        //    ███───███─█──█─█───█─█──█─█──█─█──█─███
        //
        // Посчитать процент стоп-слов
        ToolRunner.run(
                new Configuration(),
                new StopWordsCount(),
                new String[]{wordcountDirectory, task3OutputDirectory, "stop_words_en.txt"}
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
        ToolRunner.run(
                new Configuration(),
                new NamesPercent(),
                new String[]{wordcountDirectory, namesDirectory}
        );
        // Сортируем имена по убыванию частоты
        ToolRunner.run(
                new Configuration(),
                new WordsOrder(),
                new String[]{namesDirectory, namesOrderedDirectory}
        );
        // Выводим только 5-е имя (5 => 0 to 4)
        ToolRunner.run(
                new Configuration(),
                new NValue(),
                new String[]{namesOrderedDirectory, task4OutputDirectory, "4"}
        );
    }
}
