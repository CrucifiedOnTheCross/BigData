package ru.bigdata.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import ru.bigdata.lab2.model.LanguageAlias;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

public final class LanguageDictionary {

    private LanguageDictionary() {
    }

    public static Dataset<LanguageAlias> readAliases(SparkSession spark, Path csvPath) {
        Path normalized = csvPath.toAbsolutePath().normalize();
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException("Input file was not found: " + normalized);
        }

        JavaRDD<String> rows = spark.read()
                .textFile(normalized.toUri().toString())
                .javaRDD()
                .zipWithIndex()
                .filter(tuple -> tuple._2 > 0)
                .keys();

        JavaPairRDD<String, String> canonicalNamesByAlias = rows
                .flatMap(line -> parseAliases(line).iterator())
                .mapToPair(alias -> new scala.Tuple2<>(alias.getAlias(), alias.getCanonicalName()))
                .reduceByKey(LanguageDictionary::preferCanonicalName);

        JavaRDD<LanguageAlias> aliases = canonicalNamesByAlias
                .map(tuple -> new LanguageAlias(tuple._2, tuple._1));

        return spark.createDataset(aliases.rdd(), Encoders.bean(LanguageAlias.class));
    }

    static Set<LanguageAlias> parseAliases(String csvLine) {
        String[] parts = csvLine.split(",", 2);
        if (parts.length != 2) {
            return Set.of();
        }

        String sourceName = parts[0].trim();
        String canonicalName = normalizeCanonicalName(sourceName);
        String wikipediaUrl = parts[1].trim();
        if (canonicalName.isBlank()) {
            return Set.of();
        }

        Set<String> aliases = new LinkedHashSet<>();
        aliases.add(normalizeAlias(canonicalName));
        aliases.add(normalizeAlias(removeParenthetical(canonicalName)));
        aliases.add(normalizeAlias(canonicalName.replace(' ', '-')));

        String urlTail = extractWikipediaTitle(wikipediaUrl);
        String decoded = URLDecoder.decode(urlTail, StandardCharsets.UTF_8)
                .replace('_', ' ');
        aliases.add(normalizeAlias(decoded));
        aliases.add(normalizeAlias(removeParenthetical(decoded)));
        aliases.add(normalizeAlias(decoded.replace(' ', '-')));

        addManualAliases(canonicalName, aliases);

        Set<LanguageAlias> result = new LinkedHashSet<>();
        for (String alias : aliases) {
            if (!alias.isBlank()) {
                result.add(new LanguageAlias(canonicalName, alias));
            }
        }
        return result;
    }

    private static String extractWikipediaTitle(String wikipediaUrl) {
        int wikiIndex = wikipediaUrl.indexOf("/wiki/");
        if (wikiIndex >= 0) {
            return wikipediaUrl.substring(wikiIndex + "/wiki/".length());
        }
        return wikipediaUrl.substring(wikipediaUrl.lastIndexOf('/') + 1);
    }

    private static String normalizeCanonicalName(String sourceName) {
        return switch (sourceName) {
            case "C++ – ISO/IEC 14882" -> "C++";
            case "Swift (Apple programming language)", "Swift (parallel scripting language)" -> "Swift";
            default -> sourceName;
        };
    }

    static String preferCanonicalName(String left, String right) {
        int leftScore = canonicalNameScore(left);
        int rightScore = canonicalNameScore(right);
        if (leftScore != rightScore) {
            return leftScore < rightScore ? left : right;
        }
        if (left.length() != right.length()) {
            return left.length() < right.length() ? left : right;
        }
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static int canonicalNameScore(String canonicalName) {
        int score = 0;
        if (canonicalName.contains("(") || canonicalName.contains(")")) {
            score += 10;
        }
        if (canonicalName.contains(" – ") || canonicalName.contains(" - ")) {
            score += 8;
        }
        if (canonicalName.contains(" and")) {
            score += 6;
        }
        return score;
    }

    private static void addManualAliases(String canonicalName, Set<String> aliases) {
        String normalized = canonicalName.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "c#" -> aliases.add("c#");
            case "c++" -> aliases.add("c++");
            case "f#" -> aliases.add("f#");
            case "go" -> {
                aliases.add("go");
                aliases.add("golang");
            }
            case "visual basic .net" -> {
                aliases.add("vb.net");
                aliases.add("visual-basic");
            }
            case "common lisp" -> aliases.add("common-lisp");
            case "objective-c" -> aliases.add("objective-c");
            case "assembly language" -> {
                aliases.add("assembly");
                aliases.add("assembly-language");
            }
            case "sql" -> aliases.add("sql");
            case "r" -> aliases.add("r");
            default -> {
            }
        }
    }

    private static String removeParenthetical(String value) {
        int bracketIndex = value.indexOf('(');
        return bracketIndex >= 0 ? value.substring(0, bracketIndex).trim() : value.trim();
    }

    private static String normalizeAlias(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT).replace(' ', '-');
    }
}
