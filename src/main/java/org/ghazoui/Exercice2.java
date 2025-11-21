package org.ghazoui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exercice2 {

    // Pattern pour parser les logs Apache
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"(\\S+) (\\S+) \\S+\" (\\d{3}) (\\d+|-)"
    );

    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Analyse Logs Apache")
                .setMaster("local[*]")
                .set("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.security.action=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.security.action=ALL-UNNAMED");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier de logs
        JavaRDD<String> lignes = sc.textFile("data/access.log");

        // Parser les lignes et créer un RDD avec les informations extraites
        JavaRDD<String[]> logsParsed = lignes.map(ligne -> {
            Matcher matcher = LOG_PATTERN.matcher(ligne);
            if (matcher.find()) {
                String ip = matcher.group(1);
                String dateTime = matcher.group(2);
                String method = matcher.group(3);
                String resource = matcher.group(4);
                String httpCode = matcher.group(5);
                String size = matcher.group(6);

                return new String[]{ip, dateTime, method, resource, httpCode, size};
            }
            return null;
        }).filter(log -> log != null);

        // Cache pour réutilisation
        logsParsed.cache();

        System.out.println("=".repeat(60));
        System.out.println("=== ANALYSE DES LOGS APACHE ===");
        System.out.println("=".repeat(60));

        // ===============================================
        // 1. Nombre total de requêtes
        // ===============================================
        long totalRequetes = logsParsed.count();
        System.out.println("\n1. Nombre total de requêtes: " + totalRequetes);

        // ===============================================
        // 2. Nombre d'erreurs (code >= 400)
        // ===============================================
        long totalErreurs = logsParsed.filter(log -> {
            int code = Integer.parseInt(log[4]);
            return code >= 400;
        }).count();

        double pourcentageErreurs = (totalErreurs * 100.0) / totalRequetes;
        System.out.println("2. Nombre total d'erreurs (≥400): " + totalErreurs);
        System.out.printf("3. Pourcentage d'erreurs: %.2f%%\n", pourcentageErreurs);

        // ===============================================
        // 4. Top 5 des IPs
        // ===============================================
        System.out.println("\n" + "=".repeat(60));
        System.out.println("4. TOP 5 DES ADRESSES IP");
        System.out.println("=".repeat(60));

        JavaPairRDD<String, Integer> requetesParIP = logsParsed
                .mapToPair(log -> new Tuple2<>(log[0], 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> top5IPs = requetesParIP
                .mapToPair(tuple -> tuple.swap()) // Inverser pour trier
                .sortByKey(false) // Tri décroissant
                .mapToPair(tuple -> tuple.swap()) // Remettre en ordre
                .take(5);

        int rank = 1;
        for (Tuple2<String, Integer> entry : top5IPs) {
            System.out.printf("%d. %s : %d requêtes\n", rank++, entry._1, entry._2);
        }

        // ===============================================
        // 5. Top 5 des ressources
        // ===============================================
        System.out.println("\n" + "=".repeat(60));
        System.out.println("5. TOP 5 DES RESSOURCES LES PLUS DEMANDÉES");
        System.out.println("=".repeat(60));

        JavaPairRDD<String, Integer> requetesParRessource = logsParsed
                .mapToPair(log -> new Tuple2<>(log[3], 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> top5Ressources = requetesParRessource
                .mapToPair(tuple -> tuple.swap())
                .sortByKey(false)
                .mapToPair(tuple -> tuple.swap())
                .take(5);

        rank = 1;
        for (Tuple2<String, Integer> entry : top5Ressources) {
            System.out.printf("%d. %s : %d requêtes\n", rank++, entry._1, entry._2);
        }

        // ===============================================
        // 6. Répartition par code HTTP
        // ===============================================
        System.out.println("\n" + "=".repeat(60));
        System.out.println("6. RÉPARTITION DES REQUÊTES PAR CODE HTTP");
        System.out.println("=".repeat(60));

        JavaPairRDD<Integer, Integer> repartitionCodes = logsParsed
                .mapToPair(log -> new Tuple2<>(Integer.parseInt(log[4]), 1))
                .reduceByKey((a, b) -> a + b)
                .sortByKey();

        List<Tuple2<Integer, Integer>> codesHTTP = repartitionCodes.collect();
        for (Tuple2<Integer, Integer> entry : codesHTTP) {
            int code = entry._1;
            int count = entry._2;
            double pct = (count * 100.0) / totalRequetes;
            String description = getCodeDescription(code);

            System.out.printf("Code %d (%s) : %d requêtes (%.2f%%)\n",
                    code, description, count, pct);
        }

        System.out.println("\n" + "=".repeat(60));

        // Libérer le cache et fermer
        logsParsed.unpersist();
        sc.close();
    }

    private static String getCodeDescription(int code) {
        if (code >= 200 && code < 300) return "Succès";
        if (code >= 300 && code < 400) return "Redirection";
        if (code >= 400 && code < 500) return "Erreur client";
        if (code >= 500 && code < 600) return "Erreur serveur";
        return "Autre";
    }
}