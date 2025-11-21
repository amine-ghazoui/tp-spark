package org.ghazoui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class Exercice1 {

    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf()
                .setAppName("Analyse des Ventes avec RDD")
                .setMaster("local[*]")
                .set("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.security.action=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.security.action=ALL-UNNAMED");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier de données
        JavaRDD<String> lines = sc.textFile("data/ventes.txt");

        System.out.println("=".repeat(60));
        System.out.println("=== ANALYSE DES VENTES ===");
        System.out.println("=".repeat(60));

        // ===============================================
        // Question 1: Total des ventes par ville
        // ===============================================
        System.out.println("\n--- Question 1: Total des ventes par ville ---\n");

        // Transformation: créer des paires (ville, prix)
        JavaPairRDD<String, Double> ventesParVille = lines.mapToPair(line -> {
            String[] parts = line.split("\\s+"); // Split par espaces
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        });

        // Réduction: calculer le total par ville
        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey(Double::sum);

        // Trier par ville (ordre alphabétique)
        JavaPairRDD<String, Double> totalParVilleTrie = totalParVille.sortByKey();

        // Afficher les résultats
        List<Tuple2<String, Double>> resultatsVille = totalParVilleTrie.collect();
        for (Tuple2<String, Double> vente : resultatsVille) {
            System.out.printf("%-15s : %10.2f DH\n", vente._1, vente._2);
        }

        // Calculer le total général
        double totalGeneral = totalParVille.values().reduce(Double::sum);
        System.out.println("-".repeat(30));
        System.out.printf("TOTAL GÉNÉRAL   : %10.2f DH\n", totalGeneral);

        // ===============================================
        // Question 2: Total des ventes par ville et par année
        // ===============================================
        System.out.println("\n" + "=".repeat(60));
        System.out.println("--- Question 2: Total des ventes par ville et par année ---");
        System.out.println("=".repeat(60) + "\n");

        // Transformation: créer des paires ((année, ville), prix)
        JavaPairRDD<Tuple2<String, String>, Double> ventesParAnneeVille = lines.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            String date = parts[0];
            String annee = date.substring(0, 4); // Extraire l'année (2025)
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);

            // Clé composite: (année, ville)
            return new Tuple2<>(new Tuple2<>(annee, ville), prix);
        });

        // Réduction: calculer le total par (année, ville)
        JavaPairRDD<Tuple2<String, String>, Double> totalParAnneeVille =
                ventesParAnneeVille.reduceByKey(Double::sum);

        // Trier par année puis par ville
        JavaPairRDD<Tuple2<String, String>, Double> totalTrie = totalParAnneeVille
                .sortByKey((cle1, cle2) -> {
                    int compAnnee = cle1._1.compareTo(cle2._1);
                    if (compAnnee != 0) return compAnnee;
                    return cle1._2.compareTo(cle2._2); // Si même année, trier par ville
                });

        // Afficher les résultats groupés par année
        List<Tuple2<Tuple2<String, String>, Double>> resultatsAnneeVille = totalTrie.collect();

        String anneeActuelle = "";
        double totalAnnee = 0.0;

        for (Tuple2<Tuple2<String, String>, Double> vente : resultatsAnneeVille) {
            String annee = vente._1._1;
            String ville = vente._1._2;
            double montant = vente._2;

            // Si nouvelle année, afficher le séparateur
            if (!annee.equals(anneeActuelle)) {
                if (!anneeActuelle.isEmpty()) {
                    // Afficher le total de l'année précédente
                    System.out.println("  " + "-".repeat(40));
                    System.out.printf("  Total %s : %10.2f DH\n\n", anneeActuelle, totalAnnee);
                }
                System.out.println("Année " + annee + ":");
                anneeActuelle = annee;
                totalAnnee = 0.0;
            }

            System.out.printf("  %-15s : %10.2f DH\n", ville, montant);
            totalAnnee += montant;
        }

        // Afficher le total de la dernière année
        if (!anneeActuelle.isEmpty()) {
            System.out.println("  " + "-".repeat(40));
            System.out.printf("  Total %s : %10.2f DH\n", anneeActuelle, totalAnnee);
        }

        // ===============================================
        // Statistiques supplémentaires
        // ===============================================
        System.out.println("\n" + "=".repeat(60));
        System.out.println("--- Statistiques supplémentaires ---");
        System.out.println("=".repeat(60) + "\n");

        // Nombre de transactions
        long nombreTransactions = lines.count();
        System.out.println("Nombre total de transactions : " + nombreTransactions);

        // Prix moyen
        double prixMoyen = totalGeneral / nombreTransactions;
        System.out.printf("Prix moyen par transaction   : %.2f DH\n", prixMoyen);

        // Ville avec le plus de ventes
        Tuple2<String, Double> villeMax = totalParVille
                .max((a, b) -> Double.compare(a._2, b._2));
        System.out.printf("Ville avec le plus de ventes : %s (%.2f DH)\n",
                villeMax._1, villeMax._2);

        // Ville avec le moins de ventes
        Tuple2<String, Double> villeMin = totalParVille
                .min((a, b) -> Double.compare(a._2, b._2));
        System.out.printf("Ville avec le moins de ventes: %s (%.2f DH)\n",
                villeMin._1, villeMin._2);

        // Répartition des produits vendus
        System.out.println("\n--- Répartition par produit ---\n");
        JavaPairRDD<String, Integer> produitsVendus = lines.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            String produit = parts[2];
            return new Tuple2<>(produit, 1);
        });

        JavaPairRDD<String, Integer> compteProduits = produitsVendus
                .reduceByKey(Integer::sum)
                .sortByKey();

        List<Tuple2<String, Integer>> listeProduits = compteProduits.collect();
        for (Tuple2<String, Integer> produit : listeProduits) {
            System.out.printf("%-15s : %d vente(s)\n", produit._1, produit._2);
        }

        // Total des ventes par produit
        System.out.println("\n--- Total des ventes par produit ---\n");
        JavaPairRDD<String, Double> ventesParProduit = lines.mapToPair(line -> {
            String[] parts = line.split("\\s+");
            String produit = parts[2];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(produit, prix);
        });

        JavaPairRDD<String, Double> totalParProduit = ventesParProduit
                .reduceByKey(Double::sum)
                .sortByKey();

        List<Tuple2<String, Double>> listeVentesProduits = totalParProduit.collect();
        for (Tuple2<String, Double> produit : listeVentesProduits) {
            System.out.printf("%-15s : %10.2f DH\n", produit._1, produit._2);
        }

        System.out.println("\n" + "=".repeat(60));
        System.out.println("Analyse terminée avec succès !");
        System.out.println("=".repeat(60));

        // Fermer le contexte Spark
        sc.close();
    }
}