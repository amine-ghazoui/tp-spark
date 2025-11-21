# TP1 - Programmation des RDDs avec Apache Spark

[![Java](https://img.shields.io/badge/Java-8+-orange.svg)](https://www.oracle.com/java/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-red.svg)](https://spark.apache.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)

> Travaux Pratiques sur la manipulation des RDDs (Resilient Distributed Datasets) avec Apache Spark en Java.

## 📋 Description

Ce projet contient deux exercices pratiques pour apprendre à travailler avec les RDDs Spark :

- **Exercice 1** : Analyse des ventes d'une entreprise (calcul des totaux par ville et par année)
- **Exercice 2** : Analyse de logs Apache (statistiques, top IP, codes HTTP, etc.)

## 🚀 Prérequis

- Java 8 ou supérieur
- Apache Maven 3.6+
- Apache Spark 3.x
- Un IDE Java (IntelliJ IDEA, Eclipse, VSCode)

## 📁 Structure du Projet

```
tp1-spark-rdd/
├── src/
│   └── main/
│       └── java/
│           └── ma/
│               └── enset/
│                   ├── Exercice1.java      # Analyse des ventes
│                   └── Exercice2.java      # Analyse des logs
├── data/
│   ├── ventes.txt                          # Données de ventes
│   └── access.log                          # Logs Apache
├── pom.xml                                 # Configuration Maven
├── run.ps1                                 # Script PowerShell (Windows)
├── run.sh                                  # Script Bash (Linux/Mac)
└── README.md
```

## 🔧 Installation

### 1. Cloner le repository

```bash
git clone https://github.com/votre-username/tp1-spark-rdd.git
cd tp1-spark-rdd
```

### 2. Créer les fichiers de données

**Fichier `data/ventes.txt`** :
```
2025-01-15 Casablanca Ordinateur 12000
2025-01-16 Rabat Téléphone 3000
2025-01-17 Casablanca Tablette 4500
2025-01-18 Marrakech Ordinateur 11500
2025-01-19 Casablanca Téléphone 2800
2025-01-20 Rabat Ordinateur 13000
2025-01-21 Marrakech Tablette 4200
2025-01-22 Casablanca Ordinateur 12500
2025-02-10 Rabat Téléphone 3200
2025-02-11 Marrakech Ordinateur 11800
```

**Fichier `data/access.log`** :
```
127.0.0.1 - - [10/Oct/2025:09:15:32 +0000] "GET /index.html HTTP/1.1" 200 1024 "http://example.com" "Mozilla/5.0"
192.168.1.10 - john [10/Oct/2025:09:17:12 +0000] "POST /login HTTP/1.1" 302 512 "-" "curl/7.68.0"
203.0.113.5 - - [10/Oct/2025:09:19:01 +0000] "GET /docs/report.pdf HTTP/1.1" 404 64 "-" "Mozilla/5.0"
198.51.100.7 - - [10/Oct/2025:09:25:48 +0000] "GET /api/data?id=123 HTTP/1.1" 500 128 "-" "PostmanRuntime/7.26.8"
192.168.1.11 - jane [10/Oct/2025:09:30:05 +0000] "GET /dashboard HTTP/1.1" 200 4096 "http://intranet" "Mozilla/5.0"
```

### 3. Compiler le projet

```bash
mvn clean package
```

## 💻 Utilisation

### Méthode 1 : Avec les scripts

**Windows (PowerShell)** :
```powershell
.\run.ps1 1      # Exécuter l'Exercice 1
.\run.ps1 2      # Exécuter l'Exercice 2
.\run.ps1 all    # Exécuter les deux exercices
```

**Linux/Mac (Bash)** :
```bash
chmod +x run.sh
./run.sh 1       # Exécuter l'Exercice 1
./run.sh 2       # Exécuter l'Exercice 2
./run.sh all     # Exécuter les deux exercices
```

### Méthode 2 : Avec spark-submit

**Exercice 1** :
```bash
spark-submit \
  --class ma.enset.Exercice1 \
  --master local[*] \
  target/tp1-spark-rdd-1.0.jar
```

**Exercice 2** :
```bash
spark-submit \
  --class ma.enset.Exercice2 \
  --master local[*] \
  target/tp1-spark-rdd-1.0.jar
```

## 📊 Résultats Attendus

### Exercice 1 - Analyse des Ventes

```
============================================================
=== ANALYSE DES VENTES ===
============================================================

--- Question 1: Total des ventes par ville ---

Casablanca      :   31800.00 DH
Marrakech       :   27500.00 DH
Rabat           :   16200.00 DH
------------------------------
TOTAL GÉNÉRAL   :   75500.00 DH

--- Question 2: Total des ventes par ville et par année ---

Année 2025:
  Casablanca      :   31800.00 DH
  Marrakech       :   27500.00 DH
  Rabat           :   16200.00 DH
```

### Exercice 2 - Analyse des Logs

```
============================================================
=== ANALYSE DES LOGS APACHE ===
============================================================

1. Nombre total de requêtes: 5
2. Nombre total d'erreurs (≥400): 2
3. Pourcentage d'erreurs: 40.00%

============================================================
4. TOP 5 DES ADRESSES IP
============================================================
1. 192.168.1.10 : 1 requêtes
2. 127.0.0.1 : 1 requêtes
...

============================================================
6. RÉPARTITION DES REQUÊTES PAR CODE HTTP
============================================================
Code 200 (Succès) : 2 requêtes (40.00%)
Code 302 (Redirection) : 1 requêtes (20.00%)
Code 404 (Erreur client) : 1 requêtes (20.00%)
Code 500 (Erreur serveur) : 1 requêtes (20.00%)
```

## 🧪 Concepts Spark Utilisés

### Transformations (Lazy)
- `map()` - Transformer chaque élément
- `filter()` - Filtrer les éléments
- `mapToPair()` - Créer des paires clé-valeur
- `reduceByKey()` - Agréger par clé
- `sortByKey()` - Trier par clé

### Actions (Eager)
- `collect()` - Récupérer tous les éléments
- `count()` - Compter les éléments
- `take(n)` - Récupérer n éléments
- `reduce()` - Agréger tous les éléments

### Optimisations
- `cache()` - Mettre en cache les RDD réutilisés
- `unpersist()` - Libérer la mémoire

## 🛠️ Technologies

- **Java 8+** - Langage de programmation
- **Apache Spark 3.5.0** - Framework de traitement distribué
- **Maven** - Gestion des dépendances
- **Scala 2.12** - Version de Scala pour Spark

## 📚 Exercices

### Exercice 1 : Analyse des Ventes
Analyse d'un fichier de ventes pour calculer :
- Le total des ventes par ville
- Le total des ventes par ville et par année
- Statistiques supplémentaires (ville max/min, répartition par produit)

### Exercice 2 : Analyse de Logs Apache
Analyse de logs web pour extraire :
- Nombre total de requêtes et d'erreurs
- Top 5 des adresses IP les plus actives
- Top 5 des ressources les plus demandées
- Répartition des codes HTTP



---

⭐ Si ce projet vous a été utile, n'oubliez pas de lui donner une étoile !
