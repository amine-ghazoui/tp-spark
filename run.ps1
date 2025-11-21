# Script PowerShell pour compiler et exécuter les exercices Spark
# Usage: .\run.ps1 [1|2|all]

param(
    [Parameter(Position=0)]
    [string]$Exercice = ""
)

# Fonction pour afficher en couleur
function Write-Color {
    param(
        [string]$Text,
        [string]$Color = "White"
    )
    Write-Host $Text -ForegroundColor $Color
}

Write-Color "==================================================" "Cyan"
Write-Color "     TP1 - Spark RDD - Execution" "Cyan"
Write-Color "==================================================" "Cyan"

# Fonction pour compiler
function Compile {
    Write-Host ""
    Write-Color "Compilation du projet..." "Yellow"

    mvn clean package -q

    if ($LASTEXITCODE -eq 0) {
        Write-Color "Compilation reussie" "Green"
        return $true
    } else {
        Write-Color "Erreur de compilation" "Red"
        return $false
    }
}

# Fonction pour exécuter Exercice 1
function Run-Ex1 {
    Write-Host ""
    Write-Color "==================================================" "Cyan"
    Write-Color "     Exercice 1 - Analyse des Ventes" "Cyan"
    Write-Color "==================================================" "Cyan"
    Write-Host ""

    spark-submit --class ma.enset.Exercice1 --master "local[*]" target/tp1-spark-rdd-1.0.jar

    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Color "Exercice 1 termine avec succes" "Green"
    } else {
        Write-Host ""
        Write-Color "Erreur lors de execution de Exercice 1" "Red"
    }
}

# Fonction pour exécuter Exercice 2
function Run-Ex2 {
    Write-Host ""
    Write-Color "==================================================" "Cyan"
    Write-Color "     Exercice 2 - Analyse des Logs" "Cyan"
    Write-Color "==================================================" "Cyan"
    Write-Host ""

    spark-submit --class ma.enset.Exercice2 --master "local[*]" target/tp1-spark-rdd-1.0.jar

    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Color "Exercice 2 termine avec succes" "Green"
    } else {
        Write-Host ""
        Write-Color "Erreur lors de execution de Exercice 2" "Red"
    }
}

# Vérifier les arguments
if ($Exercice -eq "") {
    Write-Color "Usage: .\run.ps1 [1|2|all]" "Yellow"
    Write-Host "  1   - Executer Exercice 1"
    Write-Host "  2   - Executer Exercice 2"
    Write-Host "  all - Executer les deux exercices"
    exit 1
}

# Compiler d'abord
$compileResult = Compile
if (-not $compileResult) {
    exit 1
}

# Exécuter selon le choix
switch ($Exercice) {
    "1" {
        Run-Ex1
    }
    "2" {
        Run-Ex2
    }
    "all" {
        Run-Ex1
        Write-Host ""
        Run-Ex2
    }
    default {
        Write-Color "Option invalide. Utilisez: 1, 2, ou all" "Red"
        exit 1
    }
}

Write-Host ""
Write-Color "==================================================" "Cyan"
Write-Color "Execution terminee !" "Green"
Write-Color "==================================================" "Cyan"