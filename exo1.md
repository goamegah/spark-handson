# Créer un job Spark

Tout cet exercice va se passer dans le fichier `fr.hymaia.exo1.main.py`

## SparkSession

Dans un premier temps, nous allons travailler en local sur notre IDE. Nous allons donc créer un SparkSession en conséquence dans la fonction `main`.

1. On définit master à local[*]
2. On donne un nom à notre job Spark : `wordcount`

* Que signifie local[*] ?
* Pourquoi avoir choisi local[*] et non pas simplement local ?

## Lire et écrire un fichier

Nous souhaitons valider que nous réussissons à lire et écrire un fichier. Pour cela nous allons réaliser un wordcount !

Rappel :
```
input :
hello hello world

output :
[('hello', 2), ('world', 1)]
```

Les transformations Spark sont déjà codés. Il ne reste plus qu'à les utiliser :

1. Lire le fichier `src/resources/exo1/data.csv`
2. Appliquer la fonction `wordcount` à notre dataframe avec le bon nom de colonne
3. On veut écrire le résultat dans `data/exo1/output` au format parquet
4. La donnée devra être partitionnée par "count"
5. Pour exécuter le code :

```shell
poetry run wordcount
```

* Quels sont les avantages du format de fichier parquet ?


[Précédent](exo0.md) <- -> [Suivant](exo2.md)
