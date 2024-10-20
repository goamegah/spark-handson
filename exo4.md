# UDF et Window functions

## La donnée

La donnée est présente dans `resources/exo4`.

## UDFs

Dans cet exercice, nous allons observer la différence de performance entre des UDFs Python et Scala. 

Nous verrons aussi comment utiliser des UDF Scala en Python.

### UDF Python

Créez le fichier `scr.fr.hymaia.exo4.python_udf.py`

Notre source de données `sell` contient les 4 colonnes suivantes :

```
id, date, category, price
0, 2019-02-17, 6, 40.0
1, 2015-10-01, 4, 69.0
```

Ajoutez une nouvelle colonne nommée `category_name` comme suit :

```
Si category < 6
category_name = food

sinon
category_name = furniture
```

Exemple :
```
id, date, category, price, category_name
0, 2019-02-17, 6, 40.0, furniture
1, 2015-10-01, 4, 69.0, food
```

### UDF Scala

Nous allons utiliser le fichier `src.fr.hymaia.exo4.scala_udf.py`

Un jar est présent dans `resources/exo4`. Il contient une UDF Scala. Pour l'utiliser, ajoutez une configuration à votre SparkSession :

```python
config('spark.jars', 'src/resources/exo4/udf.jar')
```

Maintenant, il faut récupérer la fonction Scala en Python. Pour cela, créez la fonction suivante :

```python
from pyspark.sql.column import Column, _to_java_column, _to_seq

def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))
```

Il ne reste plus qu'à utiliser notre UDF comme précédemment.

Pour exécuter votre job Spark, lancez :
```shell
poetry run scala_udf
```

### Sans UDF

Créez le fichier `scr.fr.hymaia.exo4.no_udf.py`

Utilisez uniquement des fonctions de Spark pour arriver au même résultat.

Exécutez les 3 jobs et comparez les temps

## Window functions

On continue dans le fichier `no_udf.py`. Notre source de données contient désormais ces 4 colonnes :

```
id, date, category, price, category_name
0, 2019-02-17, 6, 40.0, furniture
1, 2015-10-01, 4, 69.0, food
```

Nous souhaitons calculer la somme des prix de chaque catégorie d'article par jour dans une colonne appelée `total_price_per_category_per_day`

Exemple :

```
id, date, category, price, category_name, total_price_per_category_per_day
0, 2019-02-17, 6, 40.0, furniture, 77.0
0, 2019-02-17, 6, 33.0, furniture, 77.0
0, 2019-02-17, 4, 70.0, food, 82.0
0, 2019-02-17, 4, 12.0, food, 82.0
0, 2019-02-18, 6, 20.0, furniture, 45.0
0, 2019-02-18, 6, 25.0, furniture, 45.0
```

Nous souhaitons également connaitre la somme des prix de chaque catégorie d'article sur les 30 derniers jours dans une colonne appelée `total_price_per_category_per_day_last_30_days`

Exemple :

```
id, date, category, price, category_name, total_price_per_category_per_day_last_30_days
0, 2019-02-16, 6, 40.0, furniture,  40.0
0, 2019-02-17, 6, 33.0, furniture, 73.0
0, 2019-02-18, 6, 70.0, furniture, 143.0
0, 2019-02-16, 4, 12.0, food, 12.0
0, 2019-02-17, 4, 20.0, food, 32.0
0, 2019-02-18, 4, 25.0, food, 57.0
```

[Précédent](exo3.md) <- -> [Suivant](exo5.md)
