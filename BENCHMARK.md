# UDF vs NoUDF Benchmark

## Performances scalaUDF, pythonUDF, noUDF

### Pour réaliser l'expérience
1. Installer les packages necessaires

A la racine du projet, exécuter la commande suivante:

```shell
$ poetry install
```

installer le plugin shell pour pouvoir instancier un sous-shell poetry 

```shell
$ poetry self add poetry-plugin-shell
```

2. Exécuter le script du test des performances

Utiliser poetry pour lancer la commande suivante:

```bash
$ poetry run udf_jobs_benchmark --runs=10
```

### Contexte et résultats

l'objectif est de déterminer la manière optimale d'implementer une fonctionnalité qui ne serait pas définis par défaut dans Spark. Pour cela, nous disposons des **UDFs** (ser Defined Functions) disponible en **_Python_** et **_Scala_**. L'exemple choisis ici, est d'ajouter une colonne (**_category\_name_**) à un **Dataframe Spark** dont les valeurs seront obtenues via une condition sur le la colonne **category**. Nous réalisons cette 
fonctionnalité dans 3 cas: Sans UDF, avec UDF Python puis avec UDF Scala.

Nous avons implémenté cette tâche grâce à l'implementation de la fonction **add_category_name**. le test de performance implique de mesurer le temps d'exécution de chacune des variantes de la
fonction add_category_name. 
 
Pour chaque job implémenté, nous disposons 4 fonctions utiles: 

- **_start_** effectuant une opération de lecture  
- **_run_** une fonction d'**encapsulation** permettant de faire appel à un enchainement d'autre fonction. 
- **_end_** une fonctionne permettant d'éffectuer une écriture.
- **__main__** la fonction principale appelant les fonction _start_, _run_ et _end_.

Nous avons lançer pour chaque job appelant la fonction **add_category_name** pour un total de **10 exécutions**, puis nous avons calculer le temps moyen pour **4 machines** ayant des caratéristiques différentes comme vous pouvez le constater sur la figure ci-dessous.

Afin que le test soit effective et étant donné que Spark est lazy, nous avons choisi d'appliquer à la fin de chaque appel une action afin de permettre à spark de déclencher effectivement les traitements.

Pour observer clairement les différences entre ces différentes approches, nous avons mené des expérimentations sur différents ordinateurs, chacun ayant des caractéristiques matérielles différentes; Le résultat est affiché dans les tableaux ci-dessous:


#### Action df.write()

###### Huawei, Intel core i7
![A text](/assets/udfVSnoudf_benchmark.png)

###### Huawei, Intel core i9

#### Action df.groupby().count().show()
Le fait de regrouper et compter sur la colonne que nous avons ajouté va provoquer un shuffle. l'objectif est de provoquer des envois de données de la jvm vers python nous permettant de constater une perte de performance.



### Discussions

Comme attendu, on peut constater que pour chaque ordinateur, l’udf python est sous-optimale, à cause des aller-retour entre la JVM et l’éxecuteur python. on peut remarquer également une différence significatif du temps d’execution entre l’udf scala et sans udf. Une hypothèse est que Catalyst ne peut pas faire d’optimisations dans l’udf scala provoquant ce décalage de performances entre les deux versions de la fonction.

Un second facteur qui peut infuencer les performances est la différence matérielle de chaque ordinateur. la machine X est plus lent car XXXXXXX

Enfin on peut envisager les autres processus au sein des machines consommant une partie des ressources comme la RAM, CPUs etc. 




