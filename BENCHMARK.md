# Performances scalaUDF, pythonUDF, noUDF

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

Afin que le test soit effective et étant donné que Spark est lazy, nous avons choisi d'appliquer à la fin de chaque appel une action afin de permettre à spark de déclencher effectivement les traitements. Par exemple un _df.show_ ne déclenchera aucun traitement à part le calcul des schémas au moment de la lecture des données. Pour cela nous avons choisis d'exécuter les actions comme **_df.write_**, **_df.groupby.count.show_** et **_df.groupby.count.count_**. Il est à noter que une action **groupby** provoquera un shuffle ce qui donnera lieu à un 2 stages et donc obligera sparl à déclencher tout le traitement d'un stage à l'autre. 

Pour observer clairement les différences entre ces différentes approches, nous avons mené des expérimentations sur différents ordinateurs, chacun ayant des caractéristiques matérielles différentes; Le résultat est affiché dans les tableaux ci-dessous:


### Action df.write()
###### Huawei, Intel core i7
![A text](/assets/udfVSnoudf_benchmark.png)

###### HP, Intel core i5
![A text](/assets/udfVSnoudf_hp.png)

###### Dell, Intel core i9
![A text](/assets/udfVSnoudf_dell.png)

### Action df.groupby().count().show()
###### Huawei, Intel core i7
Le fait de regrouper et compter sur la colonne que nous avons ajouté va provoquer un shuffle. l'objectif est de provoquer des envois de données de la jvm vers python nous permettant de constater une perte de performance.
![A text](/assets/udf_groupby_huawei.png)

###### HP, Intel core i5
![A text](/assets/udf_groupby_hp.png)

###### Dell, Intel core i9
![A text](/assets/udf_groupby_dell.png)

### Action df.groupby().count().count()
###### Huawei, Intel core i7
Le fait de regrouper et compter sur la colonne que nous avons ajouté va provoquer un shuffle. l'objectif est de provoquer des envois de données de la jvm vers python nous permettant de constater une perte de performance.
![A text](/assets/udf_groupby_count_count_huawei.png)

###### Dell, Intel core i9
![A text](/assets/udf_groupby_count_count_dell.png)


### Discussions

L’UDF Python est globalement sous-optimale en raison des échanges entre la JVM et Python. 

L’UDF Scala montre des performances inférieures à une exécution sans UDF, probablement à cause des limitations de Catalyst en termes d’optimisation. 

Les performances sont aussi influencées par les caractéristiques matérielles des machines (i9 > i7 > i5) et les processus en arrière-plan. Comme, nous pouvons le constater la **Machine i9** a plus de ressource en terme de nombre de **core** physique, de **RAM** que la **Machine i7** qui lui en a plus que la **machine i5**.

Enfin, l’action **write** est plus lente que les variantes **groupby**(). il s'agit des actions tous les deux parallélisable. Cependant une **écriture** prendra plus de temps qu'un **groupby** entrainant un shuffle. 



