### Comparaison des Performances des UDF dans Spark

Lors de nos tests, nous avons mesuré les temps d'exécution de différentes méthodes dans Spark :

- **UDF en Python** : `0.0286` secondes  
  Les fonctions définies par l'utilisateur en Python sont souvent plus lentes que les fonctions intégrées de Spark. Cela s'explique par le coût de la sérialisation et désérialisation entre l'environnement JVM (Java Virtual Machine) et Python via PySpark.

- **Fonctions Intégrées (sans UDF)** : `0.0191` secondes  
  Utiliser les fonctions intégrées de Spark est généralement la méthode la plus rapide. Comme tout se passe dans la JVM, il n'y a pas de coûts supplémentaires liés à la communication entre Python et Spark.

- **UDF en Scala** : `0.0470` secondes  
  Bien que les UDF en Scala soient exécutées directement dans la JVM, elles se sont révélées plus lentes que les fonctions intégrées dans notre cas. Cela pourrait être dû à une surcharge liée à la définition et à l'exécution de l'UDF en Scala par rapport à l'utilisation directe des API de Spark.

### Conclusion

En résumé, les fonctions intégrées de Spark offrent les meilleures performances. Les UDF, qu'elles soient en Python ou en Scala, ajoutent une surcharge qui peut ralentir l'exécution. En général, il est conseillé d'utiliser les fonctions natives de Spark autant que possible pour optimiser les performances.
