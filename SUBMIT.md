# How use spark-submit locally

Pour exécuter son job Spark, un outil très pratique existe : `spark-submit`.

Petit problème : Poetry génère des packages python au format tar.gz ou wheel alors que `spark-submit` n'accepte que
des **zip** ou des **egg**.

Nous allons utiliser setuptools pour générer un package compatible avec `spark-submit`.

## Installation

```shell
$ poetry add setuptools
```

## Configuration

Créez un fichier `setup.py` à la racine de votre projet :

```python
from setuptools import setup, find_packages

setup(
    name='spark-app',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
)
```

## Génération du package

```shell
$ poetry build
```

## Exécution

```shell
$ spark-submit --master local[*] dist/spark_app-0.1.tar.gz
```

## Conclusion

`spark-submit` est un outil très pratique pour lancer des jobs Spark en local. Cependant, il est déconseillé pour
déployer sur un cluster Hadoop ou Kubernetes. Il existe d'autres outils plus moderne pour cela.
