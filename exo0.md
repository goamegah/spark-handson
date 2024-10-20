# Installer Spark

## Prérequis

### Windows

Installer WSL et Ubuntu via Microsoft Store.

Pour travailler dans de bonnes conditions je recommande VScode avec le plugin **Remote Development**.

Une fois WSL et Ubuntu d'installés, toutes les prochaines étapes seront à faire dans le terminal Ubuntu.

### Pour tout le monde

#### Poetry

##### MacOS

```shell
brew install poetry
```

##### Ubuntu

Installer poetry en suivant la [documentation du site officiel](https://python-poetry.org/docs/#installation)

**Attention à ne pas installer poetry avec pip ou pip3.**

Mettez à jour votre PATH comme indiqué à la fin de l'installation de `poetry` en l'ajoutant à votre `~/.bashrc`.

#### Python 3.10

```bash
sudo apt update
sudo apt install python3.10
```

## Les dépendances

1. Cloner le repo github
2. Ouvrir le projet dans VScode

```bash
code <dossier créé par git clone>
```

3. Installer les dépendances du projet :

```shell
poetry install
```

## Spark

### MacOS

```shell
brew install apache-spark
```

### Ubuntu

1. Ouvrez la page de [téléchargement de Spark](https://spark.apache.org/downloads.html) et suivez les instructions.
2. Dézippez et déplacez le contenu du dossier dans `/opt/spark`

Pour vérifier que c'est bon, lancez :

```bash
$ ls /opt/spark
bin   data      jars        LICENSE   NOTICE  R          RELEASE  yarn
conf  examples  kubernetes  licenses  python  README.md  sbin
```

Et vérifiez que votre prompt est le même.

3. Créez les variables d'environnements suivantes depuis votre `~/.bashrc` :

```shell
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
```

4. Installer java

```bash
sudo apt install openjdk-11-jdk
```

### Vérifier que ça fonctionne

Pour vérifier que l'installation est bonne, on lance les tests unitaires de l'exercice 0 :

```shell
poetry run pytest
```

Vous devez avoir un test au vert.

-> [Suivant](exo1.md)
