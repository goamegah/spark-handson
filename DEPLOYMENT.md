# Déployer et exécuter un job Spark

## Packaging

Pour déployer une application Spark, il faut d'abord packager son code. Avec poetry il existe une commande pour faire ça :

```shell
poetry build
```

Pour cela il faut modifier le fichier `pyproject.toml` et indiquer comment packager son code. Voici le lien vers la doc poetry pour [configurer le packaging](https://python-poetry.org/docs/pyproject/#packages).

## Cloud

### AWS Glue

Glue est un service managé sur AWS pour faire exécuter des jobs Spark serverless. C'est le plus simple à utiliser, en contre-partie c'est très rigide niveau configuration. Les seules exécuteurs utilisables font 4 CPUs et 16 Go de RAM (1 DPU) ou 2 DPU.

#### Infra as code

1. Créer un bucket S3

Pour stocker notre code et notre donnée, nous allons avoir besoin d'un bucket S3.
Dans le fichier `infra/bucket.tf`, voici le code pour créer notre bucket :

```terraform
resource "aws_s3_bucket" "bucket" {
  bucket = "goamegah-spark-handson-bucket"

  tags = local.tags
}
```

2. Créer les permissions pour notre job glue avec IAM

Pour donner le droit à notre job Glue d'accéder à votre bucket S3, on crée un role IAM et une policy IAM.
Dans le fichier `infra/iam.tf`, voici le code pour créer nos ressources IAM :

```terraform
data "aws_iam_policy_document" "glue_assume_role_policy_document" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_role_policy_document" {
  statement {
    actions = [
      "s3:*",
      "kms:*",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    effect = "Allow"
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_policy" {
  name = "glue_policy"
  path = "/"
  policy = data.aws_iam_policy_document.glue_role_policy_document.json
}

resource "aws_iam_role" "glue-role" {
  name                = "glue_role"
  assume_role_policy  = data.aws_iam_policy_document.glue_assume_role_policy_document.json
  managed_policy_arns = [aws_iam_policy.glue_policy.arn]
}
```

3. Créer son job Glue

Maintenant nous pouvons créer notre job Glue et lui attribuer son role IAM et le chemin vers notre application Spark dans S3.
voici le code dans le fichier `infra/glue.tf` pour créer notre job Glue :

```terraform
resource "aws_glue_job" "exo2_glue_job" {
  name     = "exo2_clean_job"
  role_arn = aws_iam_role.glue-role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "Standard"


  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "VALUE_1"
    "--PARAM_2"                         = "VALUE_2"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights" = "true"
    "--enable-metrics" = "true"
    "--enable-spark-ui " = "true"
  }

  tags = local.tags
}
```

#### Déployer

1. [Installer terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
2. Créer le fichier `~/.aws/credentials`

Ouvrir le fichier `~/.aws/credentials` et ajouter les lignes suivantes :

**Remarque:**, il faut remplacer `???` par les valeurs des clés d'accès de votre compte AWS.

```text
[esgi-spark-core]
aws_access_key_id = ???
aws_secret_access_key = ???
```

3. Déclarer 2 variables d'environnement :

```shell
$ export AWS_PROFILE=esgi-spark-core
$ export AWS_REGION=eu-west-1
```

4. Déployer avec terraform

```shell
$ cd infra
$ terraform init
$ terraform plan
$ terraform apply
```

5. Uploader son package python dans S3

![A text](/assets/goamegah_bucket.png)


Un job Glue est composé de 2 éléments :

* Un script d'initialisation :

dans le fichier `src/fr/hymaia/exo2_glue_job.py` :

```python
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import sys
from src import cleanJob

# TODO : import custom spark code dependencies

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job.init(args['JOB_NAME'], args)

    df_city = spark.read.option(key="header", value="true").csv(
        "s3://goamegah-spark-handson-bucket/resources/exo2/city_zipcode.csv")

    df_client = spark.read.option(key="header", value="true").csv(
        "s3://goamegah-spark-handson-bucket/resources/exo2/clients_bdd.csv")

    adults_city_df = cleanJob(df_client, df_city)

    # write data on s3
    adults_city_df.write.mode("overwrite").parquet("s3://goamegah-spark-handson-bucket/data/exo2/output/clean")

    # TODO : call function to run spark transformations

    job.commit()
```

Nous allons transférer les données qu'a besoin notre job Glue sur S3 :

→ Transférer les données sur **S3**
```shell
$ aws s3 cp src/resources/exo2/clients_bdd.csv s3://goamegah-spark-handson-bucket/resources/exo2/clients_bdd.csv
$  aws s3 cp src/resources/exo2/city_zipcode.csv s3://goamegah-spark-handson-bucket/resources/exo2/city_zipcode.csv
```

![A text](/assets/goamegah_s3_resources_exo2.png)

→ Transférer le script(lui-même) sur **S3** :

```shell
$ aws s3 cp src/fr/hymaia/exo2_glue_job.py s3://goamegah-spark-handson-bucket/spark-jobs/
```

![A text](/assets/goamegah_s3_sparkjobs.png)

* Un package qui contient le job Spark, celui que vous allez générer avec poetry

Qu'on soit en Scala ou en Python, c'est la même chose.
Les deux éléments sont en général déployés sur S3. Pour cela, lancez la commande suivante :

→ Transférer le package au format wheel sur **S3**

```shell
$ aws s3 cp dist/spark_handson-0.1.0-py3-none-any.whl s3://goamegah-spark-handson-bucket/wheel/spark_handson-0.1.0-py3-none-any.whl
```

![A text](/assets/goamegah_s3_wheel.png)

#### Lancement du job
→ L'exécution se fait avec le bouton run qui va exécuter notre application spark.

![A text](/assets/goamegah_Glue_ETL_jobs.png)

→ On peut visualiser le statut de l'éxécution

![A text](/assets/goamegah_Glue_ETL_jobs_status.png)