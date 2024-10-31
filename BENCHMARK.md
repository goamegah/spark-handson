# UDF vs NoUDF Benchmark

## How run benchmark
1. Install required packages

At the root folder install dependencies using poetry 

```python
$ poetry install
```

2. Run main.py 

```python
$ poetry run spark_benchmark_job --runs=10
```

## Results over 10 runs

#### Huawei, Intel core i7
![A text](/assets/udfVSnoudf_benchmark.png)