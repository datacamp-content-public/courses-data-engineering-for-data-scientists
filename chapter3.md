---
title: 'Test your data pipeline'
description: ""
---

## Writing unit tests for Pyspark

```yaml
type: VideoExercise
key: 844a8c4ae6
xp: 50
```

`@projector_key`
409a19bd1e4c9516db4b76c184632c8b

---

## Insert exercise title here

```yaml
type: NormalExercise
key: b41b6daddf
xp: 100
```



`@instructions`
An alternative you will see frequently as well is to separate the column names from the values, like this(3).
```python
column_names = ("price", 
                "quantity", 
                "exchange_rate_to_euro")
records = [(10, 1, 2.), (22, 1, 4.3)]
df = spark.createDataFrame(records, 
                           column_names)
```{{3}}

`@hint`


`@pre_exercise_code`
```{python}

```

`@sample_code`
```{python}

```

`@solution`
```{python}

```

`@sct`
```{python}

```
