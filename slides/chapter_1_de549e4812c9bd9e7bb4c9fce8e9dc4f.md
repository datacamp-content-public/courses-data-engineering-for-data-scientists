---
title: Insert title here
key: de549e4812c9bd9e7bb4c9fce8e9dc4f

---
## Writing Unit Tests for Pyspark

```yaml
type: "TitleSlide"
key: "ff07a4e2e5"
```

`@lower_third`

name: Oliver Willekens
title: Data Engineer at Data Minded


`@script`
In the previous section we covered the different kinds of tests, now let's look at some problems with our original code and how we can write unit tests and create reusable components.


---
## Our earlier Spark application doesn't run locally

```yaml
type: "FullCodeSlide"
key: "a0cb6d6e5e"
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")

prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))

(unit_prices_with_ratings
 .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
 .orderBy(col("unit_price_in_euro").desc())
 .limit(10)
 .repartition(1)
 .write
 .csv("s3://dc-course/top10diapers"))
```


`@script`
Our application has hard-coded dependencies to S3 paths we might not have access to and surely, we don't want to impact our production data from our local machine. So as a first step, let's remove these hard coded paths.


---
## Remove hard-coded paths

```yaml
type: "FullCodeSlide"
key: "66e09eb241"
center_content: false
```

`@part1`
```python

def create_top10_dataset(prices, exchange_rates, ratings):
    # prices_with_ratings = prices.join(ratings, ["brand", "model"])
    return (prices
            .transform(partial(link_with_ratings, ratings=ratings))
            .transform(partial(link_with_exchange_rates, rates=exchange_rates))
            .transform(calculate_unit_price_in_euro)
            .transform(filter_best)
            .transform(select_top_n_best)
            )


def select_top_n_best(df, limit=10):
    return (df
            .select("date", "brand", "model", "store", "absorption_rate",
                    "comfort", "unit_price_in_euro")
            .orderBy(col("unit_price_in_euro").desc())
            .limit(limit))


def link_with_ratings(ratings, prices):
    return prices.join(ratings, ["brand", "model"])


def link_with_exchange_rates(prices, rates):
    return prices.join(rates, ["currency", "date"])


def calculate_unit_price_in_euro(df):
    return df.withColumn(
        "unit_price_in_euro",
        col("price") / col("quantity") * col("exchange_rate_to_euro"))


def filter_best(df):
    return df.filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))


def write_data(df):
    (df
     .repartition(1)
     .write
     .csv("s3://dc-course/top10diapers"))

```


`@script`
We’ve extracted the parts where the dataframes are being read and written to their own functions and created another function, `create_top10_dataset`, that executes the main logic. That function accepts 3 Spark dataframes. What we’ve won here is that our main logic can be tested using in-memory Dataframes that are completely loose from any data in files.


---
## Insert title here...

```yaml
type: "FullCodeSlide"
key: "df26bee985"
```

`@part1`



`@script`



---
## Let's practice!

```yaml
type: "FinalSlide"
key: "2c236e13af"
```

`@script`
Now it's your turn to dive into a few exercises, then we'll move forward with automating the testing process.

