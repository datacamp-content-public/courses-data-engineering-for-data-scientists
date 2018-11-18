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
title: Data Engineer 


`@script`
Welcome back. In the previous session we've covered different kinds of tests. In this session, we will learn how to write unit tests for our Pyspark application. In doing so, we will restructure our code and create more reusable components.


---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "a0cb6d6e5e"
disable_transition: true
center_content: false
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```{{1}}

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])

```{{2}}


`@script`
The application we’ve been writing till now looks like this:
* in the first part, data is being loaded from some location, using the spark DataFrameReader objects. In the example, the location was an S3 bucket, but it could be a database or a file on a local filesystem.
* in the second part, we created a wide table, by joining the datasets and appending a column, which is based on the already existing ones.
* in the last part, the data is filtered down to only those records we’re interested in and sorted. We only take the top 10 records and write this result away.


---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "6610fd83c1"
disable_transition: true
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```


`@script`



---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "c3f91cf8e8"
disable_transition: true
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```

```python
(unit_prices_with_ratings
 .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
```


`@script`



---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "37d84ade10"
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```

```python
(unit_prices_with_ratings
 .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
 .orderBy(col("unit_price_in_euro").desc())
```


`@script`



---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "27451ecb98"
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```

```python
(unit_prices_with_ratings
 .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
 .orderBy(col("unit_price_in_euro").desc())
 .limit(10)
```


`@script`



---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "7d70f54fc4"
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```

```python
(unit_prices_with_ratings
 .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
 .orderBy(col("unit_price_in_euro").desc())
 .limit(10)
 .repartition(1)
 .write
 .csv("s3://dc-course/top10diapers"))
```


`@script`



---
## Our earlier Spark application... doesn't run locally

```yaml
type: "FullCodeSlide"
key: "c083e56692"
disable_transition: true
```

`@part1`
```python
exchange_rates = spark.read.csv("s3://dc-course/exchange_rates")
retail_prices = spark.read.csv("s3://dc-course/prices")
ratings = spark.read.csv("s3://dc-course/diaper_ratings")
```

```python
prices_with_ratings = retail_prices.join(ratings, ["brand", "model"])
unit_prices_with_ratings = (prices_with_ratings
                            .join(exchange_rates, ["currency", "date"])
                            .withColumn("unit_price_in_euro",
                                        col("price") / col("quantity") 
                                        * col("exchange_rate_to_euro")))
```

```python
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
## Remove hard-coded paths & extract RW logic

```yaml
type: "FullCodeSlide"
key: "66e09eb241"
center_content: false
```

`@part1`
```python

def load_data():
   ...

def create_top10_dataset(prices, exchange_rates, ratings):
    prices_with_ratings = prices.join(ratings, ["brand", "model"])
    unit_prices_with_ratings = (prices_with_ratings
                                .join(exchange_rates, ["currency", "date"])
                                .withColumn("unit_price_in_euro",
                                            col("price") / col("quantity") 
                                            * col("exchange_rate_to_euro")))

    return (unit_prices_with_ratings
            .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
            .select("date", "brand", "model", "store", "absorption_rate",
                    "comfort", "unit_price_in_euro")
            .orderBy(col("unit_price_in_euro").desc())
            .limit(10))

def write_data(df):
   ...
```


`@script`
We’ve extracted the parts where the dataframes are being read and written to their own functions and created another function, `create_top10_dataset`, that executes the main logic. That function accepts 3 Spark dataframes. Optionally, the read and write functions can get the paths from a data catalogue, which could be passed in as a dictionary, e.g.


---
## Creating in-memory DataFrames

```yaml
type: "FullCodeSlide"
key: "9fe02bd136"
```

`@part1`
Downsides to working with files:
* hard to maintain {{1}}
* breaks code-locality {{2}}
* improperly sampled {{3}}

Consider making in-memory Spark DataFrames:{{4}}
```python
prices = [("Babys-R-Us", "UK", "Pampers", "Extra Dry", 10, "GBP", 12,
                   date(2018, 11, 12))]
col_names_prices = ("store", "countrycode", "brand", "model",
                    "price", "currency", "quantity", "date")
prices_df = spark.createDataFrame(prices, col_names_prices)
exchanges_df = ...
ratings_df = ...
create_top10_dataset(prices_df, exchange_rates_df, ratings_df)
```{{4}}


`@script`
There are several downsides about working with files.
For starters, they are hard to maintain: binary files can't be properly viewed and even non-binary files likes CSVs pose navigation challenges when there's too much data.

Next, using files also breaks code-locality. It's the concept where parts of the code that are being used together should be closely grouped together as well. By having sample data in separate files, you increase the distance between understanding what goes into a function and what that function does to the data.

Finally, data in files is often improperly sampled. You will want to test the behaviour of your code when given both regular and edge cases. The latter is often underrepresented in your sample files. It would also become cumbersome to extract just those edge cases and verify the transformations.

Here’s an example of how you could create in-memory DataFrames. The data itself can be easily changed, by altering the list of tuples.
We still have a lot of work being done by `create_top10_dataset` though, which makes it hard to test.


---
## Create small, reusable and well-named functions

```yaml
type: "FullCodeSlide"
key: "4a0650ccf8"
disable_transition: false
```

`@part1`
```python
def create_top10_dataset(prices, exchange_rates, ratings):
    prices_with_ratings = prices.join(ratings, ["brand", "model"])
    unit_prices_with_ratings = (prices_with_ratings
                                .join(exchange_rates, ["currency", "date"])
                                .withColumn("unit_price_in_euro",
                                            col("price") / col("quantity") 
                                            * col("exchange_rate_to_euro")))

    return (unit_prices_with_ratings
            .filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))
            .select("date", "brand", "model", "store", "absorption_rate",
                    "comfort", "unit_price_in_euro")
            .orderBy(col("unit_price_in_euro").desc())
            .limit(10))```


`@script`
The set of transformations that the function `create_top10_dataset` executes can be split into smaller pieces. To get to `unit_prices_with_ratings` for example, 3 DataFrames are being joined and one column is being added which implements a mathematical function. These smaller transformations lend themselves to simpler testing if they were factored out. Let’s see how this can be done.


---
## Create small, reusable and well-named functions

```yaml
type: "FullCodeSlide"
key: "df26bee985"
disable_transition: true
```

`@part1`
```python
def link_with_ratings(ratings, prices):
    return prices.join(ratings, ["brand", "model"])

def link_with_exchange_rates(prices, rates):
    return prices.join(rates, ["currency", "date"])

def calculate_unit_price_in_euro(df):
    return df.withColumn(
        "unit_price_in_euro",
        col("price") / col("quantity") * col("exchange_rate_to_euro"))

def filter_acceptable_diapers(df):
    return df.filter((col("absorption_rate") >= 4) & (col("comfort") >= 3))

def select_top_n_best(df, limit=10):
    return (df
            .select("date", "brand", "model", "store", "absorption_rate",
                    "comfort", "unit_price_in_euro")
            .orderBy(col("unit_price_in_euro").desc())
            .limit(limit))
```


`@script`
Here we have recreated the same functionality as before, but have splitted the transformations on the DataFrame into smaller pieces. While it may seem silly to write a new function for only a single transformation, each transformation by itself can be tested. And reused.


---
## Example of testing a single transformation

```yaml
type: "FullCodeSlide"
key: "f9dfc54cfe"
```

`@part1`
```python
def test_calculate_unit_price_in_euro(self):
    record = dict(price=10, quantity=5, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(price=10, quantity=5, exchange_rate_to_euro=2.,
                           unit_price_in_euro=4.)
    expected = self.spark.createDataFrame([expected_record])
    self.assertDataFrameEqual(result, expected)

def test_calculate_unit_price_in_euro_divide_by_zero(self):
    record = dict(price=10, quantity=0, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(price=10, quantity=0, exchange_rate_to_euro=2.,
                           unit_price_in_euro=None)
    expected = self.spark.createDataFrame([expected_record], result.schema)
    self.assertDataFrameEqual(result, expected)```


`@script`
As you can see, these transformations are now easy to test. We have written two tests for the _same_ function, one where we’re testing normal usage and where we’re testing what would happen if the data behaves anomalous. In the second test, a division by zero would occur. Spark handles this by replacing the outcome with the undefined value, which maps to Python’s None singleton.


---
## Putting it all together and reusing components

```yaml
type: "FullCodeSlide"
key: "2cea900710"
```

`@part1`
```python
def create_top10_dataset(prices, exchange_rates, ratings):
    df = (prices
          .transform(partial(link_with_ratings, ratings=ratings))
          .transform(partial(link_with_exchange_rates, rates=exchange_rates))
          .transform(calculate_unit_price_in_euro)
          .transform(filter_acceptable_diapers)
          .transform(select_top_n_best)
          )
    return df```

```python
def create_weekly_brand_scores(prices, exchange_rates, ratings):
    df = (prices
          .transform(partial(link_with_ratings, ratings=ratings))
          .transform(partial(link_with_exchange_rates, rates=exchange_rates))
          .transform(calculate_unit_price_in_euro)
          .transform(add_year_and_week)
          .transform(aggregate_by_week)
          )
    return df
```{{1}}


`@script`
Those modular functions we made before can now easily be chained, using the `transform` method of Spark DataFrames.

We can now also re-use them in entirely different data pipelines. Here's another example where we reuse a lot of what we have written before with only two new functions which we'll explore in the exercices.


---
## Let's practice!

```yaml
type: "FinalSlide"
key: "2c236e13af"
```

`@script`
Now it's your turn to dive into a few exercises, then we'll move forward with automating the testing process.

