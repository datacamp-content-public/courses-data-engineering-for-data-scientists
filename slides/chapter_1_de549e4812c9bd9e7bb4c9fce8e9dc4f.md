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
* in the second part, we created a wide table, by joining the datasets.…


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
... and appending a column, which is based on the already existing ones.


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
* in the last part, the data is filtered down to only those records we’re interested in …


---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "37d84ade10"
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
```


`@script`
...and sorted.


---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "27451ecb98"
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
```


`@script`
...We only take the top 10 records …


---
## Our earlier Spark application…

```yaml
type: "FullCodeSlide"
key: "7d70f54fc4"
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
…and write this result away.


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
Our application depends on having access to files on Amazon's Simple Storage Service. That also makes it hard to test the functionality of the transformation part of our ETL pipeline. So as a first step, let's factor out these hard coded paths.


---
## Separate extraction, transformation and loading

```yaml
type: "FullCodeSlide"
key: "309beb2013"
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
When we now revise our original Spark application, we would rewrite it like this. We’ve extracted the parts where the dataframes are being read and written to their own functions and created another function, `create_top10_dataset`, that executes the main logic.  Optionally, the read and write functions can get the paths from a data catalogue, which could be passed in as a dictionary, e.g.

We still have a lot of work being done by `create_top10_dataset` though, which makes that particular function hard to test. Let’s inspect the function more closely and see how we can break it up into pieces that are easier to test functionally.


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
W

Here’s an example of how you could create in-memory DataFrames. You create a list of tuples representing the data, one tuple for each row or object. You pass that to `createDataFrame` together with the corresponding column names and you end up with a DataFrame that you can then pass around to other functions. Using this technique we can  factor out the reading from and writing to files or databases.


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
disable_transition: false
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
disable_transition: false
```

`@part1`
```python
def test_calculate_unit_price_in_euro(self):
	record = dict(price=10, quantity=5, exchange_rate_to_euro=2.)
	df = self.spark.createDataFrame([record])
```{{1}}


`@script`
Let us now test one of these smaller building blocks, like the calculation of the unit price in euros. To do so, we only need a dataframe with 3 variables: the price in some currency, the quantity and an exchange rate for proper comparisons.

We start by creating an in-memory dataframe. This time, we use another approach: rather than splitting up the data in a list of tuples and a set of column names, we can also create a dataframe directly from a list of dictionaries, similar to how it can be done in the Pandas module too.


---
## Example of testing a single transformation

```yaml
type: "FullCodeSlide"
key: "7d6a222941"
disable_transition: true
```

`@part1`
```python
def test_calculate_unit_price_in_euro(self):
    record = dict(price=10, quantity=5, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])    
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(unit_price_in_euro=4., **record)
    expected = self.spark.createDataFrame([expected_record])
```


`@script`
A typical test regards the function we're testing as a black box: given some known input, there's an expected output. It's that return value that we want to scrutinize.


---
## Example of testing a single transformation

```yaml
type: "FullCodeSlide"
key: "62b6447554"
```

`@part1`
```python
def test_calculate_unit_price_in_euro(self):
    record = dict(price=10, quantity=5, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])    
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(unit_price_in_euro=4., **record)
    expected = self.spark.createDataFrame([expected_record])
    self.assertDataFrameEqual(result, expected)
```


`@script`
To compare two dataframes, a helper function called `assertDataFrameEqual` is created which checks several things for you. You may ignore the inner workings of this method for now, just keep in mind that it is a useful abstraction, with which we can assert that the dataframes are equivalent.

A test like this works well in a unit testing framework, like pytest.


---
## Example of testing a single transformation

```yaml
type: "FullCodeSlide"
key: "555bc353ab"
```

`@part1`
```python
def test_calculate_unit_price_in_euro(self):
    record = dict(price=10, quantity=5, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])    
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(unit_price_in_euro=4., **record)
    expected = self.spark.createDataFrame([expected_record])
    self.assertDataFrameEqual(result, expected)
``` 

```python
def test_calculate_unit_price_in_euro_divide_by_zero(self):
    record = dict(price=10, quantity=0, exchange_rate_to_euro=2.)
    df = self.spark.createDataFrame([record])
    result = calculate_unit_price_in_euro(df)

    expected_record = dict(unit_price_in_euro=None, **record)
    expected = self.spark.createDataFrame([expected_record], result.schema)
    self.assertDataFrameEqual(result, expected)
```{{2}}


`@script`
Now, this first unit test only checks the normal behavior. We would do well checking also anomalous behavior and asserting our function still works as intended. 

In this second test, which is almost identical to the first, we pass in data where the _quantity_ is unknown, so that a division by zero would occur. Spark handles this by replacing the outcome with the undefined value, which maps to Python’s None singleton. So we should find that in the return value of function under test.

These two examples show how a simple function, like adding a column based on some combination of other columns, typically have a whole set of tests associated.


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
The modular functions we made before can now easily be chained, using the `transform` method of Spark DataFrames. Python's `partial` function from the module `functools` also comes into good use here, as it allows us to create partial functions, where the last remaining atribute, which is the dataframe we start with, gets filled in by the transform method.

We can now also re-use the small functions we've made in entirely different data pipelines. Here's another example where we reuse a lot of what we have written before with only two new functions which we'll explore in the exercices.


---
## Let's practice!

```yaml
type: "FinalSlide"
key: "2c236e13af"
```

`@script`
Now it's your turn to dive into a few exercises, then we'll move forward with automating the testing process.

