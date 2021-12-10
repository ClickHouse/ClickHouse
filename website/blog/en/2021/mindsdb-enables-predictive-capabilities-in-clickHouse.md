---
title: 'MindsDB enables predictive capabilities in ClickHouse database'
image: 'https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/featured.png'
date: '2021-12-09'
author: '[Ilya Yatsishin](https://github.com/qoega)'
tags: ['company', 'case-study', 'MindsDB']
---

In this article, we will be reviewing how we can integrate predictive capabilities powered by machine learning with the ClickHouse database. ClickHouse is a fast, open-source, column-oriented SQL database that is very useful for data analysis and real time analytics. The project is maintained and supported by [Clickhouse](https://clickhouse.com/), Inc. We will be exploring its features in tasks that require data preparation in support of machine learning. 

The predictive capability is offered through MindsDB, a platform that enables running machine learning models automatically directly inside your database using only simple SQL commands. MindsDB democratizes machine learning and enables anyone to perform sophisticated machine learning-based forecasts right where the data lives.

We will go through the entire flow of a challenging use case for traditional machine learning around forecasting of large multivariate time series and how the combination of ClickHouse and MindsDB enables you to achieve this in a very simple and efficient way.

## Optimizing the Machine Learning Lifecycle

The machine learning lifecycle is a topic that is still being refined, but the main stages that compose this flow are Preparation, Modeling, and Deployment.

![ML - Lifecyle](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/ml-lifecyle.png)

Each of these three main stages is broken down into more clearly defined steps. For example, the Data Preparation step is generally broken down into Data Acquisition, Data Cleaning and Labeling, and Feature Engineering.

### Data that is Already in a Database is ML-friendly

Data preparation accounts for about 80% of the work of data scientists and at the same time, 57% of them consider data cleaning as the least enjoyable part of their job according to a [Forbes survey](https://www.forbes.com/sites/gilpress/2016/03/23/data-preparation-most-time-consuming-least-enjoyable-data-science-task-survey-says/?sh=41ca60756f63).

If your company has already gone through the hurdles of acquiring data, loading it into the database, then most likely it will already be in a clean and structured format, in a predefined schema. 

![ML - Lifecyle](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/ml-lifecyle-data.png)

### SQL as a feature engineering tool

Additionally, for any machine learning problem, Data Acquisition and Data Cleaning are only the first steps. Most often the initial dataset is not enough for producing satisfactory results from your models. That is where data scientists and machine learning engineers need to step in and enrich the datasets by applying different feature engineering techniques.

SQL is a very powerful tool for data transformation, and your dataset's features are actually columns in a database table.

![Feature Engineering](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/feature-engineering.png)

These features are then selected and transformed to create new features, which are going to be used in the training of the machine learning model. Using the data model described above, we can generate some extra features that describe our sales. For example we can create new features that contain the number of orders a product has been included in, and the percentage of that product’s price out of the overall order price.

```mysql
SELECT
	Product.pk_idProduct
	, Product.description
	, TBL_store.storeName
	, count(Orders.pk_idOrder) as number_of_orders
	, avg(Product.price / Orders.pricing) as product_percentage_of_order
FROM Product
	INNER JOIN OrderProduct
		on Product.pk_idProduct = OrderProduct.product
	INNER JOIN Orders
		on Orders.pk_idOrder = OrderProduct.order
	INNER JOIN TBL_store
		on TBL_store.PK_id_store = Product.seller
GROUP BY Product.pk_idProduct
	, Product.description
	, TBL_store.storeName
```

Because SQL is such a powerful tool, we should make use of it and generate the transformations that are possible, directly from the database.

![Feature Engineering 2](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/feature-engineering-data.png)

ClickHouse offers capabilities to do many transformations, over very large datasets. As opposed to the general way of creating new features for your dataset, extracting data and manipulating it through Python, creating your new features in ClickHouse is much faster.

### Machine Learning models as AI-Tables

After data preparation, we get to the point where MindsDB jumps in and provides a construct that simplifies the modeling and deployment of the machine learning model.

![ML Lifecyle - Clickhouse](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/ml-lifecyle-clickhouse.png)

This construct, called AI tables, is a MindsDB specific feature that allows you to treat a machine learning model just like a normal table. You create this AI table in MindsDB just like you would create a table in a regular database and then you can expose this table to ClickHouse through the external table capabilities.

This simple 1 minute video nicely illustrates the concept of AI Tables:

[![MindsDB - AI Tables](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/ai-tables-video.jpg)](https://www.youtube.com/watch?v=G_S4K_9cCmI)

Creating your own AI Table is very easy and below you have the syntax for creating it on top of your dataset.

```mysql
CREATE PREDICTOR <predictor_name>
  TRAIN FROM { (<select_statement>) | <namespace|integration>.<view|table> | <url> }
  [ TEST FROM { (<select_statement>) | <namespace|integration>.<view|table> | <url> } ]
[ ORDER BY <order_col> [{ASC|DESC}] ]
[ GROUP BY <col1,> [, <col2>, ...] ]
[ WINDOW <window_size> ]
PREDICT <col_name_in_from_to_forecast>

[ MODEL = {auto | <json_config> | <url>} ]
```

This enables us to think about a machine learning deployment no different than how you create tables. So, as soon as you create a model as a table in the database, it has already been deployed. And the only thing you need to take care of is what happens if the table schema changes, that’s when you need to either create a new model or retrain the model.

In conclusion, all of the deployment and modeling is abstracted to this very simple construct which we call “AI Tables” and which enables you to expose this table in other databases, like ClickHouse.

### Building Your Datasets in ClickHouse

Although it’s a fairly young product when compared to other similar tools in the analytic database market, ClickHouse has many advantages when compared to the more known tools and even new features that enables it to surpass others in terms of performance.

- **Single portable C++ binary** - enables very fast, sub 60 seconds installation
- **Runs anywhere** - it runs in any Linux-based environment, like cloud VM’s, containers and even bare-metal servers or laptops
- **Advanced SQL capabilities** - it has some additional extensions built on top of regular SQL syntax that that give it some added power
- **Column storage** - provides you benefits in terms of performance and benefits in terms of very high data compression
- **Distributed querying** - millisecond response time due to queries being distributed across nodes and across CPU cores
- **Sharding and replication** - enables scaling from laptop size to hundreds of nodes
- **Apache 2.0 licensing** - enables ClickHouse to be used for any business purpose

ClickHouse has thousands of installations worldwide used by numerous large companies, like Bloomberg, Uber, Walmart, Ebay, Yandex and more.

### Data Exploration

As explained in our previous sections, the most time-consuming part of any machine learning pipeline is Data Preparation. It requires knowledge about the data, which is why we always start out with Data Exploration.

At this step, we need to understand what information we have and what features are available to evaluate the quality of data to either just train the model with it or make some improvements to the datasets. Below we can see an example of a trip data dataset in ClickHouse, with 1.3billion rows of data about New York taxi rides being queried in order to analyze the quality of the data

```mysql
SELECT
  count() AS rides,
  avg(fare_amount) AS avg,
  min(fare_amount) AS min,
  max(fare_amount) AS max
FROM default.tripdata
```

![Outliers](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/outliers.png)

As you can see here we have some outliers that will negatively impact the machine learning model, so let’s dig deeper into it with Clickhouse tools.

Let’s write a query to do a deep dive into these distributions even further, to better understand the data. This query enables you to create a histogram view in just a couple of seconds for this large dataset and see the distribution of the outliers. 

```mysql
SELECT h_bin.1 AS lo, h_bin.2 AS hi, h_bin.3 AS count FROM
(
  SELECT histogram(5)(fare_amount) h
  FROM default.tripdata WHERE fare_amount < 0
) ARRAY JOIN h AS h_bin
```

![Histogram Query](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/visual-short.png)

We can see that the distribution of our histogram query also contains a count column. Some of the results in this column are fractional numbers that don’t necessarily represent a count of rows. Actually, [based on the documentation](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/parametric-functions/#histogram), this column actually contains the height of a bin in a histogram. 

Because we try to fit our entire dataset into a histogram with 5 bins, specified through the histogram(5)(fare_amount) function call and the number of items in our dataset isn’t normally distributed, the height of our bins will not necessarily be equal. Thus, some of our heights will have a number that will proportionally represent the number of values in that specific bin, relative to the total number of values in our dataset.

If this is still a bit confusing, we can try to use the [bar() visualization](https://clickhouse.com/docs/en/sql-reference/functions/other-functions/#function-bar) in ClickHouse to generate a more visual result of the distribution of our dataset.

![Histogram Query - Bar](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/visual.png)

We can see that the bar column contains a visual representation of the distribution of our dataset, split into our 5 bins. Because the first two bins both contain only 1 value, the bar display is too small to be visible, however, when we start having a few more values the bar is also displayed.

Additionally we can see a large amount of small negative fare values that we don’t want to be included into the model training dataset. If we reverse the filtering for our dataset and only look at the positive fare_amount values, we can see that the number of “clean” data points is much higher. Because we have such large values, we’re going to set the min value for our bar function to 10000000 so that the distribution is more clearly visible.

![Lo Hi Count Full](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/lo-hi-count-full.png)

### Data Cleaning and Aggregating

Now that we have identified that our dataset contains outliers, we will need to remove them in order to have a clean dataset. We’re going to filter out all negative amounts and only take into consideration fare amounts that are less than $500. As we need to predict data for each taxi vendor, we will aggregate the dataset by *vendor_id*.

```mysql
SELECT
    toStartOfHour(pickup_datetime) AS pickup_hour,
    vendor_id,
    sum(fare_amount) AS fares
FROM default.tripdata
WHERE total_amount >= 0 AND total_amount <= 500
GROUP BY pickup_hour, vendor_id 
ORDER BY pickup_hour, vendor_id
```

We can further reduce the size of our dataset by downsampling the timestamp data to hour intervals and aggregating all data that falls within an hour interval.

### Handling very large datasets with ClickHouse

Running any query on a massive dataset is usually very expensive in terms of the resources used and the time required to generate the data. This can cause headaches when we have to run the query multiple times, generate new features with complex transformations or when the source data ages out and we need a refreshed version. However, ClickHouse has a solution for this, **materialized views**.

![Materialized Views](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/material-views.png)

As opposed to a general SQL View, where the view just encapsulates the SQL query and reruns it on every execution, the materialized view runs only once and the data is fed into a materialized view table. We can then query this new table and every time data is added to the original source tables, this view table is also updated.

![Consumer 1](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/consumer-1.png)

As you can see above, we can always query the materialized view and know for sure that we are always getting the most up-to-date datasets, based on our original data. We can then use the dataset in this materialized view and train our machine learning model, without having to worry about stale data.

Materialized views also have a lot of benefits in terms of performance compared to generic views and they are sometimes even up to **20x faster** in ClickHouse, on datasets that exceed 1 billion rows.

You can also make use of ClickHouse clusters and have data extended to multiple shards to extract the best performance out of the data warehouse. You can create materialized views on these subsets of data and then later unify them under a distributed table construct, which is like an umbrella over the data from each of the nodes.

![Shard](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/shard.png)

Whenever you need to query this data, you query just the one distributed table, which automatically handles retrieving data from multiple nodes throughout your cluster.

![Consumer 2](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/consumers-2.png)

This is a very powerful technique that can enable you to interrogate trillions of rows of data, aggregate them, and transform them in useful ways. From this point on, we can proceed with the machine learning part and can even do a deeper analysis of our datasets.

### Building forecasts from complex Multivariate Time Series data

Let’s now predict demand for taxi rides based on the New York City taxi “trip data” dataset we just presented. We will be focusing on only a subset composed of vendor_id, the pickup time, and the taxi fare columns.

![Taxi Trips](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/taxi-trips.png)

### Understanding our Data Better

We can do a deeper dive into the subset of data generated with ClickHouse and plot the stream of revenue, split on an hourly basis. The green line plot on the bottom left shows the hourly amount in fares for the CMT company.

![Preprocessed data](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/preprocess-data.png)

However, we can also see that there is a difference in the distribution of fares not only throughout the day for a single taxi vendor but also between the taxi vendors themselves, as shown in the plot below. Each company has different dynamics through time, which makes this problem harder because we now don’t have a single series of data, but multiple.

![Dataset](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/dataset.png)

### The Challenge of Multivariate Time Series Forecasting

Knowing that our dataset contains multiple series of data is an important piece of information to be aware of when building the data forecasting pipeline. If a team of data scientists or machine learning engineers need to forecast any time series that is important for you to get insights from, they need to be aware of the fact that depending on how your grouped data looks like, they might be looking at hundreds or thousands of series.

Training such machine learning models can be very time-consuming and resource-expensive and depending on the type of insight you want to extract and the type of model you use, scaling this to thousands of models that predict their own time series will be very difficult to scale.

At MindsDB we have been dealing with this problem for some time now and we have been able to automate this process, using any type of data coming from any database, like ClickHouse.

## How MindsDB Automates building ML Models

### MindsDB Predictive Engine - Technical Details 

Our approach revolves around applying a flexible philosophy that will enable us to tackle any type of machine learning problem, not necessarily only time series problems. This is done by applying our encoder-mixer philosophy.

![Flexible Encoder](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/flexible-encoder.png)

Depending on the type of data for each column, we instantiate an Encoder for that column. It will be tasked with developing an informative encoding from the data in that column.

For example, if we have a column with simple numbers that don’t need to be trained in order to solve a time series problem, the Encoder can be just a simple set of rules that does not require training. However, if a column contains free text, the Encoder will instantiate a Transformer neural network that will learn to produce a summary of that text.

The next step is to instantiate a Mixer, which is a machine learning model tasked with doing the final prediction, based on the results of the Encoder. This type of philosophy provides a very flexible approach to predicting numerical data, categorical data, regression from text, and time-series data.

### Automatic and Dynamic Data Normalization in MindsDB

Before we start training this model with our data, we might have to do some specific data cleaning, like doing dynamic normalization. This implies normalizing each of our data series so that our Mixer model learns faster and better.

MindsDB captures statistics of the dataset and normalizes each series while the Mixer model learns to predict future values using these normalized values.

Temporal information is also encoded by disaggregating timestamps into sinusoidal components.

![sinusoidal components](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/consumers-2.png)

This enables arbitrary date handling and facilitates working with unevenly sampled series. This method is useful when your time series data are unevenly spaced and your measurements are not regular.

In short, for time-series problems, the machine learning pipeline works like in the image below. The input data on the top-left side contains non-temporal information, which is fed into the Encoder and then passed into the Mixer.

![Mixers](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/mixers.png)

But, for the temporal information, both the timestamps and the series of data themselves (in this case, the total amount of fares received in each hour, for each company) are automatically normalized and passed through a Recurrent Encoder (RNN encoder). The RNN infuses a stronger notion of temporality in the descriptor.

All of these encoded features are passed to the Mixer, which can be one of two types: 

- Neural network Mixer composed of two internal streams, one of which uses an autoregressive process to do a base prediction and give a ballpark value, and a secondary stream that fine-tunes this prediction, for each series
- Gradient booster mixer using LightGBM, on top of which sits the Optuna library, which enables a very thorough stepwise hyperparameter search

This ensures that we have identified the best model for our predictions, out of dozens of machine learning models.

## How to create and use Predictive AI Tables in ClickHouse database

### Using a single SQL query to train Multivariate Time-Series predictor

The above information about technical approach, normalization, encoding-mixer approach may sound complex for people without a machine learning background but in reality you are not required to know all these details to make predictions inside databases. What MindsDB does with the AI Tables approach is to enable anyone who knows just SQL to automatically build predictive models and query them. It is just as simple as running a single SQL command.

For example, this query will train a single model from multivariate time-series data to forecast taxi fares from the above mentioned dataset:

```mysql
CREATE PREDICTOR fares_forecaster_demo FROM Clickhouse (
 	SELECT VENDOR_ID, PICKUP_DATETIME, FARE_AMOUNT
 	FROM DEFAULT.TRIPDATA
 	WHERE DATE > '2010-01-01'
) PREDICT FARE_AMOUNT
ORDER BY DATE
GROUP BY VENDOR_ID
WINDOW 10
HORIZON 7;
```

Let’s discuss the statement above. We create a predictive AI Table using the CREATE PREDICTOR statement and specifying the database from which the training data comes. The code in yellow selects the filtered training data. After that, we use the PREDICT keyword to specify the column whose data we want to forecast, in our case the amount of fares

Next, there are some standard SQL clauses, such as ORDER BY, GROUP BY, WINDOW, and HORIZON. By using the ORDER BY clause with the DATE column as its argument, we emphasize that we deal with the time-series problem, and we want to order the rows by date. The GROUP BY clause divides the data into partitions. Here, each partition relates to a particular taxi company (vendor_id). We take into account just the last 10 rows for every given prediction. Hence, we use WINDOW 10. To prepare the forecast of the taxi fares we define HORIZON 7, which means we want to forecast 7 hours ahead.

### Getting the Forecasts

We are ready to go to the last step, which is using the predictive model to get future data. One way is to query the fares_forecaster_demo predictive model directly. You just make a Select statement passing the conditions for the forecast in a Where clause.

But we consider a time-series problem. Therefore, it is recommended that we join our predictive model to the table with historical data.

```mysql
SELECT tb.VENDOR_ID, tb.FARE_AMOUNT as PREDICTED_FARES
FROM Clickhouse.DEFAULT.TRIPDATA as ta
JOIN mindsdb.fares_forecaster_demo as tb 
WHERE ta.VENDOR_ID = "CMT" AND ta.DATE > LATEST
LIMIT 7;
```

Let’s analyze it. We join the table that stores historical data (i.e. Clickhouse.DEFAULT.TRIPDATA) to our predictive model table (i.e. mindsdb.fares_forecaster_demo). The queried information is the taxi vendor and the predicted number of fares per vendor. By specifying the MindsDB-provided condition ta.DATE > LATEST, we make sure to get the future number of rides per route.

### Visualizing the Forecasts

We can connect a BI tool to MindsDB predictive AI Tables to visualize the predictions in a nice way. You can see how this is done for the previously trained predictor in Looker. We connected the table we joined and we can see historical data along with the forecast that MindsDB made for the same date and time. In this case the green line represents actual data and the blue line is the forecast.

![Visualizing the Forecasts](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/visualize.png)

You can see that for the first 10 predictions the forecast is not accurate, that’s because the predictor just starts learning from the historical data (remember, we indicated a Window of 10 predictions when training it), but after that the forecast is becoming quite accurate.

### Automatic Detection of Anomalies

Using this prediction philosophy, MindsDB can also detect and flag anomalies in its predictions. Below we present the plot for a different dataset, a power consumption dataset for the Pondy state in India. 

This is a time-series prediction of t+1, meaning that the model is looking at all the previous consumption values in a time slice and tries to predict the next step, in this case, it is trying to predict the power consumption for the next day. The green line in the plot shows the actual power consumption value and the purple line is the MindsDB prediction, using all the values up to that time step to train the machine learning model.

![Single Group](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/single-group.png)

By default, MindsDB has a confidence threshold estimate, denoted by the gray area around the predicted trend. Whenever the real value crosses the bounds of this confidence interval, this can be flagged automatically as an anomalous behavior and the person monitoring this system can have a deeper look and see if something is going on.

Similar to the training of this single series model, MindsDB can automatically learn and predict for multiple groups of data. You can train with the entire dataset for this problem and get predictions for all states in India. This is very convenient as it abstracts most of the data pipeline processing.

![Multiple Groups](https://blog-images.clickhouse.com/en/2021/mindsdb-enables-predictive-capabilities-in-clickHouse/multiple-group.png)

### Bring your own ML blocks

MindsDB enables you to customize parts of the processing pipeline, but in addition to that you can also bring your own modules. For example, if you are a machine learning engineer, we enable you to bring in your own data preparation module, your own machine learning model, to fit your needs better.

For example, if you prefer replacing the RNN model with a classical ARIMA model for time series prediction, we want to give you this possibility. Or, in the analysis module, if you want to run your custom data analysis on the results of the prediction.

If you want to try this feature, visit MindsDB [Lightwood](https://github.com/mindsdb/lightwood) repo for more info or reach out via [Slack](https://mindsdbcommunity.slack.com/join/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ#/shared-invite/email) or [Github](https://github.com/mindsdb/mindsdb/discussions) and we will assist you.

## New ML features that are coming to AI Tables:

### Predicting Data from Streams

One of the major tasks MindsDB is working on now is trying to predict data from data streams, instead of from just a database. The goal is to create a predictor that reads streaming data coming from tools like Redis and Kafka and creates a forecast of things that will happen.

### Improve Forecasts for Long Horizons

The next feature we’re working on is improving forecasts for long time horizons that include categorical data alongside temporal data. This is a challenging task because we need to impute in multiple different columns what we think is going to happen, but we’re confident we can improve this.

### Detecting gradual anomalies

The current anomalies detection algorithm works very well with sudden anomalies in the data but needs to be improved to detect anomalies that occur to elements happening outside of the data series themselves. This is something we’re continuously working on improving.

## Conclusions

In this article we have guided you through the machine learning workflow. You saw how to use ClickHouse’s powerful tools like materialized views, to better and more effectively handle data cleaning and preparation, especially for the large datasets with billions of rows.

Then we dived into the concept of AI Tables from MindsDB, how they can be used within ClickHouse to automatically build predictive models and make forecasts using simple SQL statements.

We used an example of a multivariate time-series problem to illustrate how MindsDB is capable of automating really complex machine learning tasks and showed how simple it could be to detect anomalies and visualize predictions by connecting AI Tables to BI tools, all through SQL.

If you want to learn more about ClickHouse Inc.'s Cloud roadmap and offerings please reach out to us [here](https://clickhouse.com/company/#contactor) to get in touch.

Try making your own predictions with MindDB yourself, by simply signing up for a [free cloud account](https://cloud.mindsdb.com/signup) or [installing it via Docker](https://docs.mindsdb.com/deployment/docker/). If you need any help feel free to throw a question in the MindsDB community via [Slack](https://mindsdbcommunity.slack.com/join/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ#/shared-invite/email) or [Github](https://github.com/mindsdb/mindsdb/discussions).

Special thanks to MindsDB and [Ilya Yatsishin](https://github.com/qoega) for authoring this case study. 

