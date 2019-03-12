# top k items to recommend
TOP_K = 10

# Select Movielens data size: 100k, 1m, 10m, or 20m
MOVIELENS_DATA_SIZE = '100k'

# Note: The DataFrame-based API for ALS currently only supports integers for user and item ids.
schema = StructType(
    (
        StructField("UserId", IntegerType()),
        StructField("MovieId", IntegerType()),
        StructField("Rating", FloatType()),
        StructField("Timestamp", LongType()),
    )
)

data = movielens.load_spark_df(spark, size=MOVIELENS_DATA_SIZE, schema=schema, dbutils=dbutils)
data.show()

train, test = spark_random_split(data, ratio=0.75, seed=42)
print ("N train", train.cache().count())
print ("N test", test.cache().count())

header = {
    "userCol": "UserId",
    "itemCol": "MovieId",
    "ratingCol": "Rating",
}


als = ALS(
    rank=10,
    maxIter=15,
    implicitPrefs=False,
    alpha=0.1,
    regParam=0.05,
    coldStartStrategy='drop',
    nonnegative=True,
    **header
)

model = als.fit(train)


# Get the cross join of all user-item pairs and score them.
users = train.select('UserId').distinct()
items = train.select('MovieId').distinct()
user_item = users.crossJoin(items)
dfs_pred = model.transform(user_item)

dfs_pred.show()

# Remove seen items.
dfs_pred_exclude_train = dfs_pred.alias("pred").join(
    train.alias("train"),
    (dfs_pred['UserId'] == train['UserId']) & (dfs_pred['MovieId'] == train['MovieId']),
    how='outer'
)

top_all = dfs_pred_exclude_train.filter(dfs_pred_exclude_train["train.Rating"].isNull()) \
    .select('pred.' + 'UserId', 'pred.' + 'MovieId', 'pred.' + "prediction")

top_all.show()
