test.show()

rank_eval = SparkRankingEvaluation(test, top_all, k = TOP_K, col_user="UserId", col_item="MovieId", 
                                    col_rating="Rating", col_prediction="prediction", 
                                    relevancy_method="top_k")
                                    
# Evaluate Ranking Metrics

print("Model:\tALS",
      "Top K:\t%d" % rank_eval.k,
      "MAP:\t%f" % rank_eval.map_at_k(),
      "NDCG:\t%f" % rank_eval.ndcg_at_k(),
      "Precision@K:\t%f" % rank_eval.precision_at_k(),
      "Recall@K:\t%f" % rank_eval.recall_at_k(), sep='\n')                                    
      
# Evaluate Rating Metrics

prediction = model.transform(test)
rating_eval = SparkRatingEvaluation(test, prediction, col_user="UserId", col_item="MovieId", 
                                    col_rating="Rating", col_prediction="prediction")

print("Model:\tALS rating prediction",
      "RMSE:\t%.2f" % rating_eval.rmse(),
      "MAE:\t%f" % rating_eval.mae(),
      "Explained variance:\t%f" % rating_eval.exp_var(),
      "R squared:\t%f" % rating_eval.rsquared(), sep='\n')      
      
model.write().overwrite().save(model_name)
model_local = "file:" + os.getcwd() + "/" + model_name
dbutils.fs.cp(model_name, model_local, True)      
