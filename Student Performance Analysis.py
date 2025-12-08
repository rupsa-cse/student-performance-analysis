print("Student Performance Analysis Notebook")
print("Rows analyzed:", len(df))
print("Overall average score:", df["average_score"].mean().round(2))
print("Excellent:", (df['performance'] == 'Excellent').sum(),
      "Good:", (df['performance'] == 'Good').sum(),
      "Needs Improvement:", (df['performance'] == 'Needs Improvement').sum())




spark_df = spark.table("retail.default.students")
display(spark_df.limit(20))



import pandas as pd
try:
    df
except NameError:
    df = spark_df.toPandas()


print("Rows:", len(df))
print("Columns and dtypes:\n", df.dtypes)
display(df.isnull().sum())

# Convert score columns to numeric 
score_cols = ["math_score", "reading_score", "writing_score", "study_hours"]
for c in score_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Drop rows with missing essential data (if any)
df = df.dropna(subset=score_cols).reset_index(drop=True)
display(df.head())




import numpy as np

# Average of three subject scores
df["average_score"] = df[["math_score","reading_score","writing_score"]].mean(axis=1).round(2)

# Performance label
df["performance"] = np.where(df["average_score"] >= 85, "Excellent",
                     np.where(df["average_score"] >= 70, "Good", "Needs Improvement"))

display(df.head())



# Basic descriptive stats
display(df[["math_score","reading_score","writing_score","average_score","study_hours"]].describe())

# Average score by gender
avg_by_gender = df.groupby("gender")["average_score"].mean().round(2)
print("Average Score by Gender:")
display(avg_by_gender)

# Counts by performance category
performance_counts = df["performance"].value_counts()
print("Performance distribution:")
display(performance_counts)



# Top performer
top = df.loc[df["average_score"].idxmax()].to_dict()
print("Top student:", top["name"], "Avg:", top["average_score"], "Performance:", top["performance"])

# Bottom performer
bottom = df.loc[df["average_score"].idxmin()].to_dict()
print("Bottom student:", bottom["name"], "Avg:", bottom["average_score"], "Performance:", bottom["performance"])

# Percentiles
p25 = np.percentile(df["average_score"], 25)
p50 = np.percentile(df["average_score"], 50)
p75 = np.percentile(df["average_score"], 75)
print(f"Percentiles - 25th: {p25:.2f}, 50th(median): {p50:.2f}, 75th: {p75:.2f}")



# Correlations
corr_study_avg = df["study_hours"].corr(df["average_score"])
corr_study_math = df["study_hours"].corr(df["math_score"])
print(f"Correlation (study_hours vs average_score): {corr_study_avg:.2f}")
print(f"Correlation (study_hours vs math_score): {corr_study_math:.2f}")

# Correlation matrix (full)
display(df[["math_score","reading_score","writing_score","average_score","study_hours"]].corr().round(2))




# Convert pandas df back to Spark DataFrame
processed_spark_df = spark.createDataFrame(df)

# Save as a managed table in the same catalog/schema (overwrite if exists)
processed_spark_df.write.mode("overwrite").saveAsTable("retail.default.student_analysis")

print("Saved table: retail.default.student_analysis")



spark_df = spark.createDataFrame(df)


spark_df.write.mode("overwrite").saveAsTable("retail.default.student_analysis_results")

print("Analysis results saved as table: retail.default.student_analysis_results")



display(df)