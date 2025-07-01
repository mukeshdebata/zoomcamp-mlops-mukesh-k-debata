#!/usr/bin/env python
# coding: utf-8

# # Homework04 - Rui Pinto

# In[2]:


import pickle
import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore")


# In[3]:


with open("model.bin", "rb") as f_in:
    dv, model = pickle.load(f_in)


# In[4]:


categorical = ["PULocationID", "DOLocationID"]


def read_data(filename):
    df = pd.read_parquet(filename)

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


df = read_data(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"
)

dicts = df[categorical].to_dict(orient="records")
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


# ## Q1. Notebook
# 
# What's the standard deviation of the predicted duration for this dataset?
# 
# - 1.24
# - 6.24 ✅
# - 12.28
# - 18.28

# In[5]:


print("The stardard deviation of the predictions is: ", round(y_pred.std(), 3))


# ## Q2 Preparing the output
# 
# Like in the course videos, we want to prepare the dataframe with the output.
# 
# First, let's create an artificial ride_id column:
# 
# ```python
# df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
# ```
# 
# Next, write the ride id and the predictions to a dataframe with results.
# 
# 
# Save it as parquet:
# 
# ```python
# df_result.to_parquet(
#     output_file,
#     engine='pyarrow',
#     compression=None,
#     index=False
# )
# ```
# 
# What's the size of the output file?
# 
# - 36M
# - 46M
# - 56M
# - 66M ✅
# 
# Note: Make sure you use the snippet above for saving the file. It should contain only these two columns. For this question, don't change the dtypes of the columns and use pyarrow, not fastparquet.

# In[6]:


df["ride_id"] = (
    df["tpep_pickup_datetime"].dt.year.astype(str).str.zfill(4)
    + "/"
    + df["tpep_pickup_datetime"].dt.month.astype(str).str.zfill(2)
    + "_"
    + df.index.astype(str)
)

df_result = df[["ride_id"]].copy()
df_result["duration"] = y_pred

os.makedirs("output", exist_ok=True)
output_file = "output/result.parquet"
df_result.to_parquet(output_file, engine="pyarrow", compression=None, index=False)


# In[7]:


size_bytes = os.path.getsize(output_file)
size_mb = size_bytes / (1024 * 1024)

print(f"The output file size: {size_bytes:,} bytes ({size_mb:.2f} MB)")


# ## Q3 Creating the scoring script
# 
# Now let's turn the notebook into a script.
# 
# Which command you need to execute for that?

# In[9]:


get_ipython().system('jupyter nbconvert --to script homework04.ipynb --output scoring')


# ## Q4. Virtual environment
# Now let's put everything into a virtual environment. We'll use pipenv for that.
# 
# Install all the required libraries. Pay attention to the Scikit-Learn version: it should be the same as in the starter notebook.
# 
# After installing the libraries, pipenv creates two files: Pipfile and Pipfile.lock. The Pipfile.lock file keeps the hashes of the dependencies we use for the virtual env.
# 
# What's the first hash for the Scikit-Learn dependency?

# In[14]:


# Find the hash of scikit-learn in the uv.lock file
# using uv has my environment

with open("../uv.lock", "r") as f:
    lines = f.readlines()

scikit_learn_found = False
for i, line in enumerate(lines):
    if "scikit-learn" in line:
        scikit_learn_found = True
        # Look for hash in nearby lines
        for j in range(i, min(i + 10, len(lines))):
            if "hash" in lines[j]:
                print(lines[j].strip())
                break
        if scikit_learn_found:
            break


# ## Q5 Parametrize the script
# Let's now make the script configurable via CLI. We'll create two parameters: year and month.
# 
# Run the script for April 2023.
# 
# What's the mean predicted duration?
# 
# - 7.29
# - 14.29 ✅
# - 21.29
# - 28.29
# 
# Hint: just add a print statement to your script.

# In[27]:


get_ipython().system('python src/scoring.py 2023 04')


# ## Q6 Docker container
# Finally, we'll package the script in the docker container. For that, you'll need to use a base image that we prepared.
# 
# This is what the content of this image is:
# 
# ```dockerfile
# FROM python:3.10.13-slim
# 
# WORKDIR /app
# COPY [ "model2.bin", "model.bin" ]
# ```
# 
# Note: you don't need to run it. We have already done it.
# 
# It is pushed to agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim, which you need to use as your base image.
# 
# That is, your Dockerfile should start with:
# 
# ```dockerfile
# FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
# 
# # do stuff here
# ```
# 
# This image already has a pickle file with a dictionary vectorizer and a model. You will need to use them.
# 
# Important: don't copy the model to the docker image. You will need to use the pickle file already in the image.
# 
# Now run the script with docker. What's the mean predicted duration for May 2023?
# 
# - 0.19 ✅
# - 7.24
# - 14.24
# - 21.19

# In[ ]:


# Dockerfile with uv
with open("Dockerfile", "r") as f:
    dockerfile_content = f.read()
print("\nDockerfile content:\n")
print(dockerfile_content)


# In[ ]:


# Docker image with the fixed Dockerfile
get_ipython().system('docker build -t taxi-duration-prediction .')


# In[24]:


# Run the container
get_ipython().system('docker run --rm taxi-duration-prediction')


# ## Bonus: upload the result to the cloud (Not graded)
# Just printing the mean duration inside the docker image doesn't seem very practical. Typically, after creating the output file, we upload it to the cloud storage.
# 
# Modify your code to upload the parquet file to S3/GCS/etc.

# ## Bonus: Use an orchestrator for batch inference
# Here we didn't use any orchestration. In practice we usually do.
# 
# Split the code into logical code blocks
# Use a workflow orchestrator for the code execution

# ## Publishing the image to dockerhub
# This is how we published the image to Docker hub:
# 
# docker build -t mlops-zoomcamp-model:2024-3.10.13-slim .
# docker tag mlops-zoomcamp-model:2024-3.10.13-slim agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
# 
# docker login --username USERNAME
# docker push agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
