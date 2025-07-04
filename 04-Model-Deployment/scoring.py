#!/usr/bin/env python
# coding: utf-8

# # Homework04

# In[1]:


import pickle
import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore")


# In[2]:


with open("model.bin", "rb") as f_in:
    dv, model = pickle.load(f_in)


# In[3]:


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


# ## Q1. Notebook What's the standard deviation of the predicted duration for this dataset?

# In[24]:


print("The stardard deviation of the predictions is: ", round(y_pred.std(), 3))


# ## Q2 Preparing the output

# In[25]:


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


# In[26]:


size_bytes = os.path.getsize(output_file)
size_mb = size_bytes / (1024 * 1024)

print(f"The output file size: {size_bytes:,} bytes ({size_mb:.2f} MB)")


# ## Q3 Creating the scoring script

# In[27]:


get_ipython().system('jupyter nbconvert --to script homework04.ipynb --output scoring')


# In[28]:


get_ipython().system('pwd')


# ## Q4. Virtual environment

# In[29]:


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

# In[6]:


get_ipython().system('python src/scoring.py 2023 04')


# ## Q6 Docker container

# In[7]:


# Dockerfile with uv
with open("Dockerfile", "r") as f:
    dockerfile_content = f.read()
print("\nDockerfile content:\n")
print(dockerfile_content)


# In[8]:


# Docker image with the fixed Dockerfile
get_ipython().system('docker build -t taxi-duration-prediction .')


# In[9]:


# Run the container
get_ipython().system('docker run --rm taxi-duration-prediction')

