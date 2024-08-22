# Labor reallocation and remote work 

This repository contains code and data for
[McDermott and Hansen (2021)](https://www.nber.org/papers/w29598).

**Abstract:**
We study how labour patterns of tech workers have changed in the wake of
COVID-19. Using real-time activity data from millions of GitHub users around the
world, we show that the pandemic triggered a sharp pattern of labor
reallocation. Users have become more active during traditional leisure times,
and spend several additional hours each week active on during evenings and
weekends. We show that this behavior has persisted far beyond any local lockdown
orders and return to work mandates, albeit tempered by regional and user
heterogeneity. Our results provide new insights into the dynamic labor effects
of the pandemic and the long-run shift to remote work. To reproduce all of the
results in the paper, clone this repo and then follow these three steps:

## Replication

The analysis is run entirely in R (4.4.1), but with calls to the Google BigQuery
via the platform's API. To reproduce all of the results from the paper, please
follow the below steps.

### Step 0: Google Cloud API Credentials

The data for this project are obtained from 
[**Google BigQuery**](https://console.cloud.google.com/bigquery). You will need
to set up your Google Cloud API key in order to run the underlying SQL calls and 
download the data. 

I have detailed instructions on how to do that 
[here](https://raw.githack.com/uo-ec607/lectures/master/14-gce-ii/14-gce-ii.html#Google_Cloud_API_Service_Account_key). 
Please use *exactly* the same environment variables (e.g. "GCE_AUTH_FILE") as I
use in the linked tutorial, otherwise the code will not work.

### Step 1. Install the necessary packages from R:

I use [**renv**](https://rstudio.github.io/renv/) to snapshot the entire project 
environment. To install all of the necessary packages and their dependencies, 
simply run:

```r
# renv::init()  ## Only needed if you didn't clone the repo as an RStudio Project
renv::restore() ## Enter "y" when prompted. See 'renv' section below for details.
```

This make take a few minutes if it is the first time that you are installing
packages into an **renv** environment.


### Step 2. Run the analysis

I use [**targets**](https://books.ropensci.org/targets/) to automate the entire 
project build (analysis, etc.). Assuming that you have set up your Google Cloud 
API key (Step 0) and installed the necessary R packages (Step 1), simply run the
following R commands at the root of the repo: 

```r
targets::tar_visnetwork() # optional: vizualise the pipeline DAG
targets::tar_make()       # run the pipeline
```

(You might be prompted to authenticate with BigQuery via your browser before it 
will allow you to download the data.)

The full pipeline takes about 20 minutes to run from start to finish on my
laptop. Most of that time is taken up sending and executing queries on BigQuery.
(The individual query targets themselves are not that large, so I don't expect
much variation even across very fast and slow internet connections.) After you
have run `targets::tar_make()` once, then all intermediate targets and results 
will be cached for immediate retrieval thereafter. You can pull all of these 
cached objects into the global environment of an interactive R session by
running:

```r
targets::tar_load_everything()
```