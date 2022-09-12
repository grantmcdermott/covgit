# Labor reallocation and remote work 

This repository contains code and data for [McDermott and Hansen (2021)](https://www.nber.org/papers/w29598).

To reproduce all of the results in the paper, clone this repo and then follow 
these three steps:

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

I use [**drake**](https://books.ropensci.org/drake/) to automate the entire 
project build (analysis, etc.). Assuming that you have set up your Google Cloud 
API key (Step 0) and installed the necessary R packages (Step 1), simply run the 
`make.R` file at the root of the repo.

You can do this interactively. Or you can source the entire script from within 
R:

```r
source('make.R')
```

Or, you can source it directly from the shell:

```sh
Rscript make.R
```

(You maybe prompted to authenticate with BigQuery via your browser before it 
will allow you to download the data.)

Regardless of how you choose to do it, note that running the `make.R` file once
will cache all of the intermediate targets and objects (e.g. data frames and 
plots). This is nice because, like standard Make tools, it will only redo the 
parts of the analysis that is has too. So we won't get billed by BigQuery every
time we run `make.R` again... although the figure would be minuscule for the 
small/efficient queries that we are running here. You can pull all of these 
cached objects into the global environment of a live R session by running:

```r
drake::loadd()
```