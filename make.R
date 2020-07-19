# Libraries ---------------------------------------------------------------

library(drake)
library(bigrquery)
library(stringr)
library(glue)
library(data.table)
library(magrittr)
library(ggplot2)
library(hrbrthemes)
theme_set(theme_ipsum_rc())
library(here)


# Environment variables ---------------------------------------------------

billing_id = Sys.getenv("GCE_DEFAULT_PROJECT_ID")
bq_auth("GCE_AUTH_FILE")


# Scripts -----------------------------------------------------------------

source('code/functions.R')
source("code/plan.R")


# Make --------------------------------------------------------------------

vis_drake_graph(plan) ## Requires: install.packages(c('visNetwork', 'lubridate'))
make(plan, verbose = 2)