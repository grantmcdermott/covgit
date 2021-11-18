# Libraries ---------------------------------------------------------------

library(drake)
library(visNetwork)
library(bigrquery)
library(stringr)
library(glue)
library(fst)
library(fixest)
library(prophet)
library(data.table)
library(magrittr)
library(sf)
library(rworldmap)
library(rgeos)
library(ggplot2)
library(ggiplot)
library(colorspace)
library(ggthemes)
library(hrbrthemes)
theme_set(theme_ipsum_rc())
library(here)


# Environment variables ---------------------------------------------------

billing = Sys.getenv("GCE_DEFAULT_PROJECT_ID")
bq_auth(path = Sys.getenv("GCE_AUTH_FILE"))


# Scripts -----------------------------------------------------------------

source('code/functions.R')
source("code/plan.R")


# Make --------------------------------------------------------------------

vis_drake_graph(plan) ## Requires: install.packages(c('visNetwork', 'lubridate'))
make(plan, verbose = 2)
