# Analysis of COVID-19's effect on GitHub activity

To reproduce all of the results, clone this repo and then 

1. Install the necesssary packages from R:

  ```r
  renv::restore() ## More details below
  ```

2. Run the make file `make.R`, e.g. from the command line, manually, or by sourcing the script from R:

  ```r
  source('make.R')
  ```


## renv

This repo contains an [**renv**](https://rstudio.github.io/renv/) lockfile to snapshot package versions and dependencies. To install all necessary packages in a sandboxed environment, run:

```r
renv::init()    ## Automatically run if you cloned/opened the repo as an RStudio project
renv::restore() ## Enter "y" when prompted
```

Note that this may take a few minutes if this is the first time that you're installing packages in an **renv** environment. However, global caching etc. mean that all future package (re)installations will be *much* quicker. I recommend taking a quick look at the **renv** [documentation](https://rstudio.github.io/renv/) for more information.

(As an aside, I have also set [RStudio Package Manager](https://packagemanager.rstudio.com/) (RSPM) as the default package repo location in the lockfile, i.e. rather than the usual CRAN. This enables fast installation of pre-compiled R package binaries on Linux, but should otherwise have no effect on other users.)