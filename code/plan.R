plan =
  drake_plan(
    
    ## All countries by date
    all = rbind(
      get_gh_activity_country(
        billing, 
        year = 2019, month = 1, 
        excl_nulls = TRUE, by_date = TRUE
        ),
      get_gh_activity_country(
        billing, 
        year = 2019, 
        excl_nulls = TRUE, by_date = TRUE
        )
      ),
    
    c20 = rbind(
      get_gh_activity_country(
        billing, 
        year = 2020, month = 1, 
        excl_nulls = TRUE, by_date = TRUE
        ),
      get_gh_activity_country(
        billing, 
        year = 2020, 
        excl_nulls = TRUE, by_date = TRUE
        )
      ),
    
    c_all = rbind(c19, c20),
    
    write_countries_all = write_fst(c_all, here('data/countries-all.fst')),

# Global ------------------------------------------------------------------

    ## Get 2015--2020 global activity data
    g = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(billing = billing, year = y)
        }
      )),
    
    ## Write to disk
    write_global = write_fst(g, here('data/global.fst')),
    
    ## Plot the difference between the early 2019 and 2020 global data.
    ## Note that we start in mid-Feb to avoid the weird bump in user activity
    ## that occured in early Feb 2019.
    g_diff_plot_events = daily_diff_plot(
      g, y = 'events', start_date = '2020-02-15', end_date = '2020-05-31'
      ),
    g_diff_plot_users = daily_diff_plot(
      g, y = 'users', start_date = '2020-02-15', end_date = '2020-05-31'
      ),


# All countries separately ------------------------------------------------

    ## Get 2015--2020 activity data for all countries
    countries = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(billing = billing, year = y, by_country = TRUE)
        }
      ))[order(country_code, date)],
    
    ## Write to disk
    write_countries = write_fst(g, here('data/countries.fst')),

# New York ----------------------------------------------------------------

    ## Get 2015--2020 (hourly) NYC data
    nyc = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          city = 'New York', state = 'NY',
          tz = 'America/New_York'
        )
        }
      )),
    
    ## Write to disk
    write_nyc = write_fst(nyc, here('data/nyc.fst')),
    
    ## Plot the difference between the early 2019 and 2020 NYC data
    nyc_diff_plot_events = daily_diff_plot(
      nyc[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    nyc_diff_plot_users = daily_diff_plot(
      nyc[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),

# San Francisco ------------------------------------------------------------

    ## Get 2015--2020 (hourly) San Francisco data
    sfo = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          city = 'San Francisco', state = 'CA',
          tz = 'America/Los_Angeles'
        )
        }
      )),
    
    ## Write to disk
    write_sfo = write_fst(sfo, here('data/sfo.fst')),
    
    ## Plot the difference between the early 2019 and 2020 San Francisco data
    sfo_diff_plot_events = daily_diff_plot(
      sfo[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sfo_diff_plot_users = daily_diff_plot(
      sfo[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),

# Seattle -----------------------------------------------------------------
    
    ## Get 2015--2020 (hourly) Seattle data
    sea = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles'
        )
      }
    )),
    
    ## Write to disk
    write_sea = write_fst(sea, here('data/sea.fst')),
    
    ## Plot the difference between the early 2019 and 2020 Seattle data
    sea_diff_plot_events = daily_diff_plot(
      sea[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_diff_plot_users = daily_diff_plot(
      sea[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    

# Seattle (Jan 2019 cohort) -----------------------------------------------

    ## Same as the above, but this time limited to the group of users who were 
    ## active during January 2019. In other words, we follow the exact same 
    ## users through and try to isolate the intensive margin for this cohort.
    sea_cohort = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'
        )
        }
      ))[, location := paste(location, 'cohort')],
    
    ## Write to disk
    write_sea_cohort = write_fst(sea_cohort, here('data/sea-cohort.fst')),
    
    ## Diff plot
    sea_cohort_diff_plot_events = daily_diff_plot(
      sea_cohort[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)],
      y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_cohort_diff_plot_users = daily_diff_plot(
      sea_cohort[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)],
      y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),


# Seattle (matched linkedin) ----------------------------------------------

    ## As above, but this time on a subset of users matched to a LinkedIn profile.
    ## Allows us to categorise by age and gender.
    sea_cohort = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'Seattle, WA (LinkedIn)', # city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
        )
      }
    )),
    
    ## Write to disk
    write_sea_linkedin = write_fst(sea_linkedin, here('data/sea-linkedin.fst')),
    
    ## Diff plot
    sea_linkedin_diff_plot_events = daily_diff_plot(
      sea_linkedin[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_linkedin_diff_plot_users = daily_diff_plot(
      sea_linkedin[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      )
  
  )
