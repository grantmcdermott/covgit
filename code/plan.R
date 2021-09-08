plan =
  drake_plan(

# Helper datasets ---------------------------------------------------------

    bad_dates = bad_dates_func(c('2015-02-01', 
                                 '2016-02-01',
                                 '2016-05-01',
                                 '2018-04-03', 
                                 '2020-08-21')),
    
    lockdown_dates = fread(here('data/lockdown-dates.csv'))[, .SD[1], by = location],
    
# Global ------------------------------------------------------------------

    ## Get 2015--2020 global activity data
    g = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(billing = billing, year = y)
        }
      )),
    
    ## Write to disk
    write_global = write_fst(g, here('data/global.fst')),
    
# All countries separately ------------------------------------------------

    ## Get 2015--2020 activity data for all countries
    countries = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(billing = billing, year = y, by_country = TRUE)
        }
      ))[!is.na(country_code), location := country_code][order(country_code, date)],
    
    ## Write to disk
    write_countries = write_fst(countries, here('data/countries.fst')),


# Cities ------------------------------------------------------------------

## Top 10 largest by identified users in the GHTorrent data (`1906` table)
## 
# 1.  London, GB        (44,759)
# 2.  New York, US      (44,413)
# 3.  San Francisco, US (40,713)
# 4.  Beijing, CN       (38,901)
# 5.  Bengaluru, IN     (35,706)
# 6.  Shanghai, CN      (25,921)
# 7.  Seattle, US       (24,205)
# 8.  Paris, FR         (22,792)
# 9.  Moscow, RU        (18,910)
# 10. Chicago, US       (18,487)

# * London ----------------------------------------------------------------

    ## Get 2015--2020 (hourly) LON data
    lon = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y, 
          hourly = TRUE,
          city = 'London', by_country = TRUE,
          tz = 'Europe/London'
        )
        }
      ))[country_code=='gb'],
    
    ## Write to disk
    write_lon = write_fst(lon, here('data/lon.fst')),


# ** London (gender matched) ----------------------------------------------

    lon_gender = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'London (gender)',
          tz = 'Europe/London',
          users_tab = 'mcd-lab.covgit.lon_users_gender_matched', 
          gender = TRUE
        )
      }
    )),

    ## Write to disk
    write_lon_gender = write_fst(lon_gender, here('data/lon-gender.fst')),

# * New York --------------------------------------------------------------

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

# ** NYC (gender matched) -------------------------------------------------

## Same as per the above, except this time matched to gender for as many users
## as possible
nyc_gender = rbindlist(lapply(
  2015:2020, function(y) {
    get_gh_activity_year(
      billing = billing, year = y,
      hourly = TRUE,
      location_add = 'New York, NY (gender)',
      tz = 'America/New_York',
      users_tab = 'mcd-lab.covgit.nyc_users_gender_matched', 
      gender = TRUE
    )
  }
)),

## Write to disk
write_nyc_gender = write_fst(nyc_gender, here('data/nyc-gender.fst')),

# * San Francisco ----------------------------------------------------------

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

# ** San Francisco (gender matched) ---------------------------------------

    ## Same as per the above, except this time matched to gender for as many users
    ## as possible
    sfo_gender = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'San Francisco, CA (gender)',
          tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sfo_users_gender_matched', 
          gender = TRUE
        )
      }
    )),
    
    ## Write to disk
    write_sfo_gender = write_fst(sfo_gender, here('data/sfo-gender.fst')),

# * Beijing ---------------------------------------------------------------

    ## Get 2015--2020 (hourly) BEI data
    bei = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y, 
          hourly = TRUE,
          city = 'Beijing',
          tz = 'Asia/Shanghai'
        )
      }
    )),
    
    ## Write to disk
    write_bei = write_fst(bei, here('data/bei.fst')),


# * Bengaluru (Bangalore) -------------------------------------------------

    ## Get 2015--2020 (hourly) BLR data
    blr = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y, 
          hourly = TRUE,
          city = 'Bengaluru', city_alias = 'Bangalore',
          tz = 'Asia/Kolkata'
        )
      }
    )),
    
    ## Write to disk
    write_blr = write_fst(blr, here('data/blr.fst')),


# ** Bengalru (gender matched) --------------------------------------------

    blr_gender = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'Bengaluru (gender)',
          tz = 'Asia/Kolkata',
          users_tab = 'mcd-lab.covgit.blr_users_gender_matched', 
          gender = TRUE
        )
      }
    )),
    
    ## Write to disk
    write_blr_gender = write_fst(blr_gender, here('data/blr-gender.fst')),


# * Seattle ---------------------------------------------------------------
    
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

# ** Seattle (Jan 2019 cohort) --------------------------------------------

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

# ** Seattle (linkedin matched) -------------------------------------------

    ## As above, but this time on a subset of users matched to a LinkedIn profile.
    ## Allows us to categorise by age and gender.
    sea_linkedin = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'Seattle, WA (LinkedIn)', # city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sea_users_linkedin',
          gender = TRUE, age_buckets = c(30, 40, 50)
        )
      }
    )),
    
    ## Write to disk
    write_sea_linkedin = write_fst(sea_linkedin, here('data/sea-linkedin.fst')),

# ** Seattle (gender matched) ---------------------------------------------

    ## Similar to the above, except this time matched to gender for all Seattle
    ## users (not just those with an identifiable LinkedIn profile)
    sea_gender = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'Seattle, WA (gender)', # city = 'Seattle', state = 'WA',
                tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sea_users_gender_matched', 
          gender = TRUE
          )
        }
      )),

    ## Write to disk
    write_sea_gender = write_fst(sea_gender, here('data/sea-gender.fst')),


# * Cities (all) ----------------------------------------------------------

    cities = rbind(lon, nyc, sfo, bei, blr, sea, fill = TRUE),


# * Gender (all) ----------------------------------------------------------

    gender = gender_prep(
      rbind(lon_gender, nyc_gender, sfo_gender, blr_gender, sea_gender)
      ),


# Plots -------------------------------------------------------------------  


# * Activity map ----------------------------------------------------------

    activity_map_plot = activity_map(countries),
    activity_map_plot_ggsave = ggsave(
      here('figs/activity-map.pdf'), 
      plot = activity_map_plot,
      width = 8, height = 5, device = cairo_pdf
    ),
    

# * Daily diffs -----------------------------------------------------------
    
    ## ** Global ----
    ## Note that we start in mid-Feb to avoid the weird bump in user activity
    ## that occured in early Feb 2019.
    g_diff_plot = daily_diff_plot(
      g, start_date = '2020-02-15', end_date = '2020-05-31',
      treat_date = '2020-03-15' ## Guestimate
    ),
    ## ** LON ----
    lon_diff_plot = daily_diff_plot(
      lon[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2019-10-01', end_date = '2020-07-31',
      treat_date = '2020-03-23' ## National stay at home order
      ),
    ## ** NYC ----
    ## Starting in Feb to avoid weird bump that occurs around that time in 2019
    nyc_diff_plot = daily_diff_plot(
      nyc[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-02-01', end_date = '2020-05-31',
      treat_date = '2020-03-22' ## New York State on PAUSE
    ),
    ## ** SFO ----
    ## Starting in Feb to avoid weird bump that occurs around that time in 2019
    sfo_diff_plot = daily_diff_plot(
      sfo[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-02-01', end_date = '2020-05-31',
      # treat_date = '2020-03-16' ## Shelter-in-place order
      treat_date = '2020-03-19' ## State-wide shelter-in-place order
    ),
    ## ** BEI ----
    ## Going back a full year to demonstrate the large effect after Chinese New Year
    bei_diff_plot = daily_diff_plot(
      bei[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2019-07-01', end_date = '2020-06-30',
      treat_date = '2020-02-10' ## Shelter-in-place order
    ),
    ## ** BLR ----
    blr_diff_plot = daily_diff_plot(
      blr[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2019-08-01', end_date = '2020-07-31',
      treat_date = '2020-03-24' ## Nationwide lockdown (slowly phased out from May 30)
    ),
    ## ** SEA ----
    sea_diff_plot = daily_diff_plot(
      sea[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-01-02', end_date = '2020-05-31',
      treat_date = c('2020-03-04', '2020-03-12', '2020-03-23') ## MS + Amazon remote work, school closure, and mayoral stay at home order mandate
    ),
    ## *** SEA (Jan 2019 cohort) ----
    sea_cohort_diff_plot = daily_diff_plot(
      sea_cohort[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-01-02', end_date = '2020-05-31',
      treat_date = c('2020-03-04', '2020-03-12', '2020-03-23') ## MS + Amazon remote work, school closure, and mayoral stay at home order mandate
    ),
    ## *** SEA (linkedin matched) ----
    sea_linkedin_diff_plot = daily_diff_plot(
      sea_linkedin[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-01-02', end_date = '2020-05-31',
      treat_date = c('2020-03-04', '2020-03-12', '2020-03-23') ## MS + Amazon remote work, school closure, and mayoral stay at home order mandate
    ),
    ## *** SEA (gender matched) ----
    sea_gender_diff_plot = daily_diff_plot(
      sea_gender[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-01-02', end_date = '2020-05-31',
      treat_date = c('2020-03-04', '2020-03-12', '2020-03-23') ## MS + Amazon remote work, school closure, and mayoral stay at home order mandate
    ),


# * Proportion figures ----------------------------------------------------

    ## ** Global (prop = weekends, measure = both) ----
    prop_global_wend = prop_plot(
      merge(g, lockdown_dates),
      prop = 'wend', measure = 'both',
      bad_dates = bad_dates,
      treat_date2 = 10, ## Global treatment date
      ylim = c(0.15, 0.25),
      scales = 'free_y',
      labeller = labeller(.multi_line=FALSE)
      ),
    prop_global_wend_ggsave = ggsave(
      here('figs/prop-global-wend.pdf'), 
      plot = prop_global_wend,
      width = 8, height = 5, device = cairo_pdf
      ),

    ## ** Cities (prop = both, measure = events) ----
    prop_cities_both_events = prop_plot(
      merge(cities, lockdown_dates, by = 'location'),
      prop = 'both', measure = 'events',
      bad_dates = bad_dates, 
      min_year = 2017, 
      treat_date2 = 10, ## Global treatment date
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
      ),
    prop_cities_both_events_ggsave = ggsave(
      here('figs/prop-cities-both-events.pdf'), 
      plot = prop_cities_both_events,
      width = 8, height = 10, device = cairo_pdf
      ),

    ## ** Gender (prop = wend, measure = events) ----
    prop_gender_wend_events = prop_plot(
      merge(gender, lockdown_dates), 
      prop = 'wend', measure = 'events', 
      bad_dates = bad_dates, min_year = 2017,
      treat_date2 = 10, ## Global treatment date
      by_gender = TRUE, 
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
      ),
    prop_gender_wend_events_ggsave = ggsave(
      here('figs/prop-gender-wend-events.pdf'), 
      plot = prop_gender_wend_events,
      width = 8, height = 10, device = cairo_pdf
      ),

    ## ** Gender (prop = ohrs, measure = events) ----
    prop_gender_ohrs_events = prop_plot(
      merge(gender, lockdown_dates), 
      prop = 'ohrs', measure = 'events', 
      bad_dates = bad_dates, min_year = 2017,
      treat_date2 = 10, ## Global treatment date
      by_gender = TRUE, 
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
      ),
    prop_gender_ohrs_events_ggsave = ggsave(
      here('figs/prop-gender-ohrs-events.pdf'), 
      plot = prop_gender_ohrs_events,
      width = 8, height = 10, device = cairo_pdf
      )

  )

