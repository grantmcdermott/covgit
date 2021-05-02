plan =
  drake_plan(
    
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

# ** Seattle (matched linkedin) -------------------------------------------

    ## As above, but this time on a subset of users matched to a LinkedIn profile.
    ## Allows us to categorise by age and gender.
    sea_linkedin = rbindlist(lapply(
      2015:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y,
          hourly = TRUE,
          location_add = 'Seattle, WA (LinkedIn)', # city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles',
          users_tab = 'mcd-lab.covgit.sea_users_linkedin' 
        )
      }
    )),
    
    ## Write to disk
    write_sea_linkedin = write_fst(sea_linkedin, here('data/sea-linkedin.fst')),


# Plots -------------------------------------------------------------------  


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
      treat_date = '2020-03-16' ## New York State on PAUSE
    ),
    ## ** SFO ----
    ## Starting in Feb to avoid weird bump that occurs around that time in 2019
    sfo_diff_plot = daily_diff_plot(
      sfo[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-02-01', end_date = '2020-05-31',
      treat_date = '2020-03-16' ## Shelter-in-place order
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
    ## *** SEA (matched linkedin) ----
    sea_linkedin_diff_plot = daily_diff_plot(
      sea_linkedin[, lapply(.SD, sum), .SDcols = c('events', 'users'), by = .(date, location)], 
      start_date = '2020-01-02', end_date = '2020-05-31',
      treat_date = c('2020-03-04', '2020-03-12', '2020-03-23') ## MS + Amazon remote work, school closure, and mayoral stay at home order mandate
    ),


# * Proportion of weekend activity ----------------------------------------

    ## ** Global ----
    g_prop_wends = prop_wends(g, ylim = c(0.15, 0.25)),
    ## ** LON ----
    lon_prop_wends = prop_wends(
      lon[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-03-23'))-1
      ),
    ## ** NYC ----
    nyc_prop_wends = prop_wends(
      nyc[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-03-16'))-1
    ),
    ## ** SFO ----
    sfo_prop_wends = prop_wends(
      sfo[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-03-16'))-1
    ),
    ## ** BEI ----
    bei_prop_wends = prop_wends(
      bei[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-02-10'))-1
    ),
    ## ** BLR ----
    blr_prop_wends = prop_wends(
      blr[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      # ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-03-24'))-1
    ),
    ## ** SEA ----
    sea_prop_wends = prop_wends(
      sea[year(date)>=2017, 
          lapply(.SD, sum), .SDcols = c('events', 'users'), 
          by = .(location, date)],
      ylim = c(0.10, 0.25), 
      end_week = 26,
      treat_line = isoweek(as.Date('2020-03-04'))-1
    ),

  )






