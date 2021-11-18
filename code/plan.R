plan =
  drake_plan(

# Helper datasets ---------------------------------------------------------

bad_dates = bad_dates_func(c('2015-02-01', 
                             '2016-02-01',
                             '2016-05-01',
                             '2018-04-03', 
                             '2019-09-12',
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

# Highlighted countries ---------------------------------------------------

## Loose criteria: Preferably 1) within the top 20 countries by users and 2) 
## single time-zoned. The latter is important (versus the main `countries` 
## dataset), since it allows for fine-grained distinction of wkend and 
## ohours activity. That being said, I'm only going to select a few 
## countries, since I just want to highlight some cases of interest (e.g. 
## different COVID policy responses) and potential heterogeneity. I also
## allow three exceptions --- US (b/c it has the most users), Brazil (mostly 
## single time-zoned), and South Africa (only 40th by users) --- for extra 
## geographic coverage.

countries_hi = rbindlist(Map(
  function(country_code, country_name, tz, user_rank) {
    rbindlist(lapply(
      2018:2020, function(y) {
        get_gh_activity_year(
          billing = billing, year = y, 
          country_code = country_code,
          geo_ringer = "EXTRACT(YEAR from DATETIME(created_at)) <= 2017",
          tz = tz)
      }
    ))[, ':=' (location = country_name, user_rank = user_rank)]
  },
  country_code = c('us', 'cn', 'de', 
                   'gb', 'fr', 'in', 
                   'br', 'jp', 'se',
                   'it', 'kr', 'za'),
  country_name = c('United States', 'China', 'Germany', 
                   'United Kingdom', 'France', 'India', 
                   'Brazil', 'Japan', 'Sweden',
                   'Italy', 'South Korea', 'South Africa'),
  tz           = c('US/Central', 'Asia/Shanghai', 'Europe/Berlin',
                   'Europe/London', 'Europe/Paris', 'Asia/Kolkata', 
                   'Brazil/East', 'Japan', 'Europe/Stockholm',
                   'Europe/Rome', 'Asia/Seoul', 'Africa/Johannesburg'),
  user_rank    = c(1, 2, 3, 
                   4, 5, 7, 
                   9, 10, 16,
                   18, 19, 40)
)),

## Write to disk
write_countries_hi = write_fst(countries_hi, here('data/countries-hi.fst')),


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
      city = 'London', geo_ringer = "country_code!='ca'",
      tz = 'Europe/London'
    )
    }
  )),

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
      city = 'Seattle', city_alias = 'Redmond',
      state = 'WA', geo_ringer = "state!='OR'",
      tz = 'America/Los_Angeles'
    )
  }
)),

## Write to disk
write_sea = write_fst(sea, here('data/sea.fst')),


# ** Seattle (linkedin matched) -------------------------------------------

## Same as the above, but this time limited to the group of users who: 
## (a) were active during January 2019, and (b) were matched against a 
## LinkedIn profile. Allows us to isolate intensive margin, as well as 
## categorise by age and gender.
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


# * Time-series plots -----------------------------------------------------

# ** Global ----
ts_global_events = ts_plot(g, bad_dates = bad_dates, measure = 'events'),
ts_global_events_ggsave = ggsave(
  here('figs/ts-global-events.pdf'), 
  plot = ts_global_events,
  width = 8, height = 5, device = cairo_pdf
  ),
ts_global_users = ts_plot(g, bad_dates = bad_dates, measure = 'users'),
ts_global_users_ggsave = ggsave(
  here('figs/ts-global-users.pdf'), 
  plot = ts_global_users,
  width = 8, height = 5, device = cairo_pdf
  ),

# ** Highlighted countries ----
## We'll break this up into two plots to conserve visual space...
ts_countries_events_1 = ts_plot(
  countries_hi[location %in% sort(unique(countries_hi$location))[1:6]], 
  bad_dates = bad_dates, measure = 'events', 
  by_location = TRUE, 
  lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
  theme = 'void'
  ),
ts_countries_events_1_ggsave = ggsave(
  here('figs/ts-countries-events-1.pdf'), 
  plot = ts_countries_events_1,
  width = 8, height = 10, device = cairo_pdf
  ),
ts_countries_events_2 = ts_plot(
  countries_hi[location %in% sort(unique(countries_hi$location))[7:12]], 
  bad_dates = bad_dates, measure = 'events', 
  by_location = TRUE, 
  lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
  theme = 'void'
  ),
ts_countries_events_2_ggsave = ggsave(
  here('figs/ts-countries-events-2.pdf'), 
  plot = ts_countries_events_2,
  width = 8, height = 10, device = cairo_pdf
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
## ** Global (prop = weekends, measure = events) ----
prop_global_wend_events = prop_plot(
  merge(g, lockdown_dates),
  prop = 'wend', measure = 'events',
  bad_dates = bad_dates,
  treat_date2 = 10, ## Global treatment date
  ylim = c(0.15, 0.25),
  title = NULL, facet_title = NULL,
  scales = 'free_y',
  labeller = labeller(.multi_line=FALSE)
  ),
prop_global_wend_events_ggsave = ggsave(
  here('figs/prop-global-wend-events.pdf'), 
  plot = prop_global_wend_events,
  width = 8, height = 5, device = cairo_pdf
  ),

## ** Cities (prop = both, measure = events) ----
prop_cities_both_events = prop_plot(
  merge(cities, lockdown_dates, by = 'location'),
  prop = 'both', measure = 'events',
  bad_dates = bad_dates, 
  min_year = 2017, 
  title = NULL,
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
  title = NULL,
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
  title = NULL,
  scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
  ),
prop_gender_ohrs_events_ggsave = ggsave(
  here('figs/prop-gender-ohrs-events.pdf'), 
  plot = prop_gender_ohrs_events,
  width = 8, height = 10, device = cairo_pdf
  ),


# Event-study regressions -------------------------------------------------

# * Gender ----------------------------------------------------------------

gender_prop = collapse_prop(
  merge(gender, lockdown_dates),
  # prop = 'ohrs',
  measure = 'both',
  bad_dates = bad_dates, 
  min_year = 2017,
  by_gender = TRUE,
  treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
  )[, lockdown_global := 10 ## We'll actually use the 'Global' week 10 date as the common lockdown treatment
    ][, ':=' (time_to_treatment = wk - lockdown, 
              time_to_treatment_global = wk - lockdown_global)][],
## ** ES female ----
es_female = feols(
    events ~ i(time_to_treatment_global, treated, -1) | location + yr + time_to_treatment_global, 
    gender_prop[gender=='Female'],
    vcov = ~location^yr,
    split = ~prop
    ),
## ** ES male ----
es_male = feols(
    events ~ i(time_to_treatment_global, treated, -1) | location + yr + time_to_treatment_global, 
    gender_prop[gender=='Male'],
    vcov = ~location^yr,
    split = ~prop
    ),
## ** Combined plot ----
es_gender_plot = ggiplot(
  list('Male' = es_male, 'Female' = es_female), 
  geom_style = 'ribbon',
  multi_style = "facet",
  xlab = 'Weeks until lockdown', 
  ylab = 'Effect on proportion of event activity',
  facet_args = list(labeller = labeller(.multi_line=FALSE)),
  theme = theme_es
  ) +
scale_color_discrete_qualitative(palette = "Harmonic", aesthetics = c('colour', 'fill')) +
labs(caption = 'Note: "Out-of-hours" defined as the period outside of 9 am to 6 pm.'),
es_gender_plot_save = ggsave(
  here('figs/es-gender-events.pdf'), 
  plot = es_gender_plot,
  width = 8, height = 5, device = cairo_pdf
  ),


# Prophet -----------------------------------------------------------------

## ** Holidays ----

## Note: Prophet requires specifying holiday-weekend interactions manually
## https://github.com/facebook/prophet/issues/1157#issuecomment-539229937

bad_dates_hols = CJ(ds = c(bad_dates, as.IDate('2020-06-10')), 
                    country_code = unique(countries_hi$country_code)),

hols = 
  rbind(
    rbindlist(lapply(
      list.files(here('data/holidays'), full.names = TRUE),
      \(f) fread(f)[year(date)>=2018, first(.SD), by = .(ds = date)
                    ][, country_code := gsub("\\.csv$", "", gsub(".*holidays_", "", f))
                      ][, .(ds, country_code)]
      )),
    bad_dates_hols
  )[, holiday := ifelse(wday(ds) %in% c(1,7), 'Holiday (weekend)', 'Holiday')
    ][, .(ds, country_code, holiday)
      ][, c('lower_window', 'upper_window') := 0
        ][month(ds)==12 & mday(ds)==25 & country_code %in% c('us', 'fr'), upper_window := 2
          ][month(ds)==12 & mday(ds)==25 & country_code!='kr', lower_window := -2
            ][month(ds)==12 & mday(ds)==26, upper_window := 1
              ][month(ds)==1 & mday(ds)==1 & country_code!='cn', lower_window := -1
                ][month(ds)==10 & mday(ds)==2 & country_code=='in', lower_window := -1] |>
  setorder(country_code, ds)


)