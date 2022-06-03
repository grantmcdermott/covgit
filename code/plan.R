plan =
  drake_plan(

# Helper dates ------------------------------------------------------------

bad_dates = as.IDate(c('2015-02-01', 
                       '2016-02-01',
                       '2016-05-01',
                       '2018-04-03', 
                       '2019-09-12',
                       '2020-06-10',
                       '2020-08-21',
                       '2021-03-09', '2021-03-10',
                       '2021-05-07', '2021-05-08', '2021-05-09', '2021-05-10', '2021-05-11',
                       '2021-08-26', '2021-08-27')),

lockdown_dates = fread(here('data/lockdown-dates.csv'))[, .SD[1], by = location],

holidays = rbindlist(lapply(
  list.files(here('data/holidays'), full.names = TRUE),
  function(f) fread(f)[, first(.SD), by = date
  ][, country_code := gsub("\\.csv$", "", gsub(".*holidays_", "", f))
  ][, .(date, holiday = name, country_code)]
)),


# Global ------------------------------------------------------------------

## Get 2015--2021 global activity data, taking account of the fact that GH
## Archive tarballs weren't generated correctly around October 2021. See:
## https://github.com/igrigorik/gharchive.org/issues/256
## https://github.com/igrigorik/gharchive.org/issues/259
g = rbindlist(lapply(
  2015:2021, function(y) {
    get_gh_activity_year(billing = billing, year = y)
    }
  ))[year(date)==2021 & isoweek(date) %in% 40:43, ## NB: GitHub Archive glitch
     c('events', 'users') := NA],

## Write to disk
write_global = write_fst(g, here('data/global.fst')),


## As above, but just push events
gpush = rbindlist(lapply(
  2015:2021, function(y) {
    get_gh_activity_year(billing = billing, year = y, event_type = 'Push')
    }
  ))[year(date)==2021 & isoweek(date) %in% 40:43, ## NB: GitHub Archive glitch
     c('events', 'users') := NA],

## Write to disk
write_global_push = write_fst(gpush, here('data/global-push-events.fst')),


## NOTE! We only have geo/location information for users (from GH Torrent) going 
## back until 2017 and up to the end of 2019. Hence, we'll limit all queries
## that require a location-based field to 2017-2020 from here on out.

# All countries separately ------------------------------------------------

## Get 2017--2020 activity data for all countries
countries = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      by_country = TRUE
      )
    },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
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
    rbindlist(Map(
      function(y, u) {
        get_gh_activity_year(
          billing = billing, 
          year = y,
          users_tab = u,
          country_code = country_code,
          geo_ringer = "EXTRACT(YEAR from DATETIME(created_at)) <= 2016",
          tz = tz)
        },
      y = 2017:2020,
      u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
            rep("ghtorrentmysql1906.MySQL1906.users", 2))
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

## Get 2017--2020 (hourly) LON data
lon = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'London', geo_ringer = "country_code!='ca'",
      tz = 'Europe/London'
    )
  },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_lon = write_fst(lon, here('data/lon.fst')),


# ** London (gender matched) ----------------------------------------------

lon_gender = rbindlist(lapply(
  2017:2020, function(y) {
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

## Get 2017--2020 (hourly) NYC data
nyc = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'New York', state = 'NY',
      tz = 'America/New_York'
    )
  },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_nyc = write_fst(nyc, here('data/nyc.fst')),

# ** NYC (gender matched) -------------------------------------------------

## Same as per the above, except this time matched to gender for as many users
## as possible
nyc_gender = rbindlist(lapply(
  2017:2020, function(y) {
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

## Get 2017--2020 (hourly) San Francisco data
sfo = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'San Francisco', state = 'CA',
      tz = 'America/Los_Angeles'
    )
  },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_sfo = write_fst(sfo, here('data/sfo.fst')),

# ** San Francisco (gender matched) ---------------------------------------

## Same as per the above, except this time matched to gender for as many users
## as possible
sfo_gender = rbindlist(lapply(
  2017:2020, function(y) {
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

## Get 2017--2020 (hourly) BEI data
bei = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'Beijing',
      tz = 'Asia/Shanghai'
      )
    },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_bei = write_fst(bei, here('data/bei.fst')),


# * Bengaluru (Bangalore) -------------------------------------------------

## Get 2017--2020 (hourly) BLR data
blr = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'Bengaluru', city_alias = 'Bangalore',
      tz = 'Asia/Kolkata'
      )
    },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_blr = write_fst(blr, here('data/blr.fst')),


# ** Bengalru (gender matched) --------------------------------------------

blr_gender = rbindlist(lapply(
  2017:2020, function(y) {
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

## Get 2017--2020 (hourly) Seattle data
sea = rbindlist(Map(
  function(y, u) {
    get_gh_activity_year(
      billing = billing, 
      year = y,
      users_tab = u,
      hourly = TRUE,
      city = 'Seattle', city_alias = 'Redmond',
      state = 'WA', geo_ringer = "state!='OR'",
      tz = 'America/Los_Angeles'
      )
    },
  y = 2017:2020,
  u = c(paste0("ghtorrent-bq.ght_", 2017:2018, "_04_01.users"), 
        rep("ghtorrentmysql1906.MySQL1906.users", 2))
  )),

## Write to disk
write_sea = write_fst(sea, here('data/sea.fst')),


# ** Seattle (linkedin matched) -------------------------------------------

## Same as the above, but this time limited to the group of users who: 
## (a) were active during January 2019, and (b) were matched against a 
## LinkedIn profile. Allows us to isolate intensive margin, as well as 
## categorise by age and gender.
sea_linkedin = rbindlist(lapply(
  2017:2020, function(y) {
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
  2017:2020, function(y) {
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


# Orgs --------------------------------------------------------------------

## Note: These are intended to be used as case-studies in the SM.

## * Microsoft ----

## https://github.com/microsoft
msft = rbindlist(lapply(
  2017:2020, function(y) {
    get_gh_activity_year(
      billing = billing, 
      year = y, 
      hourly = TRUE,
      # org_ids = c(microsoft = 6154722, MicrosoftDocs = 22479449, Azure = 6844498),
      org_ids = c(microsoft = 6154722), ## Limit to main MSFT repo to avoid cloud ramp-up effects
      city = 'Seattle', city_alias = 'Redmond',
      state = 'WA', geo_ringer = "state!='OR'",
      tz = 'America/Los_Angeles')
  }
)),

## Write to disk
write_msft = write_fst(msft, here('data/msft.fst')),


## * Alibaba ----

## https://github.com/alibaba
baba = rbindlist(lapply(
  2017:2020, function(y) {
    get_gh_activity_year(
      billing = billing, 
      year = y, 
      hourly = TRUE,
      org_ids = c(alibaba = 1961952), 
      tz = 'Asia/Shanghai')
  }
))[, location := 'Beijing'], ## really, Hangzhou (but fine for Chinese lockdown matching)

## Write to disk
write_baba = write_fst(baba, here('data/baba.fst')),


## UK Government Digital Service ----

## https://github.com/alphagov
agov = rbindlist(lapply(
  2017:2020, function(y) {
    get_gh_activity_year(
      billing = billing, 
      year = y, 
      hourly = TRUE,
      org_ids = c(alphagov = 596977),
      tz = 'Europe/London'
    )
  }
))[, location := 'London'],

## Write to disk
write_agov = write_fst(agov, here('data/agov.fst')),


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
# Note: We'll limit these to 2019-2020 and break them up into separate plots to 
# conserve visual space...
## Events
ts_countries_events_1 = ts_plot(
  countries_hi[year(date) %in% 2019:2020][
    location %in% sort(unique(countries_hi$location))[1:6]], 
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
  countries_hi[year(date) %in% 2019:2020][
    location %in% sort(unique(countries_hi$location))[7:12]], 
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
## Productivity
ts_countries_productivity_1 = ts_plot(
  countries_hi[year(date) %in% 2019:2020][
    location %in% sort(unique(countries_hi$location))[1:6]][
    week(date) %in% 2:50], 
  bad_dates = bad_dates, measure = 'productivity', 
  by_location = TRUE, 
  lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
  theme = 'void'
),
ts_countries_productivity_1_ggsave = ggsave(
  here('figs/ts-countries-productivity-1.pdf'), 
  plot = ts_countries_productivity_1,
  width = 8, height = 10, device = cairo_pdf
),
ts_countries_productivity_2 = ts_plot(
  countries_hi[year(date) %in% 2019:2020][
    location %in% sort(unique(countries_hi$location))[7:12]][
    week(date) %in% 2:50],  
  bad_dates = bad_dates, measure = 'productivity', 
  by_location = TRUE, 
  lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
  theme = 'void'
),
ts_countries_productivity_2_ggsave = ggsave(
  here('figs/ts-countries-productivity-2.pdf'), 
  plot = ts_countries_productivity_2,
  width = 8, height = 10, device = cairo_pdf
),


# * Hourly plots (cities) -------------------------------------------------

hourly_plot_lon = hourly_plot(cities[location=="London"]),
hourly_plot_lon_ggsave = ggsave(here("figs/hourly-lon.pdf"),
                                plot = hourly_plot_lon,
                                width = 8, height = 5, device = cairo_pdf),

hourly_plot_nyc = hourly_plot(cities[location=="New York, NY"]),
hourly_plot_nyc_ggsave = ggsave(here("figs/hourly-nyc.pdf"),
                                plot = hourly_plot_nyc,
                                width = 8, height = 5, device = cairo_pdf),

hourly_plot_sfo = hourly_plot(cities[location=="San Francisco, CA"]),
hourly_plot_sfo_ggsave = ggsave(here("figs/hourly-sfo.pdf"),
                                plot = hourly_plot_sfo,
                                width = 8, height = 5, device = cairo_pdf),

hourly_plot_bei = hourly_plot(cities[location=="Beijing"]),
hourly_plot_bei_ggsave = ggsave(here("figs/hourly-bei.pdf"),
                                plot = hourly_plot_bei,
                                width = 8, height = 5, device = cairo_pdf),

hourly_plot_blr = hourly_plot(cities[location=="Bengaluru"]),
hourly_plot_blr_ggsave = ggsave(here("figs/hourly-blr.pdf"),
                                plot = hourly_plot_blr,
                                width = 8, height = 5, device = cairo_pdf),

hourly_plot_sea = hourly_plot(cities[location=="Seattle, WA"]),
hourly_plot_sea_ggsave = ggsave(here("figs/hourly-sea.pdf"),
                                plot = hourly_plot_sea,
                                width = 8, height = 5, device = cairo_pdf),


# * Proportion plots ------------------------------------------------------

## ** Global (prop = weekends, measure = both) ----
prop_global_wend = prop_plot(
  merge(g, lockdown_dates),
  prop = 'wend', measure = 'both',
  bad_dates = bad_dates,
  highlight_year = 2020:2021,
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
  merge(g[year(date)!=2021], lockdown_dates), ## For main text we exclude 2021 data
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
## ** Global (prop = weekends, measure = push events) ----
prop_global_wend_pushes = prop_plot(
  merge(gpush[year(date)!=2021], lockdown_dates),
  prop = 'wend', measure = 'events',
  bad_dates = bad_dates,
  treat_date2 = 10, ## Global treatment date
  # ylim = c(0.15, 0.25),
  title = NULL, facet_title = NULL,
  scales = 'free_y',
  labeller = labeller(.multi_line=FALSE)
),
prop_global_wend_pushes_ggsave = ggsave(
  here('figs/prop-global-wend-pushes.pdf'), 
  plot = prop_global_wend_pushes,
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

## ** Orgs ----

## Microsoft
prop_msft = prop_plot(
  merge(msft, lockdown_dates, by = 'location')[
    , location := 'Microsoft'],
  prop = 'both', measure = 'events',
  bad_dates = bad_dates, 
  # treat_date2 = 10, 
  title = NULL,
  scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
  ),
prop_msft_ggsave = ggsave(
  here('figs/prop-msft.pdf'), 
  plot = prop_msft,
  width = 8, height = 5, device = cairo_pdf
  ),

## Alibaba
prop_baba = prop_plot(
  merge(baba, lockdown_dates, by = 'location')[
    , location := 'Alibaba'],
  prop = 'both', measure = 'events',
  bad_dates = bad_dates, 
  # treat_date2 = 10,
  title = NULL,
  scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
  ),
prop_baba_ggsave = ggsave(
  here('figs/prop-baba.pdf'), 
  plot = prop_baba,
  width = 8, height = 5, device = cairo_pdf
  ),

## UK AlphaGov
prop_agov = prop_plot(
  merge(agov, lockdown_dates, by = 'location')[
    , location := 'UK government digital service'],
  prop = 'both', measure = 'events',
  bad_dates = bad_dates, 
  treat_date2 = isoweek(as.IDate(c('2020-06-01', '2020-06-15', '2020-06-23'))), 
  title = NULL,
  scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
  ),
prop_agov_ggsave = ggsave(
  here('figs/prop-agov.pdf'), 
  plot = prop_agov,
  width = 8, height = 5, device = cairo_pdf
  ),


# Event-study regressions -------------------------------------------------

hols_prop = 
  holidays[, .N, 
           by = .(country_code, yr=year(date), wk=isoweek(date), 
                  wend=ifelse(wday(date) %in% c(1,7), 'hols_wend', 'hols_wk'))] %>% 
  dcast(... + N~wend),

# * Gender ----------------------------------------------------------------

gender_prop = collapse_prop(
  merge(gender, lockdown_dates),
  # prop = 'ohrs',
  measure = 'both',
  bad_dates = setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage
  min_year = 2017,
  by_gender = TRUE,
  treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
  )[, lockdown_global := 10 ## We'll actually use the 'Global' week 10 date as the common lockdown treatment
   ][, ':=' (time_to_treatment = wk - lockdown, 
              time_to_treatment_global = wk - lockdown_global)
   ] %>%
  merge(hols_prop[, -'N'], all.x=TRUE, by = c('country_code', 'yr', 'wk'))  %>%
  .[is.na(hols_wend), hols_wend := 0] %>%
  .[is.na(hols_wk), hols_wk := 0],

## ** ES female ----
es_female = feols(
    events ~ i(time_to_treatment_global, treated, -1)  +
      hols_wk + hols_wend + bdates_wk + bdates_wend | 
      location + yr + time_to_treatment_global, 
    gender_prop[gender=='Female'],
    vcov = ~location^yr,
    split = ~prop
    ),
## ** ES male ----
es_male = feols(
    events ~ i(time_to_treatment_global, treated, -1) +
      hols_wk + hols_wend + bdates_wk + bdates_wend | 
      location + yr + time_to_treatment_global, 
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


# * Extra regs ------------------------------------------------------------

g_prop = collapse_prop(
  merge(g, lockdown_dates),
  prop = 'wend',
  bad_dates = setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage
  treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
  )[, time_to_treatment := wk - lockdown][],

## ** ES global ----
es_global = feols(
  events ~ i(time_to_treatment, treated, -1) + bdates_wk + bdates_wend 
  | yr + time_to_treatment, 
  g_prop,
  vcov = ~yr
  ),

cities_prop = collapse_prop(
  merge(cities, lockdown_dates)[, 
                                country_code := fcase(location=='London', 'gb',
                                                      location=='Beijing', 'cn',
                                                      default = 'us')],
  measure = 'both',
  bad_dates =  setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage, 
  min_year = 2017,
  treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
  ) %>%
  .[, lockdown_global := 10] %>% ## We'll actually use the 'Global' week 10 date as the common lockdown treatment 
  .[, ':=' (time_to_treatment_local = wk - lockdown, 
            time_to_treatment_global = wk - lockdown_global)] %>%
  .[, time_to_treatment := ifelse(location=='Beijing', 
                                  time_to_treatment_local, 
                                  time_to_treatment_global)] %>%
  merge(hols_prop[, -'N'], all.x=TRUE, by = c('country_code', 'yr', 'wk'))  %>%
  .[is.na(hols_wend), hols_wend := 0] %>%
  .[is.na(hols_wk), hols_wk := 0],

## ** ES cities wend ----
es_cities_wend = feols(
  events ~ i(time_to_treatment, treated, -1) +
    hols_wk + hols_wend + bdates_wk + bdates_wend | 
    location + yr + time_to_treatment, 
  cities_prop[prop=='Weekends'],
  vcov = ~location^yr,
  fsplit = ~location
  ),

## ** ES cities ohrs ----
es_cities_ohrs = feols(
  events ~ i(time_to_treatment, treated, -1) +
    hols_wk + hols_wend + bdates_wk + bdates_wend | 
    location + yr + time_to_treatment_global, 
  cities_prop[prop=='Out-of-hours'],
  vcov = ~location^yr,
  fsplit = ~location
  ),

## ** Export tabs ----
etable_wend = etable(
    es_global, es_cities_wend, 
    dict = c(treated = "2020", time_to_treatment = "Lockdown",
             events = "Events", yr = "Year", location = "Location"),
    drop = "[[:digit:]]{2}$|bdates|hols", 
    headers = list("^:_:Location" = c("Global", "Cities (all)", tail(names(es_cities_wend), -1))),
    se.row = FALSE,
    notes = c("Clustered standard errors (by location--year) in parentheses."),
    style.tex = style.tex(main = "aer"),
    file = "tabs/es-wend.tex", replace = TRUE
    ),

etable_ohrs = etable(
    es_cities_ohrs, 
    dict = c(treated = "2020", time_to_treatment = "Lockdown",
             events = "Events", yr = "Year", location = "Location"),
    drop = "[[:digit:]]{2}$|bdates|hols", 
    headers = list("^:_:Location" = c("Cities (all)", tail(names(es_cities_wend), -1))),
    se.row = FALSE,
    notes = c("Clustered standard errors (by location--year) in parentheses."),
    style.tex = style.tex(main = "aer"),
    file = "tabs/es-ohrs.tex", replace = TRUE
    ),


# Prophet -----------------------------------------------------------------

## ** Holidays ----

## Note: Prophet requires specifying holiday-weekend interactions manually
## https://github.com/facebook/prophet/issues/1157#issuecomment-539229937
## The date field must also be called "ds".

bad_dates_hols = CJ(ds = sort(c(bad_dates, as.IDate('2018-04-04'))), 
                    holiday  = 'GitHub down',
                    country_code = unique(countries_hi$country_code)),

hols = 
  rbind(
    holidays[year(date)>=2017, .(ds = date, holiday, country_code)],
    bad_dates_hols
  )[wday(ds) %in% c(1,7), holiday := 'Holiday (weekend)'
  ][, .(ds, country_code, holiday)
  ][, c('lower_window', 'upper_window') := 0
  ][country_code=='us' & holiday=='Thanksgiving Day', ':=' (upper_window = 1, 
                                                            lower_window = -1)
  ][country_code %in% c('us', 'fr') & month(ds)==12 & mday(ds)==25, upper_window := 2
  ][country_code!='kr' & month(ds)==12 & mday(ds)==25, lower_window := -2
  ][month(ds)==12 & mday(ds)==26, upper_window := 1
  ][country_code!='cn' & month(ds)==1 & mday(ds)==1, lower_window := -2
  # ][country_code=='kr' & month(ds)==1 & mday(ds)==25, lower_window := -1
  ][country_code=='cn' & holiday=='Spring Festival Eve', lower_window := -2
  ][country_code %in% c('cn','in','jp','kr') & ds==as.IDate('2019-09-12'), upper_window := 1
  ][country_code=='se' & month(ds)==6 & mday(ds)>=20 & holiday=='Holiday (weekend)', lower_window := -1
  ][country_code=='in' & month(ds)==10 & mday(ds)==2, lower_window := -1] %>%
  setorder(country_code, ds),

## ** Highlighted countries ----

prophet_co = rbindlist(lapply(
  split(countries_hi, countries_hi$country_code), 
  function(x) prophet_fc(x, holidays = hols, level = 0.9, 
                         seasonality.mode = 'multiplicative',
                         outliers = c('2020-06-10', '2019-09-12', 
                                      '2020-01-24', ## Only KR but enough of a pain that will remove for all
                                      '2019-12-23', '2019-12-30', '2018-12-30'))
  )),
write_prophet_co = write_fst(prophet_co, here('data/prophet-countries.fst')),


## *** Plots ----

## Difference trends (default)
prophet_co_plot = prophet_plot(
  prophet_co[ds>='2019-09-01' & ds<='2020-06-30'], forecast = '2020-01-01'
  ),
prophet_co_plot_ggsave = ggsave(
  here('figs/prophet-countries.pdf'), 
  plot = prophet_co_plot,
  width = 16, height = 9, device = cairo_pdf
  ),
prophet_co_plot_placebo = prophet_plot(
  prophet_co[ds>='2018-09-01' & ds<='2019-06-30']#, forecast = '2020-01-01'
  ),
prophet_co_plot_placebo_ggsave = ggsave(
  here('figs/prophet-countries-placebo.pdf'), 
  plot = prophet_co_plot_placebo,
  width = 16, height = 9, device = cairo_pdf
  ),
## Difference percentage
prophet_co_plot_perc = prophet_plot(
  prophet_co[ds>='2019-09-01' & ds<='2020-06-30'], forecast = '2020-01-01',
  type = 'diffperc'
),
prophet_co_plot_perc_ggsave = ggsave(
  here('figs/prophet-countries-perc.pdf'), 
  plot = prophet_co_plot_perc,
  width = 16, height = 9, device = cairo_pdf
),
## Raw trends
prophet_co_plot_raw = prophet_plot(
  prophet_co[ds>='2019-09-01' & ds<='2020-06-30'], forecast = '2020-01-01',
  type = 'raw'
  ),
prophet_co_plot_raw_ggsave = ggsave(
  here('figs/prophet-countries-raw.pdf'), 
  plot = prophet_co_plot_raw,
  width = 16, height = 9, device = cairo_pdf
  ),
prophet_co_plot_raw_placebo = prophet_plot(
  prophet_co[ds>='2018-09-01' & ds<='2019-06-30'], #forecast = '2020-01-01'
  type = 'raw'
  ),
prophet_co_plot_raw_placebo_ggsave = ggsave(
  here('figs/prophet-countries-raw-placebo.pdf'), 
  plot = prophet_co_plot_raw_placebo,
  width = 16, height = 9, device = cairo_pdf
  )



)
