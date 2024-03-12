# Libraries ---------------------------------------------------------------

library(targets)
library(visNetwork)

# Set target options:
tar_option_set(
  # packages = c(
  #   "bigrquery", "stringi", "glue",
  #   # "fst",
  #   "fixest", "prophet",
  #   "data.table", "magrittr",
  #   "sf", "rworldmap",
  #   "ggplot2", "ggfixest", "colorspace", "ggthemes", "hrbrthemes",
  #   "here"
  #   ),
  format = "qs" # default storage format
  # Set other options as needed.
)

library(bigrquery)
library(stringi)
library(glue)
# library(fst)
library(qs)
library(fixest)
library(prophet)
library(data.table)
library(magrittr)
library(sf)
library(rworldmap)
# library(rgeos)
library(ggplot2)
library(ggfixest)
library(colorspace)
library(ggthemes)
library(hrbrthemes)
theme_set(theme_ipsum_rc())
library(here)


# Environment variables ---------------------------------------------------

billing = Sys.getenv("GCE_DEFAULT_PROJECT_ID")
bigrquery::bq_auth(path = Sys.getenv("GCE_AUTH_FILE"))


# Scripts -----------------------------------------------------------------

source("code/functions.R")

list(
  
  tar_target(lockdown_dates_csv, here("data/lockdown-dates.csv"), format = "file"),
  tar_target(lockdown_dates, get_lockdown_dates(lockdown_dates_csv)),
  
  tar_target(bad_dates, get_bad_dates()),
  
  tar_target(holidays_files, here('data/holidays'), format = "file"),
  tar_target(holidays, get_holidays(holidays_files)),
  
  # Global ------------------------------------------------------------------
  
  ## Get 2015--2021 global activity data, taking account of the fact that GH
  ## Archive tarballs weren't generated correctly around October 2021. See:
  ## https://github.com/igrigorik/gharchive.org/issues/259i
  tar_target(
    g,
    rbindlist(lapply(
      2015:2023, function(y) {
        get_gh_activity_year(billing = billing, year = y)
        }
      ))[
        year(date)==2021 & isoweek(date) %in% 40:43, ## NB: GitHub Archive glitch
        c('events', 'users') := NA
        ]
  ),
  
  ## As above, but just push events
  tar_target(
    gpush,
    rbindlist(lapply(
      2015:2023, function(y) {
        get_gh_activity_year(billing = billing, year = y, event_type = 'Push')
        }
    ))[
      year(date)==2021 & isoweek(date) %in% 40:43, ## NB: GitHub Archive glitch
      c('events', 'users') := NA
      ]
  ),
  
  ## NOTE! We only have geo/location information for users (from GH Torrent) going 
  ## back until 2017 and up to the end of 2019. Hence, we'll limit all queries
  ## that require a location-based field to 2017-2020 from here on out.
  
  # All countries separately ------------------------------------------------
  
  ## Get 2017--2020 activity data for all countries
  tar_target(
    countries,
    rbindlist(Map(
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
    ))[
        !is.na(country_code),
        location := country_code
      ][
        order(country_code, date)
      ]
  ),
  
  
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
  
  tar_target(
    countries_hi,
    rbindlist(Map(
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
        ))[
          ,
          ':=' (location = country_name, user_rank = user_rank)
          ]
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
    ))
  ),
  
  
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
  tar_target(
    lon,
    rbindlist(Map(
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
  ))
  ),
  
  
  # ** London (gender matched) ----------------------------------------------
  
  tar_target(
    lon_gender,
    rbindlist(lapply(
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
    ))
  ),
  
  # * New York --------------------------------------------------------------
  
  ## Get 2017--2020 (hourly) NYC data
  tar_target(
    nyc,
    rbindlist(Map(
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
    ))
  ),
  
  # ** NYC (gender matched) -------------------------------------------------
  
  ## Same as per the above, except this time matched to gender for as many users
  ## as possible
  tar_target(
    nyc_gender,
    rbindlist(lapply(
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
    ))
  ),
  
  
  # * San Francisco ----------------------------------------------------------
  
  ## Get 2017--2020 (hourly) San Francisco data
  tar_target(
    sfo,
    rbindlist(Map(
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
    ))
  ),
  
  # ** San Francisco (gender matched) ---------------------------------------
  
  ## Same as per the above, except this time matched to gender for as many users
  ## as possible
  tar_target(
    sfo_gender,
    rbindlist(lapply(
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
    ))
  ),
  
  # * Beijing ---------------------------------------------------------------
  
  ## Get 2017--2020 (hourly) BEI data
  tar_target(
    bei,
    rbindlist(Map(
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
    ))
  ),
  
  
  # * Bengaluru (Bangalore) -------------------------------------------------
  
  ## Get 2017--2020 (hourly) BLR data
  tar_target(
    blr,
    rbindlist(Map(
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
    ))
  ),
  
  # ** Bengalru (gender matched) --------------------------------------------
  
  tar_target(
    blr_gender,
    rbindlist(lapply(
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
    ))
  ),
  
  # * Seattle ---------------------------------------------------------------
  
  ## Get 2017--2020 (hourly) Seattle data
  tar_target(
    sea,
    rbindlist(Map(
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
    ))
  ),
  
  # ** Seattle (linkedin matched) -------------------------------------------
  
  ## Same as the above, but this time matched by (statistically-imputed) gender
  tar_target(
    sea_gender,
    rbindlist(lapply(
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
    ))
  ),
  
  # * Cities (all) ----------------------------------------------------------
  
  tar_target(cities, rbind(lon, nyc, sfo, bei, blr, sea, fill = TRUE)),
  
  
  # * Gender (all) ----------------------------------------------------------
  
  tar_target(
    gender,
    gender_prep(
      rbind(lon_gender, nyc_gender, sfo_gender, blr_gender, sea_gender)
    )
  ),
  
  # Orgs --------------------------------------------------------------------
  
  ## Note: These are intended to be used as case-studies in the SM.
  
  ## * Microsoft ----
  
  ## https://github.com/microsoft
  tar_target(
    msft,
    rbindlist(lapply(
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
    ))
  ),
  
  ## * Alibaba ----
  
  ## https://github.com/alibaba
  tar_target(
    baba,
    rbindlist(lapply(
      2017:2020, function(y) {
        get_gh_activity_year(
          billing = billing, 
          year = y, 
          hourly = TRUE,
          org_ids = c(alibaba = 1961952), 
          tz = 'Asia/Shanghai')
      }
    ))[, location := 'Beijing'] ## really, Hangzhou (but fine for Chinese lockdown matching)
  ),
  
  ## * UK Government Digital Service ----
  
  ## https://github.com/alphagov
  tar_target(
    agov,
    rbindlist(lapply(
      2017:2020, function(y) {
        get_gh_activity_year(
          billing = billing, 
          year = y, 
          hourly = TRUE,
          org_ids = c(alphagov = 596977),
          tz = 'Europe/London'
        )
      }
    ))[, location := 'London']
  ),
  
  ## * Combined orgs ----
  tar_target(
    orgs,
    rbind(
      merge(msft, lockdown_dates, by = 'location')[, location := 'Microsoft'],
      merge(baba, lockdown_dates, by = 'location')[, location := 'Alibaba'],
      merge(agov, lockdown_dates, by = 'location')[, location := 'UK government digital service'],
      fill = TRUE
    )[, c('users_tab', 'country_code', 'comment') := NULL]
  ),
  
  # Hobbyists ----------------------------------------------------------------
  
  ## Note: These are intended to be used as case-studies in the SM. I'll manually
  ## specify the lockdown date as the start of week 10 (2 Mar) to make plotting
  ## easier for the joined dataset further below.
  
  ## * Home Assistant ----
  
  # https://github.com/home-assistant
  tar_target(
    hasst,
    rbindlist(lapply(
      2017:2020, function(y) {
        get_gh_activity_year(
          billing = billing, 
          year = y, 
          hourly = TRUE,
          org_ids = c(hasst = 13844975),
          tz = 'America/New_York' # founder / lead dev is based in brooklyn
        )
      }
    ))[, location := 'Home Assistant']
  ),
  
  ## * KiCad ----
  
  # https://github.com/KiCad
  tar_target(
    kicad,
    rbindlist(lapply(
      2017:2020, function(y) {
        get_gh_activity_year(
          billing = billing, 
          year = y, 
          hourly = TRUE,
          org_ids = c(kicad = 3374914)
        )
      }
    ))[, location := 'KiCad']
  ),
  
  ## * Combined hobbyists ----
  tar_target(
    hobbyists,
    rbind(hasst, kicad)[, lockdown := as.IDate('2020-03-02')]
  ),
  
  
  # Plots -------------------------------------------------------------------  
  
  
  # * Activity map ----------------------------------------------------------
  
  tar_target(activity_map_plot, activity_map(countries)),
  # tar_target(
  #   activity_map_plot_ggsave,
  #   ggsave(
  #     here('figs/activity-map.pdf'),
  #     plot = activity_map_plot,
  #     width = 8, height = 5, device = cairo_pdf
  #   )
  # ),
  
  
  
  # * Time-series plots -----------------------------------------------------
  
  # ** Global ----
  tar_target(ts_global_events, ts_plot(g, bad_dates = bad_dates, measure = 'events')),
  
  tar_target(ts_global_users, ts_plot(g, bad_dates = bad_dates, measure = 'users')),
  
  # ** Highlighted countries ----
  # Note: We'll limit these to 2019-2020 and break them up into separate plots to 
  # conserve visual space...
  ## Events
  tar_target(
    ts_countries_events_1,
    ts_plot(
      countries_hi[year(date) %in% 2019:2020][
        location %in% sort(unique(countries_hi$location))[1:6]], 
      bad_dates = bad_dates, measure = 'events', 
      by_location = TRUE, 
      lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
      theme = 'void'
    )
  ),
  tar_target(
    ts_countries_events_2,
      ts_plot(
      countries_hi[year(date) %in% 2019:2020][
        location %in% sort(unique(countries_hi$location))[7:12]], 
      bad_dates = bad_dates, measure = 'events', 
      by_location = TRUE, 
      lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
      theme = 'void'
    )
  ),
  ## Productivity
  tar_target(
    ts_countries_productivity_1,
      ts_plot(
      countries_hi[year(date) %in% 2019:2020][
        location %in% sort(unique(countries_hi$location))[1:6]][
          week(date) %in% 2:50], 
      bad_dates = bad_dates, measure = 'productivity', 
      by_location = TRUE, 
      lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
      theme = 'void'
    )
  ),
  tar_target(
    ts_countries_productivity_2,
    ts_plot(
      countries_hi[year(date) %in% 2019:2020][
        location %in% sort(unique(countries_hi$location))[7:12]][
          week(date) %in% 2:50],  
      bad_dates = bad_dates, measure = 'productivity', 
      by_location = TRUE, 
      lockdown_dates = as.IDate(c('2020-02-10', '2020-03-02')),
      theme = 'void'
    )
  ),
  
  # * Hourly plots (cities) -------------------------------------------------
  
  tar_target(hourly_plot_lon, hourly_plot(cities[location=="London"])),
  # hourly_plot_lon_ggsave = ggsave(here("figs/hourly-lon.pdf"),
  #                                 plot = hourly_plot_lon,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  tar_target(hourly_plot_nyc, hourly_plot(cities[location=="New York, NY"])),
  # hourly_plot_nyc_ggsave = ggsave(here("figs/hourly-nyc.pdf"),
  #                                 plot = hourly_plot_nyc,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  tar_target(hourly_plot_sfo, hourly_plot(cities[location=="San Francisco, CA"])),
  # hourly_plot_sfo_ggsave = ggsave(here("figs/hourly-sfo.pdf"),
  #                                 plot = hourly_plot_sfo,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  tar_target(hourly_plot_bei, hourly_plot(cities[location=="Beijing"])),
  # hourly_plot_bei_ggsave = ggsave(here("figs/hourly-bei.pdf"),
  #                                 plot = hourly_plot_bei,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  tar_target(hourly_plot_blr, hourly_plot(cities[location=="Bengaluru"])),
  # hourly_plot_blr_ggsave = ggsave(here("figs/hourly-blr.pdf"),
  #                                 plot = hourly_plot_blr,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  tar_target(hourly_plot_sea, hourly_plot(cities[location=="Seattle, WA"])),
  # hourly_plot_sea_ggsave = ggsave(here("figs/hourly-sea.pdf"),
  #                                 plot = hourly_plot_sea,
  #                                 width = 8, height = 5, device = cairo_pdf),
  
  
  
  # * Proportion plots ------------------------------------------------------
  
  ## ** Global (prop = weekends, measure = both) ----
  tar_target(
    prop_global_wend,
    prop_plot(
      merge(g, lockdown_dates),
      prop = 'wend', measure = 'both',
      bad_dates = bad_dates,
      highlight_year = 2020:2023,
      treat_date2 = 10, ## Global treatment date
      ylim = c(0.15, 0.25),
      scales = 'free_y',
      labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_global_wend_ggsave = ggsave(
  #   here('figs/prop-global-wend.pdf'), 
  #   plot = prop_global_wend,
  #   width = 8, height = 5, device = cairo_pdf
  # ),
  ## ** Global (prop = weekends, measure = events) ----
  tar_target(
    prop_global_wend_events,
    prop_plot(
      # merge(g[year(date)<2021], lockdown_dates), ## For main text we exclude 2021+ data
      merge(g, lockdown_dates),
      prop = 'wend', measure = 'events',
      bad_dates = bad_dates,
      highlight_year = 2020:2023,
      treat_date2 = 10, ## Global treatment date
      ylim = c(0.15, 0.25),
      title = NULL, facet_title = NULL,
      scales = 'free_y',
      labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_global_wend_events_ggsave = ggsave(
  #   here('figs/prop-global-wend-events.pdf'), 
  #   plot = prop_global_wend_events,
  #   width = 8, height = 5, device = cairo_pdf
  # ),
  # tar_target(
  #   prop_global_wend_events21,
  #   prop_plot(
  #     merge(g, lockdown_dates), 
  #     prop = 'wend', measure = 'events',
  #     bad_dates = bad_dates,
  #     highlight_year = 2020:2022,
  #     treat_date2 = 10, ## Global treatment date
  #     ylim = c(0.15, 0.25),
  #     title = NULL, facet_title = NULL,
  #     scales = 'free_y',
  #     labeller = labeller(.multi_line=FALSE)
  #   )
  # ),
  # prop_global_wend_events21_ggsave = ggsave(
  #   here('figs/prop-global-wend-events21.pdf'), 
  #   plot = prop_global_wend_events21,
  #   width = 8, height = 5, device = cairo_pdf
  # ),
  ## ** Global (prop = weekends, measure = push events) ----
  tar_target(
    prop_global_wend_pushes,
    prop_plot(
      # merge(gpush[year(date)<2021], lockdown_dates),
      merge(gpush, lockdown_dates),
      prop = 'wend', measure = 'events',
      bad_dates = bad_dates,
      treat_date2 = 10, ## Global treatment date
      highlight_year = 2020:2023,
      # ylim = c(0.15, 0.25),
      title = NULL, facet_title = NULL,
      scales = 'free_y',
      labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_global_wend_pushes_ggsave = ggsave(
  #   here('figs/prop-global-wend-pushes.pdf'), 
  #   plot = prop_global_wend_pushes,
  #   width = 8, height = 5, device = cairo_pdf
  # ),
  
  
  ## ** Cities (prop = both, measure = events) ----
  tar_target(
    prop_cities_both_events,
    prop_plot(
      merge(cities, lockdown_dates, by = 'location'),
      prop = 'both', measure = 'events',
      bad_dates = bad_dates,
      # min_year = 2017, 
      title = NULL,
      treat_date2 = 10, ## Global treatment date
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_cities_both_events_ggsave = ggsave(
  #   here('figs/prop-cities-both-events.pdf'), 
  #   plot = prop_cities_both_events,
  #   width = 10, height = 8, device = cairo_pdf
  # ),
  
  ## ** Gender (prop = wend, measure = events) ----
  tar_target(
    prop_gender_wend_events,
    prop_plot(
      merge(gender, lockdown_dates), 
      prop = 'wend', measure = 'events', 
      bad_dates = bad_dates, #min_year = 2017,
      treat_date2 = 10, ## Global treatment date
      by_gender = TRUE, 
      title = NULL,
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_gender_wend_events_ggsave = ggsave(
  #   here('figs/prop-gender-wend-events.pdf'), 
  #   plot = prop_gender_wend_events,
  #   width = 10, height = 8, device = cairo_pdf
  # ),
  
  ## ** Gender (prop = ohrs, measure = events) ----
  tar_target(
    prop_gender_ohrs_events,
    prop_plot(
      merge(gender, lockdown_dates), 
      prop = 'ohrs', measure = 'events', 
      bad_dates = bad_dates, #min_year = 2017,
      treat_date2 = 10, ## Global treatment date
      by_gender = TRUE, 
      title = NULL,
      scales = 'free_y', ncol = 2, labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_gender_ohrs_events_ggsave = ggsave(
  #   here('figs/prop-gender-ohrs-events.pdf'), 
  #   plot = prop_gender_ohrs_events,
  #   width = 10, height = 8, device = cairo_pdf
  # ),
  
  ## ** Orgs ----
  
  tar_target(
    prop_orgs,
    prop_plot(
      orgs,
      prop = 'both', measure = 'events',
      bad_dates = bad_dates, 
      title = NULL,
      scales = 'free_y', 
      ncol = 2, 
      labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_orgs_ggsave = ggsave(
  #   here('figs/prop-orgs.pdf'), 
  #   plot = prop_orgs,
  #   width = 8, height = 6.4, device = cairo_pdf
  # ),
  
  ## ** Hobbyists ----
  
  tar_target(
    prop_hobbyists,
    prop_plot(
      hobbyists,
      prop = 'both', measure = 'events',
      bad_dates = bad_dates, 
      title = NULL,
      scales = 'free_y', 
      ncol = 2, 
      labeller = labeller(.multi_line=FALSE)
    )
  ),
  # prop_hobbyists_ggsave = ggsave(
  #   here('figs/prop-hobbyists.pdf'), 
  #   plot = prop_hobbyists,
  #   width = 8, height = 6.4, device = cairo_pdf
  # ),
  
  
  # Event-study regressions -------------------------------------------------
  
  tar_target(
    hols_prop, 
    holidays[
      ,
      .N,
      by = .(country_code, yr=year(date), wk=isoweek(date),
             wend=ifelse(wday(date) %in% c(1,7), 'hols_wend', 'hols_wk'))
    ] %>% 
    dcast(formula = ... + N ~ wend)
  ),
  
  # * Gender ----------------------------------------------------------------
  
  tar_target(
    gender_prop,
    collapse_prop(
      merge(gender, lockdown_dates),
      # prop = 'ohrs',
      measure = 'both',
      bad_dates = setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage
      # min_year = 2017,
      by_gender = TRUE,
      treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
    )[, lockdown_global := 10 ## We'll actually use the 'Global' week 10 date as the common lockdown treatment
    ][, ':=' (time_to_treatment = wk - lockdown, 
              time_to_treatment_global = wk - lockdown_global)
    ] %>%
      merge(hols_prop[, -'N'], all.x=TRUE, by = c('country_code', 'yr', 'wk'))  %>%
      .[is.na(hols_wend), hols_wend := 0] %>%
      .[is.na(hols_wk), hols_wk := 0]
  ),
  
  ## ** ES female ----
  tar_target(
    es_female,
    feols(
      events ~ i(time_to_treatment_global, treated, -1)  +
        hols_wk + hols_wend + bdates_wk + bdates_wend | 
        location + yr + time_to_treatment_global, 
      gender_prop[gender=='Female'],
      vcov = ~location^yr,
      split = ~prop
    )
  ),
  ## ** ES male ----
  tar_target(
    es_male,
    feols(
      events ~ i(time_to_treatment_global, treated, -1) +
        hols_wk + hols_wend + bdates_wk + bdates_wend | 
        location + yr + time_to_treatment_global, 
      gender_prop[gender=='Male'],
      vcov = ~location^yr,
      split = ~prop
    )
  ),
  ## ** Combined plot ----
  tar_target(
    es_gender_plot,
    ggiplot(
      list('Male' = es_male, 'Female' = es_female), 
      geom_style = 'ribbon',
      multi_style = "facet",
      xlab = 'Weeks until lockdown', 
      ylab = 'Effect on proportion of event activity',
      facet_args = list(labeller = labeller(.multi_line=FALSE)),
      theme = theme_es
    ) +
      scale_color_discrete_qualitative(palette = "Harmonic", aesthetics = c('colour', 'fill')) +
      labs(caption = 'Note: "Out-of-hours" defined as the period outside of 9 am to 6 pm.')
  ),
  
  
  # * Extra regs ------------------------------------------------------------
  
  tar_target(
    g_prop,
    collapse_prop(
      merge(g[year(date)<=2020], lockdown_dates),
      prop = 'wend',
      bad_dates = setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage
      treatment_window = -10:20 ## Event-study running from 10 weeks before lockdown 'til 20 weeks after
    )[, time_to_treatment := wk - lockdown][]
  ),
  
  ## ** ES global ----
  tar_target(
    es_global,
    feols(
      events ~ i(time_to_treatment, treated, -1) + bdates_wk + bdates_wend 
      | yr + time_to_treatment, 
      g_prop,
      vcov = ~yr
    )
  ),
  
  tar_target(
    cities_prop,
    collapse_prop(
      merge(cities, lockdown_dates)[, 
                                    country_code := fcase(location=='London', 'gb',
                                                          location=='Beijing', 'cn',
                                                          default = 'us')],
      measure = 'both',
      bad_dates =  setdiff(bad_dates, as.IDate('2020-06-10')), ## Latter was only a minor outage, 
      # min_year = 2017,
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
      .[is.na(hols_wk), hols_wk := 0]
  ),
  
  ## ** ES cities wend ----
  tar_target(
    es_cities_wend,
    feols(
      events ~ i(time_to_treatment, treated, -1) +
        hols_wk + hols_wend + bdates_wk + bdates_wend | 
        location + yr + time_to_treatment, 
      cities_prop[prop=='Weekends'],
      vcov = ~location^yr,
      fsplit = ~location
    )
  ),
  
  ## ** ES cities ohrs ----
  tar_target(
    es_cities_ohrs,
    feols(
      events ~ i(time_to_treatment, treated, -1) +
        hols_wk + hols_wend + bdates_wk + bdates_wend | 
        location + yr + time_to_treatment, 
      cities_prop[prop=='Out-of-hours'],
      vcov = ~location^yr,
      fsplit = ~location
    )
  ),
  
  ## ** Export tabs ----
  tar_target(
    etable_wend,
    etable(
      es_global, es_cities_wend, 
      dict = c(treated = "Treated", time_to_treatment = "Lockdown",
               events = "Events", yr = "Year", location = "Location"),
      drop = "[[:digit:]]{2}$|bdates|hols", 
      headers = list("^:_:Location" = c("Global", "Cities (all)", tail(names(es_cities_wend), -1))),
      se.row = FALSE,
      notes = c("Clustered standard errors by location--year in parentheses."),
      style.tex = style.tex(main = "aer"),
      file = "tabs/es-wend.tex", replace = TRUE
    )
  ),
  
  tar_target(
    etable_ohrs,
    etable(
      es_cities_ohrs, 
      dict = c(treated = "Treated", time_to_treatment = "Lockdown",
               events = "Events", yr = "Year", location = "Location"),
      drop = "[[:digit:]]{2}$|bdates|hols", 
      headers = list("^:_:Location" = c("Cities (all)", tail(names(es_cities_wend), -1))),
      se.row = FALSE,
      notes = c("Clustered standard errors by location--year in parentheses."),
      style.tex = style.tex(main = "aer"),
      file = "tabs/es-ohrs.tex", replace = TRUE
    )
  ),
  
  
  # Prophet -----------------------------------------------------------------
  
  ## ** Holidays ----
  
  ## Note: Prophet requires specifying holiday-weekend interactions manually
  ## https://github.com/facebook/prophet/issues/1157#issuecomment-539229937
  ## The date field must also be called "ds".
  
  tar_target(
    bad_dates_hols,
    CJ(
      ds = sort(c(bad_dates, as.IDate('2018-04-04'))),
      holiday  = 'GitHub down',
      country_code = unique(countries_hi$country_code)
    ),
  ),
  
  tar_target(
    hols,
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
      setorder(country_code, ds)
    ),
  
  ## ** Highlighted countries ----
  
  tar_target(
    prophet_co,
    rbindlist(lapply(
      split(countries_hi, countries_hi$country_code), 
      function(x) prophet_fc(
        x, 
        holidays = hols,
        train_cutoff = '2020-01-31', # One month into 2020 as anchor
        n.changepoints = 37, # One (potential) changepoint per month
        seasonality.mode = 'multiplicative',
        outliers = c(
          '2020-06-10', '2019-09-12',
          '2020-01-24', ## Only KR but enough of a pain that will remove for all
          '2019-12-23', '2019-12-30', '2018-12-30'
        ),
        level = 0.9
      )
    ))
  ),
  # write_prophet_co = write_fst(prophet_co, here('data/prophet-countries.fst')),
  
  
  ## *** Plots ----
  
  ## Difference percentage
  tar_target(
    prophet_co_plot_perc,
    prophet_plot(
      prophet_co[ds>='2019-09-01' & ds<='2020-07-30'], forecast = '2020-02-01',
      type = 'diffperc'
    )
  ),
  # prophet_co_plot_perc_ggsave = ggsave(
  #   here('figs/prophet-countries-perc.pdf'), 
  #   plot = prophet_co_plot_perc,
  #   width = 16, height = 9, device = cairo_pdf
  # ),
  tar_target(
    prophet_co_plot_perc_placebo,
      prophet_plot(
      prophet_co[ds>='2018-09-01' & ds<='2019-07-30'],
      type = 'diffperc'
    )
  ),
  # prophet_co_plot_perc_placebo_ggsave = ggsave(
  #   here('figs/prophet-countries-perc-placebo.pdf'), 
  #   plot = prophet_co_plot_perc_placebo,
  #   width = 16, height = 9, device = cairo_pdf
  # ),
  
  
  # ACS ---------------------------------------------------------------------
  
  tar_target(acs_wfh, read_acs_wfh(here("data/acs-wfh.csv"))),
  
  tar_target(acs_wfh_plot, plot_acs_wfh(acs_wfh))#,
  # acs_wfh_plot_ggsave = ggsave(
  #   here('figs/acs-wfh.pdf'), 
  #   plot = acs_wfh_plot,
  #   width = 8, height = 5, device = cairo_pdf
  # )

  
)

