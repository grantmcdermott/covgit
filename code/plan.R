plan =
  drake_plan(
    
    ## Countries (summarised by year)
    c19_summ = rbind(
      as.data.table(get_gh_activity_country(billing, year = 2019, month = 1)),
      as.data.table(get_gh_activity_country(billing, year = 2019))
      )[, lapply(.SD, sum), by = .(country_code)][order(-users)] %>%
      .[, year := 2019],
    
    c20_summ = rbind(
      as.data.table(get_gh_activity_country(billing, year = 2020, month = 1)),
      as.data.table(get_gh_activity_country(billing, year = 2020))
      )[, lapply(.SD, sum), by = .(country_code)][order(-users)] %>%
      .[, year := 2020],
    
    c_all_summ = rbind(c19_summ, c20_summ),
    
    ## Country by date
    c19 = rbind(
      as.data.table(get_gh_activity_country(
        billing, year = 2019, month = 1, excl_nulls = TRUE, by_date = TRUE
        )),
      as.data.table(get_gh_activity_country(
        billing, year = 2019, excl_nulls = TRUE, by_date = TRUE))
      ),
    
    c20 = rbind(
      as.data.table(get_gh_activity_country(
        billing, year = 2020, month = 1, excl_nulls = TRUE, by_date = TRUE
      )),
      as.data.table(get_gh_activity_country(
        billing, year = 2020, excl_nulls = TRUE, by_date = TRUE))
      ),
    
    c_all = rbind(c19, c20),
    
    write_countries_all = fwrite(c_all, here('data/countries-all.csv')),

# Global ------------------------------------------------------------------

    ## Get 2020 global event data
    g20 = rbind(
      as.data.table(get_gh_activity(billing, year = 2020, month = 1)), 
      as.data.table(get_gh_activity(billing, year = 2020))
      ),
    
    ## Get 2019 global event data
    g19 = rbind(
      as.data.table(get_gh_activity(billing, year = 2019, month = 1)), 
      as.data.table(get_gh_activity(billing, year = 2019))
      ),
    
    ## Join global data tables
    g = rbind(g19, g20),
    
    ## Write to disk
    write_global = fwrite(g, here('data/global.csv')),
    
    ## Plot the difference between the early 2019 and 2020 global data.
    ## Note that we start in mid-Feb to avoid the weird bump in user activity
    ## that occured in early Feb 2019.
    g_diff_plot_events = daily_diff_plot(
      g, y = 'events', start_date = '2020-02-15', end_date = '2020-05-31'
      ),
    g_diff_plot_users = daily_diff_plot(
      g, y = 'users', start_date = '2020-02-15', end_date = '2020-05-31'
      ),


# San Francisco ------------------------------------------------------------

    ## Get 2019 Seattle data
    sfo19 = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2019, month = 1, 
        city = 'San Francisco', state = 'CA',
        tz = 'America/Los_Angeles'
      )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2019, 
        city = 'San Francisco', state = 'CA',
        tz = 'America/Los_Angeles'
      ))
    )[, 
      .(events = sum(events), users = max(users)), 
      by = .(date, location, users_tab, event_type)], ## Need to aggregate Jan 31st TZ overlaps
    
    ## Get 2020 Seattle data
    sfo20 = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2020, month = 1, 
        city = 'San Francisco', state = 'CA',
        tz = 'America/Los_Angeles'
      )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2020, 
        city = 'San Francisco', state = 'CA',
        tz = 'America/Los_Angeles'
      ))
    )[, 
      .(events = sum(events), users = max(users)), 
      by = .(date, location, users_tab, event_type)], ## Need to aggregate Jan 31st TZ overlaps
    
    ## Join San Francisco data tables
    sfo = rbind(sfo19, sfo20),
    
    ## Write to disk
    write_sfo = fwrite(sfo, here('data/sfo.csv')),
    
    ## Plot the difference between the early 2019 and 2020 San Francisco data
    sfo_diff_plot_events = daily_diff_plot(
      sea, y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sfo_diff_plot_users = daily_diff_plot(
      sea, y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),

# Seattle -----------------------------------------------------------------
    
    ## Get 2019 Seattle data
    sea19 = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2019, month = 1, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles'
      )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2019, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles'
      ))
    )[, 
      .(events = sum(events), users = max(users)), 
      by = .(date, location, users_tab, event_type)], ## Need to aggregate Jan 31st TZ overlaps

    ## Get 2020 Seattle data
    sea20 = rbind(
      as.data.table(get_gh_activity(
          billing,
          year = 2020, month = 1, 
          city = 'Seattle', state = 'WA',
          tz = 'America/Los_Angeles'
          )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2020, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles'
        ))
      )[, 
        .(events = sum(events), users = max(users)), 
        by = .(date, location, users_tab, event_type)], ## Need to aggregate Jan 31st TZ overlaps
    
    ## Join Seattle data tables
    sea = rbind(sea19, sea20),
    
    ## Write to disk
    write_sea = fwrite(sea, here('data/sea.csv')),
    
    ## Plot the difference between the early 2019 and 2020 Seattle data
    sea_diff_plot_events = daily_diff_plot(
      sea, y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_diff_plot_users = daily_diff_plot(
      sea, y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    

# Seattle (Jan 2019 cohort) -----------------------------------------------

    ## Same as the above, but this time limited to the group of users who were 
    ## active during January 2019. In other words, we follow the exact same 
    ## users through and try to isolate the intensive margin for this cohort.
    sea19_cohort = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2019, month = 1, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'
        )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2019, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'
        ))
      )[, 
        .(events = sum(events), users = max(users)), 
        by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps

    sea20_cohort = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2020, month = 1, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'
        )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2020, 
        city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'
        ))
      )[, 
        .(events = sum(events), users = max(users)), 
        by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    # Join Seattle cohort data
    sea_cohort = rbind(sea19_cohort, sea20_cohort)[, location := paste(location, 'cohort')],
    
    ## Write to disk
    write_sea_cohort = fwrite(sea_cohort, here('data/sea-cohort.csv')),
    
    ## Diff plot
    sea_cohort_diff_plot_events = daily_diff_plot(
      sea_cohort, y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_cohort_diff_plot_users = daily_diff_plot(
      sea_cohort, y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      ),


# Seattle (matched linkedin) ----------------------------------------------

    ## As above, but this time on a subset of users matched to a LinkedIn profile.
    ## Allows us to categorise by age and gender.
    sea19_linkedin = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2019, month = 1, 
        # city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
        gender = TRUE, 
        age_buckets = c(30, 40, 50)
      )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2019,
        # city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
        gender = TRUE, 
        age_buckets = c(30, 40, 50)
      ))
    )[, 
      .(events = sum(events), users = max(users)), 
      by = .(date, gender, age, users_tab)], ## Need to aggregate again b/c of a few TZ overlaps
  
    sea20_linkedin = rbind(
      as.data.table(get_gh_activity(
        billing,
        year = 2020, month = 1, 
        # city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
        gender = TRUE, 
        age_buckets = c(30, 40, 50)
        )), 
      as.data.table(get_gh_activity(
        billing,
        year = 2020,
        # city = 'Seattle', state = 'WA',
        tz = 'America/Los_Angeles',
        users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
        gender = TRUE, 
        age_buckets = c(30, 40, 50)
        ))
      )[, 
      .(events = sum(events), users = max(users)), 
      by = .(date, gender, age, users_tab)], ## Need to aggregate again b/c of a few TZ overlaps
  
    # Join Seattle cohort data
    sea_linkedin = rbind(sea19_linkedin, sea20_linkedin)[, location := 'seattle_linkedin'],
    
    ## Write to disk
    write_sea_linkedin = fwrite(sea_linkedin, here('data/sea-linkedin.csv')),
    
    ## Diff plot
    sea_linkedin_diff_plot_events = daily_diff_plot(
      sea_linkedin, y = 'events', start_date = '2020-01-02', end_date = '2020-05-31'
      ),
    sea_linkedin_diff_plot_users = daily_diff_plot(
      sea_linkedin, y = 'users', start_date = '2020-01-02', end_date = '2020-05-31'
      )
  
  )
