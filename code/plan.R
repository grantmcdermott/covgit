plan =
  drake_plan(

# Global ------------------------------------------------------------------

    ## Get 2021 global data (to date)
    seq21 = setdiff(1:(which(month.abb==months(Sys.Date(), abbreviate = TRUE))-1),
                    0), 
    
    g21 = rbindlist(lapply(seq21, function(m) get_gh_pushes(year = 2021, month = m))),
    
    ## Get 2020 global data
    g20 = rbind(as.data.table(get_gh_pushes(year = 2020, month = 1)), 
                as.data.table(get_gh_pushes(year = 2020))[year(date) != 2021]),
    
    ## Get 2019 global data
    g19 = rbind(as.data.table(get_gh_pushes(year = 2019, month = 1)), 
                as.data.table(get_gh_pushes(year = 2019))[year(date) != 2020]),
    
    ## Join global data tables
    g = rbind(g19, g20, g21),
    
    ## Write to disk
    fwrite_global = fwrite(g, here('data/global.csv')),
    
    ## Plot the difference between the global 2019 and 2020 data
    g_diff_plot = daily_diff_plot(g, '2020-01-02', '2020-12-31'),


# Seattle -----------------------------------------------------------------

    ## Get daily 2021 Seattle data (to date)
    sea21 = rbindlist(
      lapply(seq21,
             function(m) get_gh_pushes(year = 2021, month = m,
                                       city = 'Seattle', state = 'WA',
                                       tz = 'America/Los_Angeles'))
      )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    ## Get 2020 Seattle data
    sea20 = rbind(
      as.data.table(get_gh_pushes(year = 2020, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles')), 
      as.data.table(get_gh_pushes(year = 2020, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles'))[year(date) != 2021]
    )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    ## Get 2019 Seattle data
    sea19 = rbind(
      as.data.table(get_gh_pushes(year = 2019, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles')), 
      as.data.table(get_gh_pushes(year = 2019, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles'))[year(date) != 2020]
      )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    ## Join Seattle data tables
    sea = rbind(sea19, sea20, sea21),
    
    ## Write to disk
    fwrite_sea = fwrite(sea, here('data/sea.csv')),
    
    ## Plot the difference between the Seattle 2019 and 2020 data
    sea_diff_plot = daily_diff_plot(sea, '2020-01-02', '2020-12-31'),


# Seattle (Jan 2019 cohort) -----------------------------------------------

    ## Same as the above, but this time limited to the group of users who were 
    ## active during January 2019. In other words, we follow the exact same 
    ## users through and try to isolate the intensive margin for this cohort.
    sea20_cohort = rbind(
      as.data.table(get_gh_pushes(year = 2020, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901')), 
      as.data.table(get_gh_pushes(year = 2020, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'))[year(date) != 2021]
    )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    sea19_cohort = rbind(
      as.data.table(get_gh_pushes(year = 2019, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901')), 
      as.data.table(get_gh_pushes(year = 2019, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'))[year(date) != 2020]
    )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    # Join Seattle cohort data
    sea_cohort = rbind(sea19_cohort, sea20_cohort)[, location := paste(location, 'cohort')],
    
    ## Write to disk
    fwrite_sea_cohort = fwrite(sea, here('data/sea-cohort.csv')),
    
    ## Diff plot
    sea_diff_plot_cohort = daily_diff_plot(sea_cohort, '2020-01-02', '2020-12-31')
    
    )
