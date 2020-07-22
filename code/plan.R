plan =
  drake_plan(
    
    ## Get 2020 global data (to date)
    g2020janjune = rbindlist(lapply(1:6, function(m) get_gh_pushes(month = m))),
    
    ## Get daily 2019 global data
    g2019 = rbind(as.data.table(get_gh_pushes(year = 2019, month = 1)), 
                  as.data.table(get_gh_pushes(year = 2019))[year(date) != 2020]),
    
    ## Plot the difference between the global 2019 and 2020 data
    global_diff_plot = daily_diff_plot(rbind(g2019, g2020janjune), '2020-01-02', '2020-06-30'),
    
    ## Get daily 2020 Seattle data (to date)
    sea2020janjun = rbindlist(
      lapply(1:6,
             function(m) get_gh_pushes(year = 2020, month = m,
                                       city = 'Seattle', state = 'WA',
                                       tz = 'America/Los_Angeles'))
      )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    ## Get 2019 Seattle data
    sea2019 = rbind(
      as.data.table(get_gh_pushes(year = 2019, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles')), 
      as.data.table(get_gh_pushes(year = 2019, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles'))[year(date) != 2020]
      )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    
    ## Plot the difference between the global 2019 and 2020 data
    sea_daily_diff_plot = daily_diff_plot(rbind(sea2019, sea2020janjun), '2020-01-02', '2020-06-30'),
    
    ## Same as the above, but this time limited to the group of users who were 
    ## active during January 2019. In other words, we follow the exact same 
    ## users through and try to isolate the intensive margin for this cohort.
    sea2020janjun_cohort = rbindlist(
      lapply(1:6,
             function(m) get_gh_pushes(year = 2020, month = m,
                                       city = 'Seattle', state = 'WA',
                                       tz = 'America/Los_Angeles',
                                       users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'))
    )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    #
    sea2019_cohort = rbind(
      as.data.table(get_gh_pushes(year = 2019, month = 1, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901')), 
      as.data.table(get_gh_pushes(year = 2019, 
                                  city = 'Seattle', state = 'WA',
                                  tz = 'America/Los_Angeles',
                                  users_tab = 'mcd-lab.covgit.sea_users_ght1906_matched_gharch201901'))[year(date) != 2020]
    )[, .(pushes = sum(pushes)), by = .(date, location)], ## Need to aggregate again b/c of TZ overlaps
    #
    sea_daily_diff_plot_cohort = daily_diff_plot(
      rbind(sea2019_cohort, sea2020janjun_cohort)[, location:=paste(location, 'ght1906_matched_gharch201901')], 
      '2020-01-02', '2020-06-30')
    
    )
