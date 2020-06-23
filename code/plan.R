plan =
  drake_plan(
    ## Get 2020 global data (to date)
    g2020janmay = rbindlist(lapply(1:5, function(m) get_gh_pushes(month = m))),
    ## Get 2019 global data
    g2019 = rbind(as.data.table(get_gh_pushes(year = 2019, month = 1)), 
                  as.data.table(get_gh_pushes(year = 2019))[year(date) != 2020]),
    ## Plot the difference between the global 2019 and 2020 data
    global_diff_plot = diff_plot(rbind(g2019, g2020janmay), '2020-01-01', '2020-05-31'),
    ## Get 2020 Settle data (to date)
    sea2020janmay = rbindlist(lapply(1:5, function(m) get_gh_pushes(month = m, city = 'Seattle', state = 'WA'))),
    ## Get 2019 Seattle data
    sea2019 = rbind(as.data.table(get_gh_pushes(year = 2019, month = 1, city = 'Seattle', state = 'WA')), 
                  as.data.table(get_gh_pushes(year = 2019, city = 'Seattle', state = 'WA'))[year(date) != 2020]
                  ),
    ## Plot the difference between the global 2019 and 2020 data
    sea_diff_plot = diff_plot(rbind(sea2019, sea2020janmay), '2020-01-01', '2020-05-31'),
    )
