# get_gh_activity ---------------------------------------------------------

#' Get counts of daily GitHub push (and commit) activity using Google BigQuery.
#'
#' BigQuery hosts several projects with relevant GitHub data, including from the
#' GHTorrent project (https://ghtorrent.org) and GH Archive 
#' (https://www.gharchive.org/). This function will automatically query the 
#' relevant project table(s) depending on the parameters provided (geographic
#' limitations, etc.)
#'
#' @param year An integer between 2017 and the current year. Defaults to 2020.
#' @param month An integer between 1 and 12. If no argument is provided
#'    the function will query data from the entire year.
#' @param city A character string, e.g. 'San Francisco' describing the 
#'    particular geographic area of interest. Ignore this argument if you'd like
#'    to query data from the entire globe.
#' @param city_alias A character string, e.g. 'SF'. Only used if the
#'   `city` argument is also provided.
#' @param state A character string, e.g. 'CA' describing the particular 
#'    geographic area of interest. Ignore this argument if you'd like to query
#'    data from the entire globe.
#' @param state_alias A character string, e.g. 'California'. Only used if the
#'   `state` argument is also provided.
#' @param tz A character string indicating the appropriate timezone for your
#'    location of interest, e.g. 'America/Los_Angeles'. If none is
#'    provided then the query defaults to 'UTC'.
#' @param hourly Logical. Should the data be aggregated at the daily (default)
#'    or hourly level?
#' @param users_tab Character string denoting BigQuery table that will be used
#'    to geo-reference users. Note that the full project.database.table path
#'    must be provided and the table variables are expected to conform to
#'    particular standards (see Details). Defaults to 
#'    "ghtorrentmysql1906.MySQL1906.users".
#' @param gender Logical. Should the results be disaggregated by gender? Only 
#'    used if the `users_tab` argument is not NULL.
#' @param age Logical. Should the results be disaggregated by age? 
#'    Alternatively, users may provide the `age_buckets` argument instead (this is
#'    recommended). Only used if the `users_tab` argument is not NULL.
#' @param age_buckets Integer vector, e.g. `c(20, 30, 40)` denoting the age
#'    "buckets" that the results will be classified according to. E.g. The 
#'    previous vector will generate four age groups: <20, 21-29, 30-39, >=40.
#'    Supersedes `age` if both arguments provided, although similarly only used 
#'    if the `users_tab` argument is not NULL.
#' @param verbose Logical. If TRUE, then the full SQL query string will be 
#'    printed to screen. Default is FALSE.
#' @param dryrun Logical. If TRUE then the function will return an estimate of
#'    how much data will be processed (1 TB ~ $5). Default is FALSE, but highly
#'    recommended to turn on for first-time queries. See Details and Examples.
#' 
#' @details If location arguments like `city` and `state` are provided, the 
#'    function will attempt to geo-reference users and limit the query search
#'    accordingly. By default this geo-referencing is done against the GHTorrent 
#'    "users" table from June 2019 (Bigquery address: 
#'    ["ghtorrentmysql1906.MySQL1906.users"](https://console.cloud.google.com/bigquery?p=ghtorrentmysql1906&d=MySQL1906&t=users&page=table)).
#'    If a different, bespoke user table is provided via the `user_tab` 
#'    argument, then geo-referencing will instead be done against that. Note,
#'    however, that the function still expects the variables in this bespoke
#'    table to conform to the same schema as the default GHTorrent one. In
#'    particular, it expects (and will use) the following variables to match
#'    users: `login`, `state`, `city`, and `location`. You will need to ensure
#'    that your bespoke users table contains these variables.
#' @return A tibble of daily push events
#' @seealso [bigrquery::bigquery()] which this function wraps.
#' @export
#' @examples
#' ## Example 1: Get daily pushes for whole world in Jan 2021 (i.e. using all 
#' ## default arguments).
#' 
#' # Use dryrun = TRUE to get a sense of how expensive a query will be before
#' # actually running it.
#' get_gh_activity(dryrun = TRUE)
#' 
#' # Including commits has a dramatic impact (costly to extract!)
#' get_gh_activity(incl_commits = TRUE, dryrun = TRUE)
#' 
#' # Now we actually execute it (excluding commits...)
#' get_gh_activity()
#' 
#' ## Example 2: Get hourly pushes for Seattle, WA over May 2020, making sure 
#' ## that we convert the timestamp data from UTC to local (i.e. PST) time. 
#' ## We'll also request hourly data instead of the default daily data.
#' get_gh_activity(month = 5, city = 'Seattle', state = 'WA', 
#'                 tz = 'America/Los_Angeles', hourly = TRUE)
#'                 
#' ## Example 3: Reference against a previously-created table of users. This
#' ## table includes information on both gender and age, so we'll use that to
#' ## return more granular information.
#' get_gh_activity(year = 2020, month = 3, tz = 'America/Los_Angeles',
#'                 users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
#'                 gender = TRUE, age_buckets = c(20, 30, 40, 50),
#'                 incl_commits = TRUE,
#'                 dryrun = TRUE)
#' 
#' @author Grant McDermott
get_gh_activity =
  function(year=NULL, 
           month=NULL, 
           city=NULL, city_alias=NULL, 
           state=NULL, state_alias=NULL,
           tz=NULL,
           hourly=FALSE,
           incl_commits=FALSE,
           users_tab=NULL,
           gender=FALSE, 
           age=FALSE, age_buckets=NULL,
           verbose=FALSE,
           dryrun=FALSE) {
    
    if (is.null(tz)) {
      tz='UTC'
      message('No timezone provided. All dates and times defaulting to UTC.')
    } else {
      ## Check that given tz at least matches one of R's built-in list pairs
      if (tz %in% OlsonNames()) {
        message('Dates and times will be converted to timezone: ', tz)
      } else {
        stop("Unexpected timezone: ", tz, 
             "\nTry picking one from `OlsonNames()` (print it in your R console).")
      }
    }
    
    if (is.null(year)) {year = 2020}
    if (is.null(month)) {month = 1}
    if (!is.null(month)) {month=sprintf("%02d", month)}
    
    gharchive_dataset = ifelse(is.null(month), "year", "month")
    
    gharchive_con = 
      DBI::dbConnect(
        bigrquery::bigquery(),
        project = "githubarchive",
        dataset = gharchive_dataset,
        billing = billing_id
        )
    
    query_tbl = paste0(year, month)
    
    ## Aggregate at daily or hourly level?
    tz_vars = paste0("DATE(created_at, '", tz, "') AS date")
    t_vars = "date"
    t_vars_order = "date ASC"
    pushes_max = 100
    if (hourly) {
      tz_vars = paste0(tz_vars, ", EXTRACT(HOUR from DATETIME(created_at, '", tz, "')) AS hr")
      t_vars = paste0(t_vars, ", hr")
      pushes_max = 30
      t_vars_order = "date ASC, hr ASC"
    }
    
    ## Are we including gender and age variables?
    ga_vars = ''
    ga_range_vars = ''
    if (gender) ga_vars = paste0(ga_vars, ", gender")
    if (age | !is.null(age_buckets)) {
      if (!is.null(age_buckets)) {
        age_buckets = sort(age_buckets) 
        age_range_buckets = paste0("RANGE_BUCKET(age, [", 
                             paste(age_buckets, collapse = ", "), 
                             "])")
        ga_range_vars = paste0(ga_vars, ", ", age_range_buckets," AS age")
      } else {
        ga_range_vars = paste0(ga_vars, ", age")
      }
      ga_vars = paste0(ga_vars, ", age")
      }
    
    t_vars = paste0(t_vars, ga_vars)
    
    ## Which event vars are we tracking? Basically, are we including commit
    ## events (expensive!). From a query-construction perspective, only matters 
    ## for right at the end of the query, since will be ignored otherwise...
    e_vars = "SUM(pushes) AS pushes"
    if (incl_commits) {
      e_vars = paste0(e_vars, ",
                      SUM(commits) AS commits")
    }
    
    pushes_query =
      glue::glue_sql(
        "
        SELECT
          ", tz_vars, ",
          actor.login as actor_login,
          CAST(JSON_EXTRACT(payload, '$.size') AS INT64) AS commits
        FROM {`query_tbl`}
        WHERE type = 'PushEvent'",
        .con = gharchive_con
      )
    
    
    ## Location-specific users
    location_null = is.null(city) & is.null(state)
    
    ## We'll run a special sub query for specific users that are defined by
    ## a predefined table and/or location.
    if (!is.null(users_tab) | !location_null) {
      
      ## Use default GHTorrent June 2019 users table if none provided
      if (is.null(users_tab)) {users_tab = "ghtorrentmysql1906.MySQL1906.users"}
      
      users_query =
        glue::glue_sql(
          "SELECT login", ga_range_vars, "
          FROM `", users_tab, "`",
          .con = DBI::ANSI() ## https://github.com/tidyverse/glue/issues/120
          )
      
      ## Users tab WHERE condition
      if (!location_null) {
        where_string = "WHERE "
        
        ## Location WHERE component
        if (!location_null) {
          
          location = ifelse(is.null(state), city, ifelse(is.null(city), state, paste0(city, ", ", state))) 
          
          where_string = glue(where_string, " 
                              location = '", location, "'")
          
          if(!is.null(city)) {
            where_string = glue(where_string," OR city = '", city, "'")
            if(!is.null(city_alias)) {
              where_string = glue(where_string," OR city = '", city_alias, "'")
            }
          }
          if (!is.null(state)) {
            ## Only filter by state on its own if no city is provided. Otherwise 
            ## will pull in (e.g.) all of CA activity when only desire San 
            ## Francisco, CA. Downside is that you may pull in some spurious cases 
            ## when two places share the same name (e.g. Portland, OR and Portland, 
            ## ME) but this seems by far the lesser of two evils.
            if(is.null(city)) {
              where_string = glue(where_string, " OR state = '", state, "'")
              if(!is.null(state_alias)) {
                where_string = glue(where_string," OR state = '", state_alias, "'")
              }
            }
          }
        message("Identifying GitHub users in ", location, " from ", users_tab, "...\n")
        }
        ## End Location WHERE component

        ## Add in combined WHERE clause
        users_query = 
          glue::glue_sql(
            users_query, "
            ", where_string)
        
      }
      ## End of User tab WHERE condition
      
      join_query = 
        glue::glue_sql(
          "
          SELECT 
          " , t_vars, ", 
            actor_login, 
            COUNT(*) AS pushes,
            SUM(commits) AS commits
          FROM (
          ({pushes_query}) AS a
          INNER JOIN ({users_query}) AS b
          ON a.actor_login = b.login
          )
          GROUP BY ", t_vars, ", actor_login
          HAVING COUNT(*) < ", pushes_max,
          .con = gharchive_con
          )
    
    } else {
      
      join_query = 
        glue::glue_sql(
          "
          SELECT 
          " , t_vars, ", 
            actor_login, 
            COUNT(*) AS pushes,
            SUM(commits) AS commits
          FROM 
          ({pushes_query}) AS a
          GROUP BY ", t_vars, ", actor_login
          HAVING COUNT(*) < ", pushes_max,
          .con = gharchive_con
        )
      
    }
    
    full_query = 
      glue::glue_sql(
        "
          SELECT 
          ", t_vars, ",
          ", e_vars, ",
          COUNT(actor_login) AS num_users
          FROM ({join_query})
          GROUP BY ", t_vars, "
          ORDER BY ", t_vars_order,
        .con = gharchive_con
      )
    
    dry_q = gsub(query_tbl, 
                 paste("githubarchive", gharchive_dataset, query_tbl, sep = "."),
                 full_query)
    
    if (verbose) message(dry_q, "\n")
    
    if (dryrun) {
      
      qsize = suppressWarnings(bq_perform_query_dry_run(dry_q, billing = billing_id))
      tbytes = grepl("TB", qsize)
      # qnum = as.numeric(gsub(" TB| MB", "", qsize))
      # qnum = ifelse(tbytes, qnum, qnum/1e3)
      qcost = sprintf("$%.2f", qsize * 5 / 1e12)  ## $5 per TB
      message("Query will process: ", prettyunits::pretty_bytes(unclass(qsize)), 
              "\nEstimated cost: ", qcost, "\n")
      
    } else {
      
      message("Running main query for daily push activity for the period ", 
              query_tbl, ".\n")
      
      ## Below query will give annoying warning about SQL to S4 class conversion, 
      ## we'd rather just suppress.
      activity_df = suppressWarnings(DBI::dbGetQuery(gharchive_con, full_query))
      
      if (!is.null(users_tab)) {
        activity_df$users_tab = users_tab
        if (!location_null) activity_df$location = location
        if (!is.null(age_buckets)) {
          eb = length(age_buckets)+1
          age_buckets_char = c(paste(age_buckets), age_buckets[eb-1])
          age_buckets_char[1] = paste0("<", age_buckets_char[1])
          age_buckets_char[eb] = paste0(">=", age_buckets_char[eb])
          if (all(diff((1+1):(eb-1)) >= 0)) {
            for (i in (1+1):(eb-1)) {
              age_buckets_char[i] = paste0(age_buckets[i-1], "--", age_buckets[i]-1)
            }
          }
          activity_df$age = age_buckets_char[activity_df$age + 1]
        }
      } else {
        activity_df$location = ifelse(location_null, 'Global', location)
      }
      
      return(activity_df)
    }
    
    DBI::dbDisconnect(gharchive_con)
    
  }


# Difference plot ---------------------------------------------------------

daily_diff_plot = 
  function(data, start_date, end_date) {
    start_date = as.Date(start_date)
    end_date = as.Date(end_date)
    ## Get the date offset for comparing year on year (i.e. match weekends with 
    ## weekends, etc.). Note that we take the median value to avoid weekday 
    ## discontinuities (i.e. start of new week in one year vs old week in 
    ## another).
    date_offset = 365 + median(wday(data$date) - wday(data$date+365)) 
    suff = gsub("[[:punct:]][[:space:]]", "_", tolower(unique(data$location)))
    d = copy(data) %>%
      .[, ':=' (location = NULL, date_offset = date + date_offset)] %>%
      melt(id.vars = 'pushes') %>%
      .[value >= start_date & value <= end_date] %>%
      .[, .(pushes = sum(pushes)), by = .(variable, value)] %>% ## ICO any remaining duplicates
      dcast(value ~ variable, value.var = 'pushes') %>%
      .[, Difference := date - date_offset]
    d = 
      melt(d, id.vars = 'value', value.name = 'pushes') %>%
      .[, grp := fifelse(variable=='date', 
                         paste(year(value)),  
                         fifelse(variable=='date_offset', 
                                 paste(year(value)-1), 
                                 paste(variable)))] %>%
      .[, pnl := factor(fifelse(variable=='Difference', 'diff', 'main'), levels = c('main', 'diff'))]
    p =
      ggplot(d, aes(value, pushes, col = grp, fill = grp, group = grp)) + 
      geom_line() +
      geom_area(data = d[pnl=='diff'], alpha = 0.3, show.legend = FALSE) +
      scale_x_date(date_breaks = '1 month', date_labels = '%b') +
      scale_y_continuous(labels = scales::comma) +
      scale_colour_brewer(palette = 'Set2', aesthetics = c('colour', 'fill')) +
      labs(y = 'No. of pushes') +
      theme(
        axis.title.x = element_blank(),
        strip.text = element_blank(),
        legend.title = element_blank(),
        legend.position = 'bottom'
        ) + 
      facet_wrap(~pnl, ncol = 1, scales = 'free_y')
    p + ggsave(here('figs', paste0(suff, '-diff.png')), width = 8, height =5)
    p + ggsave(here('figs/PDF', paste0(suff, '-diff.pdf')), width = 8, height =5, device = cairo_pdf)
  }
