# completeDT --------------------------------------------------------------

## Borrowed/adapted from here: https://stackoverflow.com/a/66523199/4115816

completeDT <- function(DT, cols, defs = NULL) {
  
  make_vals <- function(col) {
    if (is.factor(col)) factor(levels(col))
    if (inherits(col, 'Date')) seq(min(col), max(col), by = '1 day')
    else unique(col)
  }
  
  mDT = do.call(CJ, c(lapply(DT[, ..cols], make_vals), list(unique=TRUE)))
  res = DT[mDT, on=names(mDT)]
  if (length(defs)) {
    res[, 
        names(defs) := Map(replace, .SD, lapply(.SD, is.na), defs), 
        .SDcols=names(defs)] 
  }
  res[]
} 


# get_gh_activity -----------------------------------------------------------

#' Get counts of daily GitHub push (and commit) activity using Google BigQuery.
#'
#' BigQuery hosts several projects with relevant GitHub data, including from the
#' GHTorrent project (https://ghtorrent.org) and GH Archive 
#' (https://www.gharchive.org/). This function will automatically query the 
#' relevant project table(s) depending on the parameters provided (geographic
#' limitations, etc.)
#'
#' @param billing Your GCP project ID. Should be a character stringer. Required.
#' @param year An integer between 2017 and the current year. Defaults to 2020.
#' @param month An integer between 1 and 12. If no argument is provided
#'    the function will query data from the entire year.
#' @param day An integer between corresponding to the day of month. Ignored if
#'    the `month` argument is not provided.
#' @param by_country Logical. Should the results be grouped by country? May
#'    conflict with location-specific arguments below (e.g `city`), so caution
#'    should be used when combining the two. Defaults to FALSE.
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
#'    or hourly level? Default is FALSE.
#' @param event_type Character vector. Which event types should be aggregated?
#'    Valid elements include "Push", "Fork", "IssueComment", etc. For a full
#'    list, see: 
#'    https://docs.github.com/en/developers/webhooks-and-events/github-event-types.
#'    Leave blank to aggregate all event types.
#' @incl_commits Logical. Should commits be aggregated too (i.e. separately)? 
#'    Note that commit activity is stored differently to other event data in the
#'    BigQuery tables and querying them is expensive! See Examples. Default is
#'    FALSE.
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
#' @return A data.table of daily (hourly) event counts
#' @seealso [bigrquery::bigquery()] which this function wraps.
#' @export
#' @examples
#' billing = Sys.getenv("GCP_PROJECT_ID") ## Replace this with your project ID
#' 
#' ## Example 1: Get daily events for whole world in Jan 2020 (i.e. using all 
#' ## default arguments).
#' 
#' # Use dryrun = TRUE to get a sense of how expensive a query will be before
#' # actually running it.
#' get_gh_activity(billing, dryrun = TRUE)
#' 
#' # Including commits has a dramatic impact (costly to extract!)
#' get_gh_activity(billing, incl_commits = TRUE, dryrun = TRUE)
#' 
#' # Now we actually execute it (excluding commits...)
#' get_gh_activity(billing)
#' 
#' ## Example 2: Get Push and Issue Comment event data for Seattle, WA over May 
#' ## 2020. Because of the location, we'll convert the timestamp data from UTC
#' ## to local (i.e. PST) time. We'll also request hourly data instead of the 
#' ## default daily data.
#' get_gh_activity(billing, 
#'               month = 5, city = 'Seattle', state = 'WA', 
#'               event_type = c('Push', 'IssueComment')
#'               tz = 'America/Los_Angeles', hourly = TRUE)
#'                 
#' ## Example 3: Reference against a previously-created table of users. This
#' ## table includes information on both gender and age, so we'll use that to
#' ## return more granular information.
#' get_gh_activity(billing,
#'               year = 2020, month = 3, tz = 'America/Los_Angeles',
#'               users_tab = 'mcd-lab.covgit.sea_users_linkedin', 
#'               gender = TRUE, age_buckets = c(30, 40, 50))
#' 
#' @author Grant McDermott
#' 
get_gh_activity =
  function(
    billing = NULL,
    year = NULL, month = NULL, day = NULL,
    by_country = NULL,
    city = NULL, city_alias=NULL, 
    state = NULL, state_alias=NULL,
    tz = NULL,
    hourly = FALSE,
    event_type = NULL, incl_commits = FALSE,
    users_tab = NULL,
    gender = FALSE, 
    age = FALSE, age_buckets = NULL,
    verbose = FALSE,
    dryrun = FALSE,
    ...
    ) {
    
    if (is.null(billing)) stop("Please provide a GCP project ID for billing.")
    
    if (is.null(tz)) {
      tz = 'UTC'
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
    
    if (is.null(year)) {
      year = 2020
      if (is.null(month)) month = 1
      }
    if (!is.null(month)) {
      month = sprintf("%02d", month)
      if (!is.null(day)) day = sprintf("%02d", day)
      }
    
    gharchive_dataset = ifelse(!is.null(day), 
                               "day",
                               ifelse(!is.null(month), "month", "year"))
    
    gharchive_con = 
      DBI::dbConnect(
        bigrquery::bigquery(),
        project = "githubarchive",
        dataset = gharchive_dataset,
        billing = billing
        )
    
    query_tbl = paste0(year, month, day)
    
    ## Aggregate at daily or hourly level?
    tz_vars = paste0("DATE(created_at, '", tz, "') AS date")
    t_vars = "date"
    # t_vars_order = "date ASC"
    events_max = 150
    if (hourly) {
      tz_vars = paste0(tz_vars, ", EXTRACT(HOUR from DATETIME(created_at, '", tz, "')) AS hr")
      t_vars = paste0(t_vars, ", hr")
      events_max = 30
      # t_vars_order = "date ASC, hr ASC"
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
    
    grp_vars = paste0(t_vars, ga_vars)
    
    ## Are we aggregating by country?
    if (is.null(by_country)) by_country = FALSE
    if (by_country) {
      grp_vars = paste0('country_code, ', grp_vars)
      ga_range_vars = paste0(ga_range_vars, ', country_code')
      # tz_vars = paste0('country_code, ', tz_vars)
      }
    
    events_query =
      glue::glue_sql(
        "
        SELECT
          ", tz_vars, ",
          actor.login as actor_login,
          CAST(JSON_EXTRACT(payload, '$.size') AS INT64) AS commits
        FROM {`query_tbl`}
        WHERE EXTRACT(YEAR FROM DATE(created_at, '", tz, "')) = {year}",
        .con = gharchive_con
      )

    
    ## Which event types are we tracking? Default is all...
    if (!is.null(event_type)) {
      
      event_type = paste0(stringr::str_to_title(event_type), 'Event')
      
      ## Add in combined WHERE clause
      events_query = 
        glue::glue_sql(
          events_query, " AND type IN ({vals*})",
          vals = event_type,
          .con = gharchive_con)
      
    }
    ## Similarly, are we including commit "events" (expensive!). From a 
    ## query-construction perspective, only matters for right at the end of the 
    ## query, since this column will be ignored otherwise...
    e_vars = "SUM(events) AS events"
    if (incl_commits) {
      e_vars = paste0(e_vars, ",
                      SUM(commits) AS commits")
    }
        
    
    ## Location-specific users
    location_null = is.null(city) & is.null(state)
    
    ## We'll run a special sub query for specific users that are defined by
    ## a predefined table and/or location.
    if (!is.null(users_tab) | by_country | !location_null) {
      
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
          " , grp_vars, ", 
            actor_login, 
            COUNT(*) AS events,
            SUM(commits) AS commits
          FROM (
          ({events_query}) AS a
          INNER JOIN ({users_query}) AS b
          ON a.actor_login = b.login
          )
          GROUP BY ", grp_vars, ", actor_login
          HAVING COUNT(*) < ", events_max,
          .con = gharchive_con
          )
    
    } else {
      
      join_query = 
        glue::glue_sql(
          "
          SELECT 
          " , grp_vars, ", 
            actor_login, 
            COUNT(*) AS events,
            SUM(commits) AS commits
          FROM 
          ({events_query}) AS a
          GROUP BY ", grp_vars, ", actor_login
          HAVING COUNT(*) < ", events_max,
          .con = gharchive_con
        )
      
    }
    
    full_query = 
      glue::glue_sql(
        "
          SELECT 
          ", grp_vars, ",
          ", e_vars, ",
          COUNT(actor_login) AS users
          FROM ({join_query})
          GROUP BY ", grp_vars, "
          ORDER BY ", grp_vars,
        .con = gharchive_con
      )
    
    dry_q = gsub(paste0("`", query_tbl, "`"), 
                 paste("githubarchive", gharchive_dataset, query_tbl, sep = "."),
                 full_query)
    
    if (verbose) message(dry_q, "\n")
    
    if (dryrun) {
      
      qsize = suppressWarnings(bq_perform_query_dry_run(dry_q, billing = billing))
      tbytes = grepl("TB", qsize)
      # qnum = as.numeric(gsub(" TB| MB", "", qsize))
      # qnum = ifelse(tbytes, qnum, qnum/1e3)
      qcost = sprintf("$%.2f", qsize * 5 / 1e12)  ## $5 per TB
      message("Query will process: ", prettyunits::pretty_bytes(unclass(qsize)), 
              "\nEstimated cost: ", qcost, "\n")
      
    } else {
      
      ## Below query will give annoying warning about SQL to S4 class conversion, 
      ## we'd rather just suppress.
      activity_dt = suppressWarnings(DBI::dbGetQuery(gharchive_con, full_query))
      setDT(activity_dt)
      
      if (is.null(event_type)) {
        activity_dt$event_type = 'all'
      } else {
        activity_dt$event_type = paste(event_type, collapse = ', ')
      }
      
      if (!is.null(users_tab)) {
        activity_dt$users_tab = users_tab
        if (!location_null) activity_dt$location = location
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
          activity_dt$age = factor(age_buckets_char[activity_dt$age + 1],
                                   levels = age_buckets_char)
        }
      } else if (!by_country) {
        activity_dt$location = ifelse(location_null, 'Global', location)
      }
      
      ## Complete implicit missing values
      jnames = names(activity_dt[, !c('users', 'events')])
      activity_dt = completeDT(activity_dt, 
                               cols = jnames, 
                               defs = c(events = 0, users = 0))
      
      return(activity_dt)
    }
    
    DBI::dbDisconnect(gharchive_con)
    
  }



# get_gh_activity_year ----------------------------------------------------

## A lightweight wrapper around get_gh_activity() designed to take care of
## one major annoyance: Each githubarchive "year" table includes the wrong 
## January data. Which is to say that includes January data for the subsequent
## year, but not for the actual year. All arguments passed on to 
## get_gh_activity(), except for the one additional "location_add" argument that
## can be used to add location data for user tables that don't provide this
## information.
## Usage example:
## g = rbindlist(lapply(
##   2015:2020, function(y) {
##     get_gh_activity_year(billing = billing, year = y)
##   }
## ))
get_gh_activity_year =
  function (location_add = NULL, ...) {
    
    dots = list(...)
    
    DT = rbind(
      get_gh_activity(month = 1, ...), 
      get_gh_activity(...)[month(date)!=1]
    )
    
    scols = 'events'
    
    if (!is.null(dots$incl_commits)) {
      if (dots$incl_commits) {
        scols = c(scols, 'commits')
      }
    }
    
    if (!("location" %in% names(DT))) {
      DT$location = ifelse(!is.null(location_add), location_add, "unknown")
    }
    
    gcols = c('date', 'hr', 'country_code', 'location', 'users_tab', 'event_type')
    gcols = intersect(gcols, names(DT))
    
    DT[, 
       c(lapply(.SD, sum), list(users = max(users))),
       .SDcols = scols,
       by = gcols]
    
  }


# Proportion of weekend activity ------------------------------------------

prop_wends = 
  function(data, measure = c('events', 'users', 'both'),
           highlight_year = NULL, highlight_col = NULL, ylim = NULL,
           start_week = 2, end_week = 50, treat_line = 10) {
    
    mcols = match.arg(measure)
    if (mcols=='both') mcols = c('events', 'users')
    
    if (is.null(highlight_year)) highlight_year = 2020
    highlight_year = paste0(highlight_year)
    if (is.null(highlight_col)) highlight_col = 'dodgerblue'
    
    col_vals = c('2015' = 'grey15', '2016' = 'grey30', '2017' = 'grey45', 
                 '2018' = 'grey60', '2019' = 'grey75', '2020' = 'grey90')
    
    col_vals[highlight_year] = highlight_col
    
    if (is.null(data$location)) data$location = toupper(data$country_code)
    
    data = 
      data %>%
      .[, 
        lapply(.SD, sum),
        .SDcols = mcols,
        by = .(location, year(date), isoweek(date), wend = wday(date) %in% c(1, 7))] %>%
      .[, 
        c(list(wend = wend), lapply(.SD, prop.table)),
        .SDcols = mcols,
        by = .(location, isoweek, year)] %>%
      .[isoweek >= start_week & isoweek<=end_week] %>%
      .[(wend)] %>%
      melt(measure = mcols)
    data %>% 
      ggplot(aes(isoweek, value, col = as.factor(year))) + 
      geom_line() + 
      geom_line(data = data[year==highlight_year], col = highlight_col) +
      geom_vline(xintercept = treat_line, lty = 2) + 
      labs(title = 'Proportion of activity on weekends',
           x = 'Week of year', y = 'Proportion') +
      scale_y_percent(limits = ylim) +
      scale_colour_manual(values = col_vals) +
      theme(legend.position = 'bottom', legend.title = element_blank()) +
      facet_wrap(~ location + stringr::str_to_title(variable))
    
  }


# Proportion of activity outside normal office hours ----------------------

prop_whours = 
  function(data, measure = c('events', 'users', 'both'), 
           highlight_year = NULL, highlight_col = NULL, ylim = NULL,
           start_week = 2, end_week = 26, treat_line = 10) {
    
    mcols = match.arg(measure)
    if (mcols=='both') mcols = c('events', 'users')
    
    if (is.null(highlight_year)) highlight_year = 2020
    highlight_year = paste0(highlight_year)
    if (is.null(highlight_col)) highlight_col = 'dodgerblue'
    
    col_vals = c('2015' = 'grey15', '2016' = 'grey30', '2017' = 'grey45', 
                 '2018' = 'grey60', '2019' = 'grey75', '2020' = 'grey90')
    
    col_vals[highlight_year] = highlight_col
    
    if (is.null(data$location)) data$location = toupper(data$country_code)
    
    data %>%
      .[, 
        lapply(.SD, sum),
        .SDcols = mcols,
        by = .(location, year(date), isoweek(date), whours = hr %in% 9:18)] %>%
      .[, 
        c(list(whours = whours), lapply(.SD, prop.table)),
        .SDcols = mcols,
        by = .(location, isoweek, year)] %>%
      .[isoweek >= start_week & isoweek<=end_week] %>%
      .[!(whours)] %>% 
      melt(measure = mcols) %>%
      ggplot(aes(isoweek, value, col = as.factor(year))) + 
      geom_line() + 
      geom_vline(xintercept = treat_line, lty = 2) + 
      labs(title = 'Proportion of activity outside normal office hours',
           caption = 'Office hours defined as 9 am to 6 pm.',
           x = 'Week of year', y = 'Proportion') +
      scale_y_percent(limits = ylim) +
      scale_colour_manual(values = col_vals) +
      theme(legend.position = 'bottom', legend.title = element_blank()) +
      facet_wrap(~ location + stringr::str_to_title(variable))
    
  }

# Difference plot ---------------------------------------------------------

daily_diff_plot = 
  function(data, measure = c('events', 'users', 'both'), 
           start_date = '2020-01-05', end_date = '2020-05-30',
           treat_date = NULL) {
    mcols = match.arg(measure)
    if (mcols=='both') mcols = c('events', 'users')
    start_date = as.Date(start_date)
    end_date = as.Date(end_date)
    if(!is.null(treat_date)) treat_date = as.IDate(treat_date)
    ## Get the date offset for comparing year on year (i.e. match weekends with 
    ## weekends, etc.). Note that we take the median value to avoid weekday 
    ## discontinuities (i.e. start of new week in one year vs old week in 
    ## another).
    date_offset = 365 + median(wday(data$date) - wday(data$date+365)) 

    d = copy(data) %>%
      .[, c('date', 'location', ..mcols)] %>%
      melt(measure = mcols, variable = 'type', value = 'y') %>%
      .[, date_offset := date + date_offset] %>%
      melt(id.vars = c('location', 'type', 'y')) %>%
      .[value >= start_date & value <= end_date] %>%
      .[, .(y = sum(y)), by = .(location, type, variable, value)] %>% ## ICO any remaining duplicates
      dcast(location + type + value ~ variable, value.var = 'y') %>%
      .[, Difference := date - date_offset]
    d = 
      melt(d, id.vars = c('location', 'type', 'value'), value.name = 'y') %>%
      .[, grp := fifelse(variable=='date', 
                         paste(year(value)),  
                         fifelse(variable=='date_offset', 
                                 paste(year(value)-1), 
                                 paste(variable)))] %>%
      .[, pnl := factor(fifelse(variable=='Difference', 'diff', 'raw'), 
                        levels = c('raw', 'diff'))]
    
    col_vals = c('2015' = 'grey10', '2016' = 'grey20', '2017' = 'grey30', 
                 '2018' = 'grey40', '2019' = 'grey50', '2020' = 'grey60',
                 'Difference' = 'black')
    highlight_year = as.character(year(start_date))
    col_vals[highlight_year] = 'dodgerblue'
    
    p =
      ggplot(d, aes(value, y, col = grp, fill = grp, group = grp)) + 
      geom_line() +
      geom_area(data = d[pnl=='diff'], alpha = 0.3, show.legend = FALSE) +
      scale_x_date(date_breaks = '1 month', date_labels = '%b') +
      scale_y_continuous(labels = scales::comma) +
      scale_colour_manual(values = col_vals, aesthetics = c('colour', 'fill')) +
      labs(y = 'Daily value') +
      theme(
        axis.title.x = element_blank(),
        legend.title = element_blank(),
        legend.position = 'bottom'
      ) + 
      geom_vline(xintercept = treat_date, lty = 2) +
      facet_wrap(
        pnl ~ location + stringr::str_to_title(type), 
        scales = 'free_y', nrow = 2,
        labeller = labeller(pnl = function(x) gsub("diff|raw", "", x))
        )
    # p + ggsave(here('figs', paste0(suff, '-diff.png')), width = 8, height =5)
    # p + ggsave(here('figs/PDF', paste0(suff, '-diff.pdf')), width = 8, height =5, device = cairo_pdf)
    return(p)
  }
