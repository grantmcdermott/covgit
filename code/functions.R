# %ni% --------------------------------------------------------------------

`%ni%` = Negate(`%in%`)

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


# Event-study plot theme --------------------------------------------------

theme_es = 
  theme_classic() + 
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), 
        legend.position = "none", 
        plot.title = element_blank(),
        text = element_text(family = hrbrthemes::font_rc),
        strip.background = element_rect(colour = "white", fill = "white"),
        strip.text = element_text(colour = "black", size = 11))


# Buffer dates ------------------------------------------------------------


## Simple function for adding a buffer around dates, e.g. where GitHub was down
buffer_dates = function(dates, window = -1:1) {
  dates = as.IDate(dates) 
  bdates = sort(as.IDate(sapply(dates, `+`, window)))
  return(bdates)
}


# get_gh_activity --------------------------------------------------------

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
#' @param by_login Logical. Should we aggregate the results at the level of 
#'    individual actors (i.e. login IDs)? Defaults to FALSE, which means that
#'    the results will be aggregated at the location or table level (e.g. daily
#'    or hourly use by city). If this argument is changed to TRUE, it will 
#'    potentially lead to a *much*, *much* larger dataset that have to be 
#'    downloaded. (Prohibitively large in some cases.) Caution should hence be 
#'    used before applying it.
#' @param by_country Logical. Should the results be grouped by country? May
#'    conflict with location-specific arguments below (e.g `city`), so caution
#'    should be used when combining the two. Defaults to FALSE, unless 
#'    `country_code` is defined in which case defaults to TRUE. 
#' @param city A character string, e.g. 'San Francisco' denoting the 
#'    particular geographic area of interest. Ignore this argument if you'd like
#'    to query data from the entire globe.
#' @param city_alias A character string, e.g. 'SF'. Only used if the
#'   `city` argument is also provided.
#' @param state A character string, e.g. 'CA' denoting the particular 
#'    geographic area of interest. Ignore this argument if you'd like to query
#'    data from the entire globe.
#' @param state_alias A character string, e.g. 'California'. Only used if the
#'   `state` argument is also provided.
#' @param country_code A character string denoting an ISO alpha-2 country code
#'    of interest (e.g. 'US' or 'CN'). See: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2.
#'    This argument is generally not needed unless you want to extract 
#'    information for an entire country as a whole.
#' @param geo_ringer A character string that will act as a catch-all "ringer"
#'    clause for additional geo-location subsetting. Should be an intelligible
#'    SQL WHERE argument like "state!='ME'". This can be useful if you want to
#'    ensure that you exclude places that share the same name as your target
#'    area of interest, e.g. Portland, OR versus Portland, ME.
#' @param tz A character string indicating the appropriate timezone for your
#'    location of interest, e.g. 'America/Los_Angeles'. If none is
#'    provided then the query defaults to 'UTC'.
#' @param repo_ids Numeric. Optional vector of repo IDs (corresponding to the 
#'    "repo.id" field in the GHArchive tables) for additional subsetting by 
#'    certain repos.
#' @param actor_ids Numeric. Optional vector of actor IDs (corresponding to the 
#'    "actor.id" field in the GHArchive tables) for additional subsetting by 
#'    certain actors.
#' @param org_ids Numeric. Optional vector of repo IDs (corresponding to the 
#'    "org.id" field in the GHArchive tables) for additional subsetting by 
#'    certain organizations.
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
#' @param age_buckets Integer vector, e.g. `c(30, 40, 50)` denoting the age
#'    "buckets" that the results will be classified according to. E.g. The 
#'    previous vector will generate four age groups: <30, 31-39, 40-49, >=50.
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
    by_login = FALSE,
    by_country = NULL,
    city = NULL, city_alias=NULL, 
    state = NULL, state_alias=NULL,
    country_code=NULL,
    geo_ringer=NULL,
    tz = NULL,
    repo_ids = NULL,
    actor_ids = NULL,
    org_ids = NULL,
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
    if (gender) ga_vars = paste0(ga_vars, ", gender")
    ga_range_vars = ga_vars
    if (age | !is.null(age_buckets)) {
      if (!is.null(age_buckets)) {
        age_buckets = sort(age_buckets) 
        age_range_buckets = paste0("RANGE_BUCKET(age, [", 
                             paste(age_buckets, collapse = ", "), 
                             "])")
        ga_range_vars = paste0(ga_range_vars, ", ", age_range_buckets," AS age")
      } else {
        ga_range_vars = paste0(ga_range_vars, ", age")
      }
      ga_vars = paste0(ga_vars, ", age")
      }
    
    grp_vars = paste0(t_vars, ga_vars)
    
    ## Are we aggregating by country?
    if (!is.null(country_code)) by_country = TRUE
    if (is.null(by_country)) by_country = FALSE
    if (by_country) {
      grp_vars = paste0('country_code, ', grp_vars)
      ga_range_vars = paste0(ga_range_vars, ', country_code')
      # tz_vars = paste0('country_code, ', tz_vars)
    }
    
    grp_vars_final = grp_vars
    ## Are we aggregating at the level of individual actors?
    if (by_login) {
      grp_vars_final = paste0('actor_login, ', grp_vars)
    }
    
    ## Event data query from GH Archive
    
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

    ## Are we limiting to specific repos, actors, or orgs?
    ## Add in combined WHERE clauses if so...
    if (!is.null(repo_ids)) {
      events_query = 
        glue::glue_sql(
          events_query, " AND repo.id IN ({vals*})",
          vals = repo_ids,
          .con = gharchive_con)
    }
    
    if (!is.null(actor_ids)) {
      events_query = 
        glue::glue_sql(
          events_query, " AND actor.id IN ({vals*})",
          vals = actor_ids,
          .con = gharchive_con)
    }
    
    if (!is.null(org_ids)) {
      events_query = 
        glue::glue_sql(
          events_query, " AND org.id IN ({vals*})",
          vals = org_ids,
          .con = gharchive_con)
    }
    
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
      if (!location_null | !is.null(country_code)) {
        where_string = "WHERE "
        
        ## Location WHERE component
        if (!location_null) {
          
          # location = ifelse(is.null(state), city, ifelse(is.null(city), state, paste0(city, ", ", state))) 
          location = 
            if (!is.null(city) && !is.null(state) ) {
              paste0(city, ", ", state)
            } else if (!is.null(city) && is.null(state)) {
              city
            } else if (is.null(city) && !is.null(state)) {
              state
            } else {
              ""
            }
          
          where_string = glue(where_string, " 
                              location = '", location, "'")
          
          if (!is.null(city)) {
            where_string = glue(where_string," OR city = '", city, "'")
            if (!is.null(city_alias)) {
              alias_string = glue(" OR city = '", city_alias, "'")
              if (!is.null(state)) {
                alias_string = glue(alias_string, " OR location = '", gsub(city, city_alias, location), "'")
              } 
              where_string = glue(where_string, alias_string)
            }
          }
          if (!is.null(state)) {
            ## Only filter by state on its own if no city is provided. Otherwise 
            ## will pull in (e.g.) all of CA activity when only desire San 
            ## Francisco, CA. Downside is that you may pull in some spurious cases 
            ## when two places share the same name (e.g. Portland, OR and Portland, 
            ## ME) but this can be filtered with the `geo_ringer` argument 
            ## below.
            if(is.null(city)) {
              where_string = glue(where_string, " OR state = '", state, "'")
              if(!is.null(state_alias)) {
                where_string = glue(where_string," OR state = '", state_alias, "'")
              }
            }
          }
        message("Identifying GitHub users in ", location, " from ", users_tab, "...\n")
        } else if (!is.null(country_code)) {
          country_code = tolower(country_code)
          where_string = glue(where_string, "country_code = '", country_code, "'")
          message("Identifying GitHub users in country ", toupper(country_code), " from ", users_tab, "...\n")
        }
        if (!is.null(geo_ringer)) {
          where_string = glue(gsub("^WHERE ", "WHERE (",  where_string), ") AND ", geo_ringer)
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
          ", grp_vars_final, ",
          ", e_vars, ",
          COUNT(actor_login) AS users
          FROM ({join_query})
          GROUP BY ", grp_vars_final, "
          ORDER BY ", grp_vars_final,
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
      
      ## Complete implicit missing values (only if not aggregating at individual
      ## level)
      if (!by_login) {
        jnames = names(activity_dt[, !c('users', 'events')])
        activity_dt = completeDT(activity_dt, 
                                 cols = jnames, 
                                 defs = c(events = 0, users = 0))
      }
      
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
    
    gcols = c('date', 'hr', 
              'actor_login', 'country_code', 
              'location', 'users_tab', 
              'event_type', 'gender', 'age')
    gcols = intersect(gcols, names(DT))
    
    DT[, 
       c(lapply(.SD, sum), list(users = max(users))),
       .SDcols = scols,
       by = gcols]
    
  }



# Times series plot -------------------------------------------------------

ts_plot = 
  function(data, 
           measure = c('events', 'users'),
           bad_dates = bad_dates, 
           buffer_bad_dates = TRUE,
           by_location = FALSE,
           lockdown_dates = NULL,
           theme = c('light', 'dark', 'void')) {
    
    measure = match.arg(measure)
    theme = match.arg(theme)
    strip_col = ifelse(theme %in% c('light', 'void'), 'black', 'white')
    
    if (buffer_bad_dates) bad_dates = buffer_dates(bad_dates)
    
    ## bad weeks
    bad_weeks = data.table(date = bad_dates, 
                           wk = isoweek(bad_dates), 
                           yr = year(bad_dates),
                           bweek = TRUE)[, .(yr, wk, bweek)]
    bad_weeks = unique(bad_weeks)
    
    ## weekly values
    wd_levels = c('Mon','Tue','Wed','Thu','Fri','Sat','Sun')
    d_wk = data[,
                lapply(.SD, sum), 
                .SDcols = measure,
                by = .(location, yr = year(date), wk = isoweek(date), 
                       wd = factor(weekdays(date, abbreviate = TRUE),
                                   levels=wd_levels))] 
    
    d_wk = merge(d_wk, bad_weeks, all.x = TRUE)[is.na(bweek)][, bweek := NULL][]
    
    d_wk = melt(d_wk, measure.vars = measure)
    
    cvals = c(sequential_hcl(7, palette = "Blues 3")[1:5+(theme=='dark')],
              rev(sequential_hcl(4, palette = "Burg")[2:3]))
    names(cvals) = levels(wd_levels)
    
    if (!is.null(lockdown_dates)) {
      lockdown_dates = data.table(date = lockdown_dates)[
        , ':=' (wk = isoweek(date), yr = year(date), lty = 1:.N)]
    }
    
    ggplot(d_wk, aes(wk, value/1e6)) +
      geom_line(aes(col=wd) ) +
      labs(x = 'Week of year', y = paste('Daily', measure, '(million)')) +
      {
        if (by_location) {
          facet_grid(location ~ yr, scales = 'free_y', switch = 'y')
        } else {
          facet_wrap(~yr, nrow = 1)
        }
      } +
      {
        if (!is.null(lockdown_dates)) {
          list(geom_vline(data = lockdown_dates,
                     aes(xintercept = wk, linetype = lty),
                     col = 'grey50'),
            scale_linetype_identity())
        }
      } +
      scale_color_manual(values = cvals) +
      { 
        if (theme=='light') {
          theme_ipsum_rc(plot_margin = margin(5, 5, 5, 5), grid = 'XY', axis_title_just = 'c')
        } else if (theme=='dark') {
          theme_modern_rc(plot_margin = margin(5, 5, 5, 5), grid = 'XY', axis_title_just = 'c')
        } else {
          theme_void(base_family = font_rc) +
            theme(strip.text = element_text(size = 11.5))
        }
      } +
      theme(
        legend.title = element_blank(),
        panel.spacing.x = unit(0.5, "lines"), 
        strip.text = element_text(hjust = 0.5, colour = strip_col)
      )
  }

# activity_map ------------------------------------------------------------

activity_map = 
  function(data) {
    gh = data[!is.na(country_code) & year(date) <= 2019,
              lapply(.SD, mean),
              .SDcols = c('events', 'users'),
              by = country_code]
    
    w = st_as_sf(rworldmap::countriesLow) %>%
      st_transform("+proj=eqearth +wktext")
    
    setDT(w)
    
    gh = merge(gh, w[, .(country_code = tolower(ISO_A2), geometry)])
    
    sphere = st_graticule(ndiscr = 10000, margin = 10e-6) %>%
      st_transform(crs = "+proj=eqearth +wktext") %>%
      st_union() %>%
      st_convex_hull()
    
    gh = st_as_sf(gh)
    
    ggplot(gh) +
      geom_sf(data = sphere, col = NA, fill = 'grey95') + # darker option: #556473 
      geom_sf(aes(fill = events), col = "white", lwd = 0.3) +
      # labs(title = "GitHub activity by country") +
      scale_fill_continuous_sequential(
        name = 'Average daily events (2015–2019)',
        palette = "Purple-Blue", 
        trans = "log", 
        breaks = scales::log_breaks(n = 5, base = 10),
        # labels = scales::comma,
        labels = scales::trans_format('log10', scales::math_format(10^.x)),
        guide = guide_colourbar(barwidth = 10, title.position = "top")
      ) +
      theme_void() +
      theme(
        text = element_text(family = "Roboto Condensed"),
        legend.position = 'bottom'
      )
  }

# gender_prep -------------------------------------------------------------

## Convenience function for dropping unisex cases and then labeling.
## Aside: Using ignore to avoid https://github.com/ropensci/drake/issues/578

gender_prep = function(data) {
  d = copy(data)
  d[, location := gsub(' \\(gender\\)', '', location)]
  d[, country_code := fcase(location=='Beijing',   'cn', ## Irrelevant for the gender DT
                            location=='Bengaluru', 'in', 
                            location=='London',    'gb', 
                            default =              'us')]
  d = d[ignore(gender)!=3]
  d$gender = factor(d$gender, labels = c('0' = 'Female', '1' = 'Male'))
  return(d)
}

# Collapse by wend/ohrs proportions ---------------------------------------

collapse_prop =
  function(data, 
           prop = c('both', 'wend', 'ohrs'), 
           work_hours = 9:18, excl_wends = FALSE, ## ohrs-specific args
           measure = c('both', 'events', 'users'), 
           by_gender = FALSE, 
           simp_loc = TRUE,
           treatment_window = NULL,
           min_year = NULL,
           bad_dates = NULL, buffer_bad_dates = TRUE,
           start_week = 2, end_week = 50,
           ...) {
    
    d = copy(data)[, date := as.IDate(date)]
    
    prop = match.arg(prop)
    if (prop=='both') prop = c('wend', 'ohrs')
    
    mcols = match.arg(measure)
    if (mcols=='both') mcols = c('events', 'users')
    
    req_cols = c('date', 'lockdown', mcols)
    if (by_gender) req_cols = c(req_cols, 'gender')
    
    
    miss_cols = !(req_cols %in% names(d))
    if (TRUE %in% miss_cols) {
      stop(paste('Expected columns: ', 
                 paste(req_cols, collapse = ', '), 
                 '\nBut the following are missing:', 
                 paste(req_cols[miss_cols], collapse = ', ')))
    }
    
    ## Extra date and calendar vars (only create if missing)
    if ('yr' %ni% names(d)) d[, yr := year(date)]
    if ('wk' %ni% names(d)) d[, wk := isoweek(date)]
    if (any(prop %ni% names(d)))  {
      if ('wend' %in% prop) d[, wend := wday(date) %in% c(1, 7)]
      if ('ohrs' %in% prop) d[, ohrs := hr %ni% work_hours]
    }
    
    d = d[wk>=start_week & wk <= end_week]
    
    if (excl_wends) {
      if (prop=='ohrs') {
        d = d[wday(date) %in% 2:6]
      } else {
        message("Can only exlude weekends if `prop` doesn't include 'wend'. Ignoring.\n")
      }
    }
    d = melt(d, measure.vars = prop, variable.name = 'prop', value.name = 'prop_val')
    
    if (length(prop)==2) {
      d[, prop := factor(prop, labels = c('wend' = 'Weekends', 'ohrs' = 'Out-of-hours'))]
    } else if (prop=='wend') {
      d[, prop := factor(prop, labels = c('wend' = 'Weekends'))]
    } else if (prop=='ohrs') {
      d[, prop := factor(prop, labels = c('ohrs' = 'Out-of-hours'))]
    }
    
    ## Min year for comparison group?
    if (!is.null(min_year)) {
      if (is.numeric(min_year)) {
        d = d[yr>=min_year] 
      } else {
        message('Minimum year is not numeric. Ignoring.\n')
      }
    } 
    
    d[, ':=' (lockdown = isoweek(lockdown), lockdown_yr = year(lockdown))]
    
    gvars = c('location', 'yr', 'wk', 'prop', 'prop_val', 'lockdown', 'lockdown_yr')
    if ('country_code' %in% names(d)) gvars = c('country_code', gvars)
    if (by_gender) gvars = c(gvars, 'gender')
    gvars2 = setdiff(gvars, c('prop_val'))
    
    setkeyv(d, gvars)
    
    d =
      d[, 
        lapply(.SD, sum, na.rm = TRUE),
        .SDcols = mcols,
        by = gvars
      ][,
        c(list(prop_val = prop_val), 
          lapply(.SD, prop.table)),
        .SDcols = mcols,
        by = gvars2
      ][(prop_val)
      ][, prop_val := NULL][]
    
    ## We need to account for bad dates at week/weekend level so that they don't
    ## distort things
    if (!is.null(bad_dates)) {
      bad_dates = as.IDate(bad_dates)
      if (buffer_bad_dates) bad_dates = buffer_dates(bad_dates)
      bad_weeks = data.table(date = bad_dates, yr = year(bad_dates),
                             wk = isoweek(bad_dates), wend = wday(bad_dates) %in% c(1,7), 
                             bdate = TRUE) %>%
        .[, .(bdate = sum(bdate)), by=.(yr, wk, wend)] %>%
        dcast(yr + wk ~ wend, sum, value.var = 'bdate') %>%
        setnames(c('FALSE', 'TRUE'), c('bdates_wk', 'bdates_wend'))
      d = merge(d, bad_weeks, all.x = TRUE, by = c('yr', 'wk'))
      d[is.na(bdates_wk), bdates_wk:=0][is.na(bdates_wend), bdates_wend:=0][]
    }
    
    ## Some treatment variables and helpers, depending on the regression spec.
    d[, time_to_treatment := wk - lockdown
    ][, treated := yr>=lockdown_yr
    ][, post := time_to_treatment>=0][]
    
    ## Limit treatment window?
    if (!is.null(treatment_window)) {
      if (is.numeric(treatment_window)) {
        d = d[time_to_treatment %in% treatment_window]
      } else {
        message('Treatment window is not numeric. Ignoring.\n')
      }
    }
    
    ## Simplify location entry by dropping state? Mostly for plotting aesthetics...
    if (simp_loc) {
      d[, location := gsub(',.*', '', location)][]
    }
    # if (is.null(data$location)) data$location = toupper(data$country_code)
    
    setorder(d, location, prop, yr, wk)
    
    return(d)
  }


# Proportions plot --------------------------------------------------------

prop_plot = 
  function(data, 
           prop = c('both', 'wend', 'ohrs'), 
           work_hours = 9:18,
           measure = c('both', 'events', 'users'), 
           by_gender = FALSE,
           simp_loc = TRUE,
           highlight_year = NULL, highlight_col = NULL, 
           ylim = NULL,
           start_week = 2, end_week = 50, 
           treat_date = NULL, treat_date2 = NULL,
           title = 'auto', facet_title = 'auto', caption = 'auto',
           scales = NULL, ncol = NULL, 
           labeller = 'label_value',
           ...) {
    
    prop = match.arg(prop)
    if (prop=='both') tcols = c('wend', 'ohrs')
    
    mcols = match.arg(measure)
    if (mcols=='both') mcols = c('events', 'users')
    
    data = collapse_prop(data, 
                         prop = prop, 
                         measure = measure, 
                         work_hours = work_hours,
                         by_gender = by_gender, 
                         simp_loc = simp_loc,
                         start_week = start_week, end_week = end_week,
                         ...)

    if (is.null(highlight_year)) highlight_year = 2020
    highlight_year = paste0(highlight_year)
    if (is.null(highlight_col)) highlight_col = '#E16A86'
    
    col_vals = highlight_col
    names(col_vals) = highlight_year
    col_vals = c('Recent years' = '#A4DDEF', 'Recent mean' = '#00A6CA', col_vals)
    lwd_vals = c(0.4, 0.7, 0.7); names(lwd_vals) = names(col_vals)
    
    gvars = c('location', 'prop', 'yr', 'wk')
    if (by_gender) gvars = c(gvars, 'gender')
    
    if (is.null(treat_date)) {
      treat_dates = data[, .(treat_date = first(lockdown)), by = location]
    }
    
    data = melt(data, measure = mcols)
    
    data_nhy_mean = data[yr!=highlight_year, 
                         .(value = mean(value), yr = first(yr)), 
                         by = setdiff(c(gvars, 'variable'), 'yr')]
    
    data[, col_grp := fifelse(yr==highlight_year, highlight_year, 'Recent years')]
    data_nhy_mean[, col_grp := 'Recent mean']
    
    title_auto = title ## for title adjustment along with facet vars below
    if (is.null(title) || title=='auto') {
      if (prop=='both') {
        title = 'Proportion of activity'
      } else if (prop=='wend') {
        title = 'Proportion of activity on weekends'
      } else {
        title = 'Proportion of activity outside normal office hours'
      }
    }
    if (caption=='auto') {
      if (prop %in% c('both', 'ohrs')) {
        start_hr = head(work_hours, 1)
        start_hr = ifelse(start_hr>12, paste(start_hr-12, 'pm'), paste(start_hr, 'am'))
        end_hr = tail(work_hours, 1)
        end_hr = ifelse(end_hr>12, paste(end_hr-12, 'pm'), paste(end_hr, 'am'))
        caption = paste0('Note: "Out-of-hours" defined as the period outside ', 
                         start_hr,' to ', end_hr, '.')
      } else {
        caption = NULL
      }
    }
    
    ## Facet vars
    facet_vars = vars(location)
    if (prop=='both') facet_vars = c(facet_vars, vars(prop))
    if (length(mcols)==2) facet_vars = c(facet_vars, vars(stringr::str_to_title(variable)))
    if (by_gender) facet_vars = c(facet_vars, vars(gender))
    
    # Extra title adjustment
    if (is.null(title_auto) || title_auto=='auto' && length(mcols)==1 && prop!='both') {
      noun = ifelse(mcols=='events', 'event activity', 'active users')
      if (prop=='wend') {
        title = paste('Proportion of', noun, 'occurring on weekends')
      } else {
        title = paste('Proportion of', noun, 'occurring out-of-hours')
      }
    }
    
    ylab = 'Proportion'
    if (is.null(title_auto)) {
      if (prop=='both') ylab = paste(ylab, 'of activity')
      if (prop=='wend') ylab = paste(ylab, 'of weekend activity')
      if (prop=='ohrs') ylab = paste(ylab, 'of out-of-hours activity')
      title = NULL
    }
    
    # Hack for theme_tufte and expanded y limits
    if (!is.null(ylim)) {
      xrange = range(0, data$wk)
      yrange = ylim
      ylim[1] = ylim[1] - (ylim[2]-ylim[1])/20 ## extra little gap btwn the axes
    }
    
    data %>% 
      ggplot(aes(wk, value, group = as.factor(yr), col = col_grp, lwd = col_grp)) + 
      geom_line() + 
      geom_line(data = data_nhy_mean) +
      geom_line(data = data[yr==highlight_year]) +
      {
        if (!is.null(treat_date)) {
          geom_vline(xintercept = treat_date, col = 'grey50', lty = 2) 
        } else {
          geom_vline(data = treat_dates, aes(xintercept = treat_date), col = 'grey50', lty = 2)
        }
      } +
      {
        if (!is.null(treat_date2)) {
          geom_vline(xintercept = treat_date2, col = 'grey50', lty = 1) 
        }
      } +
      labs(x = 'Week of year', y = ylab, title = title, caption = caption) +
      scale_y_percent(limits = ylim) +
      scale_colour_manual(values = col_vals) +
      scale_size_manual(values = lwd_vals) +
      facet_wrap(facet_vars, scales = scales, ncol = ncol, labeller = labeller) +
      # theme(legend.position = 'bottom', legend.title = element_blank()) +
      {
        if (!is.null(ylim)) {
          geom_rangeframe(data = data.frame(x = xrange, y = yrange), 
                          aes(x, y), inherit.aes = FALSE)
        } else {
          geom_rangeframe(colour='black', show.legend = FALSE)
        }
      } +
      coord_cartesian(clip="off") +
      theme_tufte(base_family = 'Roboto Condensed', base_size = 12) + 
      {
        if(is.null(facet_title)) theme(strip.text = element_blank())
      } +
      theme(legend.position = 'bottom', legend.title = element_blank())
    
  }


# Prophet forecast --------------------------------------------------------

prophet_fc =
  function(data, 
           holidays,
           ycol = c('events', 'users'),
           train_cutoff = '2019-12-31',
           outliers = NULL,
           level = 0.8) {
    
    ycol = match.arg(ycol)
    
    setnames(data, ycol, 'y')
    if ('ds' %ni% names(data)) setnames(data, 'date', 'ds')
    
    pred_dt = data[, .(ds, y, country_code, location)]
    if (!is.null(outliers)) {
      pred_dt[ds %in% as.IDate(outliers), y := NA]
    }
    cc = first(data$country_code)
    holidays = holidays[country_code==cc, .(ds, holiday, lower_window, upper_window)]        
    
    mod = 
      prophet(
        pred_dt[ds<=as.IDate(train_cutoff)], 
        holidays = holidays,
        yearly.seasonality = TRUE,
        daily.seasonality = FALSE,
        interval.width = level
      )
    
    fc = predict(mod, pred_dt, holidays = holidays)
    
    pred_dt$pred = fc$yhat
    pred_dt$lwr  = fc$yhat_lower
    pred_dt$upr  = fc$yhat_upper
    
    pred_cols = c('pred', 'lwr', 'upr')
    pred_dt[, paste0(pred_cols, '_dev') := lapply(.SD, function(x) y - x), .SDcols = pred_cols][]
    
    # Change the ycol name back
    setnames(pred_dt, 'y', ycol)
    
    return(pred_dt)
  }


# Prophet plot ------------------------------------------------------------

prophet_plot =
  function(data, 
           type = c("diff", "raw"),
           y = c('events', 'users'),
           forecast = NULL, forecast_labs_shift = 10,
           vlines = NULL
  ) {
    
    dat = copy(data)
    type = match.arg(type)
    if (type=='raw') {
      y = match.arg(y)
      if (y %ni% names(dat)) stop('Column `', y, '` not found in the provided data. ')
      dat = setnames(dat, y, 'yraw')
      dat[is.na(yraw), c('pred', 'lwr', 'upr') := NA]
      ylab = paste("GitHub", gsub("s$", "", y), "activity ('000)")
    } else {
      ylab = paste("Observed less predicted GitHub", gsub("s$", "", y), "activity ('000)")
    }
    
    gg = ggplot(dat, aes(x= ds))
    
    if (type=='diff') {
      gg = gg +
        geom_line(aes(y = pred_dev), col = '#7DB0DD') +
        geom_ribbon(aes(ymin = lwr_dev, ymax = upr_dev), alpha = 0.3, fill = '#7DB0DD') +
        geom_hline(yintercept = 0) 
    } else {
      gg = gg +
        geom_line(aes(y = yraw, col = 'Observed')) +
        geom_line(aes(y = pred, col = 'Predicted')) +
        geom_ribbon(
          aes(ymin=lwr, ymax = upr, fill = 'Predicted'), 
          alpha = 0.3, show.legend = FALSE
        ) +
        scale_colour_manual(
          values = c('Observed' = 'black', 'Predicted' = '#7DB0DD'),
          aesthetics = c('colour', 'fill')
        )
    }
    
    if (!is.null(forecast)) {
      gg = gg +
        geom_vline(xintercept = as.IDate(forecast), lty = 1, col = 'grey50') +
        annotate(
          "text", x = as.IDate(forecast)-forecast_labs_shift, y = Inf, 
          hjust=1,vjust=1, label = "Training\n⟵", size = 3, col = 'grey50'
        ) +
        annotate(
          "text", x = as.IDate(forecast)+forecast_labs_shift, y = Inf, 
          hjust=0,vjust=1, label = "Forecast\n⟶", size = 3, col = 'grey50'
        )
    }
    
    gg + 
      facet_wrap(~location, scales = 'free_y') + 
      geom_vline(xintercept = as.IDate(vlines), lty = 2, col = 'grey50') +
      scale_x_date(date_labels = "%b '%y") +
      scale_y_continuous(labels = function(x) x/1e3) +
      labs(x = 'Date', y = ylab) +
      theme(
        legend.position = 'bottom', legend.title = element_blank(),
        panel.grid.minor = element_blank(),
        strip.text.x = element_text(hjust = 0.5)
      ) 
  }
