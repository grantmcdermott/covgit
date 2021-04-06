# get_gh_pushes -----------------------------------------------------------

#' Get counts of daily GitHub push activity using Google BigQuery
#'
#' BigQuery hosts several projects with relevant GitHub data, including from the
#' GHTorrent project (https://ghtorrent.org) and GH Archive 
#' (https://www.gharchive.org/). This function will automatically query the 
#' relevant project table(s) depending on the parameters provided (geographic
#' limitations, etc.)
#'
#' @param year An integer between 2017 and the current year (the default).
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
#' @param gender Character string, e.g. 'male' or 'female'. Only used if the
#'    `users_tab` argument is not NULL.
#' @param age Integer vector, e.g. 20:29 or c(20, 29) denoting the upper and
#'    lower bounds on user age. Passed internally to `range()`, so a single
#'    integer will also work. Only used if the `users_tab` argument is not NULL.
#' @param dryrun Logical. Should the query be executed? (Default FALSE means 
#'    yes.) If switched to TRUE, then the full SQL quert string will be printed
#'    to screen.
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
#' ## Get daily pushes for whole world in Jan 2020 (all defaults)
#' get_gh_pushes()
#' ## Get hourly pushes for Seattle, WA over May 2020, making sure to convert
#' ## timestamp data from UTC to local (i.e. PST) time.
#' get_gh_pushes_testing(month = 5, city = 'Seattle', 
#'                       state = 'WA', tz = 'America/Los_Angeles', 
#'                       hourly = TRUE)
#' @author Grant McDermott
get_gh_pushes =
  function(year=NULL, 
           month=NULL, 
           city=NULL, city_alias=NULL, 
           state=NULL, state_alias=NULL,
           tz=NULL,
           hourly=FALSE,
           users_tab=NULL,
           gender=NULL, age=NULL,
           dryrun=FALSE) {
    
    if (is.null(tz)) {
      tz='UTC'
      message('No timezone provided. All dates and times defaulting to UTC.')
    } else {
      ## Check that given tz at least matches one of R's built-in list pairs
      if (tz %in% OlsonNames()) {
        message('Note: Dates and times will be converted to timezone: ', tz)
      } else {
        stop("Unexpected timezone: ", tz, 
             "\nTry picking one from `OlsonNames()` (print it in your R console).")
      }
    }
    
    if (is.null(year)) {year = data.table::year(Sys.Date())}
    if (is.null(month) & year == data.table::year(Sys.Date())) {month = 1} ## Won't have all data for current year
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
    pushes_max = 250
    if (hourly) {
      tz_vars = paste0(tz_vars, ", EXTRACT(HOUR from DATETIME(created_at, '", tz, "')) AS hr")
      t_vars = paste0(t_vars, ", hr")
      pushes_max = 30
      t_vars_order = "date ASC, hr ASC"
    }
    
    pushes_query =
      glue::glue_sql(
        "
        SELECT
          ", tz_vars, ",
          actor.login as actor_login,
          COUNT(*) AS pushes
        FROM {`query_tbl`}
        WHERE type = 'PushEvent'
        GROUP BY ", t_vars, ", actor_login
        HAVING COUNT(*) < ", pushes_max,
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
          "SELECT login 
          FROM `", users_tab, "`",
          .con = DBI::ANSI() ## https://github.com/tidyverse/glue/issues/120
          )
      
      ## Users tab WHERE condition
      if (!location_null | !is.null(gender) | !is.null(age)) {
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
        
        ## Gender WHERE component
        if (!is.null(gender)) {
          if (where_string!="WHERE ") {
            where_string =
              glue(where_string, "
                   AND ")
          }
          where_string = glue(where_string, "gender = '", gender, "'")
        }
        ## End Gender WHERE component
        
        ## Age WHERE component
        if (!is.null(age)) {
          age = range(age)
          if (where_string!="WHERE ") {
            where_string =
              glue(where_string, "
                   AND ")
          }
          where_string = glue(where_string, "age BETWEEN ", age[1], " AND ", age[2])
        }
        ## End Age WHERE component
       
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
          SELECT ", t_vars, ", actor_login, pushes
          FROM (
          ({pushes_query}) AS a
          INNER JOIN ({users_query}) AS b
          ON a.actor_login = b.login
          )
          ",
          .con = gharchive_con
          )
      
      full_query = 
        glue::glue_sql(
          "
          SELECT 
          ", t_vars, ",
          SUM(pushes) AS pushes,
          COUNT(actor_login) AS num_users
          FROM ({join_query})
          GROUP BY ", t_vars, "
          ORDER BY ", t_vars_order,
          .con = gharchive_con
          )
    
    } else {
      
      full_query = 
        glue::glue_sql(
          "
          SELECT 
            ", t_vars, ",
            SUM(pushes) AS pushes,
            COUNT(actor_login) AS num_users
          FROM ({pushes_query})
          GROUP BY ", t_vars, "
          ORDER BY ", t_vars_order,
          .con = gharchive_con
        )
    }
    
    message("Running main query for daily push activity for the period ", query_tbl, ".\n")
    
    if (dryrun) {
      print(full_query)
    } else {
      ## Below query will give annoying warning about SQL to S4 class conversion, 
      ## we'd rather just suppress.
      pushes_df = suppressWarnings(DBI::dbGetQuery(gharchive_con, full_query))
      pushes_df$location = ifelse(location_null, 'Global', location)
      
      return(pushes_df)
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
