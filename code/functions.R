# get_gh_pushes -----------------------------------------------------------

#' Get counts of daily GitHub push activity using Google BigQuery
#'
#' BigQuery hosts several projects with relevant GitHub data, including from the
#' GHTorrent project (https://ghtorrent.org) and GH Archive 
#' (https://www.gharchive.org/). This function will automatically query the 
#' relevant project table(s) depending on the parameters provided (geographic
#' limitations, etc.)
#'
#' @param year An integer between 2017 and 2020 (the default).
#' @param month An integer between 1 and 12. If no argument is provided
#'    the function will query data from the entire year.
#' @param city A character string, e.g. 'San Francisco' describing the 
#'    particular geographic area of interest. Ignore this argument if you'd like
#'    to query data from the entire globe.
#' @param city_alias A character string, e.g. 'SF'. Only used if the
#'   `city` argument is also provided.
#' @param state A character string, e.g. 'CA' describing the particular 
#'    geographic area of interest.Ignore this argument if you'd like to query
#'    data from the entire globe.
#' @param state_alias A character string, e.g. 'California'. Only used if the
#'   `state` argument is also provided.
#' @param tz A character string indicating the appropriate timezone for your
#'    location of interest, e.g. 'America/Los_Angeles'. If none is
#'    provided then the query defaults to 'UTC'.
#' @param hourly Logical. Should the data be aggregated at the daily (default)
#'    or hourly level?
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

#' get_gh_pushes()
#' @author Grant McDermott
get_gh_pushes =
  function(year=NULL, 
           month=NULL, 
           city=NULL, city_alias=NULL, 
           state=NULL, state_alias=NULL,
           tz=NULL,
           hourly=FALSE) {
    
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
    
    if (is.null(year)) {year = 2020}
    if (is.null(month) & year == 2020) {month = 1} ## Don't have all the data for 2020 yet
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
    pushes_max = 250
    if (hourly) {
      tz_vars = paste0(tz_vars, ", EXTRACT(HOUR from DATETIME(created_at, '", tz, "')) AS hr")
      t_vars = paste0(t_vars, ", hr")
      pushes_max = 30
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
    
    if (!location_null) {
      
      location = ifelse(is.null(state), city, ifelse(is.null(city), state, paste0(city, ", ", state)))
      
      users_query =
        glue::glue_sql(
          "SELECT login
          FROM `ghtorrentmysql1906.MySQL1906.users`
          WHERE `location` = '", location, "'"
          )
      
      if(!is.null(city)) {
        users_query = glue::glue_sql(users_query," OR `city` = '", city, "'")
        if(!is.null(city_alias)) {
          users_query = glue::glue_sql(users_query," OR `city` = '", city_alias, "'")
        }
      }
      if (!is.null(state)) {
        users_query = glue::glue_sql(users_query, " OR `state` = '", state, "'")
        if(!is.null(state_alias)) {
          users_query = glue::glue_sql(users_query," OR `state` = '", state_alias, "'")
        }
      }
      
      message("Identifying GitHub users in ", location, " from GHTorrent users database...\n")
      
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
            SUM(pushes) AS pushes
          FROM ({join_query})
          GROUP BY date, hr
          ORDER BY date ASC, hr ASC
          ",
          .con = gharchive_con
        )
      
    } else {
      
      full_query = 
        glue::glue_sql(
          "
          SELECT 
            ", t_vars, ",
            SUM(pushes) AS pushes
          FROM ({pushes_query})
          GROUP BY date, hr
          ORDER BY date ASC, hr ASC
          ",
          .con = gharchive_con
        )
    }
    
    message("Running main query for daily push activity for the period ", query_tbl, ".\n")
    
    ## Below query will give annoying warning about SQL to S4 class conversion, 
    ## we'd rather just suppress.
    daily_pushes = suppressWarnings(DBI::dbGetQuery(gharchive_con, full_query))
    daily_pushes$location = ifelse(location_null, 'Global', location)
    
    return(daily_pushes)
    
    DBI::dbDisconnect(gharchive_con)
    
  }


# Difference plot ---------------------------------------------------------

diff_plot = 
  function(data, start_date, end_date) {
    start_date = as.Date(start_date)
    end_date = as.Date(end_date)
    location = unique(data$location)
    d = 
      copy(data) %>%
      .[, location := NULL] %>%
      .[, date_offset := date + 365] %>%
      .[, date_offset := date + 365 + wday(date) - wday(date_offset)] %>%
      melt(id.vars = 'pushes') %>%
      .[value >= start_date & value <= end_date] %>%
      dcast(value ~ variable, value.var = 'pushes') %>%
      .[, diff := date - date_offset]
    # ggplot(d, aes(value, diff)) + 
    #   geom_line() +
    #   scale_x_date(date_labels = '%b %y') +
    #   scale_y_continuous(labels = scales::comma) +
    #   labs(y = 'Difference') +
    #   theme(axis.title.x = element_blank())
    d = 
      melt(d, id.vars = 'value', value.name = 'pushes') %>%
      .[, grp := fifelse(variable=='date', 
                         paste(year(value)),  
                         fifelse(variable=='date_offset', 
                                 paste(year(value)-1), 
                                 paste(variable)))] %>%
      .[, pnl := factor(fifelse(variable=='diff', 'diff', 'main'), levels = c('main', 'diff'))]
    p =
      ggplot(d, aes(value, pushes, col = grp, group = grp)) + 
      geom_line() +
      scale_x_date(date_labels = '%b %y') +
      scale_y_continuous(labels = scales::comma) +
      labs(y = 'Pushes') +
      theme(axis.title.x = element_blank(), legend.title = element_blank()) + 
      facet_wrap(~pnl, ncol = 1, scales = 'free_y')
    p + ggsave(here('figs', paste0(tolower(location), '-diff.png')), width = 8, height =5)
    p + ggsave(here('figs/PDF', paste0(tolower(location), '-diff.pdf')), width = 8, height =5, device = cairo_pdf)
  }
