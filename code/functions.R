# get_gh_pushes -----------------------------------------------------------

#' Get counts of daily GitHub push activity using Google BigQuery
#'
#' BigQuery hosts several projects with relevant GitHub data, including from the
#' GHTorrent project (https://ghtorrent.org) and GH Archive 
#' (https://www.gharchive.org/). This function will automatically query the 
#' relevant project table(s) depending on the parameters provided (geographic
#' limitations, etc.)
#'
#' @param year An integer. Must be between 2017 and 2020 (the default).
#' @param month Optional. An integer between 1 and 12. If none is provided
#'    the function will query data from the entire year.
#' @param state Optional. A character string, e.g. 'NY'.
#' @param state_alias Optional. A character string, e.g. 'New York'.
#' @return A tibble of daily push events
#' @seealso [bigrquery::bigquery()] which this function wraps.
#' @export
#' @examples
#' bq_ght_push()
#' @author Grant McDermott
get_gh_pushes =
  function(year=NULL, month=NULL, state=NULL, state_alias=NULL) {
    
    if (is.null(year)) {year = 2020}
    if (is.null(month) & year == 2020) {month = 1} ## Don't have all the data for 2020 yet
    if (!is.null(month)) {
      month = paste0(month)
      if (stringr::str_count(month)==1) {month = paste0("0", month)}
    }
    
    gharchive_dataset = ifelse(is.null(month), "year", "month")
    
    gharchive_con = 
      DBI::dbConnect(
        bigrquery::bigquery(),
        project = "githubarchive",
        dataset = gharchive_dataset,
        billing = billing_id
      )
    
    query_tbl = paste0(year, month)
    
    pushes_query =
      glue::glue_sql(
        "
        SELECT * 
        FROM(
          SELECT
            DATE(created_at) AS date,
            actor.login as actor_login,
            COUNT(*) AS pushes
          FROM  {`query_tbl`}
          WHERE type = 'PushEvent'
          GROUP BY date, actor_login
          )
        WHERE pushes < 250
        ",
        .con = gharchive_con
      )
    
    if (!is.null(state)) {
      users_query =
        glue::glue_sql(
          "SELECT login
          FROM `ghtorrentmysql1906.MySQL1906.users`
          WHERE `state` = '", state, "'"
        )
      if(!is.null(state_alias)) {
        users_query = 
          glue::glue_sql(
            users_query,
            " OR `state` = '", state_alias, "'"
            )
      }
      message("Identifying GitHub users in ", state, " from GHTorrent users database...\n")
      
      
      join_query = 
        glue::glue_sql(
        "
        SELECT date, actor_login, pushes
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
            date,
            SUM(pushes) AS pushes
          FROM ({join_query})
          GROUP BY date
          ORDER BY date ASC
          ",
          .con = gharchive_con
        )
      
    } else {
      
      full_query = 
        glue::glue_sql(
          "
          SELECT 
            date,
            SUM(pushes) AS pushes
          FROM ({pushes_query})
          GROUP BY date
          ORDER BY date ASC
          ",
          .con = gharchive_con
          )
    }
    
    message("Running main query for daily push activity for the period ", query_tbl, ".\n")
    
    ## Below query will give annoying warning about SQL to S4 class conversion, 
    ## we'd rather just suppress.
    daily_pushes = suppressWarnings(DBI::dbGetQuery(gharchive_con, full_query))
    
    return(daily_pushes)
    
    DBI::dbDisconnect(gharchive_con)
    
  }
