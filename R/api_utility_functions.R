##' update_intermediate
##'
##' Updates the intermediate list that contains auxiliary objects.
##' The intermediate list  is used to produce the aggregate list.
##' It therefore needs to be updated combining the present intermediate object
##' with the new events that have been registered
##'
##' The \code{"new_events"} dataframe should have been processed using the
##' \code{"prepare_events"} function. The latter adds in particular
##' \code{"time_spent"}  and \code{"duration_session"}, possibly taking into
##' account that the session spans from a previous call to the API
##' The \code{"intermediate"} is a list with the following objects:
##' - \code{"last_event"}
##' - \code{"visited_units"}
##' - \code{"users"}
##' - \code{"visitors"}
##' - \code{"user_url_time"}
##' - \code{"daily_effort"}
##'
##'@param intermediate: a list with many objects.
##'@param new_events: a dataframe, see details below.
##'@return a list with the same structure than \code{intermediate} but where
##'the objects have been updated taking into account the information contained
##'in \code{"new_events"}
##'
update_intermediate <- function(intermediate, new_events){
  ## ------------------------------------------------------------------------------
  ##
  ## last_event is a dataframe that contains, for each url and user, the date of the
  ## last registered event, and the time spent by this user in this url at the date of
  ## the last registered event
  ##
  ## ------------------------------------------------------------------------------
  if (!is.null(intermediate$last_event)){
    intermediate$last_event <-
      intermediate$last_event %>%
      dplyr::mutate(
        date_last_event = lubridate::ymd_hms(date_last_event),
        time_spent = as.double(time_spent),
        percentage = as.double(percentage)
      )
  }
  intermediate$last_event <-
    dplyr::bind_rows(
      intermediate$last_event,
      new_events %>%
        dplyr::select(
          url,
          user,
          date_last_event = date,
          time_spent,
          percentage
        )
    ) %>%
    dplyr::arrange(date_last_event) %>%
    dplyr::group_by(url, user) %>%
    dplyr::summarise(
      date_last_event = tail(date_last_event, 1L),
      time_spent = tail(time_spent, 1L),
      percentage = tail(percentage, 1L)
    )


  ## visited_units is a df which contains url and its "current" title, which
  ## is the "latest" title (the title can be changed by the teacher along
  ## the event registration
  intermediate$visited_units <-
    dplyr::bind_rows(
      intermediate$visited_units,
      new_events %>%
        dplyr::arrange(date) %>%
        dplyr::select(url, title)
    ) %>%
    dplyr::group_by(url) %>%
    dplyr::summarise(current_title = tail(title, 1L))
  ## users is a dataframe which contains users that have interacted with the platform
  intermediate$users <-  dplyr::bind_rows(
    intermediate$users,
    new_events %>%
      dplyr::select(user, email, name)
  ) %>%
    dplyr::distinct(.)
  ## -------------------------------------------------------------------------
  ##
  ## visitors is a dataframe that contains, for each url, a list column
  ## have_visited, with the users' id that have visited this url, and a list
  ## column have_completed, with the users' id that have completed the url
  ## -------------------------------------------------------------------------
  max_percentages <- new_events %>%
    dplyr::select(url, user, percentage) %>%
    dplyr::group_by(url, user) %>%
    dplyr::summarise(max_percentage = max(percentage))
  visitors_new <- max_percentages %>%
    dplyr::group_by(url) %>%
    tidyr::nest() %>%
    dplyr::mutate(
      have_visited = purrr::map(
        data,
        ~ .x$user
      ),
      have_completed = purrr::map(
        data,
        ~ dplyr::filter(.x, max_percentage >= 100)$user
      )
    ) %>%
    dplyr::select(- data)
  intermediate$visitors <- dplyr::bind_rows(
    intermediate$visitors,
    visitors_new
  ) %>%
    dplyr::group_by(url) %>%
    tidyr::nest(.) %>%
    dplyr::mutate(
      have_visited = purrr::map(
        data,
        ~ unique(purrr::reduce(.x$have_visited, c))
      ),
      have_completed = purrr::map(
        data,
        ~ unique(purrr::reduce(.x$have_completed, c))
      )
    ) %>%
    dplyr::select( - data)
  ## ------------------------------------------------------------------------------
  ##
  ## user_url_time a dataframe that contains, for each user, url and percentage
  ## the time spent (maximum time at that percentage from the first login in
  ## that url)  and the time_to_achieve (minimum time to achieve this
  ## percentage from the first login in that url)
  ##
  ## ------------------------------------------------------------------------------
  user_url_time_new <- new_events %>%
    dplyr::select(url, user, date, time_spent, percentage) %>%
    dplyr::group_by(user, url, percentage) %>%
    dplyr::arrange(date) %>%
    dplyr::summarise(
      time_to_achieve = min(time_spent),
      time_spent = max(time_spent)
    )
  if (!is.null(intermediate$user_url_time)){
    intermediate$user_url_time <-
      intermediate$user_url_time %>%
      dplyr::mutate_at(
        dplyr::vars(percentage:time_spent),
        .funs = dplyr::funs(as.double)
      )
  }

  intermediate$user_url_time <-
    dplyr::bind_rows(
      intermediate$user_url_time,
      user_url_time_new
    ) %>%
    dplyr::group_by(user, url, percentage) %>%
    dplyr::summarise(
      time_to_achieve = min(time_to_achieve),
      time_spent = max(time_spent)
    )
  ## ------------------------------------------------------------------------
  ##
  ##   daily_effort: dataframe with, for each user, the time spent daily
  ##
  ## -------------------------------------------------------------------------
  ##       browser()
  if (!is.null(intermediate$daily_effort)){
    intermediate$daily_effort <-
      intermediate$daily_effort %>%
      dplyr::mutate(
        day = lubridate::ymd(day, tz = "UTC"),
        time_spent = as.double(time_spent)
      )
  }

  logins <- new_events %>%
    dplyr::filter(type == "LoggedIn")
  ## We need to split duration_session between two days if the session begins a day
  ## and ends the day after
  ## We begin by discarding session where there is no need to split
  ## the duration of the session
  ##
  logins <- logins %>%
    dplyr::mutate(
      requires_split = lubridate::floor_date(date, unit = "day") !=
        lubridate::floor_date(date + duration_session, unit = "day")
    )
  daily_effort_empty <- tibble::tibble(
    user = character(),
    date = as.Date(character()),
    session = integer(),
    duration_session = numeric(),
    day = as.Date(character()),
    duration = as.difftime(numeric(), units = "secs")
  )

  if (sum(!logins$requires_split) > 0){

    daily_effort_no_split <- logins %>%
      dplyr::filter(!requires_split) %>%
      dplyr::ungroup() %>%
      dplyr::select(
        user,
        date,
        duration_session
      ) %>%
      dplyr::mutate(
        day = lubridate::floor_date(date, unit = "day"),
        duration = as.difftime(duration_session, units = "secs")
      )
  } else {
    daily_effort_no_split <- daily_effort_empty
  }

  if(sum(logins$requires_split) > 0){
    logins_with_split <- logins %>%
      dplyr::filter(requires_split) %>%
      dplyr::mutate(
        df_duration = purrr::map2(
          date,
          duration_session,
          ~ split_duration(.x, .y)
        )
      )
    ##Cogemos el Loggin de cada sesión, le calculamos su duración y se la
    ## asignamos a ese día
    daily_effort_with_split <- logins_with_split %>%
      dplyr::ungroup() %>%
      dplyr::select(
        user,
        date,
        duration_session,
        df_duration
      ) %>%
      tidyr::unnest()
    ## we impose to work in seconds for further displaying
    units(daily_effort_with_split$duration) <- "secs"
  } else {
    daily_effort_with_split <- daily_effort_empty
  }
  daily_effort <-
    rbind(
      daily_effort_no_split,
      daily_effort_with_split
    ) %>%
    dplyr::group_by(user, day) %>%
    dplyr::summarise(
      time_spent = as.numeric(sum(duration))
    ) %>%
    dplyr::left_join(intermediate$users)
  intermediate$daily_effort <-
    dplyr::bind_rows(
      intermediate$daily_effort,
      daily_effort
    ) %>%
    dplyr::group_by(user, day) %>%
    dplyr::summarise(
      time_spent = as.numeric(sum(time_spent))
    ) %>%
    dplyr::left_join(intermediate$users)

  ##
  ## -------------------------------------------------------------------------
  ##

  intermediate
}

##' intermediate2aggregate
##'
##' Computes, from an \code{"intermediate"} list, a list \code{"agregate"}
##' which contains the objects that can be directly used for plotting in shiny
##' without further manipulation.
##'
##' The \code{"intermediate"} object is a list with the following objects:
##' - \code{"last_event"}
##' - \code{"visited_units"}
##' - \code{"users"}
##' - \code{"visitors"}
##' - \code{"user_url_time"}
##' - \code{"daily_effort"}
##' Those objects are intermediate objects which can be updated using the
##' information contained in new events, but must be transformed for plotting
##' Think for example that if a mean were to be plot, intermediate would
##' contain both the sum and the number of elements. These two quantities can
##' be updated upon arrival of new information, but the aggregated quantity
##' of interest is the mean.
##'
##'
##'@param intermediate: a list with many objects, see details
##'@return a list with the following objects:
##' - \code{"number_visited_units"}
##' - \code{"number_users"}
##' - \code{"users"}
##' - \code{"visited_completed_units"}
##' - \code{"user_url_visited_completed"}
##' - \code{"user_url_time_percentage"}
##' - \code{"percentage_user_wide"}
##' - \code{"time_user_wide"}
##' - \code{"objectives_quartiles"}
##' - \code{"list_paths"}
##' - \code{"daily_effort"}
##'
intermediate2aggregate <- function(intermediate){
  aggregate <- list()
  ##
  aggregate$number_visited_units <- length(intermediate$visited_units)
  ##
  aggregate$number_users <- length(intermediate$users$user)
  ## still to be done
  aggregate$unidades <- NULL
  ## users
  aggregate$users <- intermediate$users ## ojo,  no hay apellidos aquí.
  ##
  ## df with url, visitors, and finishers.
  aggregate$visited_completed_units <- intermediate$visitors %>%
    dplyr::mutate(
      visitors = purrr::map_int(
        have_visited,
        ~ length(.x)
      ),
      finishers = purrr::map_int(
        have_completed,
        ~ length(.x)
      )
    ) %>%
    dplyr::select( - have_visited, - have_completed) %>%
    dplyr::left_join(intermediate$visited_units)

  ##
  user_url <- intermediate$user_url_time %>%
    dplyr::group_by(user, url)%>%
    dplyr::summarise(
      time_spent = max(time_spent),
      maxpercentage = max(percentage)
    )
  user_url_visited <- user_url %>%
    dplyr::group_by(user) %>%
    dplyr::summarise(
      number = n(),
      time_spent = sum(time_spent),
      type = "visited"
    )
  user_url_completed <- user_url %>%
    dplyr::filter(maxpercentage >= 100) %>%
    dplyr::group_by(user) %>%
    dplyr::summarise(
      number = n(),
      time_spent = sum(time_spent),
      type = "completed"
    )
  aggregate$user_url_visited_completed <- dplyr::bind_rows(
    user_url_visited,
    user_url_completed
  )
  ## ------------------------------------------------------------------------------
  ##
  ## time_user
  ##
  ## ------------------------------------------------------------------------------
  aggregate$user_url_time_percentage <- intermediate$user_url_time %>%
    dplyr::group_by(user, url) %>%
    dplyr::summarise(
      maxpercentage = max(percentage),
      time_url = max(time_spent)
    )
  ## ------------------------------------------------------------------------------
  ##
  ## percentage_user_wide: wide df with as many columns as units url, using the current
  ## title
  ##
  ## ------------------------------------------------------------------------------
  lookup <- setNames(
    c("user", intermediate$visited_units$current_title),
    c("user", intermediate$visited_units$url)
  )
  aggregate$percentage_user_wide <-
    aggregate$user_url_time_percentage %>%
    dplyr::left_join(intermediate$visited_units, by = "url") %>%
    dplyr::select(user, url, maxpercentage) %>%
    tidyr::spread(
      key  = "url",
      value = "maxpercentage"
    )
  names(aggregate$percentage_user_wide) <- lookup[names(aggregate$percentage_user_wide)]


  ## ------------------------------------------------------------------------------
  ##
  ## time_user_wide: wide df with as many columns as units url, using the current
  ## title
  ##
  ## ------------------------------------------------------------------------------
  aggregate$time_user_wide <- aggregate$user_url_time_percentage %>%
    dplyr::left_join(intermediate$visited_units, by = "url") %>%
    dplyr::select(user, url, time_url) %>%
    tidyr::spread(
      key  = "url",
      value = "time_url"
    )
  names(aggregate$time_user_wide) <- lookup[names(aggregate$time_user_wide)]
  ## -------------------------------------------------------------------------
  ##
  ## objectives_quartiles: df
  ##
  ## -------------------------------------------------------------------------
  aggregate$objectives_quartiles <- intermediate$user_url_time %>%
    dplyr::filter(percentage > 0) %>%
    dplyr::mutate(time_to_achieve = round(time_to_achieve / 60, 1)) %>%
    dplyr::group_by(url, percentage) %>%
    dplyr::summarise(
      q1 = quantile(time_to_achieve, 0.25, na.rm = TRUE),
      q2 = quantile(time_to_achieve, 0.5, na.rm = TRUE),
      q3 = quantile(time_to_achieve, 0.75, na.rm = TRUE),
      n_q = sum(!is.na(time_to_achieve))
    )
  ## -------------------------------------------------------------------------
  ##
  ## list_paths.
  ##
  ## -------------------------------------------------------------------------
  objectives_quartiles.long <- aggregate$objectives_quartiles %>%
    tidyr::gather(key = "variable", value = "quartile", q1:q3 ) %>%
    dplyr::group_by(url, variable) %>%
    dplyr::mutate(
      path = c(TRUE, diff(quartile) < 0),
      path = cumsum(path)
    )
  ## l_df <- split(
  ##        objectives_quartiles.long,
  ##         list(
  ##            objectives_quartiles.long$url,
  ##            objectives_quartiles.long$variable,
  ##            objectives_quartiles.long$trozo)
  ## )
  objectives_quartiles_df <- objectives_quartiles.long %>%
    dplyr::mutate(variable2 = variable) %>%
    dplyr::group_by(url, path, variable2) %>%
    tidyr::nest()

  paths <- purrr::map(objectives_quartiles_df$data, ~ prepare_path(.x))
  aggregate$list_paths <- list(paths = paths, url = objectives_quartiles_df$url)

  ## -------------------------------------------------------------------------
  aggregate$daily_effort <- intermediate$daily_effort
  ## -------------------------------------------------------------------------
  aggregate
}
