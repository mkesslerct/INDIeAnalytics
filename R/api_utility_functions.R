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
      mutate(
        date_last_event = lubridate::ymd_hms(date_last_event),
        time_spent = as.double(time_spent),
        percentage = as.double(percentage)
      )
  }
  intermediate$last_event <-
    bind_rows(
      intermediate$last_event,
      new_events %>%
        select(
          url,
          user,
          date_last_event = date,
          time_spent,
          percentage
        )
    ) %>%
    arrange(date_last_event) %>%
    group_by(url, user) %>%
    summarise(
      date_last_event = tail(date_last_event, 1L),
      time_spent = tail(time_spent, 1L),
      percentage = tail(percentage, 1L)
    )


  ## visited_units is a df which contains url and its "current" title, which
  ## is the "latest" title (the title can be changed by the teacher along
  ## the event registration
  intermediate$visited_units <-
    bind_rows(
      intermediate$visited_units,
      events %>%
        arrange(date) %>%
        select(url, title)
    ) %>%
    group_by(url) %>%
    summarise(current_title = tail(title, 1L))
  ## users is a dataframe which contains users that have interacted with the platform
  intermediate$users <-  bind_rows(
    intermediate$users,
    events %>%
      select(user, email, name)
  ) %>%
    distinct(.)
  ## -------------------------------------------------------------------------
  ##
  ## visitors is a dataframe that contains, for each url, a list column
  ## have_visited, with the users' id that have visited this url, and a list
  ## column have_completed, with the users' id that have completed the url
  ## -------------------------------------------------------------------------
  max_percentages <- new_events %>%
    select(url, user, percentage) %>%
    group_by(url, user) %>%
    summarise(max_percentage = max(percentage))
  visitors_new <- max_percentages %>%
    group_by(url) %>%
    tidyr::nest() %>%
    mutate(
      have_visited = purrr::map(
        data,
        ~ .x$user
      ),
      have_completed = purrr::map(
        data,
        ~ filter(.x, max_percentage >= 100)$user
      )
    ) %>%
    select(- data)
  intermediate$visitors <- bind_rows(
    intermediate$visitors,
    visitors_new
  ) %>%
    group_by(url) %>%
    tidyr::nest(.) %>%
    mutate(
      have_visited = purrr::map(
        data,
        ~ unique(purrr::reduce(.x$have_visited, c))
      ),
      have_completed = purrr::map(
        data,
        ~ unique(purrr::reduce(.x$have_completed, c))
      )
    ) %>%
    select( - data)
  ## ------------------------------------------------------------------------------
  ##
  ## user_url_time a dataframe that contains, for each user, url and percentage
  ## the time spent (maximum time at that percentage from the first login in
  ## that url)  and the time_to_achieve (minimum time to achieve this
  ## percentage from the first login in that url)
  ##
  ## ------------------------------------------------------------------------------
  user_url_time_new <- new_events %>%
    select(url, user, date, time_spent, percentage) %>%
    group_by(user, url, percentage) %>%
    arrange(date) %>%
    summarise(
      time_to_achieve = min(time_spent),
      time_spent = max(time_spent),
    )
  if (!is.null(intermediate$user_url_time)){
    intermediate$user_url_time <-
      intermediate$user_url_time %>%
      mutate_at(
        vars(percentage:time_spent),
        .funs = funs(as.double)
      )
  }

  intermediate$user_url_time <-
    bind_rows(
      intermediate$user_url_time,
      user_url_time_new
    ) %>%
    group_by(user, url, percentage) %>%
    summarise(
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
      mutate(
        day = lubridate::ymd(day, tz = "UTC"),
        time_spent = as.double(time_spent)
      )
  }

  logins <- new_events %>%
    filter(type == "LoggedIn")
  ## We need to split duration_session between two days if the session begins a day
  ## and ends the day after
  ## We begin by discarding session where there is no need to split
  ## the duration of the session
  ##
  logins <- logins %>%
    mutate(
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
      filter(!requires_split) %>%
      ungroup() %>%
      select(
        user,
        date,
        duration_session
      ) %>%
      mutate(
        day = lubridate::floor_date(date, unit = "day"),
        duration = as.difftime(duration_session, units = "secs")
      )
  } else {
    daily_effort_no_split <- daily_effort_empty
  }

  if(sum(logins$requires_split) > 0){
    logins_with_split <- logins %>%
      filter(requires_split) %>%
      mutate(
        df_duration = purrr::map2(
          date,
          duration_session,
          ~ split_duration(.x, .y)
        )
      )
    ##Cogemos el Loggin de cada sesión, le calculamos su duración y se la
    ## asignamos a ese día
    daily_effort_with_split <- logins_with_split %>%
      ungroup() %>%
      select(
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
    group_by(user, day) %>%
    summarise(
      time_spent = as.numeric(sum(duration))
    ) %>%
    left_join(intermediate$users)
  intermediate$daily_effort <-
    bind_rows(
      intermediate$daily_effort,
      daily_effort
    ) %>%
    group_by(user, day) %>%
    summarise(
      time_spent = as.numeric(sum(time_spent))
    ) %>%
    left_join(intermediate$users)

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
    mutate(
      visitors = purrr::map_int(
        have_visited,
        ~ length(.x)
      ),
      finishers = purrr::map_int(
        have_completed,
        ~ length(.x)
      )
    ) %>%
    select( - have_visited, - have_completed) %>%
    left_join(intermediate$visited_units)

  ##
  user_url <- intermediate$user_url_time %>%
    group_by(user, url)%>%
    summarise(
      time_spent = max(time_spent),
      maxpercentage = max(percentage)
    )
  user_url_visited <- user_url %>%
    group_by(user) %>%
    summarise(
      number = n(),
      time_spent = sum(time_spent),
      type = "visited"
    )
  user_url_completed <- user_url %>%
    filter(maxpercentage >= 100) %>%
    group_by(user) %>%
    summarise(
      number = n(),
      time_spent = sum(time_spent),
      type = "completed"
    )
  aggregate$user_url_visited_completed <- bind_rows(
    user_url_visited,
    user_url_completed
  )
  ## ------------------------------------------------------------------------------
  ##
  ## time_user
  ##
  ## ------------------------------------------------------------------------------
  aggregate$user_url_time_percentage <- intermediate$user_url_time %>%
    group_by(user, url) %>%
    summarise(
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
    left_join(intermediate$visited_units, by = "url") %>%
    select(user, url, maxpercentage) %>%
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
    left_join(intermediate$visited_units, by = "url") %>%
    select(user, url, time_url) %>%
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
    filter(percentage > 0) %>%
    mutate(time_to_achieve = round(time_to_achieve / 60, 1)) %>%
    group_by(url, percentage) %>%
    summarise(
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
    group_by(url, variable) %>%
    mutate(
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
    mutate(variable2 = variable) %>%
    group_by(url, path, variable2) %>%
    tidyr::nest()

  paths <- purrr::map(objectives_quartiles_df$data, ~ prepare_path(.x))
  aggregate$list_paths <- list(paths = paths, url = objectives_quartiles_df$url)

  ## -------------------------------------------------------------------------
  aggregate$daily_effort <- intermediate$daily_effort
  ## -------------------------------------------------------------------------
  aggregate
}
