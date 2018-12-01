split_duration <- function(date, duration){
  ## given a date (start of the session) and the duration of the session, the
  ## output is a tibble, with all the days the session spans over, and the
  ## corresponding duration of the session these days.
  ##
  days <- seq(
    from = lubridate::floor_date(date, unit = "day"),
    to = lubridate::floor_date(date + duration, unit = "day"),
    by = "day"
  )
  dates_auxiliary <- c(
    date,
    seq(
      from = lubridate::ceiling_date(date, unit = "day"),
      by = "day",
      length.out = (length(days) - 1)
    ),
    date + duration
  )
  tibble::tibble(day = days, duration = diff(dates_auxiliary))
}
##
## ----------------------------------------------------------------------------
##
## Prepares a list to send to the highcharts API, through hc_add_series_list
##
## -----------------------------------------------------------------------------
##
##
colors_q <- setNames(
  c("black", "blue", "green"),
  c("q1", "q2", "q3")
)
prepare_path <- function(df){
  ##    browser()
  list(
    data = purrr::pmap(
      list(df$quartile, df$percentage, df$n_q, df$variable),
      function(a, b, c, d) list(
        x = a,
        y = b,
        n = c,
        name = d,
        marker = list(enabled = FALSE)
      )
    ),
    name = df$variable[1L],
    color = setNames(colors_q[df$variable[1L]], NULL),
    step = "left"
  )
}



prepare_events <- function(events, intermediate = NULL){
  ## manipulates events as read from json, adding duration etc...
  ## date is converted to datetime object
  ## percentage is converted to numeric
  ## The column time_spent is added, which measures, for each event,
  ## the time spent by the user in the url, up to the current event
  ## The column duration_session is added, which indicates the duration of the
  ## session the event belongs to
  ##
  ##  browser()
  events <- events %>%
    dplyr::mutate(
      percentage = as.numeric(percentage),
      date = lubridate::mdy_hms(date)
    ) %>%
    dplyr::arrange(date) %>% #arrange(date, type_id) %>% (type id para Login primero)
    dplyr::group_by(url, user) %>%
    dplyr::mutate(session = cumsum(type == "LoggedIn")) %>%
    dplyr::ungroup()
  events <- events %>%
    dplyr::filter(profile == "alumno")

  ## -------------------------------------------------------------------------
  ##
  ## we use intermediate$last_event, which contains, for this user and url,
  ## the date of the last registered event, and time_spent in the url by the
  ## user at the  date of the last registered event, and percentage achieved
  ## -------------------------------------------------------------------------
  ##
  if (length(intermediate$last_event) > 0 &
      sum(events$session == 0) > 0) {
    intermediate$last_event <-
      intermediate$last_event %>%
      dplyr::mutate(
        date_last_event = lubridate::ymd_hms(date_last_event),
        time_spent = as.double(time_spent),
        percentage = as.double(percentage)
      )
    interrupted_sessions_last_event <- events %>%
      dplyr::filter(session == 0) %>%
      dplyr::select(user, url) %>%
      dplyr::distinct() %>%
      dplyr::left_join(intermediate$last_event) %>%
      dplyr::rename(date = date_last_event) %>%
      dplyr::mutate(type = "LoggedIn")
    ## problema con el tipo de date, que era date_last_event
    events <- dplyr::bind_rows(
      interrupted_sessions_last_event,
      events
    )
    events <- events %>%
      dplyr::group_by(url, user) %>%
      dplyr::mutate(session = cumsum(type == "LoggedIn")) %>%
      dplyr::ungroup()
  }

  ## ------------------------------------------------------------------------------
  ##
  ## ellapsed_time: for each event, time from beginning of session up to event
  ##
  ## ------------------------------------------------------------------------------
  events <- events  %>%
    dplyr::group_by(user, session, url) %>%
    dplyr::mutate(
      ellapsed_time =  as.numeric(date) - as.numeric(date[1L])
    )

  ##
  ## builds a dataframe "sessions". it computes for each session (and user, url)
  ## the duration of the session, and the previous_duration, which is the time spent
  ## by the user in this url, previously to the session.
  ## sessions: user, session, url, duration_session, previous_duration
  ##
  sessions <- events %>%
    dplyr::group_by(user, session,  url) %>%
    dplyr::summarise(
      duration_session =  diff(as.numeric(range(date)))
    )  %>%
    dplyr::group_by(user, url) %>%
    dplyr::mutate(
      previous_duration = cumsum(
        c(0, duration_session)[1:length(duration_session)]
      )
    )
  ##

  if (length(intermediate$last_event) > 0){
    sessions <- sessions %>%
      dplyr::left_join(intermediate$last_event) %>%
      dplyr::mutate(previous_duration = previous_duration +
                      dplyr::if_else(
                 is.na(time_spent),
                 0,
                 time_spent
               )
      ) %>%
      dplyr::select( - date_last_event, - time_spent, - percentage)
  }
  ## "time_spent" by the user in the url up to the current event is computed
  ## It sums the time spent in the url during previous sessions, and the
  events <- events %>%
    dplyr::left_join(sessions)
  events <- events %>%
    dplyr::mutate(
      time_spent = ellapsed_time +
        previous_duration
    )
  dplyr::ungroup(events) %>%
    dplyr::select(
      - session,
      - ellapsed_time,
      - previous_duration
    )

}
