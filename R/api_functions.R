## ------------------------------------------------------------------------------
##
## The ultimate function that is used for the api
##
## -----------------------------------------------------------------------------

##' generate_aggregate
##'
##' Combines a previous intermediate object with the information contained
##' in new events to generate an updated intermediate object and the
##' corresponding aggregate list for plotting.
##'
##' The json_events_intermediate is a json object that must contain
##' \code{"new_events"} (a json string) and \code{"intermediate"} (a json string).\cr
##' The \code{"new_events"} json string is an array of
##' objects (set of name/value pairs). The name/value pairs must contain the following
##' objects:
##' - user,
##' - name,
##' - email,
##' - date,
##' - type,
##' - percentage,
##' - url,
##' - title,
##' - profile\cr
##'  and has the following structure:\cr
##'  \\{"user":"22","name":"John Doe","title":"Unit 1: Phrasal verbs", ...\\}\cr
##' The \code{"intermediate"} json string is a object (set of name/value pairs),
##' the names are "last_event", "visited_units", "users", "visitors",
##' "user_url_time" and "daily_effort".\cr
##' The function returns a list with two elements:\cr
##' \code{intermediate} is a list that contains the following objects\cr
##' - \code{"last_event"}
##' - \code{"visited_units"}
##' - \code{"users"}
##' - \code{"visitors"}
##' - \code{"user_url_time"}
##' - \code{"daily_effort"}
##' \code{aggregate} which is a list that contains the following objects
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
##'
##' @param json_events_intermediate a json string (see details below)
##' @return a list with two elements: \code{"updated_intermediate"} and
##' \code{"aggregate"}.
##' @importFrom magrittr %>%
##' @export
generate_aggregate<- function(json_events_intermediate){
  ## ------------------------------------------------------------------------------
  ##
  ## Combines the different functions defined above to produce the aggregate
  ## json. It requires the json_events_intermediate to contain new_events and
  ## intermediate. The output is the aggregate list
  ## ------------------------------------------------------------------------------
  list_events_intermediate <- jsonlite::fromJSON(json_events_intermediate)
  new_events <- prepare_events(
    events = list_events_intermediate$new_events,
    intermediate = list_events_intermediate$intermediate
  )
  updated_intermediate <- update_intermediate(
    intermediate = list_events_intermediate$intermediate,
    new_events = new_events
  )
  list(
    updated_intermediate = updated_intermediate,
    aggregate = intermediate2aggregate(updated_intermediate)
  )
}



