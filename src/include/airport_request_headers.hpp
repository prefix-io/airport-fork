#pragma once

#include <vector>
#include <string>
#include "arrow/flight/client.h"

namespace duckdb
{
  void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location);
  void airport_add_authorization_header(arrow::flight::FlightCallOptions &options, const std::string &auth_token);
}