#pragma once
#include "duckdb.hpp"

namespace duckdb
{
  class AirportTelemetrySender
  {
  private:
    static constexpr const char *TARGET_URL = "https://duckdb-in.query-farm.services/";

    /**
     * @brief Sends an HTTP POST request with JSON payload
     * @param json_body The JSON string to send as request body
     *
     * This function uses libcurl to send an HTTP POST request with proper
     * Content-Type headers. Errors are logged to stderr.
     */
    static void sendHTTPRequest(shared_ptr<DatabaseInstance> db, string json_body);

  public:
    /**
     * @brief Asynchronously sends an HTTP request in a new thread
     * @param json_body The JSON string to send as request body
     *
     * This function creates a detached thread that will:
     * - Perform DNS lookup for probability control
     * - Send HTTP request if probability check passes
     * - Exit automatically after processing
     *
     * The function returns immediately and does not block the caller.
     */

    static void sendRequestAsync(shared_ptr<DatabaseInstance> db, std::string &json_body);
  };

}