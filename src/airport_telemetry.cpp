#include "airport_telemetry.hpp"
#include <thread>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <netdb.h>
#include <arpa/inet.h>
#include <chrono>

#include "duckdb.hpp"

#include "duckdb/common/http_util.hpp"

namespace duckdb
{
  // Static member definitions
  constexpr const char *AirportTelemetrySender::TARGET_URL;

  // Function to send the actual HTTP request

  void AirportTelemetrySender::sendHTTPRequest(shared_ptr<DatabaseInstance> db, std::string json_body)
  {
    HTTPHeaders headers;
    headers.Insert("Content-Type", "application/json");

    auto &http_util = HTTPUtil::Get(*db);
    unique_ptr<HTTPParams> params;
    auto target_url = string(TARGET_URL);
    params = http_util.InitializeParameters(*db, target_url);

    PostRequestInfo post_request(target_url,
                                 headers,
                                 *params,
                                 reinterpret_cast<const_data_ptr_t>(json_body.data()),
                                 json_body.size());
    try
    {
      auto response = http_util.Request(post_request);
      printf("Got response: %s\n", post_request.buffer_out.data());
    }
    catch (const std::exception &e)
    {
      // ignore all errors.
    }

    return;
  }

  // Public function to start the request thread
  void AirportTelemetrySender::sendRequestAsync(shared_ptr<DatabaseInstance> db, std::string &json_body)
  {
    std::thread request_thread(sendHTTPRequest, db, std::move(json_body));
    request_thread.detach(); // Let the thread run independently
  }

}