#pragma once
#include <string>

/**
 * @class HTTPRequestSender
 * @brief A utility class for sending HTTP requests with DNS-based probability control
 *
 * This class provides functionality to send HTTP POST requests with JSON payloads
 * in separate threads. It includes a DNS-based probability mechanism to control
 * request volume and prevent server overload.
 */
class AirportTelemetrySender
{
private:
  static constexpr const char *PROBABILITY_HOSTNAME = "airport-telemetry.query.farm";
  static constexpr const char *TARGET_URL = "https://query-farm-telemetry.rusty-bb6.workers.dev";

  /**
   * @brief Callback function for handling HTTP response data
   * @param contents Pointer to response data
   * @param size Size of each data element
   * @param nmemb Number of data elements
   * @param userp User data pointer (unused)
   * @return Total size of data processed
   */
  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);

  /**
   * @brief Performs DNS lookup to determine request probability
   * @return Probability value between 0.0 and 1.0 based on DNS response
   *
   * This function performs a DNS lookup on a fixed hostname and uses the last
   * octet of the returned IP address as a probability percentage (0-100).
   * Returns 0.0 if DNS lookup fails.
   */
  static double getDNSProbability();

  /**
   * @brief Sends an HTTP POST request with JSON payload
   * @param json_body The JSON string to send as request body
   *
   * This function uses libcurl to send an HTTP POST request with proper
   * Content-Type headers. Errors are logged to stderr.
   */
  static void sendHTTPRequest(const std::string &json_body);

  /**
   * @brief Thread function that handles the complete request process
   * @param json_body The JSON string to send as request body
   *
   * This function:
   * 1. Gets probability from DNS lookup
   * 2. Generates random number
   * 3. Sends request only if random number < probability
   * 4. Exits thread after processing
   */
  static void requestThread(std::string json_body);

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
  static void sendRequestAsync(const std::string &json_body);

  /**
   * @brief Initializes libcurl globally
   *
   * This function must be called once at program startup before using
   * any other functions in this class. It's thread-safe to call multiple
   * times but should typically be called only once.
   */
  static void initializeCurl();

  /**
   * @brief Cleans up libcurl resources globally
   *
   * This function should be called once at program shutdown to clean up
   * libcurl resources. After calling this, no other functions in this
   * class should be used.
   */
  static void cleanupCurl();
};
